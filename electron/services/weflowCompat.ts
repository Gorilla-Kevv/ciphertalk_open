import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'http'
import type { MainProcessContext } from '../main/context'
import { chatService } from './chatService'
import { normalizeMessage } from './mcp/readService'
import type { McpMessageItem } from './mcp/types'
import type { ChatSession } from './chat/types'

/**
 * WeFlow 兼容 SSE 推送服务。
 *
 * 给 Akasha 桥接（及其它 WeFlow 生态）提供一个 WeFlow 风格的接口：
 *   - GET /api/v1/messages?limit=1&access_token=...   健康检查（返回 200）
 *   - GET /api/v1/push/messages?access_token=...      SSE 实时推送新消息
 *
 * 新消息检测复用 CipherTalk 现有的 monitorBridge → chatService 'dbChange' 事件
 * （与 notifyService 同款判定：时间线前进 + 有未读），再用游标增量拉取消息体。
 * 默认端口 5031（对齐 WeFlow），可用 config 键 weflowCompatPort / weflowCompatToken 覆盖。
 */

const DEBOUNCE_MS = 300
const SESSION_QUERY_LIMIT = 150

interface SessionSnap {
  lastTs: number
  unread: number
}

function isGroupSession(username: string): boolean {
  return username.includes('@chatroom')
}

function isWantedSession(session: ChatSession): boolean {
  const username = String(session.username || '')
  if (!username) return false
  if (username.startsWith('@placeholder') || username.startsWith('brandsessionholder')) return false
  if (session.isFoldGroup || session.isOfficialFolder) return false
  return true
}

function toWeflowMessage(session: ChatSession, item: McpMessageItem): Record<string, unknown> {
  const username = String(session.username || '')
  const isGroup = isGroupSession(username)
  let content = String(item.text || '')
  if (item.kind === 'image') content = '[图片]'
  else if (item.kind === 'emoji') content = '[动画表情]'
  else if (item.kind === 'voice') content = '[语音]'
  else if (item.kind === 'video') content = '[视频]'
  else if (!content) content = `[${item.kind}]`

  return {
    content,
    type: item.kind === 'voice' ? 34 : 0,
    sourceName: item.sender?.displayName || session.displayName || '',
    talkerId: item.sender?.username || '',
    sessionId: username,
    groupName: isGroup ? (session.displayName || '') : '',
    sessionType: isGroup ? 'group' : '',
    senderName: item.sender?.displayName || '',
    rawid: `${username}:${item.messageId}`,
    timestamp: Number(item.timestamp || 0),
    mediaType: item.kind,
    mediaUrl: '',
    media_local_path: item.media?.localPath || null,
  }
}

class WeflowCompatService {
  private ctx: MainProcessContext | null = null
  private server: Server | null = null
  private clients = new Set<ServerResponse>()
  private snapshot = new Map<string, SessionSnap>()
  private debounceTimer: NodeJS.Timeout | null = null
  private checking = false
  private port = 5031
  private token = ''

  async start(ctx: MainProcessContext): Promise<{ success: boolean; error?: string }> {
    this.ctx = ctx
    const configService = ctx.getConfigService()
    this.port = Number(process.env.CIPHERTALK_WEFLOW_PORT || configService?.get('weflowCompatPort') || 5031)
    this.token = String(process.env.CIPHERTALK_WEFLOW_TOKEN || configService?.get('weflowCompatToken') || '')

    return new Promise((resolve) => {
      const server = createServer((req, res) => { void this.handleRequest(req, res) })
      server.on('error', (err: NodeJS.ErrnoException) => {
        const msg = err.code === 'EADDRINUSE' ? `端口 ${this.port} 被占用` : err.message
        resolve({ success: false, error: msg })
      })
      server.listen(this.port, '127.0.0.1', () => {
        this.server = server
        chatService.on('dbChange', this.onDbChange)
        console.log(`[WeflowCompat] SSE 服务已启动 http://127.0.0.1:${this.port}`)
        resolve({ success: true })
      })
    })
  }

  stop(): void {
    chatService.off('dbChange', this.onDbChange)
    if (this.debounceTimer) clearTimeout(this.debounceTimer)
    this.debounceTimer = null
    for (const res of this.clients) {
      try { res.end() } catch { /* ignore */ }
    }
    this.clients.clear()
    if (this.server) {
      this.server.close()
      this.server = null
    }
  }

  // ---------- 新消息检测（复用 dbChange 事件，不新增轮询） ----------

  private onDbChange = (payload: { table?: string }): void => {
    const table = String(payload?.table || '')
    if (table !== 'Message' && table !== 'Session') return
    if (this.clients.size === 0) return
    if (this.debounceTimer) clearTimeout(this.debounceTimer)
    this.debounceTimer = setTimeout(() => {
      this.debounceTimer = null
      void this.check()
    }, DEBOUNCE_MS)
  }

  private async check(): Promise<void> {
    if (this.checking || this.clients.size === 0) return
    this.checking = true
    try {
      const result = await chatService.getSessions(0, SESSION_QUERY_LIMIT)
      if (!result.success || !Array.isArray(result.sessions)) return
      for (const session of result.sessions) {
        if (!isWantedSession(session)) continue
        const username = String(session.username || '')
        const cur: SessionSnap = {
          lastTs: Number(session.lastTimestamp || session.sortTimestamp || 0),
          unread: Number(session.unreadCount || 0),
        }
        const prev = this.snapshot.get(username)
        this.snapshot.set(username, cur)
        if (!prev) continue // 首轮播种，不补历史
        // 只按时间线前进判断新消息，不依赖 unread（已读的消息机器人也要响应；自己发的由 pushNewMessages 过滤）
        if (cur.lastTs <= prev.lastTs) continue
        await this.pushNewMessages(session, prev.lastTs)
      }
    } catch (e) {
      this.ctx?.getLogService()?.warn('WeflowCompat', '检查新消息失败', { error: String(e) })
    } finally {
      this.checking = false
    }
  }

  private async pushNewMessages(session: ChatSession, sinceTs: number): Promise<void> {
    const username = String(session.username || '')
    try {
      // 游标用 sinceTs（秒）+ localId 0：拉取 create_time > sinceTs 的新消息（增量，不扫历史）
      const res = await chatService.getMessagesAfter(username, 0, 50, sinceTs, 0)
      if (!res.success || !Array.isArray(res.messages)) return
      for (const msg of res.messages) {
        if (Number(msg.isSend) === 1) continue // 跳过自己发出的
        try {
          const item = await normalizeMessage(username, msg, { includeMediaPaths: true, includeRaw: false })
          this.broadcast(toWeflowMessage(session, item))
        } catch {
          // 单条归一化失败不影响其它消息
        }
      }
    } catch (e) {
      this.ctx?.getLogService()?.warn('WeflowCompat', `拉取会话 ${username} 新消息失败`, { error: String(e) })
    }
  }

  private broadcast(data: Record<string, unknown>): void {
    if (this.clients.size === 0) return
    const payload = `data: ${JSON.stringify(data)}\n\n`
    for (const res of this.clients) {
      try {
        res.write(payload)
      } catch {
        this.clients.delete(res)
      }
    }
  }

  // ---------- HTTP ----------

  private handleRequest(req: IncomingMessage, res: ServerResponse): void {
    const url = new URL(req.url || '/', `http://${req.headers.host || '127.0.0.1'}`)
    const pathname = url.pathname

    if (pathname === '/api/v1/messages') {
      if (!this.isAuthorized(url)) { this.json(res, 401, { error: 'invalid token' }); return }
      this.json(res, 200, { messages: [] })
      return
    }

    if (pathname === '/api/v1/push/messages') {
      if (!this.isAuthorized(url)) { this.json(res, 401, { error: 'invalid token' }); return }
      this.handleSse(req, res)
      return
    }

    this.json(res, 404, { error: 'not found' })
  }

  private isAuthorized(url: URL): boolean {
    if (!this.token) return true
    return url.searchParams.get('access_token') === this.token
  }

  private handleSse(req: IncomingMessage, res: ServerResponse): void {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
    })
    res.write(': connected\n\n')
    this.clients.add(res)

    const keepalive = setInterval(() => {
      try { res.write(': keepalive\n\n') } catch { /* ignore */ }
    }, 15000)

    res.on('close', () => {
      clearInterval(keepalive)
      this.clients.delete(res)
    })
  }

  private json(res: ServerResponse, status: number, data: unknown): void {
    res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' })
    res.end(JSON.stringify(data))
  }
}

export const weflowCompatService = new WeflowCompatService()
