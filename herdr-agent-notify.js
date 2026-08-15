#!/usr/bin/env node
/**
 * herdr-agent-notify.js — 事件驱动的 agent 状态系统通知
 *
 * 直连 herdr API 命名管道，生命周期与 herdr server 绑定：
 *   连接在 -> 工作；连接断 -> 指数退避重连；server 重启 -> 自动恢复。
 * 通常以 herdr 插件形式运行（startup 命令，随 server 启停），也可独立运行。
 *
 * 事件模型（经实测验证，protocol 19）：
 *   - 全局事件流：订阅 type 用点号（pane.updated / pane.exited ...），推送的事件名是
 *     下划线（pane_updated / pane_exited ...），覆盖所有被服务器监视的 pane，但高频
 *     （~100ms-1s）且携带检测循环伪影状态（idle/unknown 每秒翻转）；
 *   - 订阅事件：点号名（pane.agent_status_changed），按 pane 过滤，真实转换的主通道
 *     （report_agent 驱动、无终端输出的转换只发此事件）；
 *   - 优先级：已跟踪（已订阅）pane 的状态只信 status_changed，pane_updated 仅补充
 *     元数据——避免伪影状态把一次转换拆成两次（双弹）或吞掉真实终态（漏报）；
 *     未跟踪的新 pane 由 pane_updated best-effort 覆盖，待 refresh 补订阅后交接；
 *   - 10s 冷却期（--cooldown-ms）：同一 pane 短时间内不重复弹窗，兜底防双弹；
 *   - pane_agent_detected 是检测循环信号（同 pane 高频重复、released/final_status
 *     不可靠），忽略；其状态变化总会伴随 status_changed 或 pane_updated，由两路覆盖。
 * 互补 + 定时刷新（默认 30s）：
 *   - 事件流在线时，状态以事件为准，快照 diff 只兑底丢失的事件（working->终态）；
 *   - 快照新增 pane 才重建订阅（补 per-pane 状态订阅），瞬时 pane（检测伪影）静默修剪；
 *   - 事件流断开期间发生的转换，由重连/刷新时的快照 diff 补发通知。
 * 当 agent 从 working 变为 idle/done/blocked 时通过 notification.show RPC 弹系统通知；
 * 通知正文带工作区（label + id）与 pane 标题（title，回退 terminal_title，超长截断）。
 *
 * 用法:
 *   node herdr-agent-notify.js
 *   node ... --name pi              # 只监控名为 pi 的 agent
 *   node ... --refresh-ms 10000     # 订阅刷新间隔（默认 30000）
 *   node ... --cooldown-ms 5000     # 同 pane 重复通知冷却期（默认 10000）
 *   node ... --include-self         # 也监控自身 pane（默认跳过）
 *   node ... --debug                # 打印每次状态转换（排查双弹/漏报）
 * 插件命令由 herdr 以插件目录为 cwd 启动，相对路径即可；日志经 herdr 插件日志
 * 或 HERDR_PLUGIN_STATE_DIR 落盘，不写入插件根目录。
 */
'use strict';
const net = require('net');
const { spawn } = require('child_process');

const SOCK = process.env.HERDR_SOCKET_PATH
  || (process.env.APPDATA ? process.env.APPDATA + '\\herdr\\herdr.sock' : null);
if (!SOCK) { console.error('找不到 herdr socket（需要 HERDR_SOCKET_PATH）'); process.exit(1); }
const PIPE = '\\\\.\\pipe\\' + SOCK;

const arg = (k) => { const i = process.argv.indexOf(k); return i >= 0 ? process.argv[i + 1] : null; };
const refreshArg = parseInt(arg('--refresh-ms') || '30000', 10);
const OPTS = {
  nameFilter: arg('--name') || null,
  includeSelf: process.argv.includes('--include-self'),
  refreshMs: Number.isFinite(refreshArg) ? Math.max(1000, refreshArg) : 30000,
  cooldownMs: Number.isFinite(parseInt(arg('--cooldown-ms') || '10000', 10)) ? Math.max(0, parseInt(arg('--cooldown-ms') || '10000', 10)) : 10000,
  debug: process.argv.includes('--debug'),
};

const dbg = (...a) => { if (OPTS.debug) log('dbg:', ...a); };

const LABELS = { idle: '空闲', working: '工作中', blocked: '需要你确认', done: '已完成', unknown: '状态未知' };
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

/** 跑一条 PowerShell 并返回 stdout（trimmed）；失败返回 null */
function psQuery(cmd) {
  return new Promise((resolve) => {
    const ps = spawn('powershell.exe', ['-NoProfile', '-NonInteractive', '-Command', cmd]);
    let out = '';
    ps.stdout.on('data', (d) => { out += d; });
    ps.on('error', () => resolve(null));
    ps.on('close', () => resolve(out.trim()));
  });
}

/** 一次性 RPC：herdr 服务器对每条连接只处理第一条请求，随后关闭 */
let rpcOnce = (method, params, timeoutMs = 8000) => {
  return new Promise((resolve, reject) => {
    const c = net.connect(PIPE);
    let buf = '';
    const t = setTimeout(() => { c.destroy(); reject(new Error('timeout: ' + method)); }, timeoutMs);
    c.on('connect', () => c.write(JSON.stringify({ id: 'r1', method, params }) + '\n'));
    c.on('data', (d) => {
      buf += d.toString();
      const nl = buf.indexOf('\n');
      if (nl >= 0) {
        clearTimeout(t);
        try {
          const m = JSON.parse(buf.slice(0, nl));
          m.error ? reject(new Error(m.error.message)) : resolve(m);
        } catch (e) { reject(e); }
        c.end();
      }
    });
    c.on('error', (e) => { clearTimeout(t); reject(e); });
  });
}

/** pane_id -> agent_status：快照对齐 + 事件增量更新 */
const state = new Map();
/** 已建立 per-pane 订阅的 pane：其状态只信 status_changed（pane_updated 高频伪影不进状态机） */
const subscribed = new Set();

/** pane_id -> { title, workspace_id }：通知展示用的 pane 上下文（事件/快照补充） */
const paneMeta = new Map();
/** workspace_id -> label：workspace.list 缓存（通知里展示工作区名） */
const wsLabels = new Map();

/** 合并 pane 展示元数据（title 可为 null，空值不覆盖旧值） */
function setPaneMeta(pid, title, workspaceId) {
  const t = typeof title === 'string' ? title.trim() : '';
  if (!t && !workspaceId) return;
  const m = paneMeta.get(pid) ?? {};
  if (t) m.title = t;
  if (workspaceId) m.workspace_id = workspaceId;
  paneMeta.set(pid, m);
}

/** 可注入的通知实现（测试时替换为记录器） */
let notifyImpl = (title, body, sound) => rpcOnce('notification.show', { title, body, sound }).then(
  (r) => log('notified:', sound, '|', title, '|', body, '|', r.result?.reason ?? 'ok'),
  (e) => log('notify failed:', e.message)
);

/** 当前订阅集：全局一条 + 每个已知 pane 的状态订阅 */
function buildSubs() {
  const subs = [{ type: 'pane.updated' }];
  for (const pid of state.keys()) subs.push({ type: 'pane.agent_status_changed', pane_id: pid });
  return subs;
}

/** 完成态：working 之后值得通知的状态 */
const TERMINAL = new Set(['idle', 'done', 'blocked']);

/** pane_id -> 上次通知时间戳：冷却期内同一 pane 不重复弹（防检测循环/多轮任务双弹） */
const lastNotifyAt = new Map();

/** 通知判定（不操作 state）：prev=working 且 cur 为完成态时通知 */
function maybeNotify(pid, name, prev, cur) {
  if (prev !== 'working') return;
  if (!TERMINAL.has(cur)) return; // unknown/异常状态不打扰
  if (OPTS.nameFilter && name !== OPTS.nameFilter) return;
  if (!OPTS.includeSelf && pid === OPTS.selfPane) return; // 跳过自身 pane
  const now = Date.now();
  if (now - (lastNotifyAt.get(pid) ?? 0) < OPTS.cooldownMs) return; // 冷却期内不重复弹
  lastNotifyAt.set(pid, now);

  const who = name ? `${name} (${pid})` : pid;
  const meta = paneMeta.get(pid);
  const ctx = [];
  if (meta?.workspace_id) {
    const label = wsLabels.get(meta.workspace_id);
    ctx.push(label ? `${label} (${meta.workspace_id})` : meta.workspace_id);
  }
  const t = meta?.title ?? '';
  if (t) ctx.push(t.length > 40 ? t.slice(0, 37) + '…' : t);
  const body = ctx.length
    ? `状态：${LABELS[cur] ?? cur} · ${ctx.join(' · ')}`
    : `状态：${LABELS[cur] ?? cur}`;
  notifyImpl(`agent ${who}`, body, cur === 'blocked' ? 'request' : 'done');
}

/** 状态转换核心：维护 state 并判定是否通知 */
function transition(pid, name, cur) {
  const prev = state.get(pid);  // undefined = 首次见到，只记录不通知
  state.set(pid, cur);
  dbg('transition', pid, prev ?? '?', '->', cur);
  maybeNotify(pid, name, prev, cur);
}

/** 全局事件是下划线名，订阅事件是点号名：归一化为点号 */
const normalizeEvent = (ev) => (ev.includes('.') ? ev : ev.replace(/_/g, '.'));

/** pane_updated 里可信的状态（unknown 是检测循环噪声，不覆盖 prev） */
const REPORTABLE = new Set(['working', ...TERMINAL]);

/**
 * 事件分发。
 * 注：订阅连接上的请求响应（含错误）不应静默吞掉，仅记录；
 * agent_detected 为检测循环信号，忽略（见头部注释）。
 */
function handleEvent(msg) {
  if (!msg.event) {
    if (msg.id === 'sub' || msg.error) log('sub conn:', JSON.stringify(msg).slice(0, 200));
    return;
  }
  const data = msg.data ?? {};
  const ev = normalizeEvent(msg.event);
  if (ev === 'pane.closed' || ev === 'pane.exited') {
    if (data.pane_id) { state.delete(data.pane_id); paneMeta.delete(data.pane_id); subscribed.delete(data.pane_id); lastNotifyAt.delete(data.pane_id); }
    return;
  }
  if (ev === 'pane.agent_status_changed') {
    if (data.pane_id) setPaneMeta(data.pane_id, data.title, data.workspace_id);
    transition(data.pane_id, data.agent ?? data.display_agent ?? null, data.agent_status ?? 'unknown');
    return;
  }
  if (ev === 'pane.updated') {
    const pane = data.pane;
    if (!pane?.pane_id || !pane.agent) return;               // 无 agent 的 pane 不关心
    setPaneMeta(pane.pane_id, pane.title ?? pane.terminal_title, pane.workspace_id);
    if (subscribed.has(pane.pane_id)) return;                 // 已订阅：状态以 status_changed 为准，
                                                              // 忽略检测循环伪影（working/idle 每秒翻转）
    const st = pane.agent_status ?? 'unknown';                // 未订阅（新 pane/订阅未建立）：best-effort，
    if (!REPORTABLE.has(st)) return;                          // 待 refresh 补订阅后交给 status_changed
    transition(pane.pane_id, pane.agent ?? pane.display_agent ?? null, st);
  }
}

let conn = null;
let refreshTimer = null;
let refreshPending = false;
let retryMs = 5000;

/** 单实例保护：发现其他 herdr-agent-notify 进程则退出（防双通知） */
function ensureSingleInstance() {
  return psQuery(
    "Get-CimInstance Win32_Process -Filter \"Name='node.exe'\" " +
    "| Where-Object { $_.CommandLine -match 'herdr-agent-notify' } | Select-Object -ExpandProperty ProcessId"
  ).then((out) => {
    const others = (out ?? '').split(/\s+/)
      .map((s) => parseInt(s, 10))
      .filter((n) => Number.isFinite(n) && n !== process.pid);
    if (others.length) {
      log('another watcher running (pid ' + others.join(',') + '), exiting');
      process.exit(0);
    }
  });
}

/** 从 agent.list 快照取 agent 列表 */
async function snapshotAgents() {
  const m = await rpcOnce('agent.list', {});
  return m.result?.agents ?? [];
}

/** 订阅连接。幂等：若已有连接先主动关闭（重建订阅路径复用）。 */
function subscribe() {
  if (conn && !conn.destroyed) { const old = conn; conn = null; old.intentional = true; old.end(); }
  const c = net.connect(PIPE);
  conn = c;
  c.intentional = false;
  subscribed.clear();
  for (const pid of state.keys()) subscribed.add(pid);   // 订阅集快照：仅这些 pane 信 status_changed
  let buf = Buffer.alloc(0);
  c.on('connect', () => {
    retryMs = 5000;   // 连接成功：重置退避
    log('connected, subscribing', state.size, 'panes ...');
    c.write(JSON.stringify({ id: 'sub', method: 'events.subscribe', params: { subscriptions: buildSubs() } }) + '\n');
  });
  c.on('data', (d) => {
    buf = Buffer.concat([buf, d]);
    let i;
    while ((i = buf.indexOf(0x0a)) >= 0) {
      const line = buf.slice(0, i).toString('utf8');  // 整行解码，多字节字符不被截断
      buf = buf.slice(i + 1);
      try { handleEvent(JSON.parse(line)); } catch (e) { log('bad msg:', line.slice(0, 120)); }
    }
  });
  c.on('error', (e) => log('pipe error:', e.message));
  c.on('close', () => {
    if (c.intentional) return;
    if (conn !== c) return;   // 已被新连接取代：不重复调度
    conn = null;
    log('disconnected, reconnecting in', retryMs, 'ms');
    setTimeout(init, retryMs);
    retryMs = Math.min(60000, retryMs * 2);   // 指数退避，封顶 60s
  });
}

/**
 * 定时刷新：agent.list 快照 -> 对齐状态。返回 true 表示订阅集需重建（有快照新增 pane）。
 * - 事件流断开时（conn==null），快照是唯一真相：断开期间漏掉的 working->终态 在此补发；
 * - 事件流在线时，状态以事件为准，快照 diff 仅兜底：prev=working 且快照已是终态时补发
 *   （事件丢失场景），否则不覆盖事件状态；
 * - pane_updated 带进 state 的瞬时 pane（检测循环伪影/已释放 pane，不在 agent.list 中）
 *   静默修剪，不触发重建（它们本就没有 per-pane 订阅）；
 * - 状态无变化时不动连接、不打日志（防噪音）。
 */
async function refresh(reason) {
  if (refreshPending) return false;
  refreshPending = true;
  try {
    const agents = await snapshotAgents();
    // 展示元数据：pane title/workspace + 工作区 label 缓存（通知内容用；失败不影响状态同步）
    for (const a of agents) if (a.pane_id) setPaneMeta(a.pane_id, a.title ?? a.terminal_title, a.workspace_id);
    try {
      const ws = await rpcOnce('workspace.list', {});
      wsLabels.clear();
      for (const w of ws.result?.workspaces ?? []) if (w.workspace_id && w.label) wsLabels.set(w.workspace_id, w.label);
    } catch { /* best-effort */ }
    const next = new Map(agents.map((a) => [a.pane_id, a.agent_status]));
    const streamDown = !conn || conn.destroyed;
    let needSub = false;
    for (const a of agents) {
      const prev = state.get(a.pane_id);
      if (prev === undefined) needSub = true;          // 快照新增 pane：需补 per-pane 订阅
      if (streamDown || prev === undefined) {
        transition(a.pane_id, a.agent ?? a.display_agent ?? null, a.agent_status);
      } else if (prev === 'working' && TERMINAL.has(a.agent_status)) {
        transition(a.pane_id, a.agent ?? a.display_agent ?? null, a.agent_status);  // 事件丢失兜底
      }
    }
    for (const pid of [...state.keys()]) if (!next.has(pid)) { state.delete(pid); subscribed.delete(pid); lastNotifyAt.delete(pid); }   // 瞬时 pane 修剪
    if (!needSub) return false;
    log('refreshed:', reason, [...state.entries()].map(([p, s]) => `${p}=${s}`).join(', '));
    return true;
  } catch (e) {
    log('refresh failed:', e.message);
    return false;
  } finally {
    refreshPending = false;
  }
}

function scheduleRefresh() {
  clearTimeout(refreshTimer);
  refreshTimer = setTimeout(async () => {
    if (await refresh('periodic')) subscribe();  // 有新 pane 加入/移除：重建订阅
    scheduleRefresh();
  }, OPTS.refreshMs);
}

async function init() {
  try {
    await refresh('init');   // 状态对齐（含断开期间的漏报补发）
    subscribe();             // 无条件连接/重连
    scheduleRefresh();
  } catch (e) {
    log('init failed:', e.message, 'retrying in 10s');   // server 未起/重启中：安静重试
    setTimeout(init, 10000);
  }
}

async function main() {
  log('herdr-agent-notify starting (refresh=' + OPTS.refreshMs + 'ms)');
  await ensureSingleInstance();
  init();
  const onExit = () => { log('stopped'); process.exit(0); };
  process.on('SIGINT', onExit);
  process.on('SIGTERM', onExit);
}

module.exports = { PIPE, rpcOnce, state, paneMeta, wsLabels, subscribed, handleEvent, buildSubs, transition, maybeNotify,
                   refresh, subscribe, setNotify: (f) => { notifyImpl = f; },
                   setRpc: (f) => { rpcOnce = f; }, LABELS, OPTS };
if (require.main === module) main();
