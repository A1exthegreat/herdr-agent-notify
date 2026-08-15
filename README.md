# herdr-agent-notify

Desktop notifications when herdr agents change state: a system toast the moment an
agent finishes working (`done`/`idle`), or when it needs your confirmation (`blocked`).

Event-driven — no polling for state changes. Watches the herdr socket API directly
and shows notifications through the server's `notification.show` RPC.

## How it works

```
herdr server ──named pipe──▶ herdr-agent-notify.js ──notification.show──▶ system toast
    │                                                                        ▲
    │  events.subscribe:                                                     │
    │   • pane_updated (global, all watched panes)                           │
    │   • pane.agent_status_changed (per-pane, report_agent-driven)          │
    └────────────────────────────────────────────────────────────────────────┘
```

- **Two complementary event channels** (verified against protocol 19): the global
  `pane_updated` stream carries statuses for every watched pane; the per-pane
  `pane.agent_status_changed` subscription covers output-less transitions.
- **No double toasts**: for panes with an active per-pane subscription,
  `pane_updated` only feeds display metadata — status transitions come from
  `pane.agent_status_changed` alone. This matters because the global stream also
  carries high-frequency detection-loop artifacts (`idle`/`unknown` flapping) that
  would otherwise split one real transition into two toasts (or swallow it). New
  panes are covered best-effort via `pane_updated` until the next refresh
  subscribes them; a 10s per-pane cooldown (`--cooldown-ms`) is the final guard.
- **Self-healing**: exponential backoff reconnect (5s → 60s cap) when the server
  restarts; transitions missed while disconnected are re-notified from the next
  `agent.list` snapshot diff.
- **Single instance**: a second watcher process detects the first and exits, so the
  `[[startup]]` hook and a manual `start` action can never double-notify.
- Notifies only on `working → idle/done/blocked`. `unknown` statuses and
  agent-detection loop noise are ignored. A pane seen for the first time never
  triggers a toast.

Notification content:

```
title: agent pi (w9:pD)
body:  状态：已完成 · 37310 (w9) · π - 37310
sound: done   (request for blocked)
```

- `title` = agent name + pane id
- `body` = localized status label + workspace `label (id)` (label from
  `workspace.list` cache; falls back to the bare id) + pane `title`
  (falls back to `terminal_title`, truncated at 40 chars)
- one transition, one toast: the two event channels are deduplicated
- **One-shot startup hook, not a supervised daemon** (herdr plugin contract): the
  `[[startup]]` hook starts the watcher once per server start; the watcher then
  heals itself. If it ever dies, use the `start` action — `[[startup]]` will also
  run again at the next server start.

## Requirements

- Windows (named pipe + PowerShell single-instance check)
- Node.js ≥ 18 on `PATH`
- herdr ≥ 0.8.0

## Install

From GitHub (also listed on [herdr.dev/plugins](https://herdr.dev/plugins/)):

```bash
herdr plugin install <owner>/herdr-agent-notify
```

Or clone/copy this directory and link it locally:

```bash
herdr plugin link /path/to/notify-plugin
```

## Usage

The watcher starts automatically with each herdr server start (startup hook).
Plugin actions (`herdr plugin action list --plugin herdr-agent-notify`):

| action | description |
| ------ | ----------- |
| `status` | Show whether the watcher is running |
| `start`  | Launch the watcher now (hidden, logs → `HERDR_PLUGIN_STATE_DIR`) |
| `stop`   | Kill the watcher process |

Standalone (no plugin registration needed):

```bash
node herdr-agent-notify.js
node herdr-agent-notify.js --name pi          # only monitor agent "pi"
node herdr-agent-notify.js --refresh-ms 10000 # subscription refresh interval
node herdr-agent-notify.js --cooldown-ms 5000 # per-pane anti-double-toast cooldown (default 10000)
node herdr-agent-notify.js --include-self     # also monitor your own pane
node herdr-agent-notify.js --debug            # log every status transition
```

Logs: startup-hook runs are captured in `herdr plugin log list --plugin herdr-agent-notify`;
manual `start` action runs write `watcher.log`/`watcher.err` under the plugin state dir
(`herdr plugin config-dir herdr-agent-notify` shows the plugin dirs).

## Development

```bash
node watcher.test.js   # in-process regression suite (no sockets needed)
```

The module exports `handleEvent`/`transition`/`refresh` plus `setNotify`/`setRpc`
injection points for testing.

## License

MIT
