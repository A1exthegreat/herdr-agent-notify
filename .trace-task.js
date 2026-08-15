// dump ALL events for one task cycle to trace the status sequence
'use strict';
const net = require('net');
const SOCK = process.env.HERDR_SOCKET_PATH || 'C:\\Users\\37310\\AppData\\Roaming\\herdr\\herdr.sock';
const PIPE = '\\\\.\\pipe\\' + SOCK;
const target = process.argv[2] || 'w9:pD';
const subs = [
  { type: 'pane.updated' },
  { type: 'pane.agent_status_changed', pane_id: target },
];
const c = net.connect(PIPE);
let buf = Buffer.alloc(0);
c.on('connect', () => {
  console.log('SUBSCRIBED to', target);
  c.write(JSON.stringify({ id: 'sub', method: 'events.subscribe', params: { subscriptions: subs } }) + '\n');
});
c.on('data', (d) => {
  buf = Buffer.concat([buf, d]);
  let i;
  while ((i = buf.indexOf(0x0a)) >= 0) {
    const line = buf.slice(0, i).toString('utf8');
    buf = buf.slice(i + 1);
    try {
      const m = JSON.parse(line);
      if (m.id === 'sub') continue;
      const p = (m.data && (m.data.pane || m.data)) || {};
      if (p.pane_id !== target && m.event !== 'pane_updated') continue;
      if (m.event === 'pane_updated' && p.pane_id !== target) continue;
      console.log(new Date().toISOString().slice(11, 23), m.event,
        '| status:', p.agent_status ?? p.agent_status, '| agent:', p.agent ?? '-');
    } catch (e) { /* ignore */ }
  }
});
c.on('error', (e) => { console.log('ERR', e.message); process.exit(1); });
setTimeout(() => { console.log('WATCH DONE'); c.end(); process.exit(0); }, 90000);
