// in-process tests for the fixed watcher logic (no sockets needed)
'use strict';
const w = require('C:/Users/37310/.herdr/notify-plugin/herdr-agent-notify.js');
const notes = [];
w.setNotify((title, body, sound) => { notes.push({ title, body, sound }); return Promise.resolve(); });

const assert = (cond, label) => { if (!cond) { console.error('FAIL:', label); process.exitCode = 1; } else console.log('ok  :', label); };

// --- 1. underscore global event pane_updated: working -> idle -> notify
w.handleEvent({ event: 'pane_updated', data: { pane: { pane_id: 'w1:p1', agent: 'pi', agent_status: 'working' } } });
assert(w.state.get('w1:p1') === 'working', 'pane_updated(underscore) working applied');
w.handleEvent({ event: 'pane_updated', data: { pane: { pane_id: 'w1:p1', agent: 'pi', agent_status: 'idle' } } });
assert(notes.length === 1 && notes[0].title.includes('pi (w1:p1)'), 'working->idle via pane_updated notifies once');

// --- 2. dot subscription event pane.agent_status_changed: working -> done
w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:p2', agent: 'pi', agent_status: 'working' } });
w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:p2', agent: 'pi', agent_status: 'done' } });
assert(notes.length === 2 && notes[1].sound === 'done', 'dot status_changed working->done notifies');

// --- 3. unknown must NOT overwrite prev=working (detection-loop noise)
w.handleEvent({ event: 'pane_updated', data: { pane: { pane_id: 'w1:p1', agent: null, agent_status: 'unknown' } } });
assert(w.state.get('w1:p1') === 'idle', 'agentless pane_updated ignored (state kept)');
w.handleEvent({ event: 'pane_updated', data: { pane: { pane_id: 'w1:p1', agent: 'pi', agent_status: 'unknown' } } });
assert(w.state.get('w1:p1') === 'idle', 'unknown status ignored (state kept)');

// --- 4. same transition via both channels -> single notify
w.handleEvent({ event: 'pane_updated', data: { pane: { pane_id: 'w1:p3', agent: 'pi', agent_status: 'working' } } });
w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:p3', agent: 'pi', agent_status: 'working' } });
w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:p3', agent: 'pi', agent_status: 'idle' } });
w.handleEvent({ event: 'pane_updated', data: { pane: { pane_id: 'w1:p3', agent: 'pi', agent_status: 'idle' } } });
assert(notes.length === 3, 'dual-channel transition notifies exactly once');

// --- 5. blocked -> request sound; first-seen never notifies
w.handleEvent({ event: 'pane_updated', data: { pane: { pane_id: 'w1:p4', agent: 'pi', agent_status: 'blocked' } } });
assert(notes.length === 3, 'first-seen terminal state does not notify');
w.handleEvent({ event: 'pane_updated', data: { pane: { pane_id: 'w1:p4', agent: 'pi', agent_status: 'working' } } });
w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:p4', agent: 'pi', agent_status: 'blocked' } });
assert(notes.length === 4 && notes[3].sound === 'request', 'working->blocked notifies with request sound');

// --- 6. pane_exited (underscore) cleans state
w.handleEvent({ event: 'pane_exited', data: { pane_id: 'w1:p2', workspace_id: 'w1' } });
assert(!w.state.has('w1:p2'), 'pane_exited(underscore) removes state');

// --- 7. sub response not treated as event, no crash
w.handleEvent({ id: 'sub', result: { type: 'subscription_started' } });

// --- 8. buildSubs shape
const subs = w.buildSubs();
assert(subs[0].type === 'pane.updated' && subs.filter(s => s.type === 'pane.agent_status_changed').length >= 1, 'buildSubs: global + per-pane');

// --- 9. refresh() with stubbed rpcOnce: missed transition while disconnected gets notified
let agents = [{ pane_id: 'w9:p9', agent: 'pi', agent_status: 'done' }];
w.setRpc(() => Promise.resolve({ result: { agents } }));
w.state.clear();
w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w9:p9', agent: 'pi', agent_status: 'working' } });
// stream goes down; agent finishes; snapshot shows done
agents = [{ pane_id: 'w9:p9', agent: 'pi', agent_status: 'done' }];
(async () => {
  const changed = await w.refresh('test');
  assert(changed === false, 'refresh: known pane, no new panes -> no rebuild needed');
  assert(notes.length === 5 && notes[4].title.includes('w9:p9'), 'missed working->done while down is re-notified');
  // --- 10. new pane first-seen via refresh: added, no notify, rebuild needed
  agents = [{ pane_id: 'w9:p9', agent: 'pi', agent_status: 'done' }, { pane_id: 'w9:pA', agent: 'pi', agent_status: 'idle' }];
  const changed2 = await w.refresh('test2');
  assert(changed2 === true, 'refresh: new snapshot pane -> rebuild needed');
  assert(notes.length === 5, 'new pane first-seen does not notify');
  // --- 10b. ephemeral pane (in state, not in snapshot) pruned silently, no rebuild
  w.handleEvent({ event: 'pane_updated', data: { pane: { pane_id: 'w9:pE', agent: 'fake-agent', agent_status: 'idle' } } });
  agents = [{ pane_id: 'w9:p9', agent: 'pi', agent_status: 'done' }, { pane_id: 'w9:pA', agent: 'pi', agent_status: 'idle' }];
  const changed3 = await w.refresh('test3');
  assert(changed3 === false, 'refresh: ephemeral pane pruned, no rebuild');
  assert(!w.state.has('w9:pE') && notes.length === 5, 'ephemeral pane removed, no notify');
  // --- 11. no change -> false, no notify
  const changed4 = await w.refresh('test4');
  assert(changed4 === false, 'refresh no-change returns false');
  console.log('done');
})();
