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

  // --- 12. notification content: workspace label + pane title
  w.paneMeta.clear(); w.wsLabels.clear();
  w.wsLabels.set('w1', 'Reflow');
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:p9', agent: 'pi', agent_status: 'working', title: 'π - 37310', workspace_id: 'w1' } });
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:p9', agent: 'pi', agent_status: 'done', title: 'π - 37310', workspace_id: 'w1' } });
  assert(notes[notes.length - 1].body === '状态：已完成 · Reflow (w1) · π - 37310', 'body = status + workspace label (id) + pane title');
  // --- 13. fallbacks: no ws label -> raw id; no title -> plain body
  w.wsLabels.clear();
  w.OPTS.cooldownMs = 0;   // 同一 pane 连续断言，关闭冷却期
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:p9', agent: 'pi', agent_status: 'working', title: 'π - 37310', workspace_id: 'w1' } });
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:p9', agent: 'pi', agent_status: 'idle', title: 'π - 37310', workspace_id: 'w1' } });
  assert(notes[notes.length - 1].body === '状态：空闲 · w1 · π - 37310', 'no label -> workspace_id only');
  w.handleEvent({ event: 'pane_updated', data: { pane: { pane_id: 'w1:pA', agent: 'pi', agent_status: 'working' } } });
  w.handleEvent({ event: 'pane_updated', data: { pane: { pane_id: 'w1:pA', agent: 'pi', agent_status: 'idle' } } });
  assert(notes[notes.length - 1].body === '状态：空闲', 'no meta -> plain body');
  w.OPTS.cooldownMs = 10000;
  // --- 14. long title capped at 40 chars; closed removes meta
  const long = 'x'.repeat(60);
  w.handleEvent({ event: 'pane_updated', data: { pane: { pane_id: 'w1:pB', agent: 'pi', agent_status: 'working', title: long, workspace_id: 'w2' } } });
  w.handleEvent({ event: 'pane_updated', data: { pane: { pane_id: 'w1:pB', agent: 'pi', agent_status: 'blocked', title: long, workspace_id: 'w2' } } });
  assert(notes[notes.length - 1].body.includes('x'.repeat(37) + '…'), 'long title truncated with ellipsis');
  w.handleEvent({ event: 'pane_exited', data: { pane_id: 'w1:pB', workspace_id: 'w1' } });
  assert(!w.paneMeta.has('w1:pB'), 'pane_exited removes pane meta');

  // --- 15. subscribed panes: pane_updated status is ignored (detection-loop artifacts)
  const n15 = notes.length;
  w.subscribed.add('w1:pS');
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:pS', agent: 'pi', agent_status: 'working' } });
  w.handleEvent({ event: 'pane_updated', data: { pane: { pane_id: 'w1:pS', agent: 'pi', agent_status: 'idle' } } });  // artifact must NOT transition
  assert(w.state.get('w1:pS') === 'working', 'subscribed pane ignores pane_updated artifact');
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:pS', agent: 'pi', agent_status: 'done' } });
  assert(notes.length === n15 + 1, 'subscribed pane: single notify from status_changed');

  // --- 16. cooldown: second working->terminal within cooldown window is suppressed
  const n16 = notes.length;
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:pC', agent: 'pi', agent_status: 'working' } });
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:pC', agent: 'pi', agent_status: 'idle' } });
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:pC', agent: 'pi', agent_status: 'working' } });
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:pC', agent: 'pi', agent_status: 'idle' } });  // suppressed
  assert(notes.length === n16 + 1, 'cooldown suppresses second notify');
  w.OPTS.cooldownMs = 0;
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:pC', agent: 'pi', agent_status: 'working' } });
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:pC', agent: 'pi', agent_status: 'idle' } });
  assert(notes.length === n16 + 2, 'cooldown=0 allows notify again');
  w.OPTS.cooldownMs = 10000;

  // --- 17. subscribed-but-untracked pane: pane_updated still ignored
  w.subscribed.add('w1:pT');
  w.handleEvent({ event: 'pane_updated', data: { pane: { pane_id: 'w1:pT', agent: 'pi', agent_status: 'working' } } });
  assert(w.state.get('w1:pT') === undefined, 'subscribed pane not tracked via pane_updated');

  // --- 18. focused pane: no toast (user can see it); --include-focused overrides
  const n18 = notes.length;
  w.OPTS.cooldownMs = 0;
  w.subscribed.add('w1:pF');
  w.handleEvent({ event: 'pane_updated', data: { pane: { pane_id: 'w1:pF', agent: 'pi', agent_status: 'working', focused: true } } });
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:pF', agent: 'pi', agent_status: 'working' } });
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:pF', agent: 'pi', agent_status: 'idle' } });
  assert(notes.length === n18, 'focused pane: no notification');
  w.handleEvent({ event: 'pane_updated', data: { pane: { pane_id: 'w1:pF', agent: 'pi', agent_status: 'working', focused: false } } });
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:pF', agent: 'pi', agent_status: 'working' } });
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:pF', agent: 'pi', agent_status: 'idle' } });
  assert(notes.length === n18 + 1, 'unfocused pane: notification fires');
  w.OPTS.includeFocused = true;
  w.handleEvent({ event: 'pane_updated', data: { pane: { pane_id: 'w1:pF', agent: 'pi', agent_status: 'working', focused: true } } });
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:pF', agent: 'pi', agent_status: 'working' } });
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:pF', agent: 'pi', agent_status: 'idle' } });
  assert(notes.length === n18 + 2, '--include-focused: focused pane notifies');
  w.OPTS.includeFocused = false;
  w.OPTS.cooldownMs = 10000;

  // --- 19. stale focused: focused 标记超过 5s 未刷新不再抑制通知
  const n19 = notes.length;
  w.OPTS.cooldownMs = 0;
  w.subscribed.add('w1:pG');
  w.handleEvent({ event: 'pane_updated', data: { pane: { pane_id: 'w1:pG', agent: 'pi', agent_status: 'working', focused: true } } });
  w.paneMeta.get('w1:pG').focusedAt = Date.now() - 6000;   // 6s 前曾聚焦，现已过期
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:pG', agent: 'pi', agent_status: 'working' } });
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:pG', agent: 'pi', agent_status: 'idle' } });
  assert(notes.length === n19 + 1, 'stale focused (>5s) does not suppress notification');
  w.paneMeta.get('w1:pG').focusedAt = Date.now();          // 新鲜 focused 仍抑制
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:pG', agent: 'pi', agent_status: 'working' } });
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w1:pG', agent: 'pi', agent_status: 'idle' } });
  assert(notes.length === n19 + 1, 'fresh focused still suppresses notification');
  w.OPTS.cooldownMs = 10000;

  // --- 20. refresh: 已知 pane 本地 idle、快照 done（working→done 整段丢失）→ 补发
  const n20 = notes.length;
  w.OPTS.cooldownMs = 0;
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w9:pM', agent: 'pi', agent_status: 'working' } });
  w.handleEvent({ event: 'pane.agent_status_changed', data: { pane_id: 'w9:pM', agent: 'pi', agent_status: 'idle' } });
  agents = [{ pane_id: 'w9:pM', agent: 'pi', agent_status: 'done' }];   // 事件流未送达 done（模拟整段丢失）
  await w.refresh('test-missed');
  assert(notes.length === n20 + 1 && notes[notes.length - 1].sound === 'done', 'whole-cycle miss (idle->done) re-notified via snapshot');
  // 已对齐后再刷新：无变化、不重复发
  agents = [{ pane_id: 'w9:pM', agent: 'pi', agent_status: 'done' }];
  await w.refresh('test-missed2');
  assert(notes.length === n20 + 1, 'no duplicate after state aligned');
  // --- 20b. 反向（done->working）只对齐不通知
  const n20b = notes.length;
  agents = [{ pane_id: 'w9:pM', agent: 'pi', agent_status: 'working' }];
  await w.refresh('test-back');
  assert(notes.length === n20b, 'terminal->working aligns silently');
  w.OPTS.cooldownMs = 10000;
  console.log('done');
})();
