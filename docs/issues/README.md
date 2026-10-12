# Known issues

Issues are kept here as Markdown files until the repository has an issue
tracker. One file per issue. 001–014 come from a review of the whole crate on
2026-10-09, numbered by priority; 015–017 from a review of `atomic-publish`
the same day. Each file notes whether the finding was reproduced or comes from
reading the code.

To close an issue, set its **Status** to `fixed` and mention the change that
fixed it, or delete the file once the fix has landed.

## Priorities

- **P0: fix now, on `message-decoding`.** Introduced by that bookmark's
  `Stream` change. Fixed.
- **P1: urgent, its own bookmark next.** The high-level `Client` shared one
  channel and one confirm channel between all tasks, so ordinary concurrent use
  corrupted publishes (002, reproduced) and could report a rejected confirmed
  publish as acked (003). Fixed by one critical section that assigns the
  confirm tag and enqueues all of a publish's frames.
- **P2: low-level correctness, next.** Bugs in the protocol layer that need
  timing, a specific call sequence or a slow consumer to hit. Roughly ordered
  by impact: 004 permanently misaligns a channel's replies, 005 and 006 are
  plain logic bugs with simple triggers, 007 and 008 lose or strand messages,
  009 is a narrow race, 010 is unbounded memory under load.
- **P3: needs a decision, then a fix.** High-level behaviour where the right
  fix is a design choice, possibly an ADR-0002 amendment, rather than an
  obvious code change. 014 is a gap in tests rather than a bug.

## Index

| # | Priority | Layer | Issue | Confidence | Status |
|---|---|---|---|---|---|
| [001](001-recovery-task-panic-swallowed.md) | P0 | high-level | A panic in a consumer's recovery task is swallowed | Read | fixed |
| [002](002-concurrent-publishes-interleave-frames.md) | P1 | low-level | Concurrent publishes on one channel interleave their frames | Reproduced | fixed |
| [003](003-confirm-tags-can-be-misattributed.md) | P1 | low-level | Confirm delivery tags can be misattributed under concurrency | Read | fixed |
| [004](004-rpc-timeout-misaligns-replies.md) | P2 | low-level | An RPC timeout misaligns every later reply on the channel | Read | open |
| [005](005-plain-publish-skips-confirm-sequence.md) | P2 | low-level | A plain publish on a confirm channel shifts every later confirm tag | Read | partly fixed |
| [006](006-basic-cancel-never-ends-consumer.md) | P2 | low-level | `basic_cancel` never ends the `Consumer` | Reproduced | open |
| [007](007-consume-ok-delivery-race.md) | P2 | low-level | Deliveries right behind `ConsumeOk` can be dropped | Plausible | open |
| [008](008-dropped-consumer-is-never-cancelled.md) | P2 | low-level | Dropping a `Consumer` never sends `basic.cancel` | Read | open |
| [009](009-rpc-reply-before-expectation-registered.md) | P2 | low-level | An RPC reply can arrive before its expectation is registered | Read | open |
| [010](010-unbounded-incoming-frame-queue.md) | P2 | low-level | The per-channel incoming frame queue is unbounded | Read | open |
| [011](011-passive-declare-blocks-session-recovery.md) | P3 | high-level | A recorded passive declare can block session recovery forever | Read | open |
| [012](012-server-cancel-recovery-drops-in-flight-acks.md) | P3 | high-level | Recovery after `ServerCancel` drops the old channel and its in-flight deliveries | Read | open |
| [013](013-confirm-channel-close-misattributed.md) | P3 | high-level | A confirm channel closed by one publish fails a concurrent one with its error | Read | open |
| [014](014-unpolled-recovery-test-does-not-guard-original.md) | P3 | tests | The unpolled-recovery test does not guard against the original consumer code | Reproduced | fixed |
| [015](015-large-publish-pins-writer-buffer.md) | P2 | low-level | A large publish is encoded whole before the writer flushes | Reproduced | fixed |
| [016](016-pending-confirms-counted-before-close.md) | P2 | low-level | A publish can register between counting pending confirms and closing | Reproduced | fixed |
| [017](017-tiny-frame-max-loops-forever.md) | P2 | low-level | A `frame_max` of 8 or less makes a publish loop forever | Read | fixed |

**Confidence:** *Reproduced*: shown against a local broker or in a unit test.
*Read*: found by reading the code; not reproduced. *Plausible*: a race that did
not reproduce in 40 runs.

## Decisions needed

- **011:** should a passive declare be recorded at all? And should one failing
  declaration fail the whole session recovery, as ADR-0002 says today?
- **012:** after `ServerCancel`, consume again on the old channel, or keep it
  open until its deliveries are settled?
- **013:** retry a confirmed publish whose send failed because another publish
  had already closed the confirm channel?
