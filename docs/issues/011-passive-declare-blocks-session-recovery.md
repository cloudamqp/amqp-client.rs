# A recorded passive declare can block session recovery forever

- **Priority:** P3
- **Layer:** high-level
- **Status:** open
- **Found:** code review, 2026-10-09 (finding #6)
- **Confidence:** From reading the code

## Where

- `src/high_level/session.rs:288` (`open_session`)
- `src/high_level/client.rs:126` (`Client::queue` records the declare)

## Problem

`Client::queue` and `Client::exchange` record every declare, including
passive ones. `open_session` fails the whole attempt if any recorded declare
or bind fails. A passive declare of something that is gone fails with 404 on
every attempt, as does a binding to an unrecorded exchange that is gone.

## Failure scenario

`client.queue("orders", passive)` to get a handle on an existing queue;
someone deletes `orders`; the connection drops → every recovery attempt hits
404 on the passive declare → the loop retries forever, and every publish and
consume on the client fails until the process restarts.

## Possible fix

Needs a decision: should a passive declare be recorded at all (a handle
without recovery), or be re-checked but not fatal? Separately: should one
failing declaration fail the whole session, or be skipped and logged so the
rest of the client keeps working? ADR-0002 currently says declaration failures
fail the attempt.
