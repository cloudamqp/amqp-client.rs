---
status: accepted
---

# High-level declares are durable by default

`Client::queue` takes a `QueueOpts`, and `Client::exchange` and its
shorthands take an `ExchangeOpts`. Both are separate from the low-level
`QueueDeclareOpts` / `ExchangeDeclareOpts`, which stay a 1:1 mapping of the
AMQP methods, transient by default.

| Declared                        | durable | exclusive | auto-delete |
| ------------------------------- | ------- | --------- | ----------- |
| exchange                        | yes     | –         | no          |
| named queue                     | yes     | no        | no          |
| named queue, `.exclusive(true)` | no      | yes       | no          |
| server-named queue (`""`)       | no      | yes       | no          |

What the caller sets with the `bool` setters (`durable`, `exclusive`,
`auto_delete`, `internal`, `arguments`) is used as is. The topology records
the resolved low-level options, so recovery declares the same thing again.
There is no `passive`: a passive declare checks a queue, it does not create
one to record.

With `QueueDeclareOpts::default()`, `client.queue("orders", ..)` made a
transient queue, gone with its messages after a broker restart (#5).
amqp-client.rb, .go and .js declare durable queues and exchanges by default,
so ported code and the README example silently lost durability. Changing it
later means deleting the queue: redeclaring with other flags is
`PRECONDITION_FAILED`.

A server-named queue is exclusive and transient: nobody else knows its name,
and recovery gives it a new one anyway. Before, `client.queue("", default)`
left a non-exclusive queue behind on every connection. An exclusive queue
dies with its connection, so durable buys nothing.

## Considered options

- **A hand-written `Default` on the low-level types.** Rejected: the low-level
  API stops mapping 1:1 to AMQP, and every low-level call that uses
  `..Default::default()` silently becomes durable.
- **High-level types with public fields and a durable `Default`.** Rejected:
  a struct cannot tell a field left alone from one set, so `""` and
  `{ exclusive: true, ..Default::default() }` would be durable unless the
  client overrode what the caller wrote.
- **Server-named queues auto-delete, like the other clients.** Rejected: an
  auto-delete queue goes with its last consumer. When the broker closes a
  consumer's channel, the queue is gone before consumer recovery consumes
  again; recovery declares a new one with a new name, and what was published
  to the old one is lost.

## Consequences

- Breaking change for every `Client::queue` / `exchange` call; acceptable
  before 1.0.
- A queue declared through the client and again through a low-level channel
  must use matching flags, e.g. `durable: true`, or the broker refuses it.
