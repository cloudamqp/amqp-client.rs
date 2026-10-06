# Issue Tracker: GitHub Issues

Issues and PRDs for this repo are tracked as GitHub issues on
[cloudamqp/amqp-client.rs](https://github.com/cloudamqp/amqp-client.rs/issues).

## Conventions

- GitHub issues are the source of truth for issue state.
- Use the `gh` CLI (`gh issue create`, `gh issue view`, `gh issue list`,
  `gh issue edit`) with `--repo cloudamqp/amqp-client.rs`.
- Use the triage vocabulary from `docs/agents/triage-labels.md` as issue
  labels.

## When a skill says "publish to the issue tracker"

Create a GitHub issue. Include enough detail for the issue to be picked up
later without relying on conversation history. Creating an issue publishes it,
so confirm the text with the user first.

## When a skill says "fetch the relevant ticket"

Read the referenced issue with `gh issue view <number> --repo
cloudamqp/amqp-client.rs`. If it cannot be found from the reference provided,
ask the user for the issue number or URL.
