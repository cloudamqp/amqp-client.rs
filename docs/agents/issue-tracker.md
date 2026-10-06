# Issue Tracker: Epiq

Issues and PRDs for this repo are tracked in Epiq:

https://ljtn.github.io/epiq/

Epiq is a repo-native terminal issue tracker. It stores work as an immutable event log and synchronizes through Git, so issue state belongs with the repository rather than a hosted SaaS tracker.

## Conventions

- Use Epiq as the source of truth for issue state.
- Use the triage vocabulary from `docs/agents/triage-labels.md` when tagging or moving issues.
- Prefer Epiq's CLI or MCP integration when creating, updating, fetching, filtering, or syncing issues.
- If a skill needs an issue reference and none is provided, ask for the Epiq issue reference or enough search terms to locate it in Epiq.

## When a skill says "publish to the issue tracker"

Create the issue or PRD in Epiq. Include enough detail for the issue to be picked up later without relying on conversation history.

## When a skill says "fetch the relevant ticket"

Read the referenced issue from Epiq. If the issue cannot be found from the reference provided, ask the user for the Epiq issue reference.
