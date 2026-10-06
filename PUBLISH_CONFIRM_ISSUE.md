# Add high-level publisher confirm methods

## Problem

The low-level `Channel` supports publisher confirms through `confirm_select` and `basic_publish_confirmed`, but the high-level `Client` only exposes plain `publish` and `publish_with_props`.

Automatic recovery should not silently invent publisher-confirm retry semantics. High-level confirmed publishing needs an explicit API and semantics of its own.

## Scope

- Add high-level publisher-confirm methods to `Client`.
- Preserve codec-based serialization and content-type behavior.
- Require explicit confirm semantics rather than automatic retry across recovery.
- Document the uncertainty case where a publish reaches the broker but the confirm is lost with the AMQP session.

## Notes

This was split out while designing automatic recovery.
