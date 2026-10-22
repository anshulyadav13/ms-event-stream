# Stream Bus — Agent Guide

This file is read automatically by AI agents and code editors (Windsurf, Claude Code, Cursor, Copilot, Devin). Follow the **Development Rules** below for every change. When testing, follow the protocol in `TESTING.md`.

## Project context

- Service: Stream Bus (event/stream bus shared package)
- Stack: TypeScript, Node.js (compiled with `tsc` to `dist/`)
- Key commands:
  - Build: `npm run build`
  - Typecheck: `npx tsc --noEmit -p tsconfig.json`
- This package is consumed by other services — API/event-contract changes here can break consumers across the platform.

## Development rules

1. **Understand before changing** — analyze the existing codebase: architecture, folder structure, patterns, naming, and style. Do not assume — follow existing implementations as reference.
2. **Minimal, production-safe changes** — no breaking changes to event/stream contracts (topics, payload schemas, message shapes) without explicit verification of all consumers. No unnecessary refactors, no duplicated logic.
3. **Code quality** — write clean, readable, maintainable, performance-friendly code following existing conventions. Verify with `npx tsc --noEmit -p tsconfig.json` and `npm run build`.
4. **Compatibility** — this is a shared library: adding fields is safe only if consumers are forward-compatible; renaming/removing fields is breaking. Follow semantic-versioning discipline.
5. **Security** — never log or commit secrets, tokens, or credentials. Keep environment configuration out of source code.
6. **Build must pass** — finish every task with `npm run build` and fix all errors.

## Definition of done

- [ ] `npm run build` succeeds
- [ ] Typecheck passes (`npx tsc --noEmit -p tsconfig.json`)
- [ ] Follows existing project patterns; no consumers broken
- [ ] Changes are minimal and backward compatible
- [ ] Event/stream contracts unchanged unless intentionally extended and verified

## Testing

Follow the full protocol in `TESTING.md` — publish/subscribe smoke testing, payload validation, and strict PASS/FAIL reporting.
