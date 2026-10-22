# Stream Bus — Testing Protocol

Follow this protocol whenever you are asked to test this package. Be honest: report failures, partial flows, and edge cases. Do not hide issues.

## Role

Act as a real-world tester verifying the stream/event bus behavior end-to-end (publish → subscribe → consume) and the correctness of payloads.

## Context

- This is a shared TypeScript package consumed by other services — test both the package itself and, where relevant, a consumer integration.
- The system is production-like — do not assume correctness; validate everything explicitly.

## Authentication tokens

- If the test scenario requires authenticated API calls, refresh tokens with the terminal aliases:
  - `logind` — regular user auth token
  - `loginda` — auth token **including admin token** (use when the scenario needs admin access)

## Coverage requirements

Test every implemented flow end-to-end. Cover all scenarios:

- Success cases
- Failure cases
- Validation errors
- Edge cases (empty payloads, large payloads, concurrent publishers/consumers, consumer restart/reconnect)

## For every event/stream, validate

- Event/topic names exactly as defined in the contracts
- Payload structure and data types of every field
- Required vs optional fields
- Delivery guarantees (ordering, at-least-once/at-most-once as designed, no duplicates)
- Error handling and retry behavior

Do not assume correctness — verify it explicitly.

## Data consistency validation (critical)

- **Publish** → verify the event is actually delivered to the correct consumer groups/streams.
- **Consume** → verify consumers receive the real, accurate, complete payload (no truncation, no stale data, no duplicate processing).
- **Schema changes** → verify any new/renamed fields behave correctly for both producers and consumers.

## Log monitoring (mandatory)

Monitor the relevant process/service logs during testing. Check for:

- Errors, warnings, stack traces, unhandled exceptions, dropped/rejected messages

If any issue appears:

1. Identify the real root cause (no temporary patches).
2. Apply a proper fix.
3. Re-test all affected flows.
4. Confirm logs are clean.

## Reporting format (strict)

For each flow tested, provide **every** field:

```
Flow/event: ...
Command/script used: ...
Expected behavior: ...
Actual behavior: ...
PASS or FAIL: ...
Issue found (if any): ...
Root cause: ...
Fix applied (if any): ...
```

Do not skip fields. Do not summarize failures.

## Completion criteria

- [ ] All flows tested; all scenarios covered
- [ ] All critical issues fixed; behavior exactly per requirements
- [ ] Published/consumed data verified real, accurate, and complete
- [ ] No unexpected errors in logs; system state stable and consistent
- [ ] All requirements fully satisfied — nothing assumed
