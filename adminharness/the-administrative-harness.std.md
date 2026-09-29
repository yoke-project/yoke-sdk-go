# The administrative harness

| | |
| --- | --- |
| **Feature** | the Go administrative library's harness: launched by the suite with the instance's root in `CONFORMANCE_INSTANCE`, it says hello for the administrative contract, turns each directive into one call of the library on the operator projection, reports what the library answered in the suite's vocabulary — a change as what it replaced, when it takes effect and its consequences, a refusal as its code and what it names — and what a subscription or a follow delivers as observations, in order; it judges nothing and speaks no wire of its own |
| **Planning item** | yoke-project/yoke-sdk-go#27 |

## yoke-sdk-go:the-administrative-harness.01 — hello first, for the administrative contract

| Field | Value |
| --- | --- |
| **Cites** | specs/90.17 · arch/90-sdks/04 §The control protocol · arch/90-sdks/04 §How it is reached |
| **Level** | L1 |
| **Method** | test |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a control socket, and an instance whose operator projection is served |
| **Action** | launch the harness with both in its environment |
| **Expected** | its first line is a hello naming the administrative contract, the language `go`, the project's SDK line, the version 1, and no unit |

## yoke-sdk-go:the-administrative-harness.02 — a change is reported as what it replaced, and a refusal as its code and what it names

| Field | Value |
| --- | --- |
| **Cites** | specs/60.31 · specs/90.23 · arch/90-sdks/04 §The control protocol · arch/60-administrative-surface/04 §What every answer carries |
| **Level** | L1 |
| **Method** | test |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a Core that answers a disable with: it was enabled, effective immediately, one unit's Session revoked; and refuses a stop with `subject.unknown` naming the unit `nobody` |
| **Action** | direct `disable` and `stop-unit` |
| **Expected** | the first result carries that it was enabled, `immediately` and the consequence; the second carries the refusal `subject.unknown` and the subject it names, and no message |

## yoke-sdk-go:the-administrative-harness.03 — what a subscription delivers is an observation, in order

| Field | Value |
| --- | --- |
| **Cites** | specs/60.49 · arch/90-sdks/04 §The control protocol · arch/60-administrative-surface/06 §What a subscription promises |
| **Level** | L1 |
| **Method** | test |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a Core that answers a subscription with a snapshot, an event and an overflow |
| **Action** | direct `subscribe` |
| **Expected** | the result is answered, and then three observations in order: `snapshot`, `event` carrying its type and subject, `overflow` |

## yoke-sdk-go:the-administrative-harness.04 — a verb it does not know is unrecognised, finish ends it, and it holds no wire

| Field | Value |
| --- | --- |
| **Cites** | arch/90-sdks/04 §What a harness is · arch/90-sdks/04 §The vocabulary |
| **Level** | L1 |
| **Method** | test |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | the harness, connected |
| **Action** | direct `emit`; then send finish; then read the harness's imports |
| **Expected** | `emit` is reported unrecognised; the harness exits zero; it imports no transport |

## yoke-sdk-go:the-administrative-harness.05 — the suite is run against it, as it is against the plugin harness

| Field | Value |
| --- | --- |
| **Cites** | specs/90.17 · specs/90.19 · arch/90-sdks/03 §The shape of a run |
| **Level** | L1 |
| **Method** | check |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | `ci/conformance.sh` |
| **Action** | read it |
| **Expected** | it builds the administrative harness and runs the suite against it, beside the plugin harness |
