# The administrative library

| | |
| --- | --- |
| **Feature** | the Go library tooling and a person's session are written with, over the same base: the two addresses computed from the instance's identity; one method per operation of the contract's union, on both projections, every request stating the contract's version; a change's answer surfaced as what it replaced, when it takes effect and what it did to each unit; a refusal as the base's error, carrying its code and its detail; on the shell, the connection and its standing subscription surfaced, and calls running concurrently, each with its own answer; and a subscription or a follow delivered in order until its caller ends it, with nothing reconnected |
| **Planning item** | yoke-project/yoke-sdk-go#24 |

## yoke-sdk-go:the-administrative-library.01 — both addresses are computed from the instance's identity, and nothing is looked up

| Field | Value |
| --- | --- |
| **Cites** | specs/60.7 · specs/90.6 · arch/60-administrative-surface/01 §One pair per instance · arch/90-sdks/01 §One project, and what the base may hold |
| **Level** | L1 |
| **Method** | test |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | the service form's runtime directory `/run/yoke`; an application instance `bench-a` under `XDG_RUNTIME_DIR=/run/user/1000`; an application name holding a `/` |
| **Action** | compute each one's addresses |
| **Expected** | `/run/yoke/operator.sock` and `/run/yoke/shell.sock`; `/run/user/1000/yoke/bench-a/operator.sock` and `…/shell.sock`; the third is refused, naming it |

## yoke-sdk-go:the-administrative-library.02 — one method per operation, each stating the contract's version, on either projection

| Field | Value |
| --- | --- |
| **Cites** | specs/90.8 · specs/90.10 · specs/90.26 · specs/60.20 · arch/90-sdks/01 §What each library declares · arch/60-administrative-surface/08 §Two services over one union |
| **Level** | L1 |
| **Method** | test |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a Core that records every request it is sent, and answers each |
| **Action** | call every operation the library offers, once through the operator projection and once through a shell connection |
| **Expected** | the sixteen operations of the union were each sent once on each projection, every request stating the version 1, and the ones answered by a stream on `Watch` |

## yoke-sdk-go:the-administrative-library.03 — a change answers what it replaced, when it takes effect, and what it did to each unit

| Field | Value |
| --- | --- |
| **Cites** | specs/60.31 · specs/60.34 · arch/60-administrative-surface/04 §What every answer carries |
| **Level** | L1 |
| **Method** | test |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a Core that answers a disable with: it was enabled, effective immediately, and two units whose Sessions were revoked |
| **Action** | disable the plugin |
| **Expected** | the library answers that it was enabled, effective immediately, and the two consequences, each naming its unit, its life and what happened |

## yoke-sdk-go:the-administrative-library.04 — a refusal is the base's error, with its code and its detail, on either projection

| Field | Value |
| --- | --- |
| **Cites** | specs/90.22 · specs/90.23 · specs/60.57 · arch/90-sdks/05 §The observable model may not differ · arch/00-system/05 §How a refusal travels |
| **Level** | L1 |
| **Method** | test |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a Core that refuses a stop with `subject.unknown` naming the unit `nobody`, and a grant with `capability.undeclared` naming `head.move` |
| **Action** | stop `nobody` and grant `head.move`, through the operator projection and through a shell connection |
| **Expected** | each is the base's refusal, carrying its code exactly, the subject's kind and identity in the first and the item in the second, alike on both projections |

## yoke-sdk-go:the-administrative-library.05 — a shell connection surfaces who it is, and its calls run concurrently

| Field | Value |
| --- | --- |
| **Cites** | specs/60.19 · specs/60.23 · arch/60-administrative-surface/02 §Interleaving, correlation and completion |
| **Level** | L1 |
| **Method** | test |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a Core whose shell opens with the connection `c-7`, the actor `ada` and the standing subscription `standing`, and answers a read of `slow` only after a read of `fast` |
| **Action** | connect; read `slow` and `fast` at once |
| **Expected** | the connection surfaces `c-7`, `ada` and its standing subscription; each read returns its own answer, `fast`'s first |

## yoke-sdk-go:the-administrative-library.06 — a subscription is delivered in order until its caller ends it, and nothing reconnects

| Field | Value |
| --- | --- |
| **Cites** | specs/60.49 · specs/90.32 · arch/60-administrative-surface/06 §What a subscription promises · arch/90-sdks/06 §It may not poll |
| **Level** | L1 |
| **Method** | test |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a Core that answers a subscription with a snapshot, two events and an overflow, and then ends the transport; and a shell whose standing subscription delivers a snapshot and an event |
| **Action** | subscribe through the operator projection and read until the end; connect a shell and read its standing subscription |
| **Expected** | the snapshot, the two events and the overflow's snapshot are delivered in that order, then the end, and the Core saw one subscription only; the shell's standing subscription delivers its snapshot and its event |

## yoke-sdk-go:the-administrative-library.07 — a follow delivers entries, and says where it fell behind

| Field | Value |
| --- | --- |
| **Cites** | specs/60.43 · arch/60-administrative-surface/05 §The log store, queried and followed |
| **Level** | L1 |
| **Method** | test |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a Core that answers a follow with two entries and then that it fell behind at 41 |
| **Action** | follow the log and read |
| **Expected** | the two entries, then the sequence 41, in that order |
