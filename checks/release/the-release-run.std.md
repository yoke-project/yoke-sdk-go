# The release run

| | |
| --- | --- |
| **Feature** | this family's release: a tag `vX.Y.Z` runs the `release` verb where it is seen, and the verb publishes this module through the published release verb of `yoke`, modules only, emitting one manifest line — the run keeps the lines for the release command to collect |
| **Planning item** | yoke-project/yoke-sdk-go#21 |

## yoke-sdk-go:the-release-run.01 — the verb publishes this module with the published release verb

| Field | Value |
| --- | --- |
| **Cites** | prj_structure/95 §The release command · prj_structure/85 §The acts |
| **Level** | L1 |
| **Method** | check |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a clean checkout |
| **Action** | read the `release` verb |
| **Expected** | it runs `yoke-release` from the module proxy with `-modules-only`, and reaches no sibling |

## yoke-sdk-go:the-release-run.02 — a release tag runs the verb, and the run keeps its lines

| Field | Value |
| --- | --- |
| **Cites** | prj_structure/95 §The release command · prj_structure/90 §9 — A release |
| **Level** | L1 |
| **Method** | check |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a clean checkout |
| **Action** | read the release workflow |
| **Expected** | it runs on a tag `v*` with the whole history, runs the `release` verb into `manifest-lines.jsonl`, and keeps that file as the artifact `manifest-lines` |

## yoke-sdk-go:the-release-run.03 — at a commit no release tag names, the verb publishes nothing and says so

| Field | Value |
| --- | --- |
| **Cites** | prj_structure/95 §The release command |
| **Level** | L1 |
| **Method** | check |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a checkout whose commit carries no release tag |
| **Action** | run the `release` verb |
| **Expected** | it exits zero with nothing on standard output, and its error stream says nothing is published from this commit |

## yoke-sdk-go:the-release-run.04 — at a release tag the SDK line disagrees with, the verb publishes nothing and refuses

| Field | Value |
| --- | --- |
| **Cites** | prj_structure/95 §The release command · specs/50.15 · specs/90.9 |
| **Level** | L1 |
| **Method** | check |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a copy of the checkout whose commit carries the release tag `v0.9.9`, which the SDK line does not name |
| **Action** | run the `release` verb |
| **Expected** | it exits non-zero with nothing on standard output, and its error stream names the line and the tag |
