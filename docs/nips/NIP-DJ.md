# NIP-DJ: Bounded Delegated Jobs

## Status

Buzz extension. Normative for delegated executable work represented by kinds `43001–43008`.

## Purpose and boundary

This protocol distinguishes a proposed delegation, explicit acceptance of responsibility, and terminal job disposition without parsing conversational prose. Ordinary messages, mentions, questions, and discussion are not jobs.

An accepted delegated job is the structural input from which BTOM may create a durable execution obligation. This protocol does not itself implement execution obligations or automatic continuation.

## Event kinds

| Kind | Symbol | Meaning |
|---|---|---|
| `43001` | `KIND_JOB_REQUEST` | Owner proposes one job to one managed agent |
| `43002` | `KIND_JOB_ACCEPTED` | Target agent accepts responsibility |
| `43003` | `KIND_JOB_REJECTED` | Target declines before acceptance; terminal |
| `43004` | `KIND_JOB_COMPLETED` | Accepted job completed; terminal |
| `43005` | `KIND_JOB_BLOCKED` | Accepted job blocked; terminal |
| `43006` | `KIND_JOB_DELEGATED` | Accepted job delegated/transferred; terminal |
| `43007` | `KIND_JOB_EXECUTION_ATTEMPT` | Runtime attempt/continuation metadata subordinate to an accepted job |
| `43008` | `KIND_JOB_SUPPLEMENTAL_CONTEXT` | Target admission of one exact source message for a continuation generation |

All events are regular append-only stored events. The relay materializes the current state transactionally for deterministic queries and restart recovery; lifecycle history remains in the signed event log.

Neither generic NIP-09 deletion nor NIP-29 channel-admin deletion events may delete delegated-job events. Revocation or disposition is represented only by the explicit lifecycle transitions below, preserving immutable history and restart recovery.

## Job request (`43001`)

Required public tags, each exactly once and with exactly two elements:

- `d`: canonical UUID identifying the job and enabling standard `#d` queries;
- `job-target`: 64-character hex pubkey of exactly one managed agent;
- `p`: exactly one copy of that same target pubkey for Nostr subscription routing;
- `h`: UUID of the originating Buzz channel.

The signed event author is the requester. Event `created_at` is the creation time and the signed event ID identifies the immutable assignment content. `content` is the non-empty assignment, limited to 65,536 UTF-8 bytes.

`job-request`, `job-parent`, and `job-successor` are forbidden on a request. A request with zero, duplicate, malformed, contradictory `p`/`job-target`, or extra-shaped required tags is invalid.

### Request authority

The initial policy is deliberately narrow: only the target agent's registered owner may request a job. Same-owner sibling agents and general channel members cannot request jobs. The target must be registered as that requester's managed agent and must be a member of the originating channel. The requester must independently pass normal channel-write membership and token-scope checks.

This policy can later be extended through an explicit delegated-authority contract. Channel membership, mentions, tool access, or a shared owner alone do not grant job-request authority.

## Lifecycle envelope (`43002–43006`)

Required tags, each exactly once:

- `d`: the original job UUID;
- `job-request`: the original `43001` event ID;
- `job-parent`: the immediate predecessor event ID;
- `h`: the immutable originating channel UUID.

Lifecycle events must be authored by the original target agent. They cannot redefine requester, target, assignment, or context; `job-target` and `p` are forbidden. `content` is an optional result/reason limited to 65,536 bytes.

For acceptance or rejection, `job-parent` is the request event. For completion, blocked, or delegation, it is the acceptance event. The relay compares this reference with the atomically locked current head, so same-second events and competing branches cannot be ordered by client timestamps or both commit. Terminal CLI commands require `--parent <acceptance-event-id>`.

`43006` additionally requires exactly one `job-successor` 64-hex managed-agent pubkey. The successor must be managed by the original requester. Other lifecycle kinds forbid `job-successor`. The original job closes as delegated/transferred; a successor's executable work requires a separately requested and accepted child job rather than mutation of the original identity.

## Execution obligation and attempts (`43007`)

An accepted, non-terminal delegated job is the durable execution obligation. No attempt event may independently open, complete, block, reject, or transfer that obligation. Runtime attempts are subordinate metadata keyed by the immutable job `d` coordinate.

Every attempt event is authored by the target agent and carries exactly one `d`, `job-request`, `attempt`, `generation`, `job-target`, `h`, and `attempt-action`. Generation starts at one and increases monotonically. The legal attempt actions are:

- `runnable`: creates the single continuation entitlement for a generation;
- `claim`: binds that entitlement to one `turn-id`, optional `session-id`, and future `lease-until`, referencing the runnable event with `attempt-parent`;
- `finish`: records a bounded `attempt-outcome` and optional diagnostic content, referencing the claim event with `attempt-parent` and the same `turn-id`.

The relay serializes attempt changes under the delegated job's community/job advisory lock. A generation is unique per job and an attempt ID is unique per community. A valid runnable or active attempt prevents another generation; an expired claim or finished attempt permits exactly the next generation. Repeated reconciliation without intervening state therefore changes nothing. A terminal job transition suppresses any runnable or active attempt in the same transaction, and an attempt finish never changes job disposition.

Normal ACP return, `EndTurn`, stop, cancellation, cancel-and-merge, session replacement, worker loss, teardown, max-turn exhaustion, timeout, and recoverable provider failure are attempt outcomes only. If the job remains accepted, reconciliation creates exactly one next runnable generation. Only kinds `43004`, `43005`, and `43006` provide terminal BTOM disposition.

## Supplemental context (`43008`)

When an admitted ordinary message arrives while an accepted job owns the channel, ACP signs a supplemental-context event before the message can affect a runtime session. Required tags are `d`, `job-request`, `job-target`, `h`, `supplemental-event`, `supplemental-author`, and positive `continuation-generation`, each exactly once with two elements. JSON content contains one `content` string copied from the source event.

The relay accepts the event only from the accepted job target, while the job remains accepted, with immutable request/channel coordinates. The exact source event must already exist in the same channel and its author and content must match. The target generation must be the current or immediately succeeding generation. These events are append-only control evidence and are not conversational input.

An optional conversational reply to an admitted source message runs in a fresh mutation-restricted follow-up session. Its only write surface is an ordinary threaded reply: the bundled broker permits the exact `buzz messages send --channel ... --content ... --reply-to ...` command shape, and the CLI revalidates that the reply target has a target-authored `43008` admission tied to a currently accepted job before every send. The broker has no lifecycle, shell-mutation, file-edit, process, Git, or configured/native MCP capability. It uses one process-global registration with no batch-static channel or reply authority; this allows native steering to add another admitted message without retaining stale per-session credentials. Supplemental content remains available to the next durable generation whether or not the optional conversational reply succeeds.

## State machine

| Current | Event | Next |
|---|---|---|
| requested | accepted | accepted |
| requested | rejected | rejected (terminal) |
| accepted | completed | completed (terminal) |
| accepted | blocked | blocked (terminal) |
| accepted | delegated | delegated/transferred (terminal) |

Every other transition is invalid. In particular, completion/block/transfer before acceptance, acceptance by a non-target, repeated acceptance, a second terminal transition, and any transition after terminal state are rejected.

Rejection means responsibility was never accepted and is distinct from BTOM `blocked`, which closes work after acceptance because forward execution cannot proceed.

## Replay, identity, and concurrency

The relay stores each signed event at most once. Replaying the identical signed event is accepted as a harmless idempotent replay and produces no second state transition or fan-out. Reusing a job UUID with a different request event is a conflict and is rejected, even when other fields match.

The relay serializes writes per `(community, job UUID)` and updates the append-only event plus materialized state in one transaction. Competing lifecycle events therefore cannot both transition the same predecessor state.

The request event ID, requester, target, channel, assignment hash, and job UUID are immutable. Every lifecycle write is checked against the materialized request identity.

## Query and recovery

The signed history is queryable by kinds and the standard single-letter `#d` filter. The materialized state records request, acceptance, terminal event, target, context, successor, and current state. It supports:

- all jobs targeting an agent;
- accepted non-terminal jobs targeting an agent;
- current state by job UUID;
- request, acceptance, and terminal event coordinates.

Because both history and current state are durable, a relay or ACP restart reconstructs the same lifecycle state without interpreting chat history.

`buzz jobs get --job <uuid>` returns one immutable event chain. `buzz jobs list --target <pubkey>` reconstructs and returns accepted non-terminal jobs for that agent, providing a restart recovery interface available inside agent sessions.

## Agent ingestion

`buzz-acp` includes targeted `43001` delivery as an invariant control-plane subscription for every discovered member channel, independent of conversational kind overrides and config rules. It validates that `job-target` is its own identity and places requests only in job-class runtime batches. It presents explicit `buzz jobs accept` and `buzz jobs reject` commands. Lifecycle events are control-plane state and never enter conversational dispatch.

The request-evaluation turn runs in a fresh restricted session. Buzz supplies the ACP `_meta.hermes.toolProfile = "decision-only"` capability-reduction extension; compatible Hermes adapters omit their native and configured toolsets for that session, then register only Buzz's explicitly supplied MCP server. Unknown MCP servers, native terminal/code/delegation/browser tools, general shell execution, and file mutation are therefore unavailable. The bundled MCP permits read-only inspection plus only the constrained `buzz jobs accept` or `buzz jobs reject` shell command. Follow-up sessions for an active accepted job use the separately restricted surface described above. Ordinary conversation with no active job and durably claimed execution sessions omit the reduction profile and retain their configured tool surfaces.

The target's matching acceptance or rejection rotates the restricted session away. ACP-launched MCP servers mark the constrained CLI environment so a successfully persisted decision call does not return control to the model; if lifecycle observation is delayed, the tool call remains pending and therefore fails closed. Acceptance records an exact-job promotion obligation. After the cancelled evaluation result clears the channel's in-flight state, Buzz reconciles that job through the same canonical state machine used by startup and periodic recovery, using the immutable `d` coordinate to avoid unrelated pagination. Accepted work receives a new unrestricted turn only after generation one has both a persisted `runnable` event and a persisted `claim` event. Periodic reconciliation remains crash/restart recovery; an evaluation that ends without a matching durable decision retains normal retry behavior. The request-evaluation turn is never itself an execution attempt.

Receipt, a human-facing pickup message, or a normal ACP turn does not accept a job. Only a valid target-authored `43002` event does.
