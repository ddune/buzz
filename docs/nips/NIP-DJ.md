# NIP-DJ: Bounded Delegated Jobs

## Status

Buzz extension. Normative for delegated executable work represented by kinds `43001–43006`.

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

All events are regular append-only stored events. The relay materializes the current state transactionally for deterministic queries and restart recovery; lifecycle history remains in the signed event log.

## Job request (`43001`)

Required public tags, each exactly once and with exactly two elements:

- `job`: canonical UUID identifying the job;
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

- `job`: the original job UUID;
- `job-request`: the original `43001` event ID;
- `job-parent`: the immediate predecessor event ID;
- `h`: the immutable originating channel UUID.

Lifecycle events must be authored by the original target agent. They cannot redefine requester, target, assignment, or context; `job-target` and `p` are forbidden. `content` is an optional result/reason limited to 65,536 bytes.

For acceptance or rejection, `job-parent` is the request event. For completion, blocked, or delegation, it is the acceptance event. The relay compares this reference with the atomically locked current head, so same-second events and competing branches cannot be ordered by client timestamps or both commit. Terminal CLI commands require `--parent <acceptance-event-id>`.

`43006` additionally requires exactly one `job-successor` 64-hex managed-agent pubkey. The successor must be managed by the original requester. Other lifecycle kinds forbid `job-successor`. The original job closes as delegated/transferred; a successor's executable work requires a separately requested and accepted child job rather than mutation of the original identity.

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

The signed history is queryable by kinds and `#job`. The materialized state records request, acceptance, terminal event, target, context, successor, and current state. It supports:

- all jobs targeting an agent;
- accepted non-terminal jobs targeting an agent;
- current state by job UUID;
- request, acceptance, and terminal event coordinates.

Because both history and current state are durable, a relay or ACP restart reconstructs the same lifecycle state without interpreting chat history.

`buzz jobs get --job <uuid>` returns one immutable event chain. `buzz jobs list --target <pubkey>` reconstructs and returns accepted non-terminal jobs for that agent, providing a restart recovery interface available inside agent sessions.

## Agent ingestion

`buzz-acp` subscribes to `43001` separately from conversational messages, validates that `job-target` is its own identity, and places job requests only in job-class runtime batches. It presents explicit `buzz jobs accept` and `buzz jobs reject` commands. Lifecycle events are control-plane state and never enter conversational dispatch.

Receipt, a human-facing pickup message, or a normal ACP turn does not accept a job. Only a valid target-authored `43002` event does.
