# Change Set A durable acceptance

This record is the authoritative current validation status for Change Set A at
remediation head `fcfb636fdce88e2be4683f96d469581f05887914`.

## Durable disposition

- Layer A — **ACCEPTED**
- Layer B — **PASS**
- Change Set A — **END-TO-END ACCEPTED**

Layer A was accepted previously and was not retested during the final Layer B
diagnostic. Historical reports that recorded an earlier inconclusive or failed
attempt remain accurate historical evidence; they do not represent the current
operative disposition.

Acceptance is limited to the defined Change Set A requirements. It does not by
itself authorize deployment, publication, activation, runtime upgrades,
automation changes, archive-policy changes, kind-9 archiving, C2c, unrelated
work, or another change set. A further Change Set A diagnostic requires a
separate reason and authorization.

## Layer B accepted diagnostic

- Diagnostic identifier: `CSA-LAYER-B-20260818T164410Z`
- Paste count: `1`
- Submit count: `1`
- Retry count: `0`
- Payload size: `159 bytes`
- Line endings: LF-only
- Payload SHA-256:
  `2dcbd522bd6613a4c4cb13628a28203d6e0bef1d3b64748181e4317043bac7ac`
- Channel: `#DP`
- Channel ID: `a6a3fb0d-9c9e-4596-b62a-78a1040d06fb`

The originating signed kind-9 event ID was retrieved from the authenticated
Buzz timeline with **More actions → Copy link**. It was not inferred from
timestamps, adjacency, content, ordering, or reconstructed event data.

- Originating kind-9 event ID:
  `d1a8ba2bcc36091c3556f38e353d3f1934edc397ebba33f4bcc56580d5440e80`
- Authoritative Copy-link evidence:
  `buzz://message?channel=a6a3fb0d-9c9e-4596-b62a-78a1040d06fb&id=d1a8ba2bcc36091c3556f38e353d3f1934edc397ebba33f4bcc56580d5440e80`

## Ledger and turn provenance

- Ledger identity:
  `f5c14e6a59a14a41269e5c631fe77d7c29467438429f167be50ce7a4e5412526`
- ACP session: `01a015c3-c8a6-7481-a138-f09d217d3839`
- Metric turn: `d8552ac8-eafa-49a4-85f6-6163d3fad0fe`
- Native Codex turn: `01a015c3-ca41-71b2-826d-73f73f6f2a31`
- Harness: `codex-acp`

## Durable kind-44200 evidence

- Durable kind-44200 event ID:
  `867153dfcfb4c4dab9951e8845c12374366b5231d1af70fd876def27f2108e5d`
- Complete `triggeringEventIds` value:

  ```json
  ["d1a8ba2bcc36091c3556f38e353d3f1934edc397ebba33f4bcc56580d5440e80"]
  ```

- Correlation verdict: **EXACT MATCH**

The complete originating 64-hex kind-9 event ID appears literally in the
durable metric's `triggeringEventIds`. The accepted causal chain is:

```text
signed kind-9 request
d1a8ba2bcc36091c3556f38e353d3f1934edc397ebba33f4bcc56580d5440e80

→ Ledger / Codex turn

→ durable kind-44200
867153dfcfb4c4dab9951e8845c12374366b5231d1af70fd876def27f2108e5d

→ triggeringEventIds
["d1a8ba2bcc36091c3556f38e353d3f1934edc397ebba33f4bcc56580d5440e80"]
```

This proves the Layer B acceptance property: the durable turn metric can carry
the authoritative signed event ID of the request that caused the turn.

## Terminal metric

- Outcome: `success`
- Terminal evidence: `prompt_response`
- Native stop reason: `EndTurn`
- Harness: `codex-acp`
- Input tokens: `61104`
- Output tokens: `162`
- Total tokens: `61266`
- Cache-read tokens: `60160`
- Costs: `null`
- `deltaReliable`: `true`

The unavailable cost value is preserved as `null`; it is not represented as
zero.

## Signed ACK and uniqueness

- Signed ACK event ID:
  `9b28fb040818159ca65a2a3fe1aa1e8e8095e0fa7c323fd3acedd345e4484df7`
- ACK content: `CSA-LAYER-B-20260818T164410Z ACK`

The accepted run produced exactly one originating request, one attributable
Ledger turn, one relevant terminal kind-44200 metric, and one accepted signed
ACK. It used no diagnostic retry and produced no conflicting telemetry.

## Qualified runtime and isolation

- Buzz 0.5.14 executable SHA-256:
  `d491b72e252612c43cdf3cd1e758754c8b83142673f474393d7df13b72eaf933`
- Relay: `wss://mmass.communities.buzz.xyz`

The accepted validation used:

- a read-only host filesystem;
- a writable cloned `/home/ddune`;
- protected read-only production `.codex/auth.json`;
- protected read-only production `.codex/config.toml`;
- protected read-only production `.codex/packages/`;
- a writable, clone-owned remainder of `.codex`;
- a separate PID namespace and `/proc`;
- an isolated `/tmp`;
- clone-routed HOME/XDG paths; and
- the existing keyring/session-bus isolation model.

Standalone Codex and ACP initialization were qualified under this layout before
the accepted telemetry diagnostic.

No production `.codex` qualification writes were attributable to the isolated
validation runtime. Production authentication and configuration hashes remained
unchanged. The host's independently active Codex desktop may mutate its own
production SQLite state, so mutable SQLite hashes were not used as an
isolation-write oracle; this record does not claim that production mutable state
never changed for any reason.

After validation, all isolated Buzz 0.5.14 processes were stopped and production
Buzz 0.5.8 was restored from:

`/home/ddune/Applications/Buzz_0.5.8_amd64.AppImage`

Restored executable SHA-256:

`a392d859165fa22f8500a950ba20ac6b8a773851e61de5732a9fd5535d4b28a8`

## Non-invalidating runtime warning

**NON-INVALIDATING RUNTIME WARNING:** Ledger initially lacked `buzz` on its
PATH. It recovered the packaged CLI into clone-local scratch/runtime state,
which extended the turn. This did not produce a duplicate signed request, a
duplicate accepted ACK, a duplicate relevant terminal metric, conflicting
telemetry, or attributable production `.codex` writes. This warning is bounded
runtime-maintenance backlog and does not reopen Change Set A.
