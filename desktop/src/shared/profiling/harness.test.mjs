/**
 * Harness-core tests for the temporary renderer profiling branch (never merges).
 *
 * Scope is the pure, deterministic core only: ring-buffer bounds + flush
 * draining, drift-sampler math, and the JSONL record schema. The DOM/timer/IPC
 * probes are integration glue exercised by Will's day of driving, not unit
 * tests.
 */

import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { computeDrift, DRIFT_THRESHOLD_MS } from "@/shared/profiling/drift.ts";
import { ProfileRecorder, RING_CAPACITY } from "@/shared/profiling/recorder.ts";

function fixedClock() {
  let t = 1000;
  return {
    now: () => (t += 1),
    wall: () => 1_700_000_000_000,
  };
}

describe("computeDrift", () => {
  it("returns null when the fire is within threshold", () => {
    assert.equal(computeDrift(1050, 1000, 100), null);
    // Exactly at threshold is not a stall (strictly greater).
    assert.equal(computeDrift(1000 + DRIFT_THRESHOLD_MS, 1000), null);
  });

  it("returns the overage when the timer fires late past threshold", () => {
    assert.equal(computeDrift(1200, 1000, 50), 200);
  });

  it("treats early fires as no stall", () => {
    assert.equal(computeDrift(980, 1000, 50), null);
  });
});

describe("ProfileRecorder ring buffer", () => {
  it("bounds the buffer at RING_CAPACITY, dropping oldest", async () => {
    const rec = new ProfileRecorder("session-1", async () => {}, fixedClock());
    for (let i = 0; i < RING_CAPACITY + 50; i++) {
      rec.record({ type: "stall", dur: i });
    }
    assert.equal(rec.size(), RING_CAPACITY);
  });

  it("flushes serialized JSONL lines and drains the buffer", async () => {
    const captured = [];
    const rec = new ProfileRecorder(
      "session-1",
      async (lines) => {
        captured.push(...lines);
      },
      fixedClock(),
    );
    rec.record({ type: "stall", dur: 42 });
    rec.record({
      type: "census",
      observerEvents: 5,
      observerAgents: 1,
      observerMaxPerAgent: 5,
      archiveEvents: 0,
      transcripts: 1,
      queryCache: 3,
      domNodes: 100,
    });
    await rec.flush();

    assert.equal(rec.size(), 0);
    assert.equal(captured.length, 2);
    const first = JSON.parse(captured[0]);
    assert.equal(first.type, "stall");
    assert.equal(first.dur, 42);
    assert.equal(first.sid, "session-1");
    assert.equal(typeof first.t, "number");
    assert.equal(typeof first.wall, "number");
    assert.equal(typeof first.up, "number");
  });

  it("stamps the drop count onto the first line after overflow", async () => {
    const captured = [];
    const rec = new ProfileRecorder(
      "session-1",
      async (lines) => {
        captured.push(...lines);
      },
      fixedClock(),
    );
    for (let i = 0; i < RING_CAPACITY + 10; i++) {
      rec.record({ type: "stall", dur: i });
    }
    await rec.flush();
    const first = JSON.parse(captured[0]);
    assert.equal(first.dropped, 10);
  });

  it("does not throw when the sink rejects, and still drains", async () => {
    const rec = new ProfileRecorder(
      "session-1",
      async () => {
        throw new Error("sink down");
      },
      fixedClock(),
    );
    rec.record({ type: "stall", dur: 1 });
    await rec.flush();
    assert.equal(rec.size(), 0);
  });
});
