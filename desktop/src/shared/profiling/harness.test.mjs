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

import {
  classifyInput,
  computeDrift,
  DRIFT_THRESHOLD_MS,
  nextStallSample,
} from "@/shared/profiling/drift.ts";
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

describe("nextStallSample", () => {
  it("records a real overrun in an eligible (foreground) interval", () => {
    // armed at 1000, interval 500 → expected 1500; observed 1800 → 300 overrun.
    const { dur, armedAt } = nextStallSample(1000, 1800, true, 500, 50);
    assert.equal(dur, 300);
    assert.equal(armedAt, 1800);
  });

  it("suppresses the record but re-arms the baseline when ineligible", () => {
    // A hidden/blurred interval defers the timer arbitrarily; the huge gap must
    // not be scored, but armedAt must advance so the next interval is clean.
    const { dur, armedAt } = nextStallSample(1000, 60_000, false, 500, 50);
    assert.equal(dur, null);
    assert.equal(armedAt, 60_000);
  });

  it("cannot manufacture a phantom stall on the interval after a return", () => {
    // Background interval [1000, 60000] re-armed to 60000 with no record.
    const background = nextStallSample(1000, 60_000, false, 500, 50);
    // First foreground interval fires on time from the re-armed baseline.
    const resumed = nextStallSample(background.armedAt, 60_500, true, 500, 50);
    assert.equal(resumed.dur, null);
  });
});

describe("classifyInput", () => {
  it("splits felt latency into paint delay and pre-dispatch queue", () => {
    // event@100, received@250 (queued 150), painted@300 (latency 50) → felt 200.
    const result = classifyInput(100, 250, 300);
    assert.deepEqual(result, { latency: 50, queued: 150 });
  });

  it("drops the queue figure when the clock basis is implausible", () => {
    // event.timeStamp on a different origin: received - timeStamp is negative.
    const result = classifyInput(9_999_999, 250, 400);
    assert.deepEqual(result, { latency: 150, queued: null });
  });

  it("returns null when total felt latency is under threshold", () => {
    // queued 10 + latency 20 = 30ms felt, below the 100ms floor.
    assert.equal(classifyInput(100, 110, 130), null);
  });

  it("uses only paint latency toward the threshold when queue is implausible", () => {
    // queued dropped (negative); latency 120 alone clears the floor.
    const result = classifyInput(9_999_999, 250, 370);
    assert.deepEqual(result, { latency: 120, queued: null });
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
