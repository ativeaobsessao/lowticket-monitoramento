import test from "node:test";
import assert from "node:assert/strict";
import {
  getBusinessSlot,
  processWithRetries,
  withTimeout,
} from "../scrape-orchestration.js";

test("finishes the first pass before retrying failed pages", async () => {
  const events = [];
  const results = await processWithRetries(
    ["slow", "ok", "failed"],
    async (slug, pass) => {
      events.push(`${pass}:${slug}`);
      if (pass === 1 && slug !== "ok") return { ok: false };
      return { ok: true };
    },
  );

  assert.deepEqual(events, [
    "1:slow",
    "1:ok",
    "1:failed",
    "2:slow",
    "2:failed",
  ]);
  assert.deepEqual(results.map(({ ok }) => ok), [true, true, true]);
});

test("does not retry successful pages and returns the last failure", async () => {
  const attempts = [];
  const results = await processWithRetries(
    ["success", "timeout"],
    async (slug, pass) => {
      attempts.push(`${pass}:${slug}`);
      return slug === "success" ? { ok: true } : { ok: false, pass };
    },
  );

  assert.deepEqual(attempts, ["1:success", "1:timeout", "2:timeout"]);
  assert.deepEqual(results, [
    { ok: true },
    { ok: false, pass: 2 },
  ]);
});

test("does not retry failures classified as non-retryable", async () => {
  const attempts = [];
  const results = await processWithRetries(
    ["invalid-url"],
    async (slug, pass) => {
      attempts.push(`${pass}:${slug}`);
      return { ok: false, retryable: false };
    },
    { shouldRetry: (result) => result.retryable },
  );

  assert.deepEqual(attempts, ["1:invalid-url"]);
  assert.deepEqual(results, [{ ok: false, retryable: false }]);
});

test("finishes every first-pass page when the batch deadline leaves no retry window", async () => {
  const attempts = [];
  const results = await processWithRetries(
    ["one", "two", "three"],
    async (slug, pass) => {
      attempts.push(`${pass}:${slug}`);
      return { ok: false, pass };
    },
    {
      shouldStartRetryPass: ({ pending }) => pending.length === 0,
    },
  );

  assert.deepEqual(attempts, ["1:one", "1:two", "1:three"]);
  assert.deepEqual(results.map(({ pass }) => pass), [1, 1, 1]);
});

test("times out a page operation and runs cleanup", async () => {
  let cleaned = false;
  await assert.rejects(
    withTimeout(
      () => new Promise(() => {}),
      10,
      async () => {
        cleaned = true;
      },
    ),
    { name: "TimeoutError" },
  );
  assert.equal(cleaned, true);
});

test("returns completed operations and propagates their errors", async () => {
  assert.equal(await withTimeout(async () => 42, 100), 42);
  await assert.rejects(withTimeout(async () => {
    throw new Error("operation failed");
  }, 100), /operation failed/);
});

test("reports cleanup errors together with the timeout", async () => {
  await assert.rejects(
    withTimeout(
      () => new Promise(() => {}),
      10,
      async () => {
        throw new Error("cleanup failed");
      },
    ),
    (error) => error instanceof AggregateError
      && error.errors.some((nested) => nested.name === "TimeoutError")
      && error.errors.some((nested) => nested.message === "cleanup failed"),
  );
});

test("rejects an invalid retry-pass limit", async () => {
  await assert.rejects(
    processWithRetries([], async () => ({ ok: true }), { maxPasses: 0 }),
    { name: "RangeError" },
  );
});

test("assigns slot 22 after midnight to the previous BRT business date", () => {
  const slot = getBusinessSlot(new Date("2026-10-03T03:30:00.000Z"));
  assert.deepEqual(slot, { slot: 22, businessDate: "2026-10-02" });
});

test("maps the three scheduled windows in BRT", () => {
  assert.deepEqual(getBusinessSlot(new Date("2026-10-03T06:00:00.000Z")), {
    slot: 3,
    businessDate: "2026-10-03",
  });
  assert.deepEqual(getBusinessSlot(new Date("2026-10-03T15:00:00.000Z")), {
    slot: 12,
    businessDate: "2026-10-03",
  });
  assert.deepEqual(getBusinessSlot(new Date("2026-10-04T01:00:00.000Z")), {
    slot: 22,
    businessDate: "2026-10-03",
  });
});
