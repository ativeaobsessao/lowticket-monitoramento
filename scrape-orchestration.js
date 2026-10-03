const BRT_OFFSET_MS = 3 * 60 * 60 * 1000;

export class TimeoutError extends Error {
  constructor(timeoutMs) {
    super(`Operation exceeded the ${timeoutMs}ms timeout.`);
    this.name = "TimeoutError";
    this.timeoutMs = timeoutMs;
  }
}

export async function withTimeout(operation, timeoutMs, onTimeout = async () => {}) {
  let timer;
  const timedOut = new Promise((resolve) => {
    timer = setTimeout(() => resolve({ timedOut: true }), timeoutMs);
  });
  const result = await Promise.race([
    Promise.resolve().then(operation).then(
      (value) => ({ value }),
      (error) => ({ error }),
    ),
    timedOut,
  ]);
  clearTimeout(timer);

  if (!result.timedOut) {
    if (Object.prototype.hasOwnProperty.call(result, "error")) throw result.error;
    return result.value;
  }

  const timeoutError = new TimeoutError(timeoutMs);
  try {
    await onTimeout(timeoutError);
  } catch (cleanupError) {
    throw new AggregateError([timeoutError, cleanupError], "Operation timed out and cleanup failed.");
  }
  throw timeoutError;
}

export async function processWithRetries(
  items,
  processItem,
  {
    maxPasses = 2,
    isSuccess = (result) => result?.ok === true,
    shouldRetry = (result) => !isSuccess(result),
    shouldStartRetryPass = () => true,
  } = {},
) {
  if (!Number.isInteger(maxPasses) || maxPasses < 1) {
    throw new RangeError("maxPasses must be a positive integer.");
  }

  let pending = [...items];
  const results = new Map();

  for (let pass = 1; pass <= maxPasses && pending.length > 0; pass += 1) {
    const failed = [];
    for (const item of pending) {
      const result = await processItem(item, pass);
      results.set(item, result);
      if (shouldRetry(result) && pass < maxPasses) failed.push(item);
    }
    pending = failed;
    if (
      pending.length > 0
      && pass < maxPasses
      && !shouldStartRetryPass({ pending: [...pending], nextPass: pass + 1 })
    ) {
      break;
    }
  }

  return items.map((item) => results.get(item));
}

export function getBusinessSlot(now = new Date()) {
  const brt = new Date(now.getTime() - BRT_OFFSET_MS);
  const hour = brt.getUTCHours();
  const slot = hour < 3 || hour >= 22 ? 22 : hour < 12 ? 3 : 12;
  const businessDate = new Date(brt);

  if (slot === 22 && hour < 3) {
    businessDate.setUTCDate(businessDate.getUTCDate() - 1);
  }

  return {
    slot,
    businessDate: businessDate.toISOString().slice(0, 10),
  };
}
