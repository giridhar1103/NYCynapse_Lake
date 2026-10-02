import random
import time
from collections.abc import Callable
from dataclasses import dataclass

# Servers sometimes ask for long pauses. Honour them, but not past this.
MAX_RETRY_AFTER = 300.0


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 5
    base: float = 1.0
    cap: float = 60.0


class Retryable(Exception):
    """A failure worth another attempt. retry_after is the server's hint, when it sends one."""

    def __init__(self, message: str, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


def backoff(policy: RetryPolicy, attempt: int, rng: Callable[[], float] = random.random) -> float:
    # Full jitter: uniform between 0 and the capped exponential step.
    return rng() * min(policy.cap, policy.base * 2**attempt)


def retry[T](
    fn: Callable[[], T],
    policy: RetryPolicy,
    *,
    sleep: Callable[[float], None] = time.sleep,
    on_retry: Callable[[int, Exception, float], None] | None = None,
) -> T:
    for attempt in range(policy.attempts):
        try:
            return fn()
        except Retryable as exc:
            if attempt == policy.attempts - 1:
                raise
            delay = backoff(policy, attempt)
            if exc.retry_after is not None:
                delay = min(max(delay, exc.retry_after), MAX_RETRY_AFTER)
            if on_retry:
                on_retry(attempt + 1, exc, delay)
            sleep(delay)
    raise AssertionError("unreachable")
