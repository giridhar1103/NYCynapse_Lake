import pytest

from nycynapse_lake.retry import Retryable, RetryPolicy, backoff, retry


def test_backoff_stays_under_cap():
    policy = RetryPolicy(attempts=10, base=1, cap=8)
    assert backoff(policy, 0, rng=lambda: 1.0) == 1
    assert backoff(policy, 2, rng=lambda: 1.0) == 4
    assert backoff(policy, 9, rng=lambda: 1.0) == 8
    assert backoff(policy, 9, rng=lambda: 0.0) == 0


def test_retries_until_success():
    calls, sleeps = [], []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise Retryable("try again")
        return "ok"

    assert retry(flaky, RetryPolicy(attempts=5), sleep=sleeps.append) == "ok"
    assert len(calls) == 3
    assert len(sleeps) == 2


def test_gives_up_after_attempts():
    def always():
        raise Retryable("down")

    with pytest.raises(Retryable):
        retry(always, RetryPolicy(attempts=3), sleep=lambda s: None)


def test_honours_server_retry_after():
    sleeps = []
    state = {"n": 0}

    def limited():
        state["n"] += 1
        if state["n"] == 1:
            raise Retryable("429", retry_after=30)
        return 1

    retry(limited, RetryPolicy(attempts=3, base=0.1, cap=10), sleep=sleeps.append)
    assert sleeps[0] >= 30


def test_other_errors_are_not_retried():
    calls = []

    def broken():
        calls.append(1)
        raise ValueError("bug")

    with pytest.raises(ValueError):
        retry(broken, RetryPolicy(attempts=5), sleep=lambda s: None)
    assert len(calls) == 1
