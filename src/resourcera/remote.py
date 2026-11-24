"""Application-reported, process-local remote service concurrency control."""

from __future__ import annotations

import math
import threading
import time
from collections import deque
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from types import TracebackType
from typing import Literal

import anyio
import anyio.lowlevel
from pydantic import Field

from resourcera.exceptions import ManagerStateError, ResourceError
from resourcera.models import FrozenModel


def _in_event_loop() -> bool:
    try:
        anyio.lowlevel.current_token()
    except Exception:
        return False
    return True


class _ThreadOwner:
    """Bind a controller's mutable state to the thread running its event loop.

    The first call made from inside an event loop (or construction inside one) binds the
    owner. Calls from any other thread are rejected instead of mutating AnyIO primitives
    off-loop; worker threads should use ``anyio.from_thread.run_sync``.
    """

    __slots__ = ("_thread",)

    def __init__(self) -> None:
        self._thread = threading.get_ident() if _in_event_loop() else None

    def check(self) -> None:
        thread = threading.get_ident()
        if self._thread == thread:
            return
        if self._thread is None and _in_event_loop():
            self._thread = thread
            return
        raise RuntimeError(
            "controllers must be used from the thread running their event loop; "
            "from a worker thread, call anyio.from_thread.run_sync(...) instead"
        )


class RemoteConcurrencyRejectedError(ResourceError):
    """A remote controller's bounded permit queue is full or timed out."""


class RemoteAdjustment(FrozenModel):
    """A bounded-history entry for one remote limit change."""

    at: datetime
    previous_limit: int = Field(ge=1)
    new_limit: int = Field(ge=1)
    reason: str


class RemoteConcurrencyState(FrozenModel):
    """A point-in-time view of one process-local remote controller."""

    key: str
    current_limit: int = Field(ge=1)
    min_concurrency: int = Field(ge=1)
    initial_concurrency: int = Field(ge=1)
    max_concurrency: int = Field(ge=1)
    authoritative_capacity: int | None = Field(default=None, ge=1)
    in_flight: int = Field(ge=0)
    pending_count: int = Field(ge=0)
    queue_capacity: int = Field(ge=1)
    success_count: int = Field(ge=0)
    overload_count: int = Field(ge=0)
    timeout_count: int = Field(ge=0)
    failure_count: int = Field(ge=0)
    latency_seconds: float | None = Field(default=None, ge=0)
    remote_queue_wait_seconds: float | None = Field(default=None, ge=0)
    cooldown_remaining_seconds: float = Field(ge=0)
    adjustment_reason: str
    history: tuple[RemoteAdjustment, ...]
    closed: bool


def _retry_after_seconds(value: str | None) -> float:
    if value is None:
        return 0.0
    if value.isascii() and value.isdigit():
        delay = float(value)
    else:
        try:
            date = parsedate_to_datetime(value)
        except (TypeError, ValueError, IndexError, OverflowError) as exc:
            raise ValueError("Retry-After must be an HTTP date or integer seconds") from exc
        if date.tzinfo is None:
            raise ValueError("Retry-After HTTP dates must include a timezone")
        delay = max(0.0, (date - datetime.now(UTC)).total_seconds())
    if not math.isfinite(delay) or delay > 86_400:
        raise ValueError("Retry-After must not exceed 86400 seconds")
    return delay


class RemoteConcurrencyController:
    """Limit requests to one endpoint using only application-supplied feedback.

    A controller belongs to the event loop and thread that first use it, and to its
    process. Feedback and acquisition from another thread raise ``RuntimeError``; use
    ``anyio.from_thread.run_sync`` to report from worker threads. Separate controller
    instances do not share state, even when their keys are equal.
    """

    def __init__(
        self,
        key: str,
        *,
        min_concurrency: int = 1,
        initial_concurrency: int = 1,
        max_concurrency: int = 32,
        max_waiters: int = 128,
        wait_timeout_seconds: float | None = None,
        observation_window_seconds: float = 1.0,
        cooldown_seconds: float = 1.0,
        minimum_samples: int = 4,
        history_size: int = 32,
    ) -> None:
        if not key or not key.strip():
            raise ValueError("key must not be empty")
        values = (min_concurrency, initial_concurrency, max_concurrency, max_waiters)
        if any(isinstance(value, bool) or not isinstance(value, int) for value in values):
            raise TypeError("concurrency bounds and max_waiters must be integers")
        if not 1 <= min_concurrency <= initial_concurrency <= max_concurrency:
            raise ValueError(
                "require 1 <= min_concurrency <= initial_concurrency <= max_concurrency"
            )
        if max_waiters < 1:
            raise ValueError("max_waiters must be positive")
        if wait_timeout_seconds is not None and not 0 < wait_timeout_seconds < float("inf"):
            raise ValueError("wait_timeout_seconds must be finite and positive")
        if not 0 < observation_window_seconds < float("inf") or not 0 < cooldown_seconds < float(
            "inf"
        ):
            raise ValueError("observation and cooldown durations must be finite and positive")
        if isinstance(minimum_samples, bool) or not isinstance(minimum_samples, int):
            raise TypeError("minimum_samples must be an integer")
        if isinstance(history_size, bool) or not isinstance(history_size, int):
            raise TypeError("history_size must be an integer")
        if minimum_samples < 2 or not 1 <= history_size <= 1024:
            raise ValueError("minimum_samples must be at least 2 and history_size must be 1-1024")
        self.key = key
        self._minimum = min_concurrency
        self._initial = initial_concurrency
        self._maximum = max_concurrency
        self._max_waiters = max_waiters
        self._timeout = wait_timeout_seconds
        self._window = observation_window_seconds
        self._cooldown = cooldown_seconds
        self._minimum_samples = minimum_samples
        self._limiter = anyio.CapacityLimiter(initial_concurrency)
        self._waiting: set[anyio.CancelScope] = set()
        self._history: deque[RemoteAdjustment] = deque(maxlen=history_size)
        self._clock = time.monotonic
        self._window_started = self._clock()
        self._cooldown_until = 0.0
        self._successes = self._failures = 0
        self._window_successes = self._window_failures = 0
        self._window_queue_samples = 0
        self._overloads = self._timeouts = 0
        self._in_flight = 0
        self._latency: float | None = None
        self._baseline_latency: float | None = None
        self._queue_wait: float | None = None
        self._baseline_queue_wait: float | None = None
        self._authoritative_capacity: int | None = None
        self._reason = "Initial limit; awaiting remote feedback."
        self._closed = False
        self._entered = False
        self._owner = _ThreadOwner()

    @property
    def state(self) -> RemoteConcurrencyState:
        """Return a cached operational snapshot without contacting the endpoint."""
        return RemoteConcurrencyState(
            key=self.key,
            current_limit=int(self._limiter.total_tokens),
            min_concurrency=self._minimum,
            initial_concurrency=self._initial,
            max_concurrency=self._maximum,
            authoritative_capacity=self._authoritative_capacity,
            in_flight=self._in_flight,
            pending_count=len(self._waiting),
            queue_capacity=self._max_waiters,
            success_count=self._successes,
            overload_count=self._overloads,
            timeout_count=self._timeouts,
            failure_count=self._failures,
            latency_seconds=self._latency,
            remote_queue_wait_seconds=self._queue_wait,
            cooldown_remaining_seconds=max(0.0, self._cooldown_until - self._clock()),
            adjustment_reason=self._reason,
            history=tuple(self._history),
            closed=self._closed,
        )

    def _set_limit(self, limit: int, reason: str) -> None:
        previous = int(self._limiter.total_tokens)
        self._reason = reason
        if limit != previous:
            self._limiter.total_tokens = limit
            self._history.append(
                RemoteAdjustment(
                    at=datetime.now(UTC),
                    previous_limit=previous,
                    new_limit=limit,
                    reason=reason,
                )
            )

    def report_success(
        self, *, latency_seconds: float, remote_queue_wait_seconds: float | None = None
    ) -> None:
        """Record one successful request and cautiously increase on sustained feedback."""
        self._owner.check()
        if self._closed:
            raise ManagerStateError("the remote controller is closed")
        if not 0 <= latency_seconds < float("inf"):
            raise ValueError("latency_seconds must be finite and non-negative")
        if remote_queue_wait_seconds is not None and not (
            0 <= remote_queue_wait_seconds < float("inf")
        ):
            raise ValueError("remote_queue_wait_seconds must be finite and non-negative")
        self._successes += 1
        self._window_successes += 1
        self._latency = (
            latency_seconds
            if self._latency is None
            else 0.8 * self._latency + 0.2 * latency_seconds
        )
        if self._baseline_latency is None:
            self._baseline_latency = latency_seconds
        if remote_queue_wait_seconds is not None:
            self._window_queue_samples += 1
            self._queue_wait = (
                remote_queue_wait_seconds
                if self._queue_wait is None
                else 0.8 * self._queue_wait + 0.2 * remote_queue_wait_seconds
            )
            if self._baseline_queue_wait is None:
                self._baseline_queue_wait = remote_queue_wait_seconds
        now = self._clock()
        samples = self._window_successes + self._window_failures
        if now < self._cooldown_until or now - self._window_started < self._window:
            return
        if samples < self._minimum_samples:
            return
        healthy_latency = self._latency <= max(self._baseline_latency * 1.5, 0.001)
        healthy_queue = self._window_queue_samples == 0 or (
            self._queue_wait is not None
            and self._baseline_queue_wait is not None
            and self._queue_wait <= max(self._baseline_queue_wait * 1.5, 0.001)
        )
        if self._window_successes / samples >= 0.9 and healthy_latency and healthy_queue:
            self._set_limit(
                min(
                    self._maximum,
                    self._authoritative_capacity or self._maximum,
                    int(self._limiter.total_tokens) + 1,
                ),
                "Increased additively after a healthy feedback window.",
            )
            self._baseline_latency = 0.9 * self._baseline_latency + 0.1 * self._latency
            if self._window_queue_samples and self._queue_wait is not None:
                assert self._baseline_queue_wait is not None
                self._baseline_queue_wait = 0.9 * self._baseline_queue_wait + 0.1 * self._queue_wait
        else:
            self._reason = "Holding: recent failures, elevated latency or remote queue wait."
        self._window_started = now
        self._window_successes = self._window_failures = 0
        self._window_queue_samples = 0

    def report_overload(
        self,
        *,
        status_code: int | None = None,
        reason: Literal[
            "overload", "timeout", "connection_failure", "network_failure"
        ] = "overload",
        retry_after: str | None = None,
    ) -> None:
        """Report overload; repeated feedback during one cooldown is aggregated."""
        self._owner.check()
        if self._closed:
            raise ManagerStateError("the remote controller is closed")
        if status_code is not None and status_code not in (429, 503):
            raise ValueError("only HTTP 429 and 503 are overload status codes")
        if reason not in ("overload", "timeout", "connection_failure", "network_failure"):
            raise ValueError("invalid overload reason")
        delay = _retry_after_seconds(retry_after)
        now = self._clock()
        self._overloads += 1
        if reason == "timeout":
            self._timeouts += 1
        self._window_failures += 1
        if now >= self._cooldown_until:
            self._set_limit(
                max(self._minimum, int(self._limiter.total_tokens) // 2),
                f"Decreased multiplicatively after {status_code or reason}.",
            )
        self._cooldown_until = max(self._cooldown_until, now + max(self._cooldown, delay))
        self._window_started = now
        self._window_successes = 0
        self._window_queue_samples = 0

    def report_failure(
        self, *, kind: Literal["authentication", "invalid_request", "application_error"]
    ) -> None:
        """Record a non-congestion failure without classifying it as overload."""
        self._owner.check()
        if self._closed:
            raise ManagerStateError("the remote controller is closed")
        if kind not in ("authentication", "invalid_request", "application_error"):
            raise ValueError("invalid non-overload failure kind")
        self._failures += 1
        self._window_failures += 1
        self._reason = f"Holding after {kind}; it is not classified as congestion."

    def report_capacity(self, capacity: int | None) -> None:
        """Set or clear an authoritative endpoint ceiling; increases remain gradual."""
        self._owner.check()
        if self._closed:
            raise ManagerStateError("the remote controller is closed")
        if capacity is not None and (isinstance(capacity, bool) or not isinstance(capacity, int)):
            raise TypeError("capacity must be an integer or None")
        if capacity is not None and capacity < self._minimum:
            raise ValueError("capacity cannot be less than min_concurrency")
        self._authoritative_capacity = capacity
        if capacity is not None and capacity < int(self._limiter.total_tokens):
            self._set_limit(capacity, "Decreased to the application-reported capacity ceiling.")
        else:
            self._reason = "Authoritative ceiling updated; increases remain gradual."

    def slot(self) -> RemoteSlot:
        """Create a single-use remote permit; no local resource lease is acquired."""
        return RemoteSlot(self)

    async def _acquire(self, token: object) -> None:
        self._owner.check()
        if self._closed:
            raise ManagerStateError("the remote controller is closed")
        if self._clock() >= self._cooldown_until:
            try:
                self._limiter.acquire_on_behalf_of_nowait(token)
            except anyio.WouldBlock:
                pass
            else:
                self._in_flight += 1
                return
        if len(self._waiting) >= self._max_waiters:
            raise RemoteConcurrencyRejectedError("The remote permit queue is full.")
        scope = anyio.CancelScope()
        self._waiting.add(scope)
        borrowed = False
        timeout = None
        try:
            with scope:
                with anyio.move_on_after(self._timeout) as timeout:
                    await self._limiter.acquire_on_behalf_of(token)
                    borrowed = True
                    while self._clock() < self._cooldown_until:  # noqa: ASYNC110
                        await anyio.sleep(self._cooldown_until - self._clock())
                    self._in_flight += 1
                    borrowed = False
            if scope.cancelled_caught:
                raise ManagerStateError("the remote controller closed while waiting")
            if timeout is not None and timeout.cancelled_caught:
                raise RemoteConcurrencyRejectedError("Waiting for a remote permit timed out.")
        finally:
            if borrowed:
                self._limiter.release_on_behalf_of(token)
            self._waiting.discard(scope)

    def _close(self) -> None:
        self._closed = True
        for scope in tuple(self._waiting):
            scope.cancel()

    async def __aenter__(self) -> RemoteConcurrencyController:
        if self._closed or self._entered:
            raise ManagerStateError("the remote controller context cannot be reused")
        self._entered = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._close()


class RemoteSlot:
    """A single remote permit that is always released on context exit."""

    def __init__(self, controller: RemoteConcurrencyController) -> None:
        self._controller = controller
        self._token: object | None = None
        self._exited = False

    async def __aenter__(self) -> None:
        if self._token is not None:
            raise ManagerStateError("a remote slot cannot be reused")
        token = object()
        await self._controller._acquire(token)
        self._token = token

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._token is None or self._exited:
            raise ManagerStateError("the remote slot was not entered")
        self._exited = True
        self._controller._in_flight -= 1
        self._controller._limiter.release_on_behalf_of(self._token)
