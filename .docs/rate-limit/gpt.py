from __future__ import annotations

import asyncio
import math
from collections import deque
from dataclasses import dataclass
from typing import Awaitable, Callable, Deque, Generic, TypeVar

TransactionT = TypeVar("TransactionT")
ResultT = TypeVar("ResultT")


@dataclass(slots=True)
class _Job(Generic[TransactionT, ResultT]):
    """A queued transaction and the Future completed when its write finishes."""

    transaction: TransactionT
    result: asyncio.Future[ResultT]


class PoolWriteRateLimiter(Generic[TransactionT, ResultT]):
    """Rate-limit asynchronous writes of transactions into a DAG POOL.

    Specification
    -------------
    * ``tps`` is a fixed positive integer. The regular slot interval is ``1 / tps``.
    * The first-ever transaction is admitted immediately. After a later idle period,
      the first transaction is also admitted immediately if at least one full interval
      has elapsed since the previous admission. An empty queue alone does not bypass
      the rate limit while that interval is still in progress.
      An immediate idle admission establishes the next regular slot at ``now + interval``.
    * Further ordinary admissions happen at most once per regular slot, in FIFO order.
      A slot with no queued transaction or no free in-flight capacity is skipped.
      Skipped slots are never accumulated and never cause a catch-up burst.
    * At most ``max_in_flight`` writes may be executing at once.
    * A successful write only releases in-flight capacity. It does not itself admit a
      waiting transaction early; queued work waits for a regular slot.
    * A failed write releases capacity and immediately admits at most one FIFO-head
      transaction, if one is waiting. This exceptional admission does not shift a
      future regular slot. If that slot is already due/past, expired slots are skipped
      and the schedule advances to the first regular slot strictly after the failure.
    * An exceptional admission does not create a reusable permission. If the queue is
      empty when a write fails, nothing is saved for future submissions.
    * Scheduling uses the event loop's monotonic clock, never wall-clock time.
    * ``write`` must raise an exception to signal failure; any normal return means the
      write succeeded. Exceptions are propagated to the corresponding ``submit`` call.
    * Cancelling the coroutine awaiting ``submit`` does not remove or cancel its job:
      accepted transactions are not silently dropped and continue through the pipeline.
    * ``aclose`` stops accepting new submissions and drains already accepted work.

    This class controls admission/start of writes; it does not retry failed transactions.
    """

    def __init__(
        self,
        *,
        tps: int,
        max_in_flight: int,
        write: Callable[[TransactionT], Awaitable[ResultT]],
    ) -> None:
        # Fail fast on invalid configuration. bool is rejected even though bool is an int
        # subclass in Python; True/False are not meaningful TPS/capacity values.
        if type(tps) is not int or tps <= 0:
            raise ValueError("tps must be a positive integer")
        if type(max_in_flight) is not int or max_in_flight <= 0:
            raise ValueError("max_in_flight must be a positive integer")

        self._interval = 1.0 / tps
        self._max_in_flight = max_in_flight
        self._write = write

        # Accepted jobs remain in this FIFO until admitted. A caller cancellation does
        # not remove a job from this deque.
        self._queue: Deque[_Job[TransactionT, ResultT]] = deque()

        # attempt_id -> job. This map is also the authoritative in-flight counter;
        # removing an attempt exactly once prevents duplicate completion from freeing
        # capacity twice.
        self._in_flight: dict[int, _Job[TransactionT, ResultT]] = {}
        self._next_attempt_id = 0
        self._tasks: set[asyncio.Task[None]] = set()

        # Absolute monotonic time for the next regular slot. Keeping absolute slots
        # avoids drifting the schedule by repeatedly sleeping for a relative interval.
        self._next_slot: float | None = None
        # Last actual admission time, also monotonic. It prevents sequential fast writes
        # from bypassing TPS merely because the queue happens to be empty between calls.
        self._last_admission: float | None = None

        self._lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._scheduler: asyncio.Task[None] | None = None
        self._closing = False

    @property
    def queued(self) -> int:
        """Number of accepted transactions still waiting for admission."""
        return len(self._queue)

    @property
    def in_flight(self) -> int:
        """Number of writes currently executing."""
        return len(self._in_flight)

    async def submit(self, transaction: TransactionT) -> ResultT:
        """Enqueue a transaction and return its write result.

        The FIFO order is the order in which calls acquire the internal lock and are
        registered by this limiter. The first-ever job starts immediately. After a
        later idle period, a job starts immediately only if at least one full interval
        has elapsed since the previous admission; otherwise it waits for a regular slot
        or the one-transaction failure exception described above.
        """
        loop = asyncio.get_running_loop()
        result: asyncio.Future[ResultT] = loop.create_future()

        # If the caller is cancelled while waiting, a later write exception must not
        # become an unobserved-Future warning. Calling exception() does not prevent a
        # non-cancelled awaiter from receiving that same exception.
        result.add_done_callback(self._consume_unobserved_exception)
        job = _Job(transaction=transaction, result=result)

        async with self._lock:
            if self._closing:
                raise RuntimeError("rate limiter is closing or already closed")

            now = loop.time()
            was_idle = not self._queue and not self._in_flight
            idle_admission_due = was_idle and (
                self._last_admission is None
                or now - self._last_admission >= self._interval
            )
            self._queue.append(job)
            self._ensure_scheduler_locked()

            if idle_admission_due:
                # First admission / genuine idle restart. If less than one interval has
                # elapsed since the previous admission, even an empty limiter must wait
                # for its regular slot so sequential callers cannot bypass TPS.
                self._next_slot = now + self._interval
                self._start_locked(self._queue.popleft())
            # On a non-idle submission, do not admit just because capacity is free.
            # The scheduler will wait for the next regular slot.
            self._wake.set()

        # Shield the job's Future: cancelling this submitter must not cancel the accepted
        # transaction or its eventual result Future.
        return await asyncio.shield(result)

    async def aclose(self) -> None:
        """Reject new submissions and wait until all accepted work has finished."""
        async with self._lock:
            self._closing = True
            scheduler = self._scheduler
            self._wake.set()

        if scheduler is not None:
            await scheduler

        # The scheduler exits only after every attempt has released its in-flight slot.
        # Await the remaining task objects as well, so aclose() is a full drain barrier.
        remaining_tasks = tuple(self._tasks)
        if remaining_tasks:
            await asyncio.gather(*remaining_tasks, return_exceptions=True)

    async def __aenter__(self) -> PoolWriteRateLimiter[TransactionT, ResultT]:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.aclose()

    def _ensure_scheduler_locked(self) -> None:
        # Called only while holding _lock and from a running event loop.
        if self._scheduler is None:
            self._scheduler = asyncio.create_task(
                self._scheduler_loop(), name="pool-write-rate-limiter"
            )

    def _start_locked(self, job: _Job[TransactionT, ResultT]) -> None:
        """Admit exactly one job. Caller must hold _lock and have free capacity."""
        if len(self._in_flight) >= self._max_in_flight:
            raise RuntimeError("internal invariant violated: no in-flight capacity")

        self._next_attempt_id += 1
        attempt_id = self._next_attempt_id
        self._last_admission = asyncio.get_running_loop().time()
        self._in_flight[attempt_id] = job

        task = asyncio.create_task(
            self._execute(attempt_id, job), name=f"pool-write-{attempt_id}"
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _execute(
        self, attempt_id: int, job: _Job[TransactionT, ResultT]
    ) -> None:
        try:
            value = await self._write(job.transaction)
        except asyncio.CancelledError as exc:
            # Cancellation is terminal too: free capacity exactly once before propagating.
            await self._finish_failure(attempt_id, job, exc)
            raise
        except Exception as exc:
            await self._finish_failure(attempt_id, job, exc)
        else:
            await self._finish_success(attempt_id, job, value)

    async def _finish_success(
        self,
        attempt_id: int,
        job: _Job[TransactionT, ResultT],
        value: ResultT,
    ) -> None:
        async with self._lock:
            # pop() is the one-time release. If the attempt was already finalized,
            # ignore it rather than decrementing capacity a second time.
            active_job = self._in_flight.pop(attempt_id, None)
            if active_job is None:
                return

            if not job.result.done():
                job.result.set_result(value)

            # Success frees a slot but MUST NOT directly start queued work. The regular
            # scheduler can admit later when its next slot arrives.
            self._wake.set()

    async def _finish_failure(
        self,
        attempt_id: int,
        job: _Job[TransactionT, ResultT],
        error: BaseException,
    ) -> None:
        loop = asyncio.get_running_loop()
        async with self._lock:
            active_job = self._in_flight.pop(attempt_id, None)
            if active_job is None:
                return

            if not job.result.done():
                job.result.set_exception(error)

            now = loop.time()
            # Failure admits one waiting job exceptionally. If the next regular slot
            # has already arrived/passed, skip all expired slots first; otherwise keep
            # the future slot unchanged. In neither case are missed slots banked.
            self._skip_expired_slots_locked(now)
            if self._queue and len(self._in_flight) < self._max_in_flight:
                self._start_locked(self._queue.popleft())
            # If the queue is empty, no exceptional permission is retained.
            self._wake.set()

    def _skip_expired_slots_locked(self, now: float) -> None:
        """Move an expired schedule to its first slot strictly after ``now``."""
        slot = self._next_slot
        if slot is None or slot > now:
            return

        # Skip the due slot and every other missed slot without admitting a burst.
        elapsed_intervals = math.floor((now - slot) / self._interval) + 1
        self._next_slot = slot + elapsed_intervals * self._interval

    async def _scheduler_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            # Clear before checking state. If another task changes state afterward, it
            # sets the Event and the wait below wakes immediately (no lost wake-up).
            self._wake.clear()
            async with self._lock:
                if self._closing and not self._queue and not self._in_flight:
                    return

                slot = self._next_slot
                timeout = None if slot is None else max(0.0, slot - loop.time())

            try:
                if timeout is None:
                    await self._wake.wait()
                else:
                    await asyncio.wait_for(self._wake.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                # A regular slot became due. Do the state transition below under lock.
                pass

            now = loop.time()
            async with self._lock:
                if self._closing and not self._queue and not self._in_flight:
                    return

                slot = self._next_slot
                if slot is None or now < slot:
                    continue

                # Consume one due slot and advance directly to the first future slot.
                # Even if the event loop was delayed across several intervals, only one
                # admission is allowed for this scheduler wake-up: no catch-up burst.
                self._skip_expired_slots_locked(now)

                if self._queue and len(self._in_flight) < self._max_in_flight:
                    self._start_locked(self._queue.popleft())
                # If capacity is full or the queue empty, this slot is simply missed.
                # Continue using the same absolute schedule; success alone doesn't grant
                # an early admission.

    @staticmethod
    def _consume_unobserved_exception(future: asyncio.Future[object]) -> None:
        if not future.cancelled():
            future.exception()
