import asyncio
import unittest

from gpt import PoolWriteRateLimiter


class PoolWriteRateLimiterTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_configuration(self):
        async def write(value):
            return value

        for tps in (0, -1, True, 1.5):
            with self.subTest(tps=tps), self.assertRaises(ValueError):
                PoolWriteRateLimiter(tps=tps, max_in_flight=1, write=write)
        for capacity in (0, -1, True, 2.5):
            with self.subTest(capacity=capacity), self.assertRaises(ValueError):
                PoolWriteRateLimiter(tps=1, max_in_flight=capacity, write=write)

    async def test_first_write_is_immediate_and_returns_result(self):
        started = asyncio.Event()

        async def write(value):
            started.set()
            return value * 2

        limiter = PoolWriteRateLimiter(tps=100, max_in_flight=1, write=write)
        try:
            self.assertEqual(await asyncio.wait_for(limiter.submit(21), 0.1), 42)
            self.assertTrue(started.is_set())
            self.assertEqual(limiter.in_flight, 0)
        finally:
            await limiter.aclose()

    async def test_ordinary_admissions_are_spaced_and_fifo(self):
        started = []
        gate = asyncio.Event()

        async def write(value):
            started.append((value, asyncio.get_running_loop().time()))
            await gate.wait()
            return value

        limiter = PoolWriteRateLimiter(tps=50, max_in_flight=5, write=write)
        jobs = [asyncio.create_task(limiter.submit(i)) for i in range(3)]
        try:
            deadline = asyncio.get_running_loop().time() + 0.2
            while len(started) < 3 and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0.002)
            self.assertEqual([item[0] for item in started], [0, 1, 2])
            self.assertGreaterEqual(started[1][1] - started[0][1], 0.015)
            self.assertGreaterEqual(started[2][1] - started[1][1], 0.015)
            gate.set()
            self.assertEqual(await asyncio.gather(*jobs), [0, 1, 2])
        finally:
            gate.set()
            await limiter.aclose()

    async def test_success_does_not_accelerate_next_admission(self):
        started = []
        first_done = asyncio.Event()
        release_first = asyncio.Event()

        async def write(value):
            started.append((value, asyncio.get_running_loop().time()))
            if value == 1:
                await release_first.wait()
                first_done.set()
            return value

        limiter = PoolWriteRateLimiter(tps=20, max_in_flight=2, write=write)
        first = asyncio.create_task(limiter.submit(1))
        second = asyncio.create_task(limiter.submit(2))
        try:
            while not started:
                await asyncio.sleep(0)
            release_first.set()
            await first_done.wait()
            # Completion is well before the next 50ms regular slot. The second write
            # must not start just because the first one finished.
            await asyncio.sleep(0.015)
            self.assertEqual([x[0] for x in started], [1])
            await asyncio.wait_for(second, 0.2)
            self.assertEqual([x[0] for x in started], [1, 2])
            self.assertGreaterEqual(started[1][1] - started[0][1], 0.035)
            await first
        finally:
            release_first.set()
            await limiter.aclose()

    async def test_failure_admits_one_waiter_immediately_and_keeps_future_slot(self):
        started = []
        fail_first = asyncio.Event()
        release_second = asyncio.Event()

        async def write(value):
            started.append((value, asyncio.get_running_loop().time()))
            if value == "A":
                await fail_first.wait()
                raise RuntimeError("synthetic write failure")
            if value == "B":
                await release_second.wait()
            return value

        limiter = PoolWriteRateLimiter(tps=20, max_in_flight=1, write=write)
        a = asyncio.create_task(limiter.submit("A"))
        b = asyncio.create_task(limiter.submit("B"))
        c = asyncio.create_task(limiter.submit("C"))
        try:
            while not started:
                await asyncio.sleep(0)
            fail_first.set()
            with self.assertRaisesRegex(RuntimeError, "synthetic"):
                await a
            # B gets the exceptional immediate admission; C still waits for the
            # original 50ms slot measured from A's first admission.
            while len(started) < 2:
                await asyncio.sleep(0)
            self.assertEqual([x[0] for x in started], ["A", "B"])
            # Let B finish before the regular slot. Its success still must not cause C
            # to start early; C should start at the original slot.
            release_second.set()
            self.assertEqual(await asyncio.wait_for(b, 0.1), "B")
            await asyncio.wait_for(c, 0.2)
            self.assertEqual([x[0] for x in started], ["A", "B", "C"])
            self.assertGreaterEqual(started[2][1] - started[0][1], 0.035)
        finally:
            release_second.set()
            for task in (a, b, c):
                if not task.done():
                    task.cancel()
            await asyncio.gather(a, b, c, return_exceptions=True)
            await limiter.aclose()

    async def test_failure_with_empty_queue_does_not_bank_extra_admission(self):
        started = []

        async def write(value):
            started.append((value, asyncio.get_running_loop().time()))
            if value == "bad":
                raise RuntimeError("failed")
            return value

        limiter = PoolWriteRateLimiter(tps=20, max_in_flight=1, write=write)
        try:
            with self.assertRaises(RuntimeError):
                await limiter.submit("bad")
            failed_at = started[0][1]
            # There was no waiter at the moment of failure, so no exceptional admission
            # is banked. This immediate follow-up still waits for the regular 50ms slot.
            self.assertEqual(await limiter.submit("good"), "good")
            self.assertEqual([x[0] for x in started], ["bad", "good"])
            self.assertGreaterEqual(started[1][1] - failed_at, 0.035)
        finally:
            await limiter.aclose()

    async def test_sequential_fast_calls_do_not_bypass_tps(self):
        started = []

        async def write(value):
            started.append((value, asyncio.get_running_loop().time()))
            return value

        limiter = PoolWriteRateLimiter(tps=20, max_in_flight=1, write=write)
        try:
            await limiter.submit("first")
            await limiter.submit("second")
            self.assertEqual([x[0] for x in started], ["first", "second"])
            self.assertGreaterEqual(started[1][1] - started[0][1], 0.035)
        finally:
            await limiter.aclose()

    async def test_max_in_flight_is_respected(self):
        active = 0
        max_seen = 0
        gate = asyncio.Event()

        async def write(value):
            nonlocal active, max_seen
            active += 1
            max_seen = max(max_seen, active)
            try:
                await gate.wait()
                return value
            finally:
                active -= 1

        limiter = PoolWriteRateLimiter(tps=100, max_in_flight=2, write=write)
        jobs = [asyncio.create_task(limiter.submit(i)) for i in range(5)]
        try:
            deadline = asyncio.get_running_loop().time() + 0.15
            while max_seen < 2 and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0.002)
            self.assertEqual(limiter.in_flight, 2)
            self.assertLessEqual(limiter.in_flight, 2)
            self.assertEqual(max_seen, 2)
        finally:
            gate.set()
            await asyncio.gather(*jobs)
            await limiter.aclose()

    async def test_close_drains_accepted_jobs(self):
        started = []

        async def write(value):
            started.append(value)
            return value

        limiter = PoolWriteRateLimiter(tps=100, max_in_flight=2, write=write)
        first = asyncio.create_task(limiter.submit(1))
        second = asyncio.create_task(limiter.submit(2))
        self.assertEqual(await first, 1)
        await limiter.aclose()
        self.assertEqual(await second, 2)
        self.assertEqual(started, [1, 2])
        with self.assertRaises(RuntimeError):
            await limiter.submit(3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
