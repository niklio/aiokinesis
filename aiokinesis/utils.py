import asyncio
from heapq import heappush
from time import time


def rate_limit_per_rolling_second(requests_per_rolling_second):
    def outer_wrapper(f):
        async def inner_wrapper(self, *args, **kwargs):
            assert isinstance(self, object), """
                rate_limit_per_rolling_second must decorate a method
            """

            # Instantiate list of requests if it doesn't exist
            if not hasattr(self, '_request_times'):
                self._request_times = []

            # Remove stale requests from list
            current_time = float(time())
            self._request_times = [
                t
                for t in self._request_times
                if current_time - t < 1
            ]

            if len(self._request_times) >= requests_per_rolling_second:
                oldest_request_time = self._request_times[0]
                await asyncio.sleep(1 - current_time + oldest_request_time)

            heappush(self._request_times, float(time()))

            return await f(self, *args, **kwargs)
        return inner_wrapper
    return outer_wrapper
