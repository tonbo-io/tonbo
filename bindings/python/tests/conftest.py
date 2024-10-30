import asyncio
import random

import pytest_asyncio


def gen_string(max_size):
    size = gen_int(0, max_size)
    charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return ''.join(random.choices(charset, k=size))


def gen_bytes(max_size):
    return gen_string(max_size).encode("utf-8")


def gen_int(lower, high):
    return random.randint(lower, high)


# async support for pytest-benchmark
# https://github.com/ionelmc/pytest-benchmark/issues/66#issuecomment-1137005280
@pytest_asyncio.fixture
def aio_benchmark(benchmark, event_loop):
    def _wrapper(func, *args, **kwargs):
        if asyncio.iscoroutinefunction(func):

            @benchmark
            def _():
                return event_loop.run_until_complete(func(*args, **kwargs))
        else:
            benchmark(func, *args, **kwargs)

    return _wrapper
