#!/usr/bin/env python3

from typing import Generator, Any
import time
import contextlib


@contextlib.contextmanager
def measure_time() -> Generator[float, Any, Any]:
    start = time.perf_counter()
    yield lambda: time.perf_counter() - start
