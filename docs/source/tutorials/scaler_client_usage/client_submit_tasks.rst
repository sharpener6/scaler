Client Submit Tasks
===================

Use :py:func:`~Client.submit()` for one function call (function + arguments).
Call ``result()`` to fetch the value lazily.

.. literalinclude:: ../../../../examples/simple_client.py
   :language: python


For many tasks with the same function, use :py:func:`~Client.map()` instead of repeated
:py:func:`~Client.submit()` calls. This avoids extra serialization overhead.
The following example is an anti-pattern.

.. testcode:: python

    import functools
    import random

    from scaler import Client, SchedulerClusterCombo

    def lookup(heavy_map: bytes, index: int):
        return index * 1


    def main():
        address = "tcp://127.0.0.1:2345"

        cluster = SchedulerClusterCombo(address=address, n_workers=3)

        # a heavy function that is expensive to serialize
        big_func = functools.partial(lookup, b"1" * 5_000_000_000)

        arguments = [random.randint(0, 100) for _ in range(100)]

        with Client(address=address) as client:
            # we incur serialization overhead for every call to client.submit -- use client.map instead
            futures = [client.submit(big_func, i) for i in arguments]
            print([fut.result() for fut in futures])

        cluster.shutdown()


    if __name__ == "__main__":
        main()


This is slow because ``big_func`` is serialized on every :py:func:`~Client.submit()` call.

If a function depends on large objects, send them once with :py:func:`~Client.send_object()`
and then call :py:func:`~Client.submit()`.
