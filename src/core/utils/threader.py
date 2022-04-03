import inspect
import threading
import os
from typing import Any
import ctypes

threadList: dict = {}
maxThreads = os.getenv('THREAD_LIMIT', 4)


def _async_raise(tid, exc_type):
    """Raises an exception in the threads with id tid"""
    if not inspect.isclass(exc_type):
        raise TypeError("Only types can be raised (not instances)")
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid), ctypes.py_object(exc_type))
    if res == 0:
        raise ValueError("invalid thread id")
    elif res != 1:
        """
        if it returns a number greater than one, you're in trouble,
        and you should call it again with exc=NULL to revert the effect
            >> ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid), 0)
        """
        ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid), None)
        raise SystemError("PyThreadState_SetAsyncExc failed")


class ThreadWithControl(threading.Thread):
    """
    We want to create threading class that can control maximum background
    agent and result after complete
    - Get return output from threading function
    - A thread class that supports raising an exception in the thread from
    another thread.
    usage:
        >> _thread = ThreadWithControl(target=lambda a: a * 2, args=(2, ))
        >> _thread.daemon = True
        >> _thread.start()
        >> print(_thread.join())
        4
    """
    threadLimiter = threading.BoundedSemaphore(maxThreads)

    def __init__(self, *args, **kwargs):
        self._return = None
        self._target = None
        self._args = None
        self._kwargs = None
        super().__init__(*args, **kwargs)
        self._stop_event = threading.Event()
        self.check_count = 0

    def run(self):
        self.threadLimiter.acquire()
        try:
            if self._target:
                self._return = self._target(*self._args, **self._kwargs)
        finally:
            del self._target, self._args, self._kwargs
            self.threadLimiter.release()

    def join(self, *args) -> Any:
        super().join(*args)
        return self._return

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def _get_my_tid(self):
        """
        determines this (self's) thread id

        CAREFUL: this function is executed in the context of the caller
        thread, to get the identity of the thread represented by this
        instance.
        """
        if not self.is_alive():
            raise threading.ThreadError("the thread is not active")

        # do we have it cached?
        if hasattr(self, "_thread_id"):
            return self._thread_id

        # no, look for it in the _active dict
        for thread_id, thread_obj in threading._active.items():
            if thread_obj is self:
                self._thread_id = thread_id
                return thread_id

        raise AssertionError("could not determine the thread's id")

    def raise_exc(self, exc_type):
        """
        Raises the given exception type in the context of this thread.

        If the thread is busy in a system call (time.sleep(),
        socket.accept(), ...), the exception is simply ignored.

        If you are sure that your exception should terminate the thread,
        one way to ensure that it works is:

            t = ThreadWithExc(...)
            ...
            t.raise_exc(SomeException)
            while t.isAlive():
                time.sleep(0.1)
                t.raise_exc(SomeException)

        If the exception is to be caught by the thread, you need a way to
        check that your thread has caught it.

        CAREFUL: this function is executed in the context of the
        caller thread, to raise an exception in the context of the
        thread represented by this instance.
        """
        _async_raise(self._get_my_tid(), exc_type)

    def terminate(self):
        """
        must raise the SystemExit type, instead of a SystemExit() instance
        due to a bug in PyThreadState_SetAsyncExc
        """
        self.raise_exc(SystemExit)
