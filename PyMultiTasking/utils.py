#!/usr/bin/env python3
# -*- coding=utf-8 -*-
"""
# PyMultiTaskingTools Utilities

This Python Package makes it easier to handle threads. This uses a schema of Task that is run by a Worker which is
managed in a Pool. The Pool uses a PriorityTaskQueue a custom class that inherits from PriorityQueue.

## Summary of Functions
get_cpu_count() -> int: <br />
    Return the number of Logic CPU Cores on the system

_async_raise(tid, exctype) -> None: <br />
    Raises the exception, causing the thread to exit

[wait_lock](#wait_lock)(lock, timeout) -> Iterator[bool]: <br />
    Meant to be used within a 'with' statement to throw an exception when timing out on a lock

[method_wait](#method_wait)(func, timeout=60, delay=0.1, incompleteVar=None, raiseExc=False, *args, **kwargs): <br />
    Allows one to safely wait on a method to return.

## Summary of Classes
[Task](#Task)(Event) <br />
    This is a wrapper class that inherits from an Event Object and is used inside the Worker. It is designed to hold
    the function ran and save the results of the function.

[PriorityTaskQueue](#PriorityTaskQueue)(PriorityQueue) <br />
    This is a simple override of the PriorityQueue class that ensures the 'item' is a Task class. It is meant to be used
    with ThreadingPool.

[ProcessTaskQueue](#ProcessTaskQueue)(JoinableQueue) <br />
    This is a simple override of the JoinableQueue class that ensures the 'item' is a Task class it is meant to be used
    with ProcessPool.

[Worker](#Worker) <br />
    This is designed to be managed by a Pool. It runs until told to stop and works tasks that come from a Queue
    maintained by the Pool. The Worker is meant to be a super class and only used to be inherited. It is the super
    class of either ThreadWorker or ProcessWorker.

[Pool](#Pool) <br />
    This manages a pool of Workers and a queue of Tasks. The workers consume tasks from the task_queue until they are
    told to stop. This is meant to be a super class and only used to be inherited. It is the super class of either
    ThreadPool or ProcessPool.

[MultipleEvents](#MultipleEvents) <br />
    Designed to take multiple events and put them together to be waited on as a whole.

---
"""

from __future__ import annotations

import multiprocessing
import time
import logging
import traceback
import inspect
import ctypes
from multiprocessing.synchronize import SemLock
from contextlib import contextmanager
from threading import Lock, RLock, Event
from multiprocessing.synchronize import Semaphore as SemephoreProcessing
from functools import wraps
from typing import Union, Optional, Iterator, Callable, Any, Type, Iterable


_log = logging.getLogger('PyMultiTasking.MultiTaskingTools')


def dummy_func(*args, **kwargs):
    return kwargs.get('_default', None)


def __async_raise(tid: int, exctype: Type[SystemExit]) -> None:
    """Raises the exception, causing the thread to exit"""

    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid), ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("Invalid thread ID")
    elif res != 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid), None)
        raise SystemError("PyThreadState_SetAsyncExc failed")


@contextmanager
def wait_lock(lock: Any, timeout: Union[int, float], blocking: bool = True, raise_exc: bool = True) -> Iterator[bool]:
    """ <a name="method_wait"></a>
        Meant to be used within a 'with' statement to throw an exception when timing out on a lock

    - :param lock: (Lock/RLock/Semephore) the lock that will be acquired or an exception will be thrown
    - :param timeout: The amount of time to wait on attempting to gain lock.
    - :return: (generator of bools) This is used with a contextmanager decorator
    """
    if isinstance(lock, SemLock):
        result = lock.acquire(timeout=timeout)
    else:
        result = lock.acquire(blocking=blocking, timeout=timeout)
    if result is False and raise_exc:
        raise RuntimeError("The Lock was unable to be obtained within the timeout: %s" % timeout)
    yield result
    if result:
        lock.release()


def safe_acquire(lock: Union[Lock, RLock], timeout: Union[int, float]) -> bool:
    """ <a name="safe_acquire"></a>
        Meant to be used as a safe way to wait on a lock. Returns False if time runs out.

    - :param lock: (Lock/RLock) the lock that will be acquired or return False
    - :param timeout: The amount of time to wait on attempting to gain lock.
    - :return: bools
    """
    e = Event()
    try:
        current_time = start_time = time.monotonic()
        while current_time < start_time + timeout:
            if lock.acquire(blocking=False):
                return True
            e.wait(timeout=0.1)
            current_time = time.monotonic()
        return lock.acquire(blocking=False)
    except Exception as e:
        _log.error(f"ERROR in safe_acquire with timeout {timeout} : {e}")
        _log.debug(f"[DEBUG] for safe_acquire: {traceback.format_exc()}")
        return False


def safe_release(lock: Union[Lock, RLock]) -> bool:
    """<a name="safe_acquire"></a>
        Meant to be used as a safe way to release a lock. Returns False if the lock has already been released.

    - :param lock: (Lock/RLock) the lock that will be acquired or return False
    - :return: bools
    """
    try:
        lock.release()
        return True
    except Exception as e:
        _log.error(f"ERROR in safe_release: {e}")
        _log.debug(f"[DEBUG] for safe_release: {traceback.format_exc()}")
        return False


def method_wait(func: Callable[..., Any], timeout: int = 60, delay: float = 0.1, delayPercent: bool = False,
                delayAscending: Optional[bool] = None, incompleteVar: Optional[bool] = None,
                raiseExc: Union[bool, Exception] = False, *args, **kwargs):
    """ <a name="method_wait"></a>Allows one to safely wait on a method to return.

    - *func*: (callable) Function to be executed using the method wait
    - *timeout*: (int, default 60) Amount of time to wait until giving up on the function
    - *delay*: (int or float, default 0.1) Amount of time to pause inbetween asking the function for results
    - *incompleteVar*: (any) Variable that indicates the function is not yet finished
    - *raiseExc*: (bool, default False) Causes methodWait to raise an exception instead of returning None
    - *args*: args that will be passed to the function
    - *kwargs*: kwargs that will be passed to the function
    - *return* could be anything
    """
    e = Event()

    def ascending_helper(num, multipler):
        if num * multipler > 1:
            yield num * multipler
            yield ascending_helper(num * multipler, multipler)
        else:
            yield num

    def delay_ascender(time_delay):
        if len(time_delay) > 1:
            e.wait(timeout=time_delay.pop())
        else:
            e.wait(timeout=time_delay[0])
        return time_delay

    def delay_by_time(time_delay):
        e.wait(timeout=time_delay)
        return time_delay

    def delay_by_percent(time_delay):
        e.wait(timeout=max(0.1, (time.monotonic() - start_time) * time_delay))
        return time_delay

    rawKwargs = {}
    rawKwargs.update(kwargs)
    kwargs.pop('failureVar', None)

    if delayPercent and delayAscending is None:
        delayer = delay_by_percent
    elif delayAscending is True:
        delay = [num for num in ascending_helper(timeout, delay)]
        delayer = delay_ascender
    elif delayAscending is False:
        delay = [num for num in ascending_helper(timeout, delay)]
        delay.sort()
        delayer = delay_ascender
    else:
        delayer = delay_by_time

    try:
        current_time = start_time = time.monotonic()
        while current_time < start_time + timeout:
            results = func(*args, **kwargs)
            if 'failureVar' in rawKwargs and rawKwargs['failureVar'] == results:
                return results
            elif results == incompleteVar:
                delay = delayer(delay)
                current_time = time.monotonic()
            elif results is not incompleteVar:
                return results
    except Exception as e:
        _log.error(f'The function {func} has thrown an exception. {e}')
        _log.debug(f'[DEBUG]: trace for error: {traceback.format_exc()}')
        if raiseExc is True:
            raise e
        elif inspect.isclass(raiseExc) and issubclass(raiseExc, BaseException):
            raise raiseExc(f'The function {func} has thrown an exception. {e}') from e
        elif isinstance(raiseExc, Exception):
            raise raiseExc from e
        return e
    return incompleteVar


# noinspection PyPep8Naming
def Limiter(num, call_only=False):
    """ This is a decorator designed to decorate Threaded and Proccessed decorators to limit the number of
        simultaneous calls.
    """

    def wrapper(func):
        sem = SemephoreProcessing(num, ctx=multiprocessing.get_context())

        @wraps(func)
        def wrapped(*args, **kwargs):
            if call_only is False:
                kwargs.update({'_worker_kwargs': {'_task_semaphore': sem}})
                return func(*args, **kwargs)
            else:
                with sem:
                    return func(*args, **kwargs)

        return wrapped
    return wrapper


# noinspection PyUnresolvedReferences
class MultiEvent(Event):
    """ <a name="MultiEvent"></a>
        Designed to only get set if more the set function is called multiple times.
    """

    _counter: int = None
    _counter_max: int = None
    _action_Lock: Lock = None

    def __init__(self, counter: int = 1):
        """ Constructor for the MultiEvent. This requires one parameter named 'counter'.

        - :param counter: (int, default 1) This is how many times 'set' method has to be called for the event to be set.
        """

        self._counter = counter
        self._counter_max = counter
        self._action_Lock = Lock()
        super(MultiEvent, self).__init__()

    def set(self) -> None:
        """ A wrapper method for 'set' in threading's modules 'Event' class. This simply counts down and will attempt
            to set the Event once the '_counter' is at zero.

        - :return: None
        """

        with self._action_Lock:
            self._counter -= 1
            if self._counter <= 0:
                super(MultiEvent, self).set()

    def clear(self) -> None:
        """ A wrapper method for 'clear' in threading's modules 'Event' class. This resets the '_counter' variable
            before calling 'clear' method of 'Event'.

        :return: None

        """
        with self._action_Lock:
            self._counter = self._counter_max
            super(MultiEvent, self).clear()

    def remaining_sets(self) -> int:
        """ Is a wrapper around the private variable '_counter'.

        - :return: (int)
        """

        with self._action_Lock:
            return self._counter

    @property
    def num_required_sets(self) -> int:
        return self._counter_max


class MultipleEvents(object):
    """ <a name="MultipleEvents"></a>
        Designed to take multiple events and put them together to be waited on as a whole.
    """

    _events = None

    def __init__(self, events: Iterable[Event]):
        """ Make a new MultipleEvents object using a iterable array of events.

        - :param events: (a list/tuple/iterable of events)
        """

        self._events = events
        super(MultipleEvents, self).__init__()

    def wait(self, timeout: int = 60, delay: Union[int, float] = 0.1) -> bool:
        """ Wait on all events by using the 'is_set' method on each event in the event list.

        - :param timeout: (int) default is 60. This will not throw an exception it will simply return False.
        - :param delay: How long to wait between checking if all events have been set.
        - :return: (bool)
        """

        if type(self._events) is not list:
            return False
        e = Event()
        endTime = time.monotonic() + timeout
        while time.monotonic() <= endTime:
            if self.isSet():
                return True
            e.wait(timeout=delay)
        return False

    def isSet(self) -> bool:
        """ Uses the 'is_set' method on each event in the list and returns True if all is set and False otherwise.

        - :return: (bool)
        """

        if not filter(MultipleEvents.wait_filter, self._events):
            return True
        return False

    def clear(self) -> None:
        """ Reset the internal flag to false on all events.

        - :return: (None)
        """

        for event in self._events:
            event.clear()

    def remove_events(self) -> None:
        """ This deletes all events making this object useless until new events are added.

        - :return: (None)
        """

        del self._events
        self._events = []

    def add_event(self, event: Event) -> None:
        """ This adds a new event to the private variable '_event'. It assumes '_event' is a List object.

        - :param event: (Event)
        - :return: (None)
        """

        if self._events is None:
            self._events = []
        self._events.append(event)

    @staticmethod
    def set() -> None:
        """ This is ignored. This object is not meant to set Events simply wait on events.

        - :return: (None)
        """

        _log.warning("This set is ignored!")

    @staticmethod
    def wait_filter(event: Event) -> bool:
        """ Simply calls and returns 'is_set' method of a given Event object.

        - :param event: (Event)
        - :return: (bool)
        """

        return not event.is_set()
