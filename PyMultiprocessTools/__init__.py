#!/usr/bin/env python3
# -*- coding=utf-8 -*-


from __future__ import annotations
import time
import logging
import traceback
import inspect
import uuid
import ctypes
from queue import PriorityQueue
from contextlib import contextmanager
from threading import Lock, RLock, Event
from functools import partial
from abc import ABC, abstractmethod
from typing import Union, Optional, Iterator, Callable, Any, Type, Iterable


_log = logging.getLogger('PyMultiprocessTools')


_DEFAULT_MAX_WORKERS = 4


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


def get_cpu_count() -> int:
    """Return the number of Logic CPU Cores on the system"""

    try:
        from multiprocessing import cpu_count
        return cpu_count()
    except:
        return _DEFAULT_MAX_WORKERS


@contextmanager
def wait_lock(lock: Union[Lock, RLock], timeout: Union[int, float]) -> Iterator[bool]:
    """ <a name="method_wait"></a>
        Meant to be used within a 'with' statement to throw an exception when timing out on a lock

    - :param lock: (Lock/RLock) the lock that will be acquired or an exception will be thrown
    - :param timeout: The amount of time to wait on attempting to gain lock.
    - :return: (generator of bools) This is used with a contextmanager decorator
    """

    result = lock.acquire(timeout=timeout)
    if result is False:
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
    try:
        current_time = start_time = time.time()
        while current_time < start_time + timeout:
            if lock.acquire(blocking=False):
                return True
            time.sleep(0.1)
            current_time = time.time()
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

    def ascending_helper(num, multipler):
        if num * multipler > 1:
            yield num * multipler
            yield ascending_helper(num * multipler, multipler)
        else:
            yield num

    def delay_ascender(time_delay):
        if len(time_delay) > 1:
            time.sleep(time_delay.pop())
        else:
            time.sleep(time_delay[0])
        return time_delay

    def delay_by_time(time_delay):
        time.sleep(time_delay)
        return time_delay

    def delay_by_percent(time_delay):
        time.sleep(max(0.1, (time.time() - start_time) * time_delay))
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
        current_time = start_time = time.time()
        while current_time < start_time + timeout:
            results = func(*args, **kwargs)
            if 'failureVar' in rawKwargs and rawKwargs['failureVar'] == results:
                return results
            elif results == incompleteVar:
                delay = delayer(delay)
                current_time = time.time()
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


class Task(Event):
    """ <a name="Task"></a>
        This is a wrapper class that inherits from an Event Object and is used inside the Worker.
        It is designed to hold the function ran and save the results of the function.
    """

    defaultriority: int = 1

    def __init__(self, fn: Callable, priority: int = 1, kill: bool = False, inject_task: bool = True,
                 store_return: bool = True, ignore_queue: bool = False, *args, **kwargs):
        super().__init__()
        self.args = args
        self.kwargs = kwargs
        self.priority = priority
        self.kill = kill
        self.ignore_queue = ignore_queue
        if isinstance(fn, partial):
            if inject_task and Task.__inspect_kwargs(fn.func):
                fn.keywords.update({'TaskObject': self})
            self.task = fn
        else:
            if inject_task and Task.__inspect_kwargs(fn):
                self.kwargs.update({'TaskObject': self})
            self.task = partial(fn, *self.args, **self.kwargs)
        self.store_return = store_return
        self.uuid = str(uuid.uuid4())
        self.worker = None
        self.results = None

    def run(self, *args, **kwargs) -> Any:
        """ This is used to run the stored partial function and store the results.

        - :param args: Positional arguments to be passed to the task.
        - :param kwargs: Keyword arguments to be passed to the task
        - :return: (Anything)
        """
        if self.is_set():
            raise Exception('A Task Object cannot be ran more than once!')

        try:
            if self.store_return:
                self.results = self.task(*args, **kwargs)
            else:
                return self.task(*args, **kwargs)
        except Exception as e:
            _log.info(f'{self} failed')
            raise e
        else:
            _log.info(f'{self} succeeded')
        finally:
            self.set()
            if self.store_return:
                return self.results

    def clear(self) -> None:
        if self.is_set():
            raise Exception('A Task Object cannot be cleared once set!')
        return super(Task, self).clear()

    @staticmethod
    def __inspect_kwargs(func, keyword='TaskObject'):
        try:
            return [key for key in inspect.signature(func).parameters.keys() if keyword == key or 'kwargs' == key]
        except:
            return []

    def __hash__(self):
        return hash(self.uuid)

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def __str__(self):
        return f'Task(UUID={self.uuid},Priority={self.priority}): {self.task.func}'

    def __gt__(self, other: Task):
        return self.priority > other.priority

    def __lt__(self, other: Task):
        return self.priority < other.priority

    def __ge__(self, other: Task):
        return self.priority >= other.priority

    def __le__(self, other: Task):
        return self.priority <= other.priority


class PriorityTaskQueue(PriorityQueue):
    """ <a name="PriorityTaskQueue"></a>
        This is a simple override of the PriorityQueue class that ensures the 'item' is a Task class
    """

    def put_nowait(self, item: Task) -> None:
        if not isinstance(item, Task):
            raise TypeError(f'[ERROR]: item is not a Task object cannot be put into PriorityTaskQueue')
        return super().put_nowait(item)

    def put(self, item: Task, block: bool = True, timeout: Union[int, float, None] = None) -> None:
        if not isinstance(item, Task):
            raise TypeError(f'[ERROR]: item is not a Task object cannot be put into PriorityTaskQueue')
        return super().put(item, block=block, timeout=timeout)


# noinspection PyUnresolvedReferences
class MultiEvent(Event):
    """ <a name="MultiEvent"></a>
        Designed to only get set if more the set function is called multiple times.
    """

    _counter: int = None
    _counterMax: int = None
    _ActionLock: Lock = None

    def __init__(self, counter: int = 1):
        """ Constructor for the MultiEvent. This requires one parameter named 'counter'.

        - :param counter: (int, default 1) This is how many times 'set' method has to be called for the event to be set.
        """

        self._counter = counter
        self._counterMax = counter
        self._ActionLock = Lock()
        super(MultiEvent, self).__init__()

    def set(self) -> None:
        """ A wrapper method for 'set' in threading's modules 'Event' class. This simply counts down and will attempt
            to set the Event once the '_counter' is at zero.

        - :return: None
        """

        with self._ActionLock:
            self._counter -= 1
            if self._counter <= 0:
                super(MultiEvent, self).set()

    def clear(self) -> None:
        """ A wrapper method for 'clear' in threading's modules 'Event' class. This resets the '_counter' variable
            before calling 'clear' method of 'Event'.

        :return: None

        """
        with self._ActionLock:
            self._counter = self._counterMax
            super(MultiEvent, self).clear()

    def remaining_sets(self) -> int:
        """ Is a wrapper around the private variable '_counter'.

        - :return: (int)
        """

        with self._ActionLock:
            return self._counter

    @property
    def num_required_sets(self) -> int:
        return self._counterMax


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

        endTime = time.time() + timeout
        while time.time() <= endTime:
            if self.isSet():
                return True
            time.sleep(delay)
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


class Worker(ABC):

    @abstractmethod
    def terminate(self, *args, **kwargs):
        pass

    @abstractmethod
    def safe_stop(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_next_task(self, *args, **kwargs):
        pass

    @abstractmethod
    def run(self, *args, **kwargs):
        pass

    @staticmethod
    @abstractmethod
    def __KILL__(*args, **kwargs):
        pass

    @property
    @abstractmethod
    def task_queue(self):
        pass

    @property
    @abstractmethod
    def timeout(self):
        pass

    @property
    @abstractmethod
    def current_priority(self):
        pass

    @property
    @abstractmethod
    def is_active(self):
        pass


class Pool(ABC):

    @abstractmethod
    def setup_workers(self, *args, **kwargs):
        pass

    @abstractmethod
    def add_worker(self, *args, **kwargs):
        pass

    @abstractmethod
    def remove_worker(self, *args, **kwargs):
        pass

    @abstractmethod
    def set_max_workers(self, *args, **kwargs):
        pass

    @abstractmethod
    def wait_completion(self, *args, **kwargs):
        pass

    @abstractmethod
    def shutdown(self, *args, **kwargs):
        pass

    @abstractmethod
    def join(self, *args, **kwargs):
        pass

    @abstractmethod
    def map(self, *args, **kwargs):
        pass

    @abstractmethod
    def submit(self, *args, **kwargs):
        pass

    @staticmethod
    @abstractmethod
    def as_completed(*args, **kwargs):
        pass

    @property
    @abstractmethod
    def unfinished_tasks(self):
        pass

    @property
    @abstractmethod
    def num_queued_tasks(self):
        pass

    @property
    @abstractmethod
    def num_active_tasks(self):
        pass

    @property
    @abstractmethod
    def has_tasks(self):
        pass

    @property
    @abstractmethod
    def is_idle(self):
        pass

    @property
    @abstractmethod
    def is_active(self):
        pass

    @property
    @abstractmethod
    def has_workers(self):
        pass

    @property
    @abstractmethod
    def needs_workers(self):
        pass

    @property
    @abstractmethod
    def num_workers(self):
        pass

    @property
    @abstractmethod
    def active_workers(self):
        pass

    @property
    @abstractmethod
    def inactive_workers(self):
        pass

    @property
    @abstractmethod
    def highest_priority(self):
        pass

    @property
    @abstractmethod
    def workers(self):
        pass

    @workers.setter
    @abstractmethod
    def workers(self, *args):
        pass

    @workers.deleter
    @abstractmethod
    def workers(self):
        pass

    @property
    @abstractmethod
    def state(self):
        pass

    @state.setter
    @abstractmethod
    def state(self, *args):
        pass

    @state.deleter
    @abstractmethod
    def state(self):
        pass
