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
    This manages a pool of Workers and a queue of Tasks. The workers consume tasks from the taskQueue until they are
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
import uuid
import ctypes
from queue import PriorityQueue, Empty, Queue
from multiprocessing.queues import JoinableQueue
from multiprocessing.synchronize import RLock, SemLock
from multiprocessing import RLock as MultiProcRLock
from contextlib import contextmanager
from threading import Lock, RLock, Event, Semaphore
from functools import partial, wraps
from typing import Union, Optional, Iterator, Callable, Any, Type, Iterable, Tuple, List


# logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(funcName)s %(lineno)s %(message)s',
#                     level=logging.DEBUG)
_log = logging.getLogger('MultiTaskingTools')


__STARTING__ = "__STARTING__"
__STOPPING__ = "__STOPING__"
__ACTIVE__ = "__ACTIVE__"
__INACTIVE__ = "__INACTIVE__"
__STOPPED__ = "__STOPPED__"
__THREADPOOL_STATES__ = {__STARTING__: __STARTING__, __STOPPING__: __STOPPING__, __ACTIVE__: __ACTIVE__,
                         __INACTIVE__: __INACTIVE__, __STOPPED__: __STOPPED__}


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
def Limiter(num, blocking=True):
    """ This is a decorator designed to decorate Threaded and Proccessed decorators to limit the number of
        simultaneous calls.
    """
    sem = Semaphore(num)

    def wrapper(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            if blocking:
                with sem:
                    return func(*args, **kwargs)
            else:
                kwargs.update({'_task_semaphore': sem})
                return func(*args, **kwargs)
        return wrapped
    return wrapper


class PriorityTaskQueue(PriorityQueue):
    """ <a name="PriorityTaskQueue"></a>
        This is a simple override of the PriorityQueue class that ensures the 'item' is a Task class meant to be used
        ONLY in ThreadingPool
    """

    def put_nowait(self, item: Task) -> None:
        if not isinstance(item, Task):
            raise TypeError('[ERROR]: item is not a Task object cannot be put into PriorityTaskQueue')
        return super().put_nowait(item)

    def put(self, item: Task, block: bool = True, timeout: Union[int, float, None] = None) -> None:
        if not isinstance(item, Task):
            raise TypeError('[ERROR]: item is not a Task object cannot be put into PriorityTaskQueue')
        return super().put(item, block=block, timeout=timeout)


class ProcessTaskQueue(JoinableQueue):
    """ <a name="ProcessTaskQueue"></a>
        This is a simple override of the JoinableQueue class that ensures the 'item' is a Task class meant to be used
        ONLY in ProcessingPool
    """

    def __init__(self, maxsize=0, *, ctx=None):
        super(ProcessTaskQueue, self).__init__(maxsize=maxsize, ctx=ctx or multiprocessing.get_context())

    def put_nowait(self, item: Task) -> None:
        if not isinstance(item, Task):
            raise TypeError('[ERROR]: item is not a Task object cannot be put into PriorityTaskQueue')
        return super().put_nowait(item)

    def put(self, item: Task, block: bool = True, timeout: Union[int, float, None] = None) -> None:
        if not isinstance(item, Task):
            raise TypeError('[ERROR]: item is not a Task object cannot be put into PriorityTaskQueue')
        return super().put(item, block=block, timeout=timeout)


class __PyMultiDec:

    wType = None
    pType = None

    def __init__(self, *args, **kwargs):
        _log.info(f'making Class Dec: args={args} - kwargs={kwargs}')
        if len(args) == 1 and callable(args[0]) and len(kwargs) == 0:
            self.func = args[0]
        else:
            self.func = None
        self.callback_func = kwargs.get('callback_func', None)
        self.kwargsLength = len(kwargs)
        self.daemon = kwargs.pop('daemon', None)
        self.pool = kwargs.pop('pool', None)
        self.personal_que = None
        self.worker = None
        if self.daemon is not None:
            if self.daemon is True:
                self.personal_que = PriorityTaskQueue()
                kwargs.update({'_worker_workerAutoKill': False, '_worker_personalQue': self.personal_que})
                self.worker = self.wType(**{k.replace('_worker_', ''): v
                                         for k, v in kwargs.items() if k.startswith('_worker_')})
            else:
                worker = self.daemon
                self.personal_que = getattr(worker, 'task_queue')
        elif self.pool is True or kwargs.get('pool_name', None) is not None:
            self.pool = self.pType.get_pool_by_name(name=kwargs.get('pool_name', None))
            if not self.pool:
                kwargs.update({'_pool_workerAutoKill': False, '_pool_name': kwargs.get('pool_name', '')})
                self.pool = self.pType(**{k.replace('_pool_', ''): v for k, v in kwargs.items()
                                                                     if k.startswith('_pool_')})

    def __call__(self, *args, **kwargs):
        _log.debug(f'calling test_dec: args={args} - kwargs={kwargs}')

        @wraps(self.func)
        def wrapper(*a, **kw):
            _log.debug(f'Within wrapper: args={a} - kwargs={kw}')
            kw.update({k.replace('_task_', ''): v for k, v in kwargs.items() if k.startswith('_task_')})
            keywords = {k.replace('_task_', ''): v for k, v in kw.items() if k.startswith('_task_')}
            for key in keywords:
                kw.pop('_task_'+key, None)
            kw.update(keywords)
            kw.update({'callback_func': self.callback_func})
            task = Task(self.func, **kw)
            if self.personal_que:
                if self.worker.killed:
                    _log.warning(f'Decorated Worker {self.worker} was killed no longer daemon for func {self.func}')
                    self.wType(target=task, **{k.replace('_worker_', ''): v
                                               for k, v in kwargs.items() if k.startswith('_worker_')}).start()
                self.personal_que.put_nowait(task)
                if self.worker.is_alive() is False:
                    self.worker.start()
            elif self.pool:
                self.pool.submit(task, submit_task_nowait=True, submit_task_autospawn=True, allow_restart=True)
            else:
                self.wType(target=task, **{k.replace('_worker_', ''): v
                                           for k, v in kwargs.items() if k.startswith('_worker_')}).start()
            return task

        if self.func is None and callable(args[0]):
            self.func = args[0]
            return wrapper
        return wrapper(*args, **kwargs)


class Task(Event):
    """ <a name="Task"></a>
        This is a wrapper class that inherits from an Event Object and is used inside the Worker.
        It is designed to hold the function ran and save the results of the function.
    """

    defaultriority: int = 1

    def __init__(self, fn: Callable, priority: int = 1, kill: bool = False, inject_task: bool = True,
                 store_return: bool = True, ignore_queue: bool = False, callback_func: Optional[Callable] = None,
                 semaphore: Optional[Semaphore] = None, *args, **kwargs):
        super().__init__()
        self.args = args
        self.kwargs = kwargs
        self.priority = priority
        self.kill = kill
        self.ignore_queue = ignore_queue
        self.callback_fun = callback_func
        self.semaphore = semaphore if semaphore is not None else Semaphore(1)
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
        self.__updateRLock = MultiProcRLock()
        self.__worker = None
        self.__results = None

    def run(self, *args, **kwargs) -> Any:
        """ This is used to run the stored partial function and store the results.

        - :param args: Positional arguments to be passed to the task.
        - :param kwargs: Keyword arguments to be passed to the task
        - :return: (Anything)
        """
        if self.is_set():
            raise Exception('A Task Object cannot be ran more than once!')

        try:
            self.semaphore.acquire()
            if self.store_return:
                self.results = self.task(*args, **kwargs)
                if self.callback_fun:
                    return self.callback_fun(self.results)
            else:
                if self.callback_fun:
                    return self.callback_fun(self.task(*args, **kwargs))
                else:
                    return self.task(*args, **kwargs)
        except Exception as e:
            _log.info(f'{self} failed')
            raise e
        else:
            _log.info(f'{self} succeeded')
        finally:
            self.set()
            self.semaphore.release()
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

    @property
    def worker(self):
        with wait_lock(self.__updateRLock, timeout=1, raise_exc=False) as acquired:
            if acquired:
                return self.__worker

    @worker.setter
    def worker(self, value):
        with wait_lock(self.__updateRLock, timeout=1, raise_exc=True):
            self.__worker = value

    @worker.deleter
    def worker(self):
        with wait_lock(self.__updateRLock, timeout=1, raise_exc=True):
            del self.__worker

    @property
    def results(self):
        with wait_lock(self.__updateRLock, timeout=1, raise_exc=False) as acquired:
            if acquired:
                return self.__results

    @results.setter
    def results(self, value):
        with wait_lock(self.__updateRLock, timeout=1, raise_exc=True):
            self.__results = value

    @results.deleter
    def results(self):
        with wait_lock(self.__updateRLock, timeout=1, raise_exc=True):
            del self.__results


class Worker:
    """ <a name="Worker"></a>
            This is designed to be managed by a ThreadPool. However, it can run on its own as well. It runs until told to
            stop and works tasks that come from a the PriorityTaskQueue maintained by the Pool.
        """

    __workerAutoKill = True
    __defaultTimeout = 10
    workerType = None
    name = None

    def __init__(self, pool: Optional[Pool] = None, workerAutoKill: bool = True, defaultTimeout: int = 10,
                 personalQue: Optional[ProcessTaskQueue] = None, target: Optional[Callable] = None,
                 name: Optional[str] = None, daemon: bool = True, log: Optional[logging] = None, **kwargs):
        self.uuid = str(uuid.uuid4())
        self.log = _log if log is None else log
        self.__defaultTimeout = defaultTimeout
        self.__timeout = defaultTimeout
        self.__personalQue = personalQue
        self.__currentTask = None
        if target is not None and not isinstance(target, Task):
            target = Task(target, kill=True, ignore_queue=True)
        super(Worker, self).__init__(target=target, name=self.uuid if name is None else name, daemon=daemon,
                                     args=kwargs.get('args', ()), kwargs=kwargs.get('kwargs', {}))
        self.pool = pool
        self.killed = False
        self.__workerAutoKill = workerAutoKill if self.__personalQue is None else False
        if pool:
            self.log.info(f'[INFO]: Starting new {self}')
            self.start()

    def __str__(self):
        return f'Worker: {self.name if self.name == self.uuid else f"{self.name}-{self.uuid}"} for Pool: {self.pool}'

    def __hash__(self):
        return hash(self.uuid)

    def safe_stop(self):
        self.killed = True

    # noinspection PyUnresolvedReferences
    def get_next_task(self) -> Union[Task, bool, None]:
        """ This gets the next Task in the taskQueue

        - :return: (Task)
        """

        try:
            self.__currentTask = None
            if self.pool is None and self._target is not None:
                self.__currentTask = self._target
                self.__currentTask.worker = self
                self.__currentTask.kill = True
                self.__currentTask.ignore_queue = True
            elif self.pool is None and self.__personalQue is None:
                self.__currentTask = None
            else:
                self.__currentTask = self.task_queue.get(timeout=self.__timeout)
                self.__currentTask.worker = self
            return self.__currentTask
        except Empty:
            if self.timeout == 0:
                return Task(Worker.__KILL__, kill=True, ignore_queue=True)
            return False
        except Exception as e:
            self.log.error(f'[ERROR]: Error in getting task: {e}')
            self.log.debug(f'[DEBUG]: trace for error in getting task: {traceback.format_exc()}')
            return None

    def run(self) -> None:
        """ This is an override of the run method within Thread that loops constantly waiting on another task.

        - :return: (None)
        """

        try:
            while not self.killed:
                task = self.get_next_task()
                if task is None:
                    self.log.info('task is None an error occurred in get_next_task method closing the thread')
                    break
                elif task is not False:
                    self.log.info(f'The task is: {task}')
                    task(*self._args, **self._kwargs)
                    self.__currentTask = None
                    if not task.ignore_queue:
                        self.task_queue.task_done()
                    if task.kill:
                        self.log.info(f'Killing thread once task is complete: {task}')
                        self.killed = True
        except Exception as e:
            self.log.error(f'[ERROR]: While Worker thread is running with task: {self.__currentTask} Error: {e}')
            self.log.debug(f'[DEBUG]: trace for error: {traceback.format_exc()}')
            if self.__currentTask is not None:
                if not self.__currentTask.isSet():
                    self.__currentTask.set()
                    if not self.__currentTask.ignore_queue:
                        getattr(self.task_queue, 'task_done', dummy_func)()
                self.__currentTask = None
        finally:
            if self.killed is not True:
                self.killed = True
            if self.pool is not None:
                self.pool.remove_worker(workerTooRemove=self)

    @staticmethod
    def __KILL__(*args, **kwargs) -> None:
        pass

    @property
    def task_queue(self):
        if self.pool is None and self.__personalQue is not None:
            return self.__personalQue
        return getattr(getattr(self, 'pool', None), 'taskQueue', None)

    @property
    def timeout(self) -> int:
        """ This property will return 0 if it thinks it doesn't need to run any longer and is ready to self terminate
        """
        if self.killed is True:
            return 0
        elif self.pool is not None and ((self.__workerAutoKill and self.pool.num_workers > 1) or
                                        self.pool.num_workers > self.pool.maxWorkers):
            self.__timeout //= 2
        elif self.__defaultTimeout != self.__timeout:
            self.__timeout = self.__defaultTimeout
        return self.__timeout

    @property
    def current_priority(self) -> int:
        """ This changes to the priority of each incoming task. """

        try:
            if self.__currentTask:
                return self.__currentTask.priority
            return 0
        except Exception as e:
            self.log.error(f'ERROR: {e}')
            return 0

    @property
    def is_active(self) -> bool:
        """ This determines if the Worker currently has a Task to work. """
        return self.__currentTask is not None


# noinspection PyPep8Naming
class Pool:
    """ <a name="ThreadPool"></a>
        This manages a pool of Workers and a queue of Tasks. The workers consume tasks from the taskQueue until they
        are told to stop.
    """

    _state = __INACTIVE__
    __regRLock = None
    __pool_registry = None

    def __init__(self, workerType: type, maxWorkers: Optional[int] = None, tasks: Optional[Queue] = None,
                 daemon: bool = True, timeout: int = 60, workerAutoKill: bool = True, prepopulate: int = 0,
                 name: str = "", log: Optional[logging] = None):
        self.workerType = workerType
        self.log = _log if log is None else log
        self.uuid = str(uuid.uuid4())
        self.name = name if name else self.uuid
        self.maxWorkers = maxWorkers or get_cpu_count()
        self.__timeout = timeout
        self.__workerAutoKill = workerAutoKill
        self.__workerListLock = RLock() if getattr(workerType, 'workerType', 'THREAD') == 'THREAD' else MultiProcRLock()
        self.__workerList = None
        self.__stateLock = RLock() if getattr(workerType, 'workerType', 'THREAD') == 'THREAD' else MultiProcRLock()
        self.__taskLock = RLock() if getattr(workerType, 'workerType', 'THREAD') == 'THREAD' else MultiProcRLock()
        self.taskQueue = tasks or PriorityTaskQueue()
        self.workers = []
        self.state = __STARTING__
        self.daemon = daemon
        self.ignoredTasks = []

        if prepopulate:
            self.setup_workers(numOfWorkers=prepopulate, workerAutoKill=self.__workerAutoKill)
        elif self.taskQueue.qsize() > 0:
            self.setup_workers(numOfWorkers=self.taskQueue.qsize() if self.taskQueue.qsize() <= self.maxWorkers
                                                                    else self.maxWorkers,
                               workerAutoKill=not daemon)
        if daemon is False:
            self.state = __ACTIVE__
            self.join(self.__timeout)
            self.shutdown(timeout=self.__timeout)

    def __enter__(self):
        self.__taskLock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.daemon is False:
                self.join(self.__timeout)
                self.shutdown(timeout=self.__timeout)
            else:
                self.wait_completion(timeout=self.__timeout)
            self.__taskLock.release()
        except Exception as e:
            self.log.error(f"ERROR in __exit__ of Pool: {e}")
            self.log.debug(f"[DEBUG] for __exit__ of Pool: {traceback.format_exc()}")

    def __str__(self):
        return f'Pool(UUID={self.uuid}, State={self._state})'

    def setup_workers(self, numOfWorkers: int = 1, workerAutoKill: Optional[bool] = None,
                      allow_restart: bool = False) -> bool:
        """ Generally only used by init. This setups Worker threads to be managed by the Pool.

        - :param numOfWorkers: (int) Number workers setup. IF the number of workers is higher then the value of
                'maxWorkers' then 'maxWorkers' is updated. The numOfWorkers is how many Workers the Pool has *not* now
                many new Workers get added.
        - :param workerAutoKill: (bool) This determines if the worker ends once their is no longer any work left in
                the 'taskQueue'.
        - :return: (bool)
        """

        if self.state in (__STOPPING__, __STOPPED__) and allow_restart is False:
            return False
        if numOfWorkers > self.maxWorkers:
            self.set_max_workers(numOfWorkers)
            numOfNewWorkers = (numOfWorkers - self.num_workers)
        elif numOfWorkers > (self.maxWorkers - self.num_workers):
            numOfNewWorkers = (self.maxWorkers - self.num_workers)
        else:
            numOfNewWorkers = numOfWorkers
        for _ in range(0, numOfNewWorkers):
            self.add_worker(workerAutoKill=self.__workerAutoKill if workerAutoKill is None else workerAutoKill,
                            allow_restart=allow_restart)
        return numOfNewWorkers > 0

    def add_worker(self, workerAutoKill: Optional[bool] = None, allow_restart: bool = False, **kwargs) -> bool:
        """ Adds a single new worker too the Pool.

        - :param workerAutoKill: (bool) This determines if the worker ends once their is no longer any work left in
        - :return: (bool)
        """

        self.log.debug("Attempting to add new worker!")
        if self.state in (__STOPPING__, __STOPPED__) and allow_restart is False:
            return False
        if self.num_workers >= self.maxWorkers:
            return False
        self.workers.append(self.workerType(self, workerAutoKill=self.__workerAutoKill if workerAutoKill is None
                                                                                       else workerAutoKill))
        return True

    def remove_worker(self, workerTooRemove: Optional[Worker] = None, timeout: int = 30,
                      allow_abandon: bool = False) -> bool:
        """ Removes a single new worker from the Pool. This can be called to remove the last Worker or you can
            specify a Worker to remove.

        - :param workerTooRemove: (Worker) This is usually sent when a Worker is self terminating
        - :param timeout: (int) 30, How much time it is willing to wait. NOTE: This is doubled when specifying a
            worker with the workerTooRemove parameter.
        - :param allow_abandon: (bool) False, This determines if the thread will simply be abandoned if it cannot
            normally remove it from the pool. It will only do this if 'safe_stop' and 'terminate' methods fail.
        - :return: (bool)
        """

        def wait_helper(wait_time, start_time, ev, wtr):
            current_time = time.monotonic()
            while current_time < start_time + wait_time and wtr in self.workers:
                ev.wait(timeout=0.1)
            return wtr not in self.workers

        try:
            if self.num_workers <= 0:
                return False
            if workerTooRemove in self.workers and workerTooRemove.killed:
                self.workers.pop(self.workers.index(workerTooRemove))
                return True
            e = Event()
            if workerTooRemove is not None:
                workerTooRemove.safe_stop()
                if wait_helper(timeout, time.monotonic(), e, workerTooRemove):
                    return True
                self.log.warning(f'[WARN]: worker({workerTooRemove}) needs to be terminated in order to be removed.')
                getattr(workerTooRemove, 'terminate', dummy_func)()
                if wait_helper(timeout, time.monotonic(), e, workerTooRemove):
                    return True
                if allow_abandon:
                    self.log.warning(f'[WARN]: worker({workerTooRemove}) is being abandoned.')
                    worker = self.workers.pop(self.workers.index(workerTooRemove))
                    if worker.killed is not True:
                        worker.killed = True
                    return True
                return False
            else:
                current_num = self.num_workers
                self.submit(Task(Worker.__KILL__, priority=self.highest_priority + 1, kill=True),
                            submit_task_autospawn=False)
                if timeout > 0:
                    current = start = time.monotonic()
                    while current < start + timeout and self.num_workers >= current_num:
                        e.wait(timeout=0.1)
                    return self.num_workers < current_num
                return True
        except Exception as e:
            self.log.error(f'[ERROR]: Error occurred while attempting to remove worker: {e}')
            self.log.debug(f'[DEBUG]: Trace for error while attempting to remove worker: {traceback.format_exc()}')
            return False
        finally:
            if self.num_workers == 0:
                self.state = __INACTIVE__

    def set_max_workers(self, maxWorkers: int) -> int:
        """ Set the maximum number of threads that will remain active. Return the maximum thread limit.

        - :param maxWorkers: (int) Max thread limit
        - :return: (int)
        """

        if type(maxWorkers) is int and maxWorkers > -1:
            self.maxWorkers = maxWorkers
        return self.maxWorkers

    def wait_completion(self, timeout: Union[int, float], delay: Union[int, float] = 0.1, block: bool = False) -> bool:
        """ This method waits until all Tasks in the PriorityTaskQueue is done. If the parameter block is True it will
            stop any new Task from being submitted while waiting.

        - :param timeout: (int/float) How long to wait for all tasks in the 'taskQueue' to be finished.
        - :param delay: (int/float) The amount of time to wait before checking again in seconds. Default 0.1.
        - :param block: (bool) This will stop new tasks from being submitted to the Queue until finished.
        - :return: (bool)
        """
        e = Event()

        def _wait_completion(waitTime: Union[int, float]) -> bool:
            current_time = start_time = time.monotonic()
            while current_time < start_time + waitTime and self.has_workers:
                if self.unfinished_tasks == 0:
                    return True
                e.wait(timeout=delay)
                current_time = time.monotonic()
            return False

        if block:
            start = time.monotonic()
            with wait_lock(self.__taskLock, timeout=timeout):
                return _wait_completion(max(0.1, (start + timeout) - time.monotonic()))
        return _wait_completion(timeout)

    def shutdown(self, timeout: Optional[int] = None, unsafe: Optional[bool] = None) -> bool:
        """ This sends a kill operation too all the workers and waits for them to complete and then removes the threads.
            It can also attempt to kill Workers in an unsafe way with the 'terminate' Worker method.

        - :param timeout: (int) The length of time to wait on tasks to be stopped
        - :param unsafe: (bool/None) True: The 'terminate' method will be called on each Worker. False: Even if the
                timeout is reached the 'terminate' method will *not* be called. None: This will attempt to safely wait
                for the Workers too finish but if timeout is reached then the 'terminate' method will be called.
        - :return: (bool)
        """

        e = Event()
        self.state = __STOPPING__
        if timeout is None:
            timeout = self.__timeout

        def _clear_helper(task):
            return task.task.func != Worker.__KILL__

        def _clear_shutdown_tasks():
            try:
                tasks = []
                while not self.taskQueue.empty():
                    tasks.append(self.taskQueue.get())
                    self.taskQueue.task_done()
                for task in filter(_clear_helper, tasks):
                    if self.has_workers:
                        self.taskQueue.put_nowait(task)
                    else:
                        self.ignoredTasks.append(task)
            except Exception as e:
                self.log.error(f'[ERROR]: Error while clearing old tasks: {e}')
                self.log.debug(f'[DEBUG]: Trace for error clearing old tasks: {traceback.format_exc()}')

        def _unsafe_shutdown():
            for worker in self.workers:
                self.log.info(f'Worker: {worker} will be killed unsafely.')
                worker.terminate()

        if unsafe:
            _unsafe_shutdown()
            e.wait(timeout=0.1)
            return self.num_workers == 0

        start_time = time.monotonic()
        with wait_lock(self.__taskLock, timeout=timeout):
            for x in range(0, self.num_workers):
                self.remove_worker(timeout=0)
            current_time = time.monotonic()
            while current_time < start_time + timeout:
                if self.num_workers <= 0:
                    self.log.info('There are no more workers. No need for forced timeout')
                    break
                e.wait(timeout=0.1)
                current_time = time.monotonic()
            if unsafe is None:
                _unsafe_shutdown()
                e.wait(timeout=0.1)
            _clear_shutdown_tasks()

        if self.num_workers == 0:
            self.state = __STOPPED__
            return True
        return False

    def join(self, timeout: int) -> bool:
        """ This first calls 'waitCompletion' with 'block=True' and then calls 'shutdown'. The goal is to try to wait
            for all Tasks to complete and then close out the Pool.

        - :param timeout: (int)The length of time to wait on both join and shutdown.
        - :return: (bool)
        """

        with wait_lock(self.__taskLock, timeout=timeout):
            start_time = time.monotonic()
            self.wait_completion(timeout, block=True)
            return self.shutdown(timeout=int(max(timeout - (time.monotonic() - start_time), 1)))

    def map(self, fn: Callable, params: Tuple, chunksize: int = 0, *args, **kwargs) -> None:
        """ A simple mapping tool that takes different params (a List of tuples formatted like [(*args, **kwargs)]) and
            pass them too a function. chunksize determines how too break up the list and distribute it across Workers.

        - :param fn: (Callable) This is something like a function or a partial that will be transformed into a Task
        - :param params: (Tuple) The Tuple should be formatted like so ( ((arg1, ), {'kwarg1': 'value'}), ...). Each
                item within the Tuple is a Tuple itself with two items. The first item is positional arguments (args)
                and the second item is keyword arguments (kwargs). Even if they are empty they need to exist.
        - :param chunksize: (int) If left at 0 the method will attempt to spread the tasks as evenly as possible.
                Otherwise it will take the number to mean how many Tasks will be given to a single Worker.
        - :param kwargs: These are keyword arguments that get passed to the 'submit' method.
        - :return: (None)
        """

        if 'submit_task_autospawn' not in kwargs:
            kwargs['submit_task_autospawn'] = True

        if chunksize == 0:
            if len(params) <= self.maxWorkers:
                chunksize = 1
            else:
                chunksize = round(len(params) / self.maxWorkers)

        def chunkHelper(func, chunkList):
            return [func(*parms[0], **parms[1]) for parms in chunkList]

        def listIntoChunks(lst):
            return [lst[i * chunksize:(i + 1) * chunksize] for i in range((len(lst) + chunksize - 1) // chunksize)]

        for item in listIntoChunks(params):
            self.submit(partial(chunkHelper, func=fn, chunkList=item), **kwargs)

    def submit(self, fn: Callable, *args, **kwargs) -> Union[Task, bool]:
        """ This is the function used to submit a Task to the Pool. Simply provide a function as 'fn' and then
            arguments that need to be passed too that function and it will create a Task and add it to the
            PriorityTaskQueue to be worked.

        - :param fn: (Callable) This is something like a function or a partial that will be transformed into a Task
        - :param args: These args will be passed to the Task object.
        - :param kwargs: The following keywords will be pulled out. 'submit_task_nowait', 'submit_task_timeout',
                'submit_task_autospawn', 'submit_task_priority'. All others will be passed to Task.
            + 'submit_task_nowait': (bool) tells 'submit' to use the 'put_nowait' method on PriorityTaskQueue.
            + 'submit_task_timeout': (int/float) how long should one wait too submit.
            + 'submit_task_autospawn': (bool/None) determines if new Worker should be spawned because of more tasks.
            + 'submit_task_priority': (int) changes the priority of the task.
        - :return: (Task/bool)
        """

        if fn is None:
            return False

        def autospawn_parser(tmpAutospawn, state):
            if state == __STOPPING__ or state == __STOPPED__:
                return False
            if self.needs_workers and tmpAutospawn is None:
                return True
            return tmpAutospawn

        nowait = kwargs.pop('submit_task_nowait', True)
        timeout = kwargs.pop('submit_task_timeout', 10)
        autospawn = autospawn_parser(kwargs.pop('submit_task_autospawn', None), self.state)
        priority = kwargs.pop('submit_task_priority', 10) or 10

        start = time.monotonic()
        with wait_lock(self.__taskLock, timeout=timeout):

            if isinstance(fn, Task):
                task = fn
            else:
                task = Task(fn, priority, *args, **kwargs)

            try:
                if nowait:
                    self.taskQueue.put_nowait(task)
                else:
                    self.taskQueue.put(task, timeout=max(0.1, (start + timeout) - time.monotonic()))
                if autospawn or autospawn is None and self.needs_workers:
                    self.add_worker(**kwargs)
                return task
            except Exception as e:
                self.log.error(f'Error in submitting task: {e}\n{traceback.format_exc()}')
                return False
            finally:
                if self.state is not __ACTIVE__ and self.num_workers > 0:
                    self.state = __ACTIVE__

    @staticmethod
    def as_completed(tasks: List[Task]):

        def _finished_tasks(task_item):
            return task_item if task_item.is_set() else None

        lengthOfTasks = len(tasks)
        finished_tasks = set()

        while len(finished_tasks) < lengthOfTasks:

            for task in filter(_finished_tasks, tasks):
                if task not in finished_tasks:
                    finished_tasks.add(task)
                    yield task
            time.sleep(0.01)

    @property
    def unfinished_tasks(self) -> int:
        """+ This calls the 'unfinishedTasks' property of PriorityTaskQueue. And is equal to the number of tasks
            submitted minus the number of times a Task has been Worked by a Worker.
        """
        return self.taskQueue.unfinished_tasks

    @property
    def num_queued_tasks(self) -> int:
        """ This is a wrapper for the 'qsize()' method from PriorityTaskQueue."""
        return self.taskQueue.qsize()

    @property
    def num_active_tasks(self) -> int:
        return self.unfinished_tasks - self.num_queued_tasks

    @property
    def has_tasks(self) -> bool:
        return self.unfinished_tasks > 0

    @property
    def is_idle(self) -> bool:
        return not (self.has_tasks and self.has_workers)

    @property
    def is_active(self) -> bool:
        """+ This determines is the Pool both has workers and has work to do. This doesn't have anything to do with a
            Pool's state.
        """
        return self.has_tasks and self.has_workers

    @property
    def has_workers(self) -> bool:
        return self.num_workers > 0

    @property
    def needs_workers(self) -> bool:
        if self.num_workers < self.maxWorkers:
            if self.num_queued_tasks > self.inactive_workers:
                return True
        return False

    @property
    def num_workers(self) -> int:
        try:
            return len(self.workers)
        except Exception:
            return 0

    @property
    def active_workers(self) -> int:
        return len([i for i in self.workers if i.is_active])

    @property
    def inactive_workers(self) -> int:
        return len([i for i in self.workers if not i.is_active])

    @property
    def highest_priority(self) -> int:
        return max([i.current_priority for i in self.workers])

    @property
    def workers(self) -> List:
        """+ This is a protected (wrapped in a lock) List of Workers managed by this pool. """
        try:
            with wait_lock(self.__workerListLock, self.__timeout):
                return self.__workerList
        except RuntimeError:
            pass

    @workers.setter
    def workers(self, value) -> None:
        try:
            with wait_lock(self.__workerListLock, self.__timeout):
                self.__workerList = value
        except RuntimeError:
            pass

    @workers.deleter
    def workers(self) -> None:
        try:
            with wait_lock(self.__workerListLock, self.__timeout):
                self.__workerList = []
        except RuntimeError:
            pass

    @property
    def state(self) -> str:
        """+ This is a string that has only 4 valid string values that determines the state of the Pool."""
        try:
            with wait_lock(self.__stateLock, self.__timeout):
                return self._state
        except RuntimeError:
            pass

    @state.setter
    def state(self, value) -> None:
        try:
            with wait_lock(self.__stateLock, self.__timeout):
                if value in __THREADPOOL_STATES__:
                    self._state = __THREADPOOL_STATES__[value]
                else:
                    raise TypeError('Invalid ThreadPool STATE: %s' % str(value))
        except RuntimeError:
            pass

    @state.deleter
    def state(self) -> None:
        try:
            with wait_lock(self.__stateLock, self.__timeout):
                self._state = __INACTIVE__
        except RuntimeError:
            pass


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