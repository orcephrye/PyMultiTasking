#!/usr/bin/env python3
# -*- coding=utf-8 -*-

# Author: Ryan Henrichson
# Version: 2.0
"""
# Threading Tools

This Python Package makes it easier to handle threads. This uses a schema of Task that is run by a Worker which is
managed in a Pool. The Pool uses a PriorityTaskQueue a custom class that inherits from PriorityQueue.

## Summary of Functions
_get_cpu_count() -> int: <br />
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
    This is a simple override of the PriorityQueue class that ensures the 'item' is a Task class

[Worker](#Worker)(threading.Thread) <br />
    This is designed to be managed by a Pool. It runs until told to stop and works tasks that come from a the
    PriorityTaskQueue maintained by the Pool.

[Pool](#Pool) <br />
    This manages a pool of Workers and a queue of Tasks. The workers consume tasks from the taskQueue until they are
    told to stop.

[MultipleEvents](#MultipleEvents) <br />
    Designed to take multiple events and put them together to be waited on as a whole.

---
"""

from __future__ import annotations

import logging
import threading
import time
import traceback
import uuid
import ctypes
import inspect
from typing import Iterator, Any, Union, Optional, List, Tuple, Type, Callable, Iterable
from contextlib import contextmanager
from functools import partial
from queue import PriorityQueue, Empty
from threading import Lock, RLock, Event


# logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(funcName)s %(lineno)s %(message)s',
#                     level=logging.INFO)
log = logging.getLogger('ThreadingPool')
# logging.getLoggerClass().manager.emittedNoHandlerWarning = 1


__STARTING__ = "__STARTING__"
__STOPPING__ = "__STOPING__"
__ACTIVE__ = "__ACTIVE__"
__INACTIVE__ = "__INACTIVE__"
__STOPPED__ = "__STOPPED__"
__THREADPOOL_STATES__ = {__STARTING__: __STARTING__, __STOPPING__: __STOPPING__, __ACTIVE__: __ACTIVE__,
                         __INACTIVE__: __INACTIVE__, __STOPPED__: __STOPPED__}
_DEFAULT_MAX_WORKERS = 4


def _get_cpu_count() -> int:
    """Return the number of Logic CPU Cores on the system"""

    try:
        from multiprocessing import cpu_count
        return cpu_count()
    except:
        return _DEFAULT_MAX_WORKERS


def _async_raise(tid: int, exctype: Type[SystemExit]) -> None:
    """Raises the exception, causing the thread to exit"""

    if not inspect.isclass(exctype):
        raise TypeError("Only types can be raised (not instances)")
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid), ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("Invalid thread ID")
    elif res != 1:
        # """if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"""
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, 0)
        raise SystemError("PyThreadState_SetAsyncExc failed")


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
        log.error(f"ERROR in safe_acquire with timeout {timeout} : {e}")
        log.debug(f"[DEBUG] for safe_acquire: {traceback.format_exc()}")
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
        log.error(f"ERROR in safe_release: {e}")
        log.debug(f"[DEBUG] for safe_release: {traceback.format_exc()}")
        return False


def method_wait(func: Callable[..., Any], timeout: int = 60, delay: float = 0.1,
                incompleteVar: Optional[bool] = None, raiseExc: bool = False, *args, **kwargs):
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

    rawKwargs = {}
    rawKwargs.update(kwargs)
    kwargs.pop('failureVar', None)
    try:
        current_time = start_time = time.time()
        while current_time < start_time + timeout:
            results = func(*args, **kwargs)
            if 'failureVar' in rawKwargs and rawKwargs['failureVar'] == results:
                return results
            elif results == incompleteVar:
                time.sleep(delay)
                current_time = time.time()
            elif results is not incompleteVar:
                return results
    except Exception as e:
        log.error(f'The function {func} timed out with timeout: {timeout} using this incompleteVar: {incompleteVar}')
        log.debug(f'[DEBUG]: trace for error: {traceback.format_exc()}')
        if raiseExc:
            if isinstance(raiseExc, Exception):
                raise raiseExc
            raise e
        return e
    return incompleteVar


class Task(Event):
    """ <a name="Task"></a>
        This is a wrapper class that inherits from an Event Object and is used inside the Worker.
        It is designed to hold the function ran and save the results of the function.
    """

    priority: int = 0  # This is used to compare Tasks within the PriorityTaskQueue

    def __init__(self, fn: Callable, priority: int = 1, *args, **kwargs):
        super().__init__()
        self.args = args
        self.kwargs = kwargs
        self.priority = priority
        self.kill = kwargs.pop('kill', False) or False
        if isinstance(fn, partial):
            self.task = fn
        else:
            self.task = partial(fn, *args, **kwargs)
        self.uuid = str(uuid.uuid4())
        self._worker = None
        self.results = None

    def run(self, *args, **kwargs) -> Any:
        """ This is used to run the stored partial function and store the results.

        - :param args: Positional arguments to be passed to the task.
        - :param kwargs: Keyword arguments to be passed to the task
        - :return: (Anything)
        """
        
        if not self.isSet():
            self.set()
        self.results = self.task(*args, **kwargs)
        return self.results

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
        return super().put(item)


class Worker(threading.Thread):
    """ <a name="Worker"></a>
        This is designed to be managed by a Pool. It runs until told to stop and works tasks that come from a the
        PriorityTaskQueue maintained by the Pool.
        
    """

    taskQueue = None
    pool = None
    killed = None  # determines the Worker state. If set to true the worker will attempt to stop once Task is complete
    uuid = None
    _workerAutoKill = None
    _currentTask = None
    _timeout = 10

    def __init__(self, pool: Pool, workerAutoKill: bool = True):
        super().__init__()
        self.daemon = True
        self.uuid = str(uuid.uuid4())
        self.pool = pool
        self.taskQueue = self.pool.taskQueue
        self.killed = False
        self._workerAutoKill = workerAutoKill
        log.info(f'[INFO]: Starting new {self}')
        self.start()

    def __str__(self):
        return f'Worker Thread: {self.uuid} for Pool: {self.pool}'

    def __hash__(self):
        return hash(self.uuid)

    def _getMyTid(self) -> int:
        """ Determines the instance's thread ID

        - :return: (int)
        """

        if not self.is_alive():
            raise threading.ThreadError("Thread is not active")

        # do we have it cached?
        if hasattr(self, "_thread_id"):
            return self._thread_id

        # no, look for it in the _active dict
        for tid, tobj in getattr(threading, '_active').items():
            if tobj is self:
                self._thread_id = tid
                return tid

        raise AssertionError("Could not determine the thread's ID")

    def terminate(self) -> None:
        """ This raises a SysExit exception onto the the Worker thread un-safely killing it.

        - :return: (None)
        """

        _async_raise(self._getMyTid(), SystemExit)

    def getNextTask(self) -> Union[Task, bool, None]:
        """ This gets the next Task in the taskQueue

        - :return: (Task)
        """

        try:
            self._currentTask = None
            self._currentTask = self.taskQueue.get(timeout=self.timeout)
            self._currentTask._worker = self
            return self._currentTask
        except Empty:
            if self.timeout == 0:
                return Task(Worker.__KILL__, priority=1, kill=True)
            return False
        except Exception as e:
            log.error(f'[ERROR]: Error in getting task: {e}')
            log.debug(f'[DEBUG]: trace for error in getting task: {traceback.format_exc()}')
            return None

    def run(self) -> None:
        """ This is an override of the run method within Thread that loops constantly waiting on another task.

        - :return: (None)
        """

        try:
            while True:
                # log.info(f'Thread getting next task')
                task = self.getNextTask()
                if task is None:
                    log.info(f'task is none closing the thread')
                    break
                elif task is not False:
                    log.info(f'The task is: {task}')
                    task()
                    if task.kill:
                        log.info(f'Killing thread once task is complete: {task}')
                        self.killed = True
                        break
                    self.taskQueue.task_done()
        except Exception as e:
            log.error(f'[ERROR]: While Worker thread is running with task: {self._currentTask} Error: {e}')
            log.debug(f'[DEBUG]: trace for error: {traceback.format_exc()}')
        finally:
            if self._currentTask is not None:
                if not self._currentTask.isSet():
                    self._currentTask.set()
                self._currentTask = None
                self.taskQueue.task_done()
            if self.killed is not True:
                self.killed = True
            self.pool.removeWorker(workerTooRemove=self)

    def isAlive(self) -> bool:
        return super().is_alive()

    @staticmethod
    def __KILL__(*args, **kwargs) -> None:
        pass

    @property
    def timeout(self) -> int:
        """+ This property will return 0 if it thinks it doesn't need to run any longer and is ready to self terminate
        """

        if (self._workerAutoKill and self.pool.numWorkers > 1) or self.pool.numWorkers > self.pool.maxWorkers:
            return 0
        if self.killed is True:
            return 0
        return self._timeout

    @property
    def currentPriority(self) -> int:
        """+ This changes to the priority of each incoming task. """

        try:
            if self._currentTask:
                return self._currentTask.priority
            return 0
        except Exception as e:
            log.error(f'ERROR: {e}')
            return 0

    @property
    def isActive(self) -> bool:
        """+ This determines if the Worker currently has a Task to work. """
        return self._currentTask is not None


class Pool:
    """ <a name="Pool"></a>
        This manages a pool of Workers and a queue of Tasks. The workers consume tasks from the taskQueue until they
        are told to stop.

    """

    uuid = None
    taskQueue = None
    maxWorkers = None
    _timeout = None
    _workerList = None
    _workerListLock = None
    _state = __INACTIVE__
    _stateLock = None
    _taskLock = None
    _workerAutoKill = None

    def __init__(self, maxWorkers: Optional[int] = None, tasks: Optional[PriorityTaskQueue] = None, daemon: bool = True,
                 timeout: int = 60, workerAutoKill: bool = True, prepopulate: int = 0):
        self.uuid = str(uuid.uuid4())
        self.maxWorkers = maxWorkers or _get_cpu_count()
        self._timeout = timeout
        self._workerAutoKill = workerAutoKill
        self._workerListLock = Lock()
        self._stateLock = Lock()
        self._taskLock = RLock()
        self.taskQueue = tasks or PriorityTaskQueue()
        self.workers = []
        self.state = __STARTING__
        if prepopulate:
            self.setupWorkers(numOfWorkers=prepopulate, workerAutoKill=False)
        elif self.taskQueue.qsize() > 0:
            self.setupWorkers(numOfWorkers=self.taskQueue.qsize() if self.taskQueue.qsize() <= 8 else 8,
                              workerAutoKill=False)
        else:
            self.setupWorkers(numOfWorkers=1, workerAutoKill=False)
        # noinspection PyTypeChecker
        # self.submit(tasks)
        if daemon is False:
            self.state = __ACTIVE__
            self.join(self._timeout)
            self.shutdown(timeout=self._timeout)

    def __enter__(self):
        self._taskLock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._taskLock.release()
        except Exception as e:
            log.error(f"ERROR in __exit__ of Pool: {e}")
            log.debug(f"[DEBUG] for __exit__ of Pool: {traceback.format_exc()}")

    def __str__(self):
        return f'Pool(UUID={self.uuid}, State={self._state})'

    def setupWorkers(self, numOfWorkers: Optional[int] = None, workerAutoKill: Optional[bool] = None) -> bool:
        """ Generally only used by init. This setups Worker threads to be managed by the Pool.

        - :param numOfWorkers: (int) Number workers setup. IF the number of workers is higher then the value of
                'maxWorkers' then 'maxWorkers' is updated. The numOfWorkers is how many Workers the Pool has *not* now
                many new Workers get added.
        - :param workerAutoKill: (bool) This determines if the worker ends once their is no longer any work left in
                the 'taskQueue'.
        - :return: (bool)
        """

        if self.state == __STOPPING__:
            return False
        if workerAutoKill is None:
            workerAutoKill = self._workerAutoKill
        if numOfWorkers is None:
            numOfWorkers = 1
        if numOfWorkers > self.maxWorkers:
            self.setMaxWorkers(numOfWorkers)
            numOfNewWorkers = (numOfWorkers - self.numWorkers)
        elif numOfWorkers > (self.maxWorkers - self.numWorkers):
            numOfNewWorkers = (self.maxWorkers - self.numWorkers)
        else:
            numOfNewWorkers = numOfWorkers
        for x in range(0, numOfNewWorkers):
            self.addWorker(workerAutoKill=workerAutoKill)
        return numOfNewWorkers > 0

    def addWorker(self, workerAutoKill: Optional[bool] = None) -> bool:
        """ Adds a single new worker too the Pool.

        - :param workerAutoKill: (bool) This determines if the worker ends once their is no longer any work left in
        - :return: (bool)
        """

        log.debug(f"Attempting to add new worker!")
        if self.state == __STOPPING__:
            return False
        if self.numWorkers >= self.maxWorkers:
            return False
        if workerAutoKill is None:
            workerAutoKill = self._workerAutoKill
        self.workers.append(Worker(self, workerAutoKill=workerAutoKill))
        return True

    def removeWorker(self, workerTooRemove: Optional[Worker] = None) -> bool:
        """ Removes a single new worker from the Pool. This can be called to remove any 1 random Worker or you can
            specify a Worker to remove.

        - :param workerTooRemove: (Worker) This is usually sent when a Worker is self terminating
        - :return: (bool)
        """

        try:
            if self.numWorkers <= 0:
                return False
            if workerTooRemove is not None:
                worker = self.workers.pop(self.workers.index(workerTooRemove))
                if worker.killed is not True:
                    log.warning(f'[WARN]: worker({worker}) was forcefully removed.')
                    worker.killed = True
                if self.numWorkers == 0:
                    self.state = __INACTIVE__
            else:
                self.submit(Task(Worker.__KILL__, priority=self.highestPriority + 1, kill=True),
                            ubmit_task_autospawn=False)
            return True
        except Exception as e:
            log.error(f'[ERROR]: Error occurred while attempting to remove worker: {e}')
            log.debug(f'[DEBUG]: Trace for error while attempting to remove worker: {traceback.format_exc()}')
            return False

    def setMaxWorkers(self, maxWorkers: int) -> int:
        """ Set the maximum number of threads that will remain active. Return the maximum thread limit.

        - :param maxWorkers: (int) Max thread limit
        - :return: (int)
        """

        if type(maxWorkers) is int and maxWorkers > -1:
            self.maxWorkers = maxWorkers
        return self.maxWorkers

    def waitCompletion(self, timeout: Union[int, float], delay: Union[int, float] = 0.1, block: bool = False) -> bool:
        """ This method waits until all Tasks in the PriorityTaskQueue is done. If the parameter block is True it will
            stop any new Task from being submitted while waiting.

        - :param timeout: (int/float) How long to wait for all tasks in the 'taskQueue' to be finished.
        - :param delay: (int/float) The amount of time to sleep before checking again in seconds. Default 0.1.
        - :param block: (bool) This will stop new tasks from being submitted to the Queue until finished.
        - :return: (bool)
        """

        def _waitCompletion(waitTime: Union[int, float]) -> bool:
            """
                NOTE: This function is used when unsafe is False.
            """
            current_time = start_time = time.time()
            while current_time < start_time + waitTime:
                if self.unfinishedTasks == 0:
                    return True
                time.sleep(delay)
                current_time = time.time()
            return False

        if block:
            with self._taskLock:
                return _waitCompletion(timeout)
        return _waitCompletion(timeout)

    def shutdown(self, timeout: Optional[int] = None, unsafe: Optional[bool] = None) -> bool:
        """ This sends a kill operation too all the workers and waits for them to complete and then removes the threads.
            It can also attempt to kill Workers in an unsafe way with the 'terminate' Worker method.

        - :param timeout: (int) The length of time to wait on tasks to be stopped
        - :param unsafe: (bool/None) True: The 'terminate' method will be called on each Worker. False: Even if the
                timeout is reached the 'terminate' method will *not* be called. None: This will attempt to safely wait
                for the Workers too finish but if timeout is reached then the 'terminate' method will be called.
        - :return: (bool)
        """

        self.state = __STOPPING__
        if timeout is None:
            timeout = self._timeout

        def _clearHelper(task):
            return task.task.func != Worker.__KILL__

        def _clearShutdownTasks():
            try:
                tasks = []
                while not self.taskQueue.empty():
                    tasks.append(self.taskQueue.get())
                    self.taskQueue.task_done()
                for task in filter(_clearHelper, tasks):
                    self.taskQueue.put_nowait(task)
            except Exception as e:
                log.error(f'[ERROR]: Error while clearing old tasks: {e}')
                log.debug(f'[DEBUG]: Trace for error clearing old tasks: {traceback.format_exc()}')

        def _unsafeShutdown():
            for worker in self.workers:
                log.info(f'Worker: {worker} will be killed unsafely.')
                worker.terminate()

        if unsafe:
            _unsafeShutdown()
            time.sleep(0.1)
            return self.numWorkers == 0

        with self._taskLock:
            for x in range(0, self.numWorkers):
                self.removeWorker()
            current_time = start_time = time.time()
            while current_time < start_time + timeout:
                if self.numWorkers <= 0:
                    log.info(f'There are no more workers. No need for forced timeout')
                    break
                time.sleep(0.1)
                current_time = time.time()
            if unsafe is None:
                _unsafeShutdown()
                time.sleep(0.1)
            _clearShutdownTasks()

        if self.numWorkers == 0:
            self.state = __STOPPED__
            return True
        return False

    def join(self, timeout: int) -> bool:
        """ This first calls 'waitCompletion' with 'block=True' and then calls 'shutdown'. The goal is to try to wait
            for all Tasks to complete and then close out the Pool.

        - :param timeout: (int)The length of time to wait on both join and shutdown.
        - :return: (bool)
        """

        with wait_lock(self._taskLock, timeout=timeout):
            start_time = time.time()
            self.waitCompletion(timeout, block=True)
            return self.shutdown(timeout=int(max(timeout - (time.time() - start_time), 1)))

    def map(self, fn: Callable, params: Tuple, chunksize: int = 0, **kwargs) -> None:
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

        def autospawnParser(tmpAutospawn, state):
            if state == __STOPPING__ or state == __STOPPED__:
                return False
            if self.needsWorkers and tmpAutospawn is None:
                return True
            return tmpAutospawn

        with self._taskLock:
            nowait = kwargs.pop('submit_task_nowait', True)
            timeout = kwargs.pop('submit_task_timeout', 10)
            autospawn = autospawnParser(kwargs.pop('submit_task_autospawn', None), self.state)
            priority = kwargs.pop('submit_task_priority', 10) or 10
            if isinstance(fn, Task):
                task = fn
            else:
                task = Task(fn, priority, *args, **kwargs)

            try:
                if nowait:
                    self.taskQueue.put_nowait(task)
                else:
                    self.taskQueue.put(task, timeout=timeout)
                # print(f'unfinishedTasks: {self.unfinishedTasks}\nworkers: '
                #       f'{self.workers}\nneedsWorkers: {self.needsWorkers}')
                if autospawn or autospawn is None and self.needsWorkers:
                    self.addWorker()
                return task
            except Exception as e:
                log.error(f'Error in submitting task: {e}\n{traceback.format_exc()}')
                return False
            finally:
                if self.state == __STARTING__:
                    self.state = __ACTIVE__

    @property
    def unfinishedTasks(self) -> int:
        """+ This calls the 'unfinishedTasks' property of PriorityTaskQueue. And is equal to the number of tasks
            submitted minus the number of times a Task has been Worked by a Worker.
        """
        return self.taskQueue.unfinished_tasks

    @property
    def numQueuedTasks(self) -> int:
        """ This is a wrapper for the 'qsize()' method from PriorityTaskQueue."""
        return self.taskQueue.qsize()

    @property
    def numActiveTasks(self) -> int:
        return self.unfinishedTasks - self.numQueuedTasks

    @property
    def hasTasks(self) -> bool:
        return self.unfinishedTasks > 0

    @property
    def isIdle(self) -> bool:
        return not (self.hasTasks and self.hasWorkers)

    @property
    def isActive(self) -> bool:
        """+ This determines is the Pool both has workers and has work to do. This doesn't have anything to do with a
            Pool's state.
        """
        return self.hasTasks and self.hasWorkers

    @property
    def hasWorkers(self) -> bool:
        return self.activeWorkers > 0

    @property
    def needsWorkers(self) -> bool:
        if self.numWorkers < self.maxWorkers:
            if self.numQueuedTasks > self.inactiveWorkers:
                return True
        return False

    @property
    def numWorkers(self) -> int:
        try:
            return len(self.workers)
        except Exception:
            return 0

    @property
    def activeWorkers(self) -> int:
        return len([i for i in self.workers if i.isActive])

    @property
    def inactiveWorkers(self) -> int:
        return len([i for i in self.workers if not i.isActive])

    @property
    def highestPriority(self) -> int:
        return max([i.currentPriority for i in self.workers])

    @property
    def workers(self) -> List:
        """+ This is a protected (wrapped in a lock) List of Workers managed by this pool. """
        try:
            with wait_lock(self._workerListLock, self._timeout):
                return self._workerList
        except RuntimeError:
            pass

    @workers.setter
    def workers(self, value) -> None:
        try:
            with wait_lock(self._workerListLock, self._timeout):
                self._workerList = value
        except RuntimeError:
            pass

    @workers.deleter
    def workers(self) -> None:
        try:
            with wait_lock(self._workerListLock, self._timeout):
                self._workerList = []
        except RuntimeError:
            pass

    @property
    def state(self) -> str:
        """+ This is a string that has only 4 valid string values that determines the state of the Pool."""
        try:
            with wait_lock(self._stateLock, self._timeout):
                return self._state
        except RuntimeError:
            pass

    @state.setter
    def state(self, value) -> None:
        try:
            with wait_lock(self._stateLock, self._timeout):
                if value in __THREADPOOL_STATES__:
                    self._state = __THREADPOOL_STATES__[value]
                else:
                    raise TypeError('Invalid ThreadPool STATE: %s' % str(value))
        except RuntimeError:
            pass

    @state.deleter
    def state(self) -> None:
        try:
            with wait_lock(self._stateLock, self._timeout):
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

    def remainingSets(self) -> int:
        """ Is a wrapper around the private variable '_counter'.

        - :return: (int)
        """

        with self._ActionLock:
            return self._counter

    @property
    def numOfRequiredSets(self) -> int:
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

        if not filter(MultipleEvents.waitFilter, self._events):
            return True
        return False

    def clear(self) -> None:
        """ Reset the internal flag to false on all events.

        - :return: (None)
        """

        for event in self._events:
            event.clear()

    def removeEvents(self) -> None:
        """ This deletes all events making this object useless until new events are added.

        - :return: (None)
        """

        del self._events
        self._events = []

    def addEvent(self, event: Event) -> None:
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

        log.warning("This set is ignored!")

    @staticmethod
    def waitFilter(event: Event) -> bool:
        """ Simply calls and returns 'is_set' method of a given Event object.

        - :param event: (Event)
        - :return: (bool)
        """

        return not event.is_set()


if __name__ == '__main__':
    print(f'This should be called as a module.')
