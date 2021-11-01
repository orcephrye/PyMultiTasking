#!/usr/bin/env python3
# -*- coding=utf-8 -*-

# Author: Ryan Henrichson
# Version: 1.0


from __future__ import annotations

import logging
import traceback
import multiprocessing
import math
import time
from functools import partial
from threading import Event
from multiprocessing import RLock, Process
from PyMultiTasking.Tasks import ProcessTask, ProcessTaskQueue
from PyMultiTasking.libs import Worker, Pool, __PyMultiDec, get_cpu_count, __INACTIVE__, __STOPPING__, __STOPPED__
from PyMultiTasking.utils import wait_lock, dummy_func
from typing import Optional


# logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(funcName)s %(lineno)s %(message)s',
#                     level=logging.DEBUG)
log = logging.getLogger('Processing')
# logging.getLoggerClass().manager.emittedNoHandlerWarning = 1


def set_start_method(method=None, force=False):
    """
        This changes the start method to 'fork' by default. It captures the exception if thrown and returns a false
        instead.
    """
    method = 'fork' if method is None else method
    if method in multiprocessing.get_all_start_methods():
        try:
            multiprocessing.set_start_method(method, force=force)
        except:
            return False
        else:
            return True


class ProcessWorker(Worker, Process):
    """ <a name="ProcessWorker"></a>
        This is designed to be managed by a ProcessPool. However, it can run on its own as well. It runs until told to
        stop and works tasks that come from a the ProcessTaskQueue maintained by the ProcessPool.
    """

    workerType = 'PROCESS'
    taskObj = ProcessTask

    def __init__(self, *args, **kwargs):
        kwargs.update({'log': log})
        super(ProcessWorker, self).__init__(*args, **kwargs)


# noinspection PyPep8Naming
class ProcessPool(Pool):
    """ <a name="ProcessPool"></a>
        This manages a pool of ProcessWorkers that get tasks from a 'ProcessTaskQueue'. The workers consume tasks from
        the taskQueue until they are told to stop. The ProcessPool class keeps a registry of all ProcessPool objects.
    """

    poolType = 'PROCESS'
    workerObj = ProcessWorker
    taskObj = ProcessTask
    queueObj = ProcessTaskQueue
    __regRLock = RLock()
    __pool_registry = []

    def __init__(self, *args, **kwargs):
        self.__waitingEvent = Event()
        kwargs.update({'log': log, 'tasks': ProcessTaskQueue()})
        if 'maxWorkers' not in kwargs:
            kwargs.update({'maxWorkers': math.ceil(get_cpu_count() / 2)})
        if 'workerAutoKill' not in kwargs:
            kwargs.update({'workerAutoKill': False})
        super(ProcessPool, self).__init__(*args, **kwargs)
        self.__timeout = self._Pool__timeout
        self.__taskLock = self._Pool__taskLock
        ProcessPool.register_pool(self)

    def add_worker(self, *args, **kwargs) -> bool:
        self.__waitingEvent.wait(timeout=0.1)
        return super(ProcessPool, self).add_worker(*args, **kwargs)

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
            while current_time < start_time + wait_time and wtr.is_alive():
                ev.wait(timeout=0.1)
            return not wtr.is_alive()

        def _filterHelper(wtr):
            return not wtr.is_alive()

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
                    self.workers.pop(self.workers.index(workerTooRemove))
                    return True
                self.log.warning(f'[WARN]: worker({workerTooRemove}) needs to be terminated in order to be removed.')
                getattr(workerTooRemove, 'terminate', dummy_func)()
                if wait_helper(timeout, time.monotonic(), e, workerTooRemove):
                    self.workers.pop(self.workers.index(workerTooRemove))
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
                self.submit(self.taskObj(Worker.__KILL__, priority=self.highest_priority + 1, kill=True),
                            submit_task_autospawn=False)
                if timeout > 0:
                    current = start = time.monotonic()
                    while current < start + timeout and self.num_workers >= current_num:
                        e.wait(timeout=0.1)
                        for worker in filter(_filterHelper, self.workers):
                            self.workers.pop(self.workers.index(worker))
                    return self.num_workers < current_num
                return True
        except Exception as e:
            self.log.error(f'[ERROR]: Error occurred while attempting to remove worker: {e}')
            self.log.debug(f'[DEBUG]: Trace for error while attempting to remove worker: {traceback.format_exc()}')
            return False
        finally:
            if self.num_workers == 0:
                self.state = __INACTIVE__

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

        def _filterHelper(wtr):
            return not wtr.is_alive()

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
            ct = st = time.monotonic()
            while ct < st + 1 and self.num_workers > 0:
                e.wait(timeout=0.1)
                for w in filter(_filterHelper, self.workers):
                    self.workers.pop(self.workers.index(w))
                ct = time.monotonic()

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
                for work in filter(_filterHelper, self.workers):
                    self.workers.pop(self.workers.index(work))
                if self.num_workers <= 0:
                    self.log.info(f'There are no more workers. No need for forced timeout')
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

    @property
    def unfinished_tasks(self) -> int:
        """+ This calls the 'unfinishedTasks' property of PriorityTaskQueue. And is equal to the number of tasks
            submitted minus the number of times a Task has been Worked by a Worker.
        """
        return getattr(getattr(self.taskQueue, '_unfinished_tasks', None),
                       'get_value', partial(dummy_func, _default=0))()

    @classmethod
    def get_pool_by_name(cls, name, timeout=60):
        with wait_lock(cls.__regRLock, timeout=timeout, raise_exc=False) as acquired:
            if acquired is False:
                return False
            for pool in cls.__pool_registry:
                if name == pool.name:
                    return pool

    @classmethod
    def get_pools(cls, name=None, timeout=60):
        with wait_lock(cls.__regRLock, timeout=timeout, raise_exc=False) as acquired:
            if acquired is False:
                return []
            if name:
                return [pool for pool in cls.__pool_registry if name == pool.name]
            return cls.__pool_registry

    @classmethod
    def join_pools(cls, timeout=60):
        with wait_lock(cls.__regRLock, timeout=timeout, raise_exc=False) as acquired:
            if acquired is False:
                return False
            for pool in cls.__pool_registry:
                pool.join(timeout=timeout)
            return True

    @classmethod
    def register_pool(cls, pool, timeout=60):
        with wait_lock(cls.__regRLock, timeout=timeout, raise_exc=True):
            if isinstance(pool, Pool):
                cls.__pool_registry.append(pool)


class Processed(__PyMultiDec):
    """<a name="Processed"></a>
        To be used as a Decorator. When decorating a function/method that callable when be run in a Python Process.
        The function will return a 'Task' object.
    """

    wType = ProcessWorker
    pType = ProcessPool
    task = ProcessTask

    def __init__(self, *args, **kwargs):
        super(Processed, self).__init__(*args, **kwargs)