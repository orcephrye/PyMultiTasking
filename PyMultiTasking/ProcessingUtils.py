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
import uuid
from functools import partial
from threading import Event
from multiprocessing import RLock, Process
from PyMultiTasking.Tasks import ProcessTask, ProcessTaskQueue
from PyMultiTasking.libs import Worker, Pool, __PyMultiDec, get_cpu_count, __INACTIVE__, __STOPPING__, __STOPPED__
from PyMultiTasking.utils import wait_lock, dummy_func
from PyMultiTasking.PipeSynchronize import PipeRegister
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
        self.comms_pipes = kwargs.get('comms_pipes', True)
        kwargs.update({'name': kwargs.pop('name', str(uuid.uuid4()))})
        if self.comms_pipes:
            self.pipereg = PipeRegister.get_pipereg_by_name(kwargs.get('name', '')+'_pool', automake=True)
        super(ProcessPool, self).__init__(*args, **kwargs)
        ProcessPool.register_pool(self)

    def add_worker(self, *args, **kwargs) -> bool:
        self.__waitingEvent.wait(timeout=0.1)
        if self.comms_pipes and self.pipereg:
            kwargs.update({'name': kwargs.pop('name', str(uuid.uuid4()))})
            _, child_conn = self.pipereg.create_safepipe(kwargs.get('name', '')+'_worker')
            kwargs.update({'communication_pipe': child_conn})
        return super(ProcessPool, self).add_worker(*args, **kwargs)

    @property
    def unfinished_tasks(self) -> int:
        """ This calls the 'unfinishedTasks' property of PriorityTaskQueue. And is equal to the number of tasks
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