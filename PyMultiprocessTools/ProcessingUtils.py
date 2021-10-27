#!/usr/bin/env python3
# -*- coding=utf-8 -*-

# Author: Ryan Henrichson
# Version: 1.0


from __future__ import annotations

import logging
import multiprocessing
from multiprocessing import RLock, Process
from PyMultiprocessTools.utils import wait_lock, Worker, Pool, __PyMultiDec


# logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(funcName)s %(lineno)s %(message)s',
#                     level=logging.INFO)
log = logging.getLogger('Processing')
# logging.getLoggerClass().manager.emittedNoHandlerWarning = 1


def set_start_method(method=None, force=False):
    """
        This changes the start method to 'spawn' by default. It captures the exception if thrown and returns a false
        instead.
    """
    method = 'spawn' if method is None else method
    if method in multiprocessing.get_all_start_methods():
        try:
            multiprocessing.set_start_method(method, force=force)
        except:
            return False
        else:
            return True


class Processed(__PyMultiDec):
    """<a name="Processed"></a>
        To be used as a Decorator. When decorating a function/method that callable when be run in a Python Process.
        The function will return a 'Task' object.
    """

    def __init__(self, *args, **kwargs):
        self.wType = ProcessWorker
        self.pType = ProcessPool
        super(Processed, self).__init__(*args, **kwargs)


class ProcessWorker(Worker, Process):
    """ <a name="ProcessWorker"></a>
        This is designed to be managed by a ProcessPool. However, it can run on its own as well. It runs until told to
        stop and works tasks that come from a the ProcessTaskQueue maintained by the ProcessPool.
    """

    __workerAutoKill = True
    __defaultTimeout = 10
    workerType = 'PROCESS'

    def __init__(self, *args, **kwargs):
        kwargs.update({'log': log})
        super(ProcessWorker, self).__init__(*args, **kwargs)


# noinspection PyPep8Naming
class ProcessPool(Pool):
    """ <a name="ProcessPool"></a>
        This manages a pool of ProcessWorkers that get tasks from a 'ProcessTaskQueue'. The workers consume tasks from
        the taskQueue until they are told to stop. The ProcessPool class keeps a registry of all ProcessPool objects.
    """

    __regRLock = RLock()
    __pool_registry = []

    def __init__(self, *args, **kwargs):
        kwargs.update({'log': log})
        super(ProcessPool, self).__init__(ProcessWorker, *args, **kwargs)
        ProcessPool.register_pool(self)

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
