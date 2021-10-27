#!/usr/bin/env python3
# -*- coding=utf-8 -*-

# Author: Ryan Henrichson
# Version: 2.0


import logging
import threading
from threading import RLock
from PyMultiTasking.utils import wait_lock, Worker, Pool, __PyMultiDec
from PyMultiTasking.utils import __async_raise as a_raise


# logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(funcName)s %(lineno)s %(message)s',
#                     level=logging.DEBUG)
log = logging.getLogger('Threading')
# logging.getLoggerClass().manager.emittedNoHandlerWarning = 1


class Threaded(__PyMultiDec):
    """<a name="Threaded"></a>
        To be used as a Decorator. When decorating a function/method that callable when be run in a Python thread.
        The function will return a 'Task' object.
    """

    def __init__(self, *args, **kwargs):
        self.wType = ThreadWorker
        self.pType = ThreadPool
        super(Threaded, self).__init__(*args, **kwargs)


class ThreadWorker(Worker, threading.Thread):
    """ <a name="ThreadWorker"></a>
        This is designed to be managed by a ThreadPool. However, it can run on its own as well. It runs until told to
        stop and works tasks that come from a the PriorityTaskQueue maintained by the Pool.
    """

    workerType = 'THREAD'

    def __init__(self, *args, **kwargs):
        kwargs.update({'log': log})
        super(ThreadWorker, self).__init__(*args, **kwargs)

    def __get_my_tid(self) -> int:
        """ Determines the instance's thread ID

        - :return: (int)
        """

        if not self.is_alive():
            raise threading.ThreadError("Thread is not active")

        if hasattr(self, "_thread_id"):
            return self._thread_id

        for tid, tobj in getattr(threading, '_active', dict()).items():
            if tobj is self:
                self._thread_id = tid
                return tid

        raise AssertionError("Could not determine the thread's ID")

    def terminate(self) -> None:
        """ This raises a SysExit exception onto the the Worker thread un-safely killing it.

        - :return: (None)
        """

        a_raise(self.__get_my_tid(), SystemExit)

    def kill(self) -> None:
        self.safe_stop()
        return self.terminate()


# noinspection PyPep8Naming
class ThreadPool(Pool):
    """ <a name="ThreadPool"></a>
        This manages a pool of ThreadWorkers that get tasks from a 'PriorityTaskQueue'. The workers consume tasks from
        the taskQueue until they are told to stop. The ThreadPool class keeps a registry of all ThreadPools objects.
    """

    __regRLock = RLock()
    __pool_registry = []

    def __init__(self, *args, **kwargs):
        kwargs.update({'log': log})
        super(ThreadPool, self).__init__(ThreadWorker, *args, **kwargs)
        ThreadPool.register_pool(self)

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


if __name__ == '__main__':
    print(f'This should be called as a module.')
