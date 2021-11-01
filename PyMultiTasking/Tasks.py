#!/usr/bin/env python3
# -*- coding=utf-8 -*-

from __future__ import annotations

import logging
import traceback
import inspect
import uuid
import time
from multiprocessing import RLock as MultiProcRLock
from threading import RLock as ThreadRLock
from threading import Semaphore as SemaphoreThreading
from threading import Event as EventThreading
from queue import PriorityQueue
import multiprocessing
from multiprocessing import Event, Semaphore
from multiprocessing.queues import JoinableQueue
from functools import partial
from abc import abstractmethod, ABC
from PyMultiTasking.PipeSynchronize import PipeRegister
from typing import Optional, Callable, Any, Union


# logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(funcName)s %(lineno)s %(message)s',
#                     level=logging.DEBUG)
_log = logging.getLogger('MultiTaskingTools')


def dummy_func(*args, **kwargs):
    return kwargs.get('_default', None)


class Task(ABC):

    priority = 0
    taskType = ''

    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def run(self, *args, **kwargs):
        pass

    @abstractmethod
    def clear(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_pipe(self, *args, **kwargs):
        pass

    @abstractmethod
    def set_original(self, *args, **kwargs):
        pass

    @abstractmethod
    def __hash__(self, *args, **kwargs):
        pass

    @abstractmethod
    def __call__(self, *args, **kwargs):
        pass

    @abstractmethod
    def __str__(self, *args, **kwargs):
        pass

    @abstractmethod
    def __gt__(self, *args, **kwargs):
        pass

    @abstractmethod
    def __lt__(self, *args, **kwargs):
        pass

    @abstractmethod
    def __ge__(self, *args, **kwargs):
        pass

    @abstractmethod
    def __le__(self, *args, **kwargs):
        pass

    @property
    @abstractmethod
    def worker(self, *args, **kwargs):
        pass

    @worker.setter
    @abstractmethod
    def worker(self, *args, **kwargs):
        pass

    @worker.deleter
    @abstractmethod
    def worker(self, *args, **kwargs):
        pass

    @property
    @abstractmethod
    def results(self, *args, **kwargs):
        pass

    @results.setter
    @abstractmethod
    def results(self, *args, **kwargs):
        pass

    @results.deleter
    @abstractmethod
    def results(self, *args, **kwargs):
        pass


class ThreadTask(EventThreading, Task):
    """ <a name="ThreadTask"></a>
        This is a wrapper class that inherits from an Event Object and is used inside the Worker.
        It is designed to hold the function ran and save the results of the function.
    """

    defaultpriority: int = 1
    taskType = 'THREAD'

    def __init__(self, fn: Callable, priority: int = 1, kill: bool = False, inject_task: bool = True,
                 store_return: bool = True,  callback_func: Optional[Callable] = None,
                 semaphore: Optional[SemaphoreThreading] = None, *args, **kwargs):
        super(ThreadTask, self).__init__()
        self.args = args
        self.kwargs = kwargs
        self.priority = priority
        self.kill = kill
        self.callback_fun = callback_func
        self.semaphore = semaphore if semaphore is not None else SemaphoreThreading(1)
        if isinstance(fn, partial):
            if inject_task and ThreadTask.__inspect_kwargs(fn.func):
                fn.keywords.update({'TaskObject': self})
            self.task = fn
        else:
            if inject_task and ThreadTask.__inspect_kwargs(fn):
                self.kwargs.update({'TaskObject': self})
            self.task = partial(fn, *self.args, **self.kwargs)
        self.store_return = store_return
        self.uuid = str(uuid.uuid4())
        self.__updateRLock = ThreadRLock()
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
            if self.store_return and self.callback_fun is None:
                return self.results

    def clear(self) -> None:
        if self.is_set():
            raise Exception('A Task Object cannot be cleared once set!')
        return super(ThreadTask, self).clear()

    def get_pipe(self):
        return getattr(self.worker, 'communication_pipe', None)

    def set_original(self, *args, **kwargs):
        return

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
        with self.__updateRLock:
            return self.__worker

    @worker.setter
    def worker(self, value):
        with self.__updateRLock:
            self.__worker = value

    @worker.deleter
    def worker(self):
        with self.__updateRLock:
            del self.__worker


    @property
    def results(self):
        with self.__updateRLock:
            return self.__results

    @results.setter
    def results(self, value):
        with self.__updateRLock:
            self.__results = value

    @results.deleter
    def results(self):
        with self.__updateRLock:
            del self.__results


class ProcessTask(Task):
    """ <a name="ThreadTask"></a>
        This is a wrapper class that inherits from an Event Object and is used inside the Worker.
        It is designed to hold the function ran and save the results of the function.
    """

    defaultpriority: int = 1
    taskType = 'PROCESS'

    def __init__(self, fn: Callable, priority: int = 1, kill: bool = False, inject_task: bool = True,
                 store_return: bool = False, callback_func: Optional[Callable] = None,
                 semaphore: Optional[Semaphore] = None, store_via_pipe: bool = True, *args, **kwargs):
        self.__original = True
        self.uuid = str(uuid.uuid4())
        self.event = Event()
        self.semaphore = semaphore if semaphore is not None else Semaphore(1)
        pipereg = PipeRegister.get_pipereg_by_name('ProcessTaskPipes')
        if pipereg is None:
            pipereg = PipeRegister(name='ProcessTaskPipes')
        self.__p_state, self.__c_state = pipereg.create_safepipe(pipe_id=self.uuid + '_state')
        self.args = args
        self.kwargs = kwargs
        self.priority = priority
        self.kill = kill
        self.callback_fun = callback_func
        if isinstance(fn, partial):
            if inject_task and ProcessTask.__inspect_kwargs(fn.func):
                fn.keywords.update({'TaskObject': self})
            self.task = fn
        else:
            if inject_task and ProcessTask.__inspect_kwargs(fn):
                self.kwargs.update({'TaskObject': self})
            self.task = partial(fn, *self.args, **self.kwargs)
        self.store_return = store_return
        self.store_via_pipe = store_via_pipe
        self.__updateRLock = ThreadRLock()
        self.__results_updated = False
        self.__worker = None
        self.__results = None

    def __getstate__(self):
        return {k: v for k, v in self.__dict__.items() if k not in ('event', 'semaphore', '_ProcessTask__p_state',
                                                                    '_ProcessTask__updateRLock')}

    def __setstate__(self, state):
        self.__dict__ = state
        self.__original = False
        self.__p_state = None
        self.event = Event()
        self.semaphore = Semaphore(1)
        self.__updateRLock = MultiProcRLock()

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
            elif self.store_via_pipe:
                if self.callback_fun:
                    results = self.callback_fun(self.task(*args, **kwargs))
                else:
                    results = self.task(*args, **kwargs)
                self.__update_state((self.is_set(),
                                     getattr(self.worker, 'uuid', None),
                                     results))
                return results
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
            if self.store_return and self.callback_fun is None:
                return self.results

    def set(self) -> None:
        if self.is_set():
            raise Exception('A Task Object cannot be set more then once!')
        self.event.set()
        self.semaphore.release()
        self.__update_state()
        self.__close_pipes()

    def clear(self) -> None:
        if self.is_set():
            raise Exception('A Task Object cannot be cleared once set!')
        return super(ProcessTask, self).clear()

    def is_set(self):
        if self.__original:
            self.sync_state()
        return self.event.is_set()

    def wait(self, timeout=None):
        if self.__original is False:
            return self.event.wait(timeout=timeout)
        if self.is_set():
            return True
        if timeout is None:
            while self.event.wait(timeout=0.1) is False:
                self.sync_state()
            return self.is_set()
        current_time = start_time = time.monotonic()
        while current_time < start_time + timeout:
            if self.event.wait(timeout=0.1):
                return True
            self.sync_state()
            current_time = time.monotonic()
        return False

    def get_pipe(self):
        return getattr(self.worker, 'communication_pipe', None)

    def set_original(self, value):
        with self.__updateRLock:
            self.__original = value

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

    def sync_state(self, state=None):
        def _sync_helper(s):
            if s is None:
                s = self.__get_state()
            if s is False:
                return False
            is_set, worker, results = s
            if self.__worker is None and worker is not None:
                self.__worker = worker
            if self.__results is None and not self.__results_updated and results is not None:
                self.results = results
            if is_set is True and self.is_set() is False:
                self.set()
            return True

        try:
            with self.__updateRLock:
                if state is not None:
                    return _sync_helper(state)
                while self.__p_state.isActive and self.__p_state.poll():
                    _sync_helper(None)
                return True
        except Exception as e:
            _log.error(f"ERROR in sync_state: {e}")
            _log.debug(f"[DEBUG] for sync_state: {traceback.format_exc()}")
            return False

    def __update_state(self, state=None):
        try:
            if self.__original is True:
                return
            with self.__c_state as pipe:
                if pipe.writable and not pipe.closed:
                    if state and len(state) == 3:
                        pipe.send(state)
                        return
                    pipe.send((self.is_set(),
                               getattr(self.worker, 'uuid', None),
                               self.__results if self.__results_updated is False else None))
        except Exception as e:
            _log.error(f"ERROR in __update_state: {e}")
            _log.debug(f"[DEBUG] for __update_state: {traceback.format_exc()}")
            return

    def __get_state(self):
        try:
            if self.__original is False:
                return False
            with self.__p_state as pipe:
                if pipe.closed:
                    return False
                if not pipe.poll():
                    return False
                state = pipe.recv()
                if len(state) != 3:
                    return False, None, None
                return state
        except Exception as e:
            _log.error(f"ERROR in __get_state: {e}")
            _log.debug(f"[DEBUG] for __get_state: {traceback.format_exc()}")
            return False, None, None

    def __close_pipes(self):

        def __clear_pipe(safe_pipe):
            with safe_pipe as pipe:
                try:
                    while pipe.closed is not False and pipe.poll():
                        pipe.recv()
                except Exception as e:
                    _log.error(f"ERROR in __clear_pipe: {e}")
                    _log.debug(f"[DEBUG] for __clear_pipe: {traceback.format_exc()}")
                finally:
                    pipe.close()

        try:
            with self.__updateRLock:
                if self.__original:
                    self.sync_state()
                    __clear_pipe(self.__p_state)
                    __clear_pipe(self.__c_state)
                    getattr(PipeRegister.get_pipereg_by_name('ProcessTaskPipes'),
                            'remove',
                            dummy_func)(pipe_id=self.uuid + '_state')
        except Exception as e:
            _log.error(f"ERROR in __close_pipes: {e}")
            _log.debug(f"[DEBUG] for __close_pipes: {traceback.format_exc()}")

    @property
    def worker(self):
        with self.__updateRLock:
            if self.__original:
                if self.__worker is None:
                    self.sync_state()
                return self.__worker
            return self.__worker

    @worker.setter
    def worker(self, value):
        with self.__updateRLock:
            if self.__original is False:
                self.__worker = value
                self.__update_state()
            else:
                self.__worker = value

    @worker.deleter
    def worker(self):
        with self.__updateRLock:
            del self.__worker

    @property
    def results(self):
        with self.__updateRLock:
            if self.__original:
                if self.__results is None:
                    self.sync_state()
                return self.__results
            return self.__results

    @results.setter
    def results(self, value):
        with self.__updateRLock:
            if self.__original is False and self.__results_updated is False:
                self.__results = value
                self.__update_state()
                self.__results_updated = True
            elif self.__original is True:
                self.__results = value
                self.__results_updated = True

    @results.deleter
    def results(self):
        with self.__updateRLock:
            del self.__results


class PriorityTaskQueue(PriorityQueue):
    """ <a name="PriorityTaskQueue"></a>
        This is a simple override of the PriorityQueue class that ensures the 'item' is a Task class meant to be used
        ONLY in ThreadingPool
    """

    def put_nowait(self, item: Task) -> None:
        if not isinstance(item, Task):
            raise TypeError(f'[ERROR]: item is not a Task object cannot be put into PriorityTaskQueue')
        return super().put_nowait(item)

    def put(self, item: Task, block: bool = True, timeout: Union[int, float, None] = None) -> None:
        if not isinstance(item, Task):
            raise TypeError(f'[ERROR]: item is not a Task object cannot be put into PriorityTaskQueue')
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
            raise TypeError(f'[ERROR]: item is not a Task object cannot be put into ProcessTaskQueue')
        return super().put_nowait(item)

    def put(self, item: Task, block: bool = True, timeout: Union[int, float, None] = None) -> None:
        if not isinstance(item, Task):
            raise TypeError(f'[ERROR]: item is not a Task object cannot be put into ProcessTaskQueue')
        return super().put(item, block=block, timeout=timeout)