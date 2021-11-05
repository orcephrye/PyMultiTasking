#!/usr/bin/env python3
# -*- coding=utf-8 -*-

from __future__ import annotations

import logging
import traceback
import multiprocessing
import threading
from multiprocessing.queues import Queue
from multiprocessing import RLock
from threading import Thread
from functools import partial
from typing import Union

from PyMultiTasking.utils import dummy_func
from PyMultiTasking.utils import __async_raise as a_raise
from PyMultiTasking.ThreadingUtils import ThreadWorker


_log = logging.getLogger('PyMultiTasking.ActionSynchronize')


class Action:

    def __init__(self, target_name, target_action, args=None, kwargs=None, returning=False, ending=False):
        self.target_name = target_name
        self.target_action = target_action
        self.args = args if args is not None else ()
        self.kwargs = kwargs if kwargs is not None else {}
        self.returning = returning
        self.ending = ending

    def __call__(self, *args, **kwargs):
        return self.target_name, self.target_action, self.returning, self.args, self.kwargs


class ActionQueue(Queue):

    def __init__(self, maxsize=0, *, ctx=None):
        super(ActionQueue, self).__init__(maxsize=maxsize, ctx=ctx or multiprocessing.get_context())

    def put_nowait(self, item: Action) -> None:
        if not isinstance(item, Action):
            raise TypeError(f'[ERROR]: item is not a Task object cannot be put into PriorityTaskQueue')
        return super().put_nowait(item)

    def put(self, item: Action, block: bool = True, timeout: Union[int, float, None] = None) -> None:
        if not isinstance(item, Action):
            raise TypeError(f'[ERROR]: item is not a Task object cannot be put into PriorityTaskQueue')
        return super().put(item, block=block, timeout=timeout)


class ActionRegister:

    def __init__(self):
        self.__action_lock = RLock()
        self.__action_reg = {}

    def register(self, target, target_name=None):
        if target_name is None:
            target_name = getattr(target, 'uuid', getattr(target, 'name', id(target)))
        with self.__action_lock:
            self.__action_reg.update({target_name: target})

    def remove(self, target_name):
        with self.__action_lock:
            if target_name in self.__action_reg:
                self.__action_reg.pop(target_name)

    def has_target(self, target_name):
        with self.__action_lock:
            return target_name in self.__action_reg

    def get_target_by_action(self, action, _default=dummy_func):
        with self.__action_lock:
            return self.get_target(action.target_name, action.target_action, _default=_default)

    def get_target(self, target_name, target_action, _default=dummy_func):
        with self.__action_lock:
            if not self.has_target(target_name):
                return _default
            if target_name == target_action:
                return self.__action_reg.get(target_name)
            else:
                return getattr(self.__action_reg.get(target_name), target_action, dummy_func)

    def call_target(self, target_name, target_action, *args, **kwargs):
        with self.__action_lock:
            return self.get_target(target_name, target_action)(*args, **kwargs)

    def call_target_by_action(self, action):
        with self.__action_lock:
            return self.get_target_by_action(action)(*action.args, **action.kwargs)

    def get_target_pipe(self, target_name):
        with self.__action_lock:
            if not self.has_target(target_name):
                return None
            return getattr(self.__action_reg.get(target_name), '_action_pipe', None)

    def get_target_pipe_by_action(self, action):
        with self.__action_lock:
            return self.get_target_pipe(action.target_name)


class ActionListener(Thread):

    def __init__(self, *args, **kwargs):
        if 'action_queue' in kwargs:
            self.action_queue = kwargs.pop('action_queue')
        else:
            self.action_queue = ActionQueue()

        if 'action_reg' in kwargs:
            self.action_reg = kwargs.pop('action_reg')
        else:
            self.action_reg = ActionRegister()

        if 'pipe_reg' in kwargs:
            self.pipe_reg = kwargs.pop('pipe_reg')
        else:
            self.pipe_reg = None

        super(ActionListener, self).__init__(*args, **kwargs)
        self.killed = False
        if kwargs.get('auto_start', True):
            self.start()

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

    def safe_stop(self):
        self.killed = True
        self.action_queue.put_nowait(Action(None, None, ending=True))

    @staticmethod
    def returning_func(return_pipe, data):
        try:
            return_pipe.send(data)
        except Exception as e:
            _log.error(f'Error in returning_func: {e}')
            _log.debug(f'[DEBUG]: trace for error in returning_func: {traceback.format_exc()}')

    def run(self):
        while not self.killed:
            action = self.action_queue.get()
            if action.ending:
                self.killed = True
                break
            target = self.action_reg.get_target_by_action(action)
            if action.returning is True:
                ThreadWorker(target=ThreadWorker(target, *action.args,
                                         store_return=False,
                                         callback_func=partial(ActionListener.returning_func,
                                                               self.action_reg.get_target_pipe_by_action(action)),
                                         **action.kwargs)).start()
            elif action.returning and self.pipe_reg is not None and self.pipe_reg.has_pipe(action.returning):
                ThreadWorker(target=ThreadWorker(target, *action.args,
                                         store_return=False,
                                         callback_func=partial(ActionListener.returning_func,
                                                               self.pipe_reg.get_parent_pipe(action.returning)),
                                         **action.kwargs)).start()
            else:
                ThreadWorker(target=ThreadWorker(target, *action.args, store_return=False, **action.kwargs)).start()
