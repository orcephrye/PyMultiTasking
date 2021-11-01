#!/usr/bin/env python3
# -*- coding=utf-8 -*-

from __future__ import annotations
import logging
import traceback
import uuid
from multiprocessing import Pipe
from multiprocessing import RLock
import threading


# logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(funcName)s %(lineno)s %(message)s',
#                     level=logging.DEBUG)
_log = logging.getLogger('PipeSynchronize')


class SafePipe:

    def __init__(self, conn, _id):
        self.conn = conn
        self.pipe_id = _id
        self.__lock = threading.Lock()

    def __enter__(self):
        self.__lock.acquire()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.__lock.locked():
            self.__lock.release()

    def __getstate__(self):
        return {k: v for k, v in self.__dict__.items() if k != '_SafePipe__lock'}

    def __setstate__(self, state):
        self.__dict__ = state
        self.__lock = threading.Lock()

    def send(self, data):
        with self.__lock:
            self.conn.send(data)

    def recv(self):
        with self.__lock:
            return self.conn.recv()

    def close(self):
        with self.__lock:
            return self.conn.close()

    def poll(self, timeout=0):
        return self.conn.poll(timeout=timeout)

    @property
    def isActive(self):
        with self.__lock:
            return not self.conn.closed


def create_safepipe(conns=None, register=None):
    if conns and len(conns) == 2:
        p_conn, c_conn = conns
    else:
        p_conn, c_conn = Pipe()
    _id = str(uuid.uuid4())
    if isinstance(register, PipeRegister):
        register.register(SafePipe(p_conn, _id), SafePipe(c_conn, _id), _id=_id)
    return SafePipe(p_conn, _id), SafePipe(c_conn, _id)


class PipeRegister:

    __regRLock = RLock()
    __pipe_registry = []

    def __init__(self, *args, **kwargs):
        self.name = kwargs.get('name', None) if kwargs.get('name', None) is not None else str(uuid.uuid4())
        self.__rlock = RLock()
        self.__pipe_reg = {}
        PipeRegister.register_pipereg(self)

    def create_safepipe(self, pipe_id=None):
        pipe_id = pipe_id if pipe_id is not None else str(uuid.uuid4())
        if self.has_pipe(pipe_id):
            raise Exception(f'[create_safepipe] Pipe ID {pipe_id} already exists in registery!')
        p_conn, c_conn = Pipe()
        self.register(SafePipe(p_conn, pipe_id), SafePipe(c_conn, pipe_id), _id=pipe_id)
        return self.get_pipes(pipe_id)

    def register(self, p_conn, c_conn, _id=None):
        with self.__rlock:
            if type(p_conn) != type(c_conn):
                raise Exception(f'Parent connection and Child connection are of two different types')
            if not isinstance(p_conn, SafePipe):
                _id = _id if _id is not None else str(uuid.uuid4())
                p_conn = SafePipe(p_conn, _id)
                c_conn = SafePipe(c_conn, _id)
            if self.has_pipe(_id):
                raise Exception(f'[register] Pipe ID {_id} already exists in registery!')
            self.__pipe_reg.update({p_conn.pipe_id: (p_conn, c_conn)})
            return p_conn.pipe_id

    def get_all_pipes(self):
        with self.__rlock:
            return self.__pipe_reg

    def get_all_ids(self):
        with self.__rlock:
            return list(self.__pipe_reg.keys())

    def has_pipe(self, pipe_id):
        with self.__rlock:
            return pipe_id in self.__pipe_reg

    def remove(self, pipe_id):
        with self.__rlock:
            if self.has_pipe(pipe_id):
                return self.__pipe_reg.pop(pipe_id, None)

    def get_pipes(self, pipe_id):
        with self.__rlock:
            if self.has_pipe(pipe_id):
                return self.__pipe_reg.get(pipe_id, ())

    def get_parent_pipe(self, pipe_id):
        with self.__rlock:
            return self.get_pipes(pipe_id)[0]

    def get_child_pipe(self, pipe_id):
        with self.__rlock:
            return self.get_pipes(pipe_id)[-1]

    def send_to_parent(self, pipe_id, data):
        with self.__rlock:
            getattr(self.get_parent_pipe(pipe_id), 'send', PipeRegister.blank_func)(data)

    def send_to_child(self, pipe_id, data):
        with self.__rlock:
            getattr(self.get_child_pipe(pipe_id), 'send', PipeRegister.blank_func)(data)

    def recv_from_parent(self, pipe_id):
        with self.__rlock:
            return getattr(self.get_parent_pipe(pipe_id), 'recv', PipeRegister.blank_func)()

    def recv_from_child(self, pipe_id):
        with self.__rlock:
            return getattr(self.get_child_pipe(pipe_id), 'recv', PipeRegister.blank_func)()

    @staticmethod
    def blank_func(*args, **kwargs):
        return None

    @classmethod
    def get_pipereg_by_name(cls, name):
        with cls.__regRLock:
            for pipereg in cls.__pipe_registry:
                if name == pipereg.name:
                    return pipereg

    @classmethod
    def get_pipereg(cls, name=None):
        with cls.__regRLock:
            if name:
                return [pool for pool in cls.__pipe_registry if name == pool.name]
            return cls.__pipe_registry

    @classmethod
    def register_pipereg(cls, pipereg):
        with cls.__regRLock:
            if isinstance(pipereg, PipeRegister):
                cls.__pipe_registry.append(pipereg)
