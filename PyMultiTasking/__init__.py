#!/usr/bin/env python3
# -*- coding=utf-8 -*-

import logging
logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(funcName)s %(lineno)s %(message)s')
_log = logging.getLogger('PyMultiTasking')
_log.setLevel(logging.ERROR)

from PyMultiTasking.utils import wait_lock, safe_acquire, safe_release, method_wait
from PyMultiTasking.utils import MultiEvent, MultipleEvents, Limiter
from PyMultiTasking.ActionSynchronize import Action, ActionQueue, ActionListener, ActionRegister
from PyMultiTasking.PipeSynchronize import SafePipe, PipeRegister, create_safepipe
from PyMultiTasking.Tasks import ThreadTask, ProcessTask, PriorityTaskQueue, ProcessTaskQueue
from PyMultiTasking.ThreadingUtils import Threaded, ThreadWorker, ThreadPool
from PyMultiTasking.ProcessingUtils import Processed, ProcessWorker, ProcessPool, set_start_method
