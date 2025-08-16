from concurrent.futures import ThreadPoolExecutor, Future, wait, FIRST_EXCEPTION, FIRST_COMPLETED
from typing import Union
import multiprocessing
from enum import Enum
import signal
from .tasks import Task
from .stop_flag import StopFlag
from .channel import Channel
import loguru
from contextlib import suppress


class Executor:
    def __init__(self, logger=loguru.logger):
        self.logger = logger
        self.current_task_id = 0
        self.executor = ThreadPoolExecutor()
        self.tasks: dict[int, Task] = {}
        self.task_args: dict[int, tuple] = {}
        self.task_kwargs: dict[int, dict] = {}
        self.futures: dict[int, Future] = {}
        self.stop_flag = StopFlag()
        self.signal_handler = lambda signum, _: self.stop_flag.set()
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGQUIT, self.signal_handler)
    
    def add_task(self, t: Task, *args, **kwargs):
        tid = self.current_task_id
        self.tasks[tid] = t
        self.task_args[tid] = args
        self.task_kwargs[tid] = kwargs
        self.current_task_id += 1
        self.logger.debug(f"added task with id {tid}")
        return tid
    
    def remove_task(self, tid: int):
        for a in [self.tasks, self.task_args, self.task_kwargs, self.futures]:
            with suppress(KeyError):
                del a[tid]
        self.logger.debug(f"removed task with id {tid}")
    
    def get_task_id(self, f):
        for tid, future in self.futures.items():
            if future is f:
                return tid
        raise ValueError("no task id found for given future")
    
    def check_task_health(self):
        self.logger.debug("starting task health checker")
        while not self.stop_flag.is_set():
            done, _ = wait(self.futures.values(), return_when=FIRST_COMPLETED)
            if done:
                for task in done:
                    tid = self.get_task_id(task)
                    if task.exception():
                        self.logger.error(f"exception in task {tid}: {task.exception()}")
                    elif not self.stop_flag.is_set():
                        self.logger.info(f"task with id {tid} completed")
                    self.remove_task(tid)
            
    def run(self):
        for tid, task in self.tasks.items():
            args = self.task_args[tid]
            kwargs = self.task_kwargs[tid]
            self.futures[tid] = self.executor.submit(task, self.stop_flag, *args, **kwargs)
        self.futures[self.current_task_id] = self.executor.submit(self.check_task_health)

    def stop(self):
        self.stop_flag.set()