from abc import ABC, abstractmethod
import threading
from typing import Protocol, Any

class Task(Protocol):
    def __call__(self, stop_flag: threading.Event, *args: Any, **kwargs: Any) -> Any: ...
    def run(self, stop_flag: threading.Event, *args: Any, **kwargs: Any) -> Any: ...