from typing import Union, TypedDict
from multiprocessing import Pipe, Queue
from multiprocessing.connection import Connection
from enum import Enum

class ChannelRegistryInfo(TypedDict):
    endpoints: list[Connection]
    pair_taken: bool

class Channel:
    registry: dict[str, ChannelRegistryInfo] = {}

    @staticmethod
    def open(name: Union[str, Enum]) -> Connection:
        registry = __class__.registry
        if isinstance(name, Enum):
            name = name.name
        if name not in registry:
            p1, p2 = Pipe()
            registry[name] = {
                "endpoints": [p1, p2],
                "pair_taken": False
            }
            return p1
        elif not registry[name]["pair_taken"]:
            registry[name]["pair_taken"] = True
            return registry[name]["endpoints"][1]
        raise Exception("both endpoints on this channel already opened")
  
class Workqueue:
    registry: dict[str, Queue] = {}

    @staticmethod
    def open(name: Union[str, Enum]) -> Queue:
        registry = __class__.registry
        if isinstance(name, Enum):
            name = name.name
        if name not in registry:
            registry[name] = Queue()
        return registry[name]