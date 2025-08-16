import pytest
from multiprocessing.connection import Connection
from enum import Enum

import easythread


class DummyEnum(Enum):
    FOO = 1
    BAR = 2


@pytest.fixture(autouse=True)
def clear_registries():
    easythread.Channel.registry.clear()
    easythread.Workqueue.registry.clear()
    yield
    easythread.Channel.registry.clear()
    easythread.Workqueue.registry.clear()


class TestChannel:
    def test_first_open_creates_and_returns_endpoint(self):
        c = easythread.Channel.open("chan1")
        assert isinstance(c, Connection)
        assert "chan1" in easythread.Channel.registry
        info = easythread.Channel.registry["chan1"]
        assert len(info["endpoints"]) == 2
        assert info["pair_taken"] is False

    def test_second_open_returns_other_endpoint_and_marks_taken(self):
        c1 = easythread.Channel.open("chan1")
        c2 = easythread.Channel.open("chan1")
        assert c1 != c2
        info = easythread.Channel.registry["chan1"]
        assert info["pair_taken"] is True

    def test_third_open_raises_exception(self):
        easythread.Channel.open("chan1")
        easythread.Channel.open("chan1")
        with pytest.raises(Exception) as e:
            easythread.Channel.open("chan1")
        assert "already opened" in str(e.value)

    def test_enum_name_used_as_key(self):
        c1 = easythread.Channel.open(DummyEnum.FOO)
        c2 = easythread.Channel.open(DummyEnum.FOO)
        assert "FOO" in easythread.Channel.registry
        assert c1 != c2


class TestWorkqueue:
    def test_first_open_creates_queue(self):
        q = easythread.Workqueue.open("queue1")
        assert q is not None
        assert "queue1" in easythread.Workqueue.registry

    def test_second_open_returns_same_queue(self):
        q1 = easythread.Workqueue.open("queue1")
        q2 = easythread.Workqueue.open("queue1")
        assert q1 is q2

    def test_enum_name_used_as_key(self):
        q1 = easythread.Workqueue.open(DummyEnum.BAR)
        q2 = easythread.Workqueue.open(DummyEnum.BAR)
        assert "BAR" in easythread.Workqueue.registry
        assert q1 is q2
