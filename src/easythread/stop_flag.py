from threading import Event
from time import sleep

class StopFlag(Event):

    def sleep(self, seconds: float, check_seconds: float = 1.0):
        seconds = float(seconds)
        if seconds < 0 or check_seconds < 0:
            raise ValueError("all values must be non-negative")
        if seconds <= check_seconds:
            sleep(seconds)
        total_seconds_slept = 0.0
        while total_seconds_slept < seconds and not self.is_set():
            sleep(check_seconds)
            total_seconds_slept += check_seconds
