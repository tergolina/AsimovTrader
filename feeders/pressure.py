from ...interface.interface import Interface
from ...interface.connectors.poll import Poll
from .feeder import Feeder
import json
from threading import Lock, Event
from queue import Queue


class Pressure(Feeder):
    pass
