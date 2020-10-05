from queue import Queue
from threading import Lock, Event
from time import time, sleep
from datetime import datetime
from ...interface.connectors.poll import Poll
from uuid import uuid4

class Module:
    def __init__(self, modules={}, parameters={}, logger=None):
        self.ID = self.__id()
        self.logger = logger

        self.lock = Lock()
        self.notification = Event()

        self.queue = Queue()
        self.queue_handler = Poll(self.message_from_queue)

        self.identify_parameters(parameters)
        self.identify_modules(modules)

        self.initialize()
        self.connect()
        self.start()

    def __id(self):
        return ''.join(str(uuid4()).split('-'))

    # Initialize ---------------------------------------------------------------
    def identify_modules(self, modules):
        self.modules = modules

    def identify_parameters(self, parameters):
        self.parameters = parameters

    def initialize(self):
        pass

    def connect(self):
        pass

    def start(self):
        pass

    # Barrirer -----------------------------------------------------------------
    def barrier(self):
        while True:
            if self.is_ready():
                break
            sleep(5)

    def is_ready(self):
        return True

    # Edit ---------------------------------------------------------------------
    def edit(self, modules=None, parameters=None):
        self.pause()

        if parameters:
            self.identify_parameters(parameters)
        if modules:
            self.identify_modules(modules)

        self.initialize()
        self.connect()

        self.play()

    def pause(self):
        pass

    def play(self):
        pass

    # Queue --------------------------------------------------------------------
    def message_to_queue(self, message):
        self.queue.put(message)

    def message_from_queue(self):
        while True:
            message = self.queue.get()
            self.handler(message)

    def handler(self, message):
        pass
