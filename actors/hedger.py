from ...interface.interface import Interface
from ...interface.connectors.poll import Poll
from .actor import Actor
from threading import Lock, Event
from datetime import datetime
from queue import Queue
from time import time
from copy import copy
from time import time, sleep
import json

class Hedger(Actor):
    def identify_modules(self, modules):
        self.target = modules['interfaces']['account'].get(self.target_name)
        self.actor = None

    def is_ready(self):
        if self.position_base is not None:
            return True
        return False

    def subscribe(self, actor):
        if self.actor is None:
            self.actor = actor
            self.watcher = Poll(self.set_trigger, trigger=actor.trigger, imediate=False)

    def start(self):
        self.timeout = 10
        self.reseter = Poll(self.reset, trigger=self.do_reset, imediate=False)
        self.managers = Poll(self.manager, n=1, frequency=1/60, trigger=self.trigger, imediate=False)

    def handle_account(self, event):
        if event == 'position':
            self.updated['position'].set()

    def handle_trade(self, event, update):
        pass

    def handle_order(self, event, update):
        pass

    def handle_error(self, update):
        pass

    def get_balance(self):
        balance = 0
        for symbol in self.balance:
            if symbol.upper() in self.pair.split('/')[0]:
                balance = self.balance[symbol]['available'] + self.balance[symbol]['reserved']
                break
        return balance

    def get_quantity(self):
        if self.actor is not None:
            quantity = self.get_position()
            quantity += self.get_balance()
            p = self.actor.get_position()
            if p is not None:
                quantity += p
                if abs(quantity) > (0.01 * p):
                    return - quantity

    def enough_quantity(self, quantity):
        if quantity is not None:
            return quantity > (0.1 * abs(self.position[self.pair]))

    def convert_quanity(self, quantity):
        if quantity is not None:
            if self.position_base is not None:
                price = self.actor.last_price
                if price is not None:
                    return int(price * abs(quantity))
            else:
                return abs(quantity)

    def manager(self):
        if self.actor is not None:
            sided_quantity = self.get_quantity()
            quantity = self.convert_quanity(sided_quantity)
            if self.enough_quantity(quantity):
                side = 'bid' if sided_quantity > 0 else 'ask'
                self.updated['position'].clear()
                if not self.place(side, None, quantity, type='market'):
                    if self.on_error.wait(self.timeout):
                        print(datetime.now(), '- [ Hedger ] Placement Error, retrying...')
                        self.trigger.set()
                self.updated['position'].wait(self.timeout)
                print(datetime.now(), '- [ Hedger ] Position updated:', self.position, self.position_base)
