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

class Taker(Actor):
    # Manage -------------------------------------------------------------------
    def exceeded_reference(self, side, price):
        if self.check_guide and (self.guide is not None):
            if side == 'bid':
                return self.guide.get('ask') <= price
            if side == 'ask':
                return self.guide.get('bid') >= price
        return True

    def manager(self, side):
        price = self.get_price(side)
        quantity = self.get_quantity(side)
        if self.exceeded_reference(side, price) and self.enough_quantity(quantity):
            self.place(side, price, quantity, type='sniper')
