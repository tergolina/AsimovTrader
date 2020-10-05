from ...interface.interface import Interface
from ...interface.connectors.poll import Poll
from .actor import Actor
from threading import Lock, Event
from queue import Queue
from time import time
from copy import copy
from time import time, sleep
from datetime import datetime
import decimal
import json

class Maker(Actor):
    # Handlers -----------------------------------------------------------------
    def handle_order(self, event, update):
        if event == 'cancel':
            self.trigger.set()

    # Control ------------------------------------------------------------------
    def enough_quantity(self, quantity):
        enough = False
        if self.bet_quantity:
            adjusted_quantity = self.target.filter_quantity(self.pair, quantity)
            enough = adjusted_quantity > (0.1 * self.bet_quantity)
            enough = enough and (adjusted_quantity >= float(self.info[self.pair]['minimum_quantity']))
            if 'minimum_volume' in self.info[self.pair]:
                enough = enough and ((float(adjusted_quantity) * self.last_price) >= float(self.info[self.pair]['minimum_volume']))
        return enough

    def enough_price_difference(self, side, new_price):
        current_price = self.orders[self.pair][side][0]['price']
        return abs(1 - (new_price / current_price)) >= self.tolerance

    def enough_quantity_difference(self, side, new_quantity):
        current_quantity = self.orders[self.pair][side][0]['quantity']
        if (current_quantity - new_quantity) > (0.1 * self.bet_quantity):
            return True
        elif (new_quantity - current_quantity) > (0.4 * self.bet_quantity):
            return True
        return False

    def has_leftovers(self, side):
        current_quantity = self.orders[self.pair][side][0]['quantity']
        return (decimal.Decimal(str(current_quantity)) != self.target.filter_quantity(self.pair, current_quantity))

    def too_many_orders(self, side):
        if self.pair in self.orders:
            return len(self.orders[self.pair][side]) > 1
        return False

    def apply_guide(self, side, price):
        if self.check_guide and (self.guide is not None):
            if side == 'bid':
                return min(self.guide.get('bid'), price)
            if side == 'ask':
                return max(self.guide.get('ask'), price)
        return price

    # Manage -------------------------------------------------------------------
    def manager(self, side):
        if not self.reseting:
            self.done_placing.clear()
            price = self.apply_guide(side, self.get_price(side))
            quantity = self.get_quantity(side)
            if self.has_order(side):
                if self.too_many_orders(side) or not self.enough_quantity(quantity):
                    self.cancel(side)
                elif self.enough_price_difference(side, price):
                    self.replace(side, price, None, type='maker')
                elif self.enough_quantity_difference(side, quantity):
                    self.cancel(side)
                    #self.replace(side, price, quantity, type='maker')
            elif self.enough_quantity(quantity):
                self.place(side, price, quantity, type='maker')
            self.done_placing.set()
