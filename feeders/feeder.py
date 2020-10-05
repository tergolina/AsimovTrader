from ...interface.interface import Interface
from ...interface.connectors.poll import Poll
from ..module.module import Module
import json
from threading import Lock, Event
from queue import Queue
from datetime import datetime

class Feeder(Module):
    # Virtual Methods from Module ----------------------------------------------
    def identify_parameters(self, parameters):
        self.pair = parameters.get('pair')
        self.exchange = parameters.get('exchange')
        self.source = parameters.get('source')

    def identify_modules(self, modules):
        self.interface = modules['interfaces']['marketdata'].get(self.exchange)

    def initialize(self):
        self.bid = None
        self.ask = None
        self.last = None
        self.book = None
        self.curve = None

    def connect(self):
        filter = {self.source: [self.pair]}
        self.interface.subscribe(self.message_to_queue, filter)
        self.watcher = Poll(self.probe, frequency=1/60)

    # Handle -------------------------------------------------------------------
    def handler(self, message):
        state = json.loads(message)
        event = state['event']
        update = state['update']
        if update is not None:
            if event == 'trade':
                if self.pair == update['pair']:
                    self.update_from_trade(update)
            elif event in ['book', 'quote']:
                if self.pair in update:
                    self.update_from_book(update[self.pair])

        elif event == 'subscription':
            marketdata = state['marketdata']
            if self.source == 'trade':
                if self.pair in marketdata['last_buy']:
                    self.update_from_trade({'price': marketdata['last_buy'][self.pair], 'side': 'buy'})
                if self.pair in marketdata['last_sell']:
                    self.update_from_trade({'price': marketdata['last_sell'][self.pair], 'side': 'sell'})
            else:
                if self.pair in marketdata['bid']:
                    self.update_from_book({'bid': marketdata['bid'][self.pair]})
                if self.pair in marketdata['ask']:
                    self.update_from_book({'bid': marketdata['ask'][self.pair]})

    # Write --------------------------------------------------------------------
    def update_from_book(self, ticker):
        if 'bid' in ticker:
            self.bid = ticker['bid']
        if 'ask' in ticker:
            self.ask = ticker['ask']
        self.notification.set()

    def update_from_trade(self, update):
        self.last = update['price']
        if update['side'] == 'buy':
            self.ask = update['price']
            if self.bid and (self.ask < self.bid):
                self.bid = self.ask
        elif update['side'] == 'sell':
            self.bid = update['price']
            if self.ask and (self.bid > self.ask):
                self.ask = self.bid
        self.notification.set()

    # Read ---------------------------------------------------------------------
    def is_ready(self):
        try:
            ref = self.get()
            return (ref['bid'] is not None) and (ref['ask'] is not None)
        except:
            return False

    def get(self, method=None):
        if method == 'last':
            return self.last
        elif method == 'mid':
            return (self.bid + self.ask) / 2
        elif method == 'bid':
            return self.bid
        elif method == 'ask':
            return self.ask
        else:
            return {'bid': self.bid, 'ask': self.ask}

    def probe(self):
        try:
            p = self.get()
        except Exception as e:
            p = str(e)
