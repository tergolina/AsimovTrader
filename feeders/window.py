from ...interface.interface import Interface
from ...interface.connectors.poll import Poll
from .feeder import Feeder
from threading import Lock, Event
from queue import Queue
from time import time, sleep
from copy import copy
from datetime import datetime
import json


'''
    params
    ---
    pair': 'ETH/USD'
    exchange: 'bitfinex'
    window: 5
    source: 'book'
    candle_size: 30
'''
class Window(Feeder):
    def identify_parameters(self, parameters):
        self.pair = parameters.get('pair')
        self.exchange = parameters.get('exchange')
        self.source = parameters.get('source')
        self.window = parameters.get('window')
        self.candle_size = parameters.get('candle_size', 60)
        self.server_timestamp = parameters.get('server_timestamp', True)

    def initialize(self):
        # Data -----------------------------------------------------------------
        self.bid_candles = [None for i in range(self.window)]
        self.ask_candles = [None for i in range(self.window)]
        self.last_candles = [None for i in range(self.window)]
        # Control --------------------------------------------------------------
        self.last_timestamp = 0
        self.fill_candles()

    def start(self):
        self.verifier = Poll(self.verify_candles, frequency=10/self.candle_size, n=1)

    def fill_candles(self):
        if self.source == 'trade':
            while None in self.last_candles:
                print(datetime.now(), '- [ Window ] Filling candles for', self.pair)
                self.last_timestamp = 0
                window = ((self.window * self.candle_size) // 60) + 1
                trades = self.interface.get_trades(self.pair, window)
                if trades != [] and trades is not None:
                    for trade in trades:
                        self.insert_trade(trade)
                sleep(1)

    # Handle -------------------------------------------------------------------
    def handler(self, message):
        state = json.loads(message)
        event = state['event']
        update = state['update']
        if (event == self.source) and (update is not None):
            timestamp = int(state['timestamp'] * (10**6))

            if event == 'trade':
                if update['pair'] == self.pair:
                    self.insert_trade(update, timestamp=timestamp)

            elif (event == 'quote') or (event == 'book'):
                if self.pair in update:
                    self.insert_quote(update, timestamp)

    # Write --------------------------------------------------------------------
    def previous_candle(self, timestamp):
        return timestamp // (self.candle_size * (10**6))

    def shift_candles(self):
        # print (datetime.now(), '- [ Window ] Shifting candles. Last:', self.last_candles, '| Bid:', self.bid_candles, '| Ask:', self.ask_candles)
        for i in range(self.window-1):
            self.bid_candles[i] = self.bid_candles[i+1]
            self.ask_candles[i] = self.ask_candles[i+1]
            self.last_candles[i] = self.last_candles[i+1]

    def verify_candles(self, timestamp=None):
        self.lock.acquire()
        timestamp = timestamp if timestamp else time() * (10**6)
        if self.previous_candle(self.last_timestamp) < self.previous_candle(timestamp):
            self.shift_candles()
            self.last_timestamp = timestamp
        self.lock.release()

    def insert_tick(self, timestamp, bid=None, ask=None, last=None):
        self.verify_candles(timestamp)
        if bid is not None:
            self.bid_candles[-1] = bid
        if ask is not None:
            self.ask_candles[-1] = ask
        if last is not None:
            self.last_candles[-1] = last
        self.notification.set()

    def insert_quote(self, quote, timestamp):
        bid = quote[self.pair]['bid']
        ask = quote[self.pair]['ask']
        self.insert_tick(timestamp, bid=bid, ask=ask)

    def insert_trade(self, trade, timestamp=None):
        if self.server_timestamp or (timestamp is None):
            timestamp = int(trade['timestamp'] * (10**6))

        price = trade['price']
        bid = price if trade['side'] == 'sell' else None
        ask = price if trade['side'] == 'buy' else None
        self.insert_tick(timestamp, bid=bid, ask=ask, last=price)

    # Read ---------------------------------------------------------------------
    def is_ready(self):
        return ((None not in self.bid_candles) and (len(self.bid_candles) > 0)
                and (None not in self.ask_candles) and (len(self.ask_candles) > 0))

    def get(self, method=None):
        if method == 'last':
            return copy(self.last_candles)
        elif method == 'bid':
            return copy(self.bid_candles)
        elif method == 'ask':
            return copy(self.ask_candles)
        elif method == 'mid':
            return [(self.bid_candles[i] + self.ask_candles[i]) / 2 for i in range(self.window)]
        else:
            return {'bid': copy(self.bid_candles), 'ask': copy(self.ask_candles)}
