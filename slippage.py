from ..interface.interface import Interface
from .ratio import Ratio
from ..interface.connectors.poll import Poll
import json
from threading import Lock, Event
from queue import Queue


'''
    curve_x = [0.05/100, 0.1/100, 0.2/100, 0.5/100, 1/100]
'''
class Slippage:
    def __init__(self, exchange=None, pairs=None, interface=None, curve_x=[], logger=None):
        self.logger = logger
        self.curve_x = curve_x
        self.curve = {}
        self.book = {}
        if self.is_interface_ok(interface):
            self.exchange = interface.name
            self.pairs = interface.blueprint['marketdata']['book']
            self.interface = interface
            self.interface.subscribe(self._handler)
        elif exchange and pairs:
            self.exchange = exchange
            self.pairs = pairs
            self.interface = self.connect()
        self.queue = {p:Queue() for p in pairs}
        self.updaters = {p:Poll(self._updater, args=[p, curve_x!=[]]) for p in pairs}

    def is_interface_ok(self, interface):
        return ((interface != None) and
                ('marketdata' in interface.blueprint) and
                ('book' in interface.blueprint['marketdata']))

    def connect(self):
        bp = {'marketdata': {'book': self.pairs}}
        return Interface(self.exchange, self._handler, blueprint=bp, logger=self.logger, quick=False)

    def _handler(self, message):
        data = json.loads(message)
        if data['event'] == 'book':
            for pair in data['update']:
                self.queue[pair].put(data['marketdata']['book'][pair])

    def _updater(self, pair, calculate):
        while True:
            book = self.queue[pair].get()
            self.book[pair] = book
            # Curve ------------------------------------------------------------
            if calculate:
                curve = {'bid': {}, 'ask': {}}
                for book_side in ['bid', 'ask']:
                    book_prices = sorted(book[book_side].keys())
                    book_prices = book_prices if (book_side == 'bid') else book_prices[::-1]
                    if book_prices != []:
                        top_price = float(book_prices[0])
                        quantity = 0
                        i = 0
                        for book_price in book_prices:
                            price = float(book_price)
                            x = self.curve_x[i]
                            if abs(1 - (price / top_price)) >= x:
                                curve[book_side][str(x)] = quantity
                                i += 1
                                if i >= len(self.curve_x):
                                    break
                            else:
                                quantity += book[book_side][book_price]
                self.curve[pair] = curve

    def get(self, pair, side=None, quantity=None):
        if not quantity:
            if pair in self.curve:
                return self.curve[pair][side] if side else self.curve[pair]
        elif side and quantity:
            last_price = None
            percent = None
            book_side = side if (side in ['bid', 'ask']) else ('ask' if (side == 'buy') else 'bid')
            book_prices = sorted(self.book[pair][book_side].keys()) if (book_side == 'bid') else sorted(self.book[pair][book_side].keys())[::-1]
            cum_quantity = 0
            average_price = 0
            for book_price in book_prices:
                last_price = float(book_price)
                book_quantity = self.book[pair][book_side][book_price]
                cum_quantity += book_quantity
                average_price += last_price * (book_quantity - max(0, cum_quantity - quantity))
                if cum_quantity >= quantity:
                    break
            average_price = average_price / quantity
            if book_prices != []:
                percent = abs(1 - (last_price/float(book_prices[0]))) if last_price else None
            return {'average_price': average_price, 'last_price': last_price, 'percent': percent}
