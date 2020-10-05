from ...interface.interface import Interface
from ...interface.connectors.poll import Poll
from .feeder import Feeder
from threading import Lock, Event
from queue import Queue
import json


'''
    curve_x = [0.05/100, 0.1/100, 0.2/100, 0.5/100, 1/100]
'''
class Slippage(Feeder):
    def identify_parameters(self, parameters):
        self.pair = parameters.get('pair')
        self.exchange = parameters.get('exchange')
        self.source = parameters.get('source', 'book')
        self.curve_x = parameters.get('curve_x')

    def connect(self):
        if self.is_interface_ok():
            self.interface.subscribe(self.message_to_queue)
        else:
            bp = self.create_blueprint()
            self.interface = Interface(self.exchange, self.message_to_queue, blueprint=bp, logger=self.logger, quick=False, book_depth=True)

    def is_interface_ok(self):
        return ((self.interface is not None) and (self.exchange == interface.name)
                and self.interface.has_marketdata_channel(self.source, pair=pair)
                and self.interface.book_depth)

    def handler(self, message):
        data = json.loads(message)
        if data['event'] == 'book':
            if self.pair in data['update']:
                self.book = data['marketdata']['book'][self.pair]
                self.notification.set()
                if self.curve_x:
                    self.update_curve()

    def update_curve(self):
        book = json.loads(json.dumps(self.book))
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
        self.curve = curve

    def get(self, side=None, trade_quantity=None, order_quantity=0, method='last_price'):
        # Melhorar calculo do slippage ideal
        method = 'last_price' if method is None else method
        if not trade_quantity:
            return self.curve[side] if side else self.curve
        elif side and trade_quantity:
            last_price = None
            ideal_price = None
            percent = None
            book_side = side if (side in ['bid', 'ask']) else ('ask' if (side == 'buy') else 'bid')
            book_prices = sorted(self.book[book_side].keys()) if (book_side == 'bid') else sorted(self.book[book_side].keys())[::-1]
            cum_quantity = 0
            average_price = 0
            for book_price in book_prices:
                last_price = float(book_price)
                book_quantity = self.book[book_side][book_price]
                cum_quantity += book_quantity
                average_price += last_price * (book_quantity - max(0, cum_quantity - trade_quantity))
                if (ideal_price is None) and ((trade_quantity - cum_quantity) < order_quantity):
                    ideal_price = last_price
                if cum_quantity >= trade_quantity:
                    break
            average_price = average_price / quantity
            if book_prices != []:
                percent = abs(1 - (last_price/float(book_prices[0]))) if last_price else None
            if method == 'average_price':
                return average_price
            elif method == 'last_price':
                return last_price
            elif method == 'ideal_price':
                return ideal_price
            elif method == 'percent':
                return percent
