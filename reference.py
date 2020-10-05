from ..interface.interface import Interface
from .ratio import Ratio
import json
from threading import Lock, Event


'''
    Exemplo de blueprint:

    blueprint = {'XRP/USDT': {'reference': {'A': {'pair': 'XRP/BTC', 'exchange': 'binance'},
                                            'B': {'pair': 'BTC/USDT', 'exchange': 'binance'}},
                              'relationship': 'A*B'},
                 'ZEC/ETH': {'reference': {'A': {'pair': 'ZEC/BTC', 'exchange': 'binance'},
                                           'B': {'pair': 'ETH/BTC', 'exchange': 'binance'}},
                             'relationship': 'A/B'},
                 'ETHUSD': {'reference': {'A': {'pair': 'BTCUSD', 'exchange': 'bitmex'}},
                              'relationship': 'A'},
                 'EOS/EUR': {'reference': {'A': {'pair': 'EOS/USD', 'exchange': 'bitfinex',
                                                  'ratio': {'pair': 'EOS/EUR', 'exchange': 'bitfinex', 'window': 40}},
                                            'B': {'pair': 'EOS/USDT', 'exchange': 'bitfinex',
                                                  'ratio': {'pair': 'EOS/EUR', 'exchange': 'bitfinex', 'window': 40}}},
                              'relationship': 'A+B'},
                 'EOS/BTC': {'reference': {'A': {'pair': 'EOS/USD', 'exchange': 'bitfinex'},
                                            'B': {'pair': 'BTC/USD', 'exchange': 'bitfinex'},
                                            'C': {'pair': 'EOS/BTC',
                                                  'exchange': 'binance',
                                                  'ratio': {'pair': 'EOS/BTC', 'exchange': 'bitfinex', 'window': 40}}},
                              'relationship': 'A/B+C'}}
'''
class Reference:
    """ Gerenciador de referencia de preco """
    def __init__(self, blueprint={}, logger=None):
        self.logger = logger
        self.bid = {}
        self.ask = {}
        self.last = {}
        self.previous = None
        self.timestamp = {}
        self.id = {}
        # Triggers -------------------------------------------------------------
        self.on_update = {p:Event() for p in blueprint}
        # Guides ---------------------------------------------------------------
        self.blueprint = blueprint
        self.compass = self.create_compass(blueprint)
        self.interfaces = self.connect(self.compass)
        self.ratio = self.create_ratio(blueprint)

    def create_ratio(self, blueprint):
        # Format blueprint -----------------------------------------------------
        ratio_bp = []
        for pair in blueprint:
            ref = blueprint[pair]['reference']
            for x in ref:
                if 'ratio' in ref[x]:
                    ratio_bp += [{'reference': {'pair': ref[x]['pair'], 'exchange': ref[x]['exchange']},
                                  'home': {'pair': ref[x]['ratio']['pair'], 'exchange': ref[x]['ratio']['exchange']},
                                  'window': ref[x]['ratio']['window']}]
        # Clear redundant ------------------------------------------------------
        repeated = []
        for i in range(len(ratio_bp)):
            for j in range(i+1, len(ratio_bp)):
                if ratio_bp[i] == ratio_bp[j]:
                    repeated += [i]
        for i in repeated[::-1]:
            del ratio_bp[i]
        return Ratio(ratio_bp, logger=self.logger)

    def create_compass(self, blueprint):
        compass = {}
        for pair in blueprint:
            for ref in blueprint[pair]['reference']:
                ref_pair = blueprint[pair]['reference'][ref]['pair']
                ref_exchange = blueprint[pair]['reference'][ref]['exchange']
                if ref_exchange not in compass:
                    compass[ref_exchange] = {}
                if ref_pair not in compass[ref_exchange]:
                    compass[ref_exchange][ref_pair] = []
                compass[ref_exchange][ref_pair] += [pair]
        self.logger.log('Reference', 'Compass: ' + str(compass), print_only=True)
        return compass

    def connect(self, compass):
        interfaces = {}
        for exchange in compass:
            self.bid[exchange] = {}
            self.ask[exchange] = {}
            self.last[exchange] = {}
            self.timestamp[exchange] = 0
            self.id[exchange] = 0
            pairs = list(compass[exchange].keys())
            if exchange in ['binance', 'bitmex']:
                bp = {'marketdata': {'trade': pairs}}
            else:
                bp = {'marketdata': {'book': pairs}}
            self.logger.log('Reference', 'Connecting to ' + exchange + ': ' + str(pairs), print_only=True)
            interfaces[exchange] = Interface(exchange, self.__handler, blueprint=bp, logger=self.logger, quick=True)
        return interfaces

    def __handler(self, message):
        data = json.loads(message)
        event = data['event']
        update = data['update']
        exchange = data['exchange']
        if update != None:
            if event == 'trade':
                # ok = True
                # if ('id' in update) and isinstance(update['id'], int) and ('pair' in update) and (self.id[exchange][update['pair']] < update['id']):
                #     self.id[exchange][update['pair']] = update['id']
                # elif ('timestamp' in update) and (self.timestamp[exchange] <= update['timestamp']):
                #     self.timestamp[exchange] = update['timestamp']
                # else:
                #     ok = False
                # if ok:
                if self.__update_from_trade(exchange, update):
                    ref_pair = update['pair']
                    for pair in self.compass[exchange][ref_pair]:
                        self.on_update[pair].set()
            elif event == 'book':
                for ref_pair in update:
                    if self.__update_from_book(exchange, ref_pair, update[ref_pair]):
                        for pair in self.compass[exchange][ref_pair]:
                            self.on_update[pair].set()

    def __update_from_book(self, exchange, pair, ticker):
        ok = False
        if 'bid' in ticker:
            self.bid[exchange][pair] = ticker['bid']
            ok = True
        if 'ask' in ticker:
            self.ask[exchange][pair] = ticker['ask']
            ok = True
        return ok

    def __update_from_trade(self, exchange, update):
        if ('price' in update) and ('pair' in update):
            pair = update['pair']
            self.last[exchange][pair] = update['price']
            if update['side'] == 'buy':
                previous = self.ask[exchange][pair] if pair in self.ask[exchange] else None
                self.ask[exchange][pair] = update['price']
                if (pair in self.bid[exchange]) and (update['price'] < self.bid[exchange][pair]):
                    self.bid[exchange][pair] = update['price']
                if update['price'] != previous:
                    return True
            elif update['side'] == 'sell':
                previous = self.bid[exchange][pair] if pair in self.bid[exchange] else None
                self.bid[exchange][pair] = update['price']
                if (pair in self.ask[exchange]) and (update['price'] > self.ask[exchange][pair]):
                    self.ask[exchange][pair] = update['price']
                if update['price'] != previous:
                    return True
        return False

    def is_ready(self, pair):
        """ Testa se ha dados para o calculo da referencia

            Args:
                pair (str): Par no formato padrao Asimov

            Returns:
                (bool): Se e possivel calcular a referencia para o par
        """
        ready = True
        if pair in self.blueprint:
            ref = self.blueprint[pair]['reference']
            for x in ref:
                ref_pair = ref[x]['pair']
                ref_exchange = ref[x]['exchange']
                if (ref_exchange in self.bid) and (ref_exchange in self.ask):
                    if (ref_pair not in self.bid[ref_exchange]) or (ref_pair not in self.ask[ref_exchange]):
                        ready = False
                else:
                    ready = False
        return ready

    def get_ratio(self, compass):
        if 'ratio' not in compass:
            return 1
        else:
            return self.ratio.get(compass['exchange'],
                                  compass['pair'],
                                  compass['ratio']['exchange'],
                                  compass['ratio']['pair'],
                                  compass['ratio']['window'])

    def get_reference(self, pair, relationship):
        """ Calcula a referencia de acordo com a blueprint

            Args:
                pair (str): Par no formato padrao Asimov

            Returns:
                (dict): Dicionario contendo bid, ask e mid da referencia
        """
        guide = self.blueprint[pair]
        relationship = guide['relationship'] if relationship == None else relationship
        bid = None
        ask = None
        if self.is_ready(pair):
            if '+' in relationship:
                bids = []
                asks = []
                items = relationship.split('+')
                for x in items:
                    ref = self.get(pair, relationship=x)
                    if ref != None:
                        bids += [ref['bid']]
                        asks += [ref['ask']]
                if bids and asks:
                    bid = min(bids)
                    ask = max(asks)
            elif '/' in relationship:
                a, b = relationship.split('/')
                ref_a = self.get(pair, relationship=a)
                ref_b = self.get(pair, relationship=b)
                if ref_a and ref_b:
                    bid = ref_a['bid'] / ref_b['ask']
                    ask = ref_a['ask'] / ref_b['bid']
            elif '*' in relationship:
                bid = 1
                ask = 1
                for x in relationship.split('*'):
                    ref = self.get(pair, relationship=x)
                    if ref:
                        bid = bid * ref['bid']
                        ask = ask * ref['ask']
                    else:
                        bid = None
                        ask = None
                        break
            else:
                # Recursion leaf -----------------------------------------------
                ratio = self.get_ratio(guide['reference'][relationship])
                if ratio:
                    ref_pair = guide['reference'][relationship]['pair']
                    ref_exchange = guide['reference'][relationship]['exchange']
                    bid = self.bid[ref_exchange][ref_pair] * ratio
                    ask = self.ask[ref_exchange][ref_pair] * ratio

            if (bid != None) and (ask != None):
                return {'bid': bid, 'ask': ask, 'mid': (bid + ask)/2}

    def get(self, pair, relationship=None):
        """ Calcula a referencia de acordo com a blueprint

            Args:
                pair (str): Par no formato padrao Asimov

            Returns:
                (dict): Dicionario contendo bid, ask e mid da referencia
        """
        reference = self.get_reference(pair, relationship)
        if reference != self.previous:
            self.previous = reference
            self.logger.log('Reference', {pair: reference}, silenced=True)
        return reference
