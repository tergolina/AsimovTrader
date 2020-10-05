from ..interface.interface import Interface
from ..interface.connectors.poll import Poll
from time import time
import pandas as pd

'''
    params
    ---
    blueprint = [{'reference': {'pair': 'BTC/USDT', 'exchange': 'binance'},
                  'home': {'pair': 'BTC/EUR', 'exchange': 'bitfinex'},
                  'window': 30,
                  'source': 'trade'},
                 {'reference': {'pair': 'ETHUSD', 'exchange': 'bitmex'},
                  'home': {'pair': 'ETH/USD', 'exchange': 'bitfinex'},
                  'window': 5,
                  'source': 'quote'}]
'''
class Ratio:
    def __init__(self, blueprint={}, logger=None):
        self.logger = logger
        self.ratio = self.initialize_ratio(blueprint)
        # Guides ---------------------------------------------------------------
        self.blueprint = blueprint
        self.interfaces = self.connect(blueprint)
        self.polls = self.create_polls(blueprint)

    def initialize_ratio(self, blueprint):
        compass = {}
        for item in blueprint:
            reference = item['reference']
            ref_exchange = reference['exchange']
            ref_pair = reference['pair']
            home = item['home']
            home_exchange = home['exchange']
            home_pair = home['pair']
            window = item['window']
            if ref_exchange not in compass:
                compass[ref_exchange] = {}
            if ref_pair not in compass[ref_exchange]:
                compass[ref_exchange][ref_pair] = {}
            if home_exchange not in compass[ref_exchange][ref_pair]:
                compass[ref_exchange][ref_pair][home_exchange] = {}
            if home_pair not in compass[ref_exchange][ref_pair][home_exchange]:
                compass[ref_exchange][ref_pair][home_exchange][home_pair] = {}
            if window not in compass[ref_exchange][ref_pair][home_exchange][home_pair]:
                compass[ref_exchange][ref_pair][home_exchange][home_pair][window] = None
        return compass

    def __handler(self, message):
        pass

    def connect(self, blueprint):
        # List all exchanges ---------------------------------------------------
        exchanges = []
        for item in blueprint:
            ref_exchange = item['reference']['exchange']
            if ref_exchange not in exchanges:
                exchanges += [ref_exchange]
            home_exchange = item['home']['exchange']
            if home_exchange not in exchanges:
                exchanges += [home_exchange]
        # Create interfaces ----------------------------------------------------
        interfaces = {}
        for exchange in exchanges:
            self.logger.log('Ratio', 'Connecting to ' + exchange, print_only=True)
            bp = {'marketdata': {}}
            interfaces[exchange] = Interface(exchange, self.__handler,
                                             blueprint=bp,
                                             logger=self.logger,
                                             quick=True)
        return interfaces

    def create_polls(self, blueprint):
        polls = []
        for item in blueprint:
            polls += [Poll(self.update_ratio, args=[item], frequency=1/45, delay=1)]

    def update_ratio(self, item):
        now = time()
        reference = item['reference']
        home = item['home']
        window = item['window']
        # Request candles ------------------------------------------------------
        home_candles = self.interfaces[home['exchange']].get_candles(home['pair'], window)
        ref_candles = self.interfaces[reference['exchange']].get_candles(reference['pair'], window)
        # Format to DataFrames -------------------------------------------------
        try:
            df_home = pd.DataFrame(home_candles).set_index('index').sort_index()
            df_home = df_home[df_home.index >= (now - (window*60))]
            df_reference = pd.DataFrame(ref_candles).set_index('index').sort_index()
            df_reference = df_reference[df_reference.index >= (now - (window*60))]
            # Calculate ratio ------------------------------------------------------
            if (not df_home.empty) and (not df_reference.empty) and (len(df_home) >= 3) and (len(df_reference) >= 3):
                df_ratio = df_home / df_reference
                ratio = df_ratio['close'].mean()
                self.ratio[reference['exchange']][reference['pair']][home['exchange']][home['pair']][window] = ratio
                self.logger.log('Ratio', 'Updated ratio for ' + home['pair'] + ': ' + str(ratio), print_only=True)
                # self.logger.log('Ratio', {item: str(self.ratio[item]) for item in self.ratio}, silenced=True)
            else:
                self.logger.log('Ratio', 'Not enough trades for ' + home['pair'], print_only=True)
        except Exception as e:
            self.logger.log('Ratio', 'Error updating ratio for ' + home['pair'] + '. Message: ' + str(e), print_only=True)

    def get(self, ref_exchange, ref_pair, home_exchange, home_pair, window):
        try:
            return self.ratio[ref_exchange][ref_pair][home_exchange][home_pair][window]
        except Exception as e:
            return None
