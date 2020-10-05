from ...interface.interface import Interface
from ...interface.connectors.poll import Poll
from ..module.module import Module
from threading import Lock, Event
from datetime import datetime
from time import time
import pandas as pd
import numpy as np
import json


class Premia(Module):
    def identify_parameters(self, parameters):
        self.window = parameters.get('window')
        self.x = parameters.get('x')
        
    def identify_modules(self, modules):
        self.target_feeder = modules.get('target_feeder')
        self.reference_feeder = modules.get('reference_feeder')

    def initialize(self):
        self.reference_history = None
        self.premia = None

    def start(self):
        self.updater = Poll(self.update_premia)

    def update_premia(self):
        target_trades = self.get_target_trades()
        reference_trades =  self.get_reference_trades(target_trades[0]['timestamp'])
        df_reference = pd.DataFrame(reference_trades)
        df_target = pd.DataFrame(target_trades)
        df_reference = df_reference.set_index('timestamp')
        df_target = df_target.set_index('timestamp')

        # Relative Spread
        df_reference['price'] = df_reference['price']
        df_reference['bid'] = df_reference['price']
        df_reference['ask'] = df_reference['price']
        df_reference.loc[df_reference['side'] == 'buy', 'bid'] = np.nan
        df_reference.loc[df_reference['side'] == 'sell', 'ask'] = np.nan
        df_reference = df_reference.ffill()
        df_reference['bid'] = df_reference[['bid', 'ask']].min(axis=1)
        df_reference['ask'] = df_reference[['bid', 'ask']].max(axis=1)
        df_reference = df_reference[['bid', 'ask']]
        
        df_target['bid'] = df_target['price']
        df_target['ask'] = df_target['price']
        df_target.loc[df_target['side'] == 'buy', 'sell'] = np.nan
        df_target.loc[df_target['side'] == 'sell', 'buy'] = np.nan
        df_target = df_target[['price', 'buy', 'sell']]

        df = df_target.join(df_reference, how='outer')
        df[['bid', 'ask']] == df[['bid', 'ask']].ffill().bfill()
        df = df[df['price'] == df['price']]
        df['bid_spread'] = (df['bid'] / df['sell']) - 1
        df['ask_spread'] = (df['buy'] / df['ask']) - 1
        df['spread'] = df[['bid_spread', 'ask_spread']].sum(1)

        spread = df['spread'].values.sort()
        self.premia = spread[int(len(spread) * self.x)]

    def get_reference_trades(self, since):
        pair = self.reference_feeder.pair
        trades = self.reference_feeder.interface.get_trades(pair, int(time() - since)+5)
        return trades

    def get_target_trades(self):
        pair = self.target_feeder.pair
        trades = self.target_feeder.interface.get_trades(pair, 6*60)
        return trades[-self.window:]

    def is_ready(self):
        return self.premia is not None

    def get(self):
        return self.premia