from ...database.utils import write_in_tickstore
from ...interface.connectors.poll import Poll
from ..module.module import Module
from datetime import datetime, timedelta, timezone
from arctic import Arctic, TICK_STORE
from arctic.date import DateRange
from time import sleep, time
from copy import deepcopy, copy
import pandas as pd
import collections
import json
import matplotlib.pyplot as plt
from arctic import TICK_STORE


class Logger(Module):
    def identify_parameters(self, parameters):
        self.credentials = parameters.get('credentials')
        self.description = parameters.get('description')
        self.silenced = parameters.get('silenced', True)
        self.period = parameters.get('period', 60)
        self.frequency = parameters.get('frequency')

    def initialize(self):
        self.buffer = {}
        if not self.frequency:
            self.frequency = 1 / max(60, self.period)

    def connect(self):
        self.lib = self.load_database()

    def start(self):
        if self.lib != None:
            self.manager = Poll(self.write_buffer, frequency=self.frequency, imediate=False)

    def load_database(self):
        name = self.description['name']
        version = self.description['version']
        credentials = self.credentials['database']
        lib = None
        try:
            if credentials != {}:
                store = Arctic('mongodb://{a}:{b}@{c}:27017/{d}'.format(a=credentials['user'],
                                                                        b=credentials['password'],
                                                                        c='10.142.0.14',
                                                                        d='arctic_STRATEGY_MM'))
                str_version = str(version).replace('.', '_')
                lib_name = 'STRATEGY_MM.LOG_' + str_version + '_' + name.upper()
                if lib_name not in store.list_libraries():
                    store.initialize_library(lib_name, TICK_STORE)
                lib = store[lib_name]
        except Exception as e:
            if not self.silenced:
                print ('Error loading library for logger:', str(e))
        return lib

    def log(self, tag, data, silenced=False, print_only=False):
        """ Loga dados

            Args:
                tag (str): Tag para classificacao do log
                data (dict): Dados a serem logados
                silenced (bool): Se deve printar o que foi logado
                print_only (bool): Se deve funcionar como um datetime print apenas
        """
        if (self.lib == None) or print_only:
            if not silenced:
                print (datetime.now(), '- [', tag, ']', data)
        elif isinstance(data, collections.Mapping):
            d = deepcopy(data)
            d['index'] = time()
            self.lock.acquire()
            if tag not in self.buffer:
                self.buffer[tag] = []
            self.buffer[tag] += [d]
            self.lock.release()
            if (not self.silenced) and (not silenced):
                print (tag, data)
        else:
            if not self.silenced:
                print ('Message should be a dictionary')

    def write_buffer(self):
        for tag in self.buffer.keys():
            if self.buffer[tag] != []:
                self.lock.acquire()
                buff = deepcopy(self.buffer[tag])
                self.buffer[tag] = []
                self.lock.release()
                for i in range(len(buff)):
                    for key in buff[i]:
                        if isinstance(buff[i][key], collections.Mapping) or isinstance(buff[i][key], list):
                            buff[i][key] = json.dumps(buff[i][key])
                data = pd.DataFrame()
                try:
                    data = pd.DataFrame(buff).set_index('index').sort_index()
                    data.index = pd.to_datetime(data.index, unit='s').tz_localize('UTC')
                    for column in data.columns:
                        if data[column].dtype == object:
                            data[column] = data[column].fillna(value='')
                except Exception as e:
                    print ('Error converting logs to dataframe:', tag, e, buff)
                try:
                    if not data.empty:
                        write_in_tickstore(data, self.lib, tag.upper())
                except Exception as e:
                    print ('Error writing logs to database:', tag, e, buff)
