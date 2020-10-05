from ...interface.interface import Interface
from ..feeders.feeder import Feeder
from ..feeders.window import Window
from ..feeders.slippage import Slippage
from ..feeders.pressure import Pressure
from ..feeders.premia import Premia
from ..feeders.reference import Reference
from ..actors.hedger import Hedger
from ..actors.maker import Maker
from ..actors.taker import Taker
from ..module.module import Module
from datetime import datetime



class Builder(Module):
    # Initialize ---------------------------------------------------------------
    def identify_parameters(self, parameters):
        self.credentials = parameters.get('credentials')
        self.blueprint = parameters.get('blueprint')

    def initialize(self):
        self.ACTORS = ['Taker', 'Maker', 'Hedger']
        self.FEEDERS = ['Feeder', 'Window', 'Slippage', 'Pressure']

    def connect(self):
        self.interfaces = self.create_interfaces()

    def start(self):
        self.modules.update(self.build())

    # Build --------------------------------------------------------------------
    def simplify(self, blueprint):
        replacements = {}
        for x in blueprint:
            if x not in replacements:
                i = 0
                for y in blueprint:
                    if blueprint[x] == blueprint[y]:
                        i += 1
                    if i > 1:
                        replacements[y] = x
        print (replacements)

    def list_modules(self, blueprint=None, include=[]):
        blueprint = self.blueprint if blueprint is None else blueprint
        modules = []
        if isinstance(blueprint, dict):
            for item in blueprint:
                if (item == 'module') and (blueprint[item] in include):
                    modules += [blueprint]
                else:
                    modules += self.list_modules(blueprint=blueprint[item], include=include)
        return modules

    def get_marketdata_blueprints(self):
        modules = self.list_modules(include=self.FEEDERS)
        blueprints = {}
        for module in modules:
            parameters = module['parameters']

            exchange = parameters['exchange']
            pair = parameters['pair']
            source = parameters['source']

            blueprints.setdefault(exchange, {})
            blueprints[exchange].setdefault('marketdata', {})
            blueprints[exchange]['marketdata'].setdefault(source, [])

            if pair not in blueprints[exchange]['marketdata'][source]:
                blueprints[exchange]['marketdata'][source] += [pair]
        return blueprints

    def get_account_blueprints(self):
        modules = self.list_modules(include=self.ACTORS)
        blueprints = {}
        for module in modules:
            parameters = module['parameters']
            account = parameters['target']
            exchange = parameters['exchange']

            if isinstance(account, list):
                for acc in account:
                    blueprints[acc] = {exchange: {'account': self.credentials['accounts'][acc]}}
            else:
                blueprints[account] = {exchange: {'account': self.credentials['accounts'][account]}}
        return blueprints

    def create_interfaces(self):
        interfaces = {'marketdata': {}, 'account': {}}

        print(datetime.now(), '- [ Builder ] Creating account Interfaces...')
        account_blueprints = self.get_account_blueprints()
        for account in account_blueprints:
            blueprints = account_blueprints[account]
            for exchange in blueprints:
                interfaces['account'][account] = Interface(exchange, blueprint=blueprints[exchange], logger=self.logger)
                print(datetime.now(), '- [ Builder ] Created account Interface for', exchange, '| Blueprint:', blueprints[exchange])

        print(datetime.now(), '- [ Builder ] Creating marketdata Interfaces...')
        marketdata_blueprints = self.get_marketdata_blueprints()
        for exchange in marketdata_blueprints:
            interfaces['marketdata'][exchange] = Interface(exchange, blueprint=marketdata_blueprints[exchange], logger=self.logger)
            print(datetime.now(), '- [ Builder ] Created marketdata Interface for', exchange, '| Blueprint:', marketdata_blueprints[exchange])

        print(datetime.now(), '- [ Builder ] Interfaces created!', interfaces)
        return interfaces

    # Build --------------------------------------------------------------------
    def build(self, modules=None, blueprint=None):
        blueprint = blueprint if blueprint is not None else self.blueprint
        modules = modules if modules is not None else {'interfaces': self.interfaces}

        for x in blueprint:
            modules.update(self.build(modules=modules, blueprint=blueprint[x].get('modules', {})))

            if x not in modules:
                name = blueprint[x]['module']
                parameters = blueprint[x].get('parameters', {})

                print(datetime.now(), '- [ Builder ] Building', x, name, '| Parameters:', parameters)
                modules[x] = self.identify(name)(modules, parameters, logger=self.logger)
        return modules

    def identify(self, name):
        if name == 'Feeder':
            return Feeder
        elif name == 'Window':
            return Window
        elif name == 'Slippage':
            return Slippage
        elif name == 'Pressure':
            return Pressure
        elif name == 'Maker':
            return Maker
        elif name == 'Taker':
            return Taker
        elif name == 'Hedger':
            return Hedger
        elif name == 'Premia':
            return Premia
        elif name == 'Reference':
            return Reference
        return Module
