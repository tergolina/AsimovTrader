from .builder.builder import Builder
from .logger.logger import Logger
from .module.module import Module
from datetime import datetime


class Trader(Module):
    def connect(self):
        print(datetime.now(), '- [ Trader ] Creating Logger...')
        self.logger = Logger(parameters=self.parameters)
        self.log_parameters()

    def start(self):
        print(datetime.now(), '- [ Trader ] Creating Builder...')
        self.builder = Builder(modules=self.modules,
                               parameters=self.parameters,
                               logger=self.logger)

    def log_parameters(self):
        d = self.parameters.copy()
        d.pop('credentials')
        self.logger.log('parameters', d)
