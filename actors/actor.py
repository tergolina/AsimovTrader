from ...interface.interface import Interface
from ...interface.connectors.poll import Poll
from ..module.module import Module
from threading import Lock, Event
from datetime import datetime
from queue import Queue
from time import time
from copy import copy
from time import time, sleep
import json

class Actor(Module):
    # Virtual Methods from Module ----------------------------------------------
    def identify_parameters(self, parameters):
        self.pair = parameters.get('pair')
        self.exchange = parameters.get('exchange')
        # Module indentifiers --------------------------------------------------
        self.reference_name = parameters.get('reference')
        self.guide_name = parameters.get('guide')
        self.target_name = parameters.get('target')
        self.hedge_name = parameters.get('hedge')
        self.actor_names = parameters.get('actors', [])
        # Parameters -----------------------------------------------------------
        self.bet_quantity = parameters.get('bet_quantity')
        self.bet_volume = parameters.get('bet_volume')
        self.premia = parameters.get('premia')
        self.frequency = parameters.get('frequency')
        self.close_only = parameters.get('close_only', False)
        self.tolerance = parameters.get('tolerance', 0)
        self.check_guide = parameters.get('check_guide', False)
        self.leverage = parameters.get('leverage', 1 if self.exchange not in ['binance', 'hitbtc'] else 0)

    def identify_modules(self, modules):
        self.target = modules['interfaces']['account'].get(self.target_name)
        self.hedge = modules.get(self.hedge_name)
        self.guide = modules.get(self.guide_name)
        self.reference = modules.get(self.reference_name)

    def initialize(self):
        # Standard Data --------------------------------------------------------
        self.bid = None
        self.ask = None
        self.orders = None
        self.position = None
        self.position_base = None
        self.balance = None
        self.info = None
        # Internal Data --------------------------------------------------------
        self.last_price = None
        # Control --------------------------------------------------------------
        self.reseting = False
        self.do_reset = Event()
        self.trigger = Event()
        self.on_error = Event()
        self.done_placing = Event()
        self.updated = {'bid': Event(), 'ask': Event(), 'position': Event(), 'orders': Event(), 'balance': Event(), 'subscription': Event()}
        self.timeout = 4
        self.error_threshold = 3
        self.consecutive_errors = {}

    def connect(self):
        filter = {'account': [self.pair]}
        self.target.subscribe(self.message_to_queue, filter)
        self.barrier()

    def is_ready(self):
        if self.reference.is_ready():
            print(datetime.now(), '- [ Actor ] Reference is ready!', self.pair)
            if self.orders is not None:
                if (self.position is not None) or (self.balance is not None):
                    print(datetime.now(), '- [ Actor ] Account is ready!', self.pair)
                    if (self.hedge is None) or self.hedge.is_ready():
                        if (self.guide is None) or self.guide.is_ready():
                            return True
        print(datetime.now(), '- [ Barrier ] Module not yet ready:', self.pair)
        return False

    def start(self):
        if self.hedge:
            self.hedge.subscribe(self)
        self.reseter = Poll(self.reset, trigger=self.do_reset, imediate=False)
        self.managers = {s: Poll(self.manager, args=[s], frequency=self.frequency, n=1, trigger=self.trigger) for s in ['bid', 'ask']}
        self.notifier = Poll(self.set_trigger, trigger=self.reference.notification)

    def pause(self):
        for s in self.managers:
            self.managers[s].pause()

    def play(self):
        for s in self.managers:
            self.managers[s].play()

    def set_trigger(self):
        self.trigger.set()

    # Update -------------------------------------------------------------------
    def update_account(self):
        self.done_placing.wait()
        self.updated['balance'].clear()
        self.updated['orders'].clear()
        self.updated['position'].clear()
        self.target.do_update['balance'].set()
        self.target.do_update['open_orders'].set()
        self.target.do_update['position'].set()
        self.updated['balance'].wait(self.timeout)
        self.updated['orders'].wait(self.timeout)
        self.updated['position'].wait(self.timeout)

    # Handler ------------------------------------------------------------------
    def handle_account(self, event):
        self.trigger.set()
        # print(datetime.now(), '- [ Actor ] Updated', event)

    def handle_trade(self, event, update):
        self.trigger.set()

    def handle_order(self, event, update):
        pass

    def handle_error(self, update):
        pass

    def handle_rate_limit(self, update):
        print(datetime.now(), '- [ Actor ] Internal rate-limit reached for', self.pair, 'at', self.exchange + '.', 'Rate limit:', self.target.rate_limit, 'Placement count:', str(self.target.placement_count))

    def handler(self, message):
        state = json.loads(message)
        self.update_state(state)
        event = state['event']
        update = state['update']

        if event in ['subscription', 'position', 'balance', 'orders']:
            self.updated[event].set()
            self.handle_account(event)

        elif event in ['buy', 'sell']:
            e = 'Bought' if event == 'buy' else 'Sold'
            print(datetime.now(), '- [ Actor ]', e, update['pair'], '-', 'Price:',update['price'], '| Quantity:', update['quantity'], '| ID:', update['id'])
            self.handle_trade(event, update)

        elif event in ['place', 'replace', 'cancel']:
            e = (event + 'ed').replace('ee', 'e')
            e = e[0].upper() + e[1:]
            print(datetime.now(), '- [ Actor ]', e, update['side'], 'order for', update['pair'], '-', 'Price:',update['price'], '| Quantity:', update['quantity'], '| ID:', update['id'])
            side = 'bid' if update['side'] == 'buy' else 'ask'
            self.updated[side].set()
            self.handle_order(event, update)

        elif event == 'error':
            self.on_error.set()
            self.handle_error(update)

        elif event == 'verify':
            if self.verify(update):
                self.do_reset.set()

        elif event == 'rate-limit':
            self.handle_rate_limit(update)

    def update_state(self, state):
        self.info = state['info']
        account = state['account']
        self.orders = account['orders']
        self.balance = account['balance']
        self.position = account['position']
        self.position_base = account['position_base']

    #---------------------------------------------------------------------------
    def get_premia(self, side):
        return self.premia

    def get_price(self, side):
        premia = self.get_premia(side)
        ref = self.reference.get()
        if isinstance(ref, dict):
            if side in ref:
                ref = ref[side]
        self.last_price = ref
        if side == 'bid':
            return ref * (1 - premia)
        elif side == 'ask':
            return ref * (1 + premia)

    def get_position(self):
        if self.position_base is not None:
            return self.position_base[self.pair]
        elif self.position is not None:
            if self.pair in self.position:
                return self.position[self.pair]
            else:
                return 0
        else:
            return None

    def get_quantity(self, side):
        quantity = 0
        if self.leverage == 0:
            a, b = self.pair.split('/')
            total_a = self.balance[a]['available'] + self.balance[a]['reserved'] if (a in self.balance) else 0
            total_b = self.balance[b]['available'] + self.balance[b]['reserved'] if (b in self.balance) else 0

            if self.bet_volume:
                self.bet_quantity = self.bet_volume / self.last_price

            # Caso seja BNB/ETH, precisa manter BNB suficiente para pagar fees
            total_a = total_a - 1 if a == 'BNB' else total_a

            if side in ['buy', 'bid']:
                quantity = min(self.bet_quantity - total_a, (total_b / self.last_price))
            else:
                quantity = total_a
        else:
            position = 0
            if self.pair in self.position:
                position = self.position[self.pair]
            if self.close_only:
                if side in ['buy', 'bid']:
                    quantity = - position
                else:
                    quantity = position
            else:
                if self.bet_volume:
                    self.bet_quantity = self.bet_volume / self.last_price

                if side in ['buy', 'bid']:
                    quantity = self.bet_quantity - position
                else:
                    quantity = self.bet_quantity + position
        return quantity

    # Control ------------------------------------------------------------------
    def verify(self, update):
        for item in update:
            if self.pair in update[item]:
                self.consecutive_errors.setdefault(item, 0)
                self.consecutive_errors[item] += 1
                print(datetime.now(), '- [ Actor ] Internal', self.pair, 'is inconsistent with server', item)
                self.print_state()
            else:
                self.consecutive_errors[item] = 0
        return self.consecutive_errors[item] > self.error_threshold

    def has_order(self, side):
        return (self.pair in self.orders) and (self.orders[self.pair][side] != [])

    def enough_quantity(self, quantity):
        enough = True
        if self.bet_quantity:
            enough = quantity > (0.1 * self.bet_quantity)
        return enough

    # Manage -------------------------------------------------------------------
    def hedger(self):
        pass

    def manager(self, side):
        pass

    # Send ---------------------------------------------------------------------
    def place(self, side, price, quantity, type='maker'):
        trade_side = 'buy' if (side == 'bid') else 'sell'
        print(datetime.now(), '- [ Actor ]', 'Placing ' + type + ' ' + side + ' order for ' + self.pair + ' - Price: ' + str(price) + ' | Quantity: ' + str(quantity))
        self.updated[side].clear()
        self.target.place_order(self.pair, trade_side, price, quantity, type=type)
        if not self.updated[side].wait(self.timeout):
            print(datetime.now(), '- [ Actor ] Placement timed out...', self.pair, side)
            return False
        return True

    def replace(self, side, price, quantity, type='maker'):
        id = self.orders[self.pair][side][0]['id']
        print(datetime.now(), '- [ Actor ]', 'Replacing ' + type + ' ' + side + ' order for ' + self.pair + ' - Price: ' + str(price) + ' | Quantity: ' + str(quantity) + ' | ID: ' + str(id))
        self.updated[side].clear()
        self.target.replace_order(id, price, quantity=quantity, type=type)
        if not self.updated[side].wait(self.timeout):
            print(datetime.now(), '- [ Actor ] Replacement timed out...', self.pair, side)
            return False
        return True

    def cancel(self, side):
        id = self.orders[self.pair][side][0]['id']
        for i in range(2):
            print(datetime.now(), '- [ Actor ]', 'Canceling ' + side + ' order for ' + self.pair + ' | ID: ' + str(id))
            self.updated[side].clear()
            self.target.cancel_order(id)
            if self.updated[side].wait(self.timeout):
                print(datetime.now(), '- [ Actor ] Cancelment timed out. Retrying...', self.pair, side)
                break
        else:
            return False
        return True

    def reset(self):
        print(datetime.now(), '- [ Actor ] Reseting', self.pair)
        self.reseting = True
        try:
            self.target.reset_orders(self.pair)
            self.target.update_position(self.pair)
            self.target.update_balance(self.pair)
            for item in self.consecutive_errors:
                self.consecutive_errors[item] = 0
            self.print_state()
        except Exception as e:
            print(datetime.now(), '- [ Actor ]', 'Reset error', self.pair, str(e))
        self.reseting = False

    def print_state(self):
        hedge_position = self.hedge.get_position() if self.hedge is not None else None
        print(datetime.now(), '- [ Actor ] State - Orders:', self.orders, '| Position:', self.get_position(), '| Balance:', self.balance, '| Hedged:', hedge_position)
