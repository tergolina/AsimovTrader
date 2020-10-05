from ...interface.connectors.poll import Poll
from .feeder import Feeder
from threading import Lock, Event
from datetime import datetime
import numbers

class Reference(Feeder):
    def identify_parameters(self, parameters):
        self.relationship = parameters.get('relationship')
        self.group_method = parameters.get('group_method')
        self.get_method = parameters.get('get_method', {})

    def identify_modules(self, modules):
        self.modules = modules

    def connect(self):
        xs = []
        for z in self.relationship.split('/'):
            for y in z.split('*'):
                for x in y.split('+'):
                    xs += [x]

        self.notifiers = {}
        for x in xs:
            if x not in self.notifiers:
                self.notifiers[x] = Poll(self.set_notification, trigger=self.modules[x].notification)

    def set_notification(self):
        self.notification.set()

    # Read ---------------------------------------------------------------------
    def group_window(self, result):
        if isinstance(result, dict):
            for key in result:
                result[key] = self.group_window(result[key])
        elif isinstance(result, list):
            if self.group_method == 'std':
                if len(result) > 1:
                    lenght = len(result)
                    mean = sum(result) / lenght
                    result = (sum([(x - mean)**2 for x in result]) / lenght)**0.5
            elif self.group_method == 'mean':
                if len(result) > 0:
                    result = sum(result) / len(result)
        return result

    def combine_references(self, result, ref, op, first_call=False):
        if first_call and (result is None):
            return ref
        elif ref is not None:
            if isinstance(ref, numbers.Real) and isinstance(result, numbers.Real):
                if op == '/':
                    return result / ref
                elif op == '*':
                    return result * ref
                elif op == 'min':
                    return min(result, ref)
                elif op == 'max':
                    return max(result, ref)
            elif isinstance(ref, dict) and isinstance(result, dict):
                if op == '+':
                    result['bid'] = self.combine_references(result['bid'], ref['bid'], 'min')
                    result['ask'] = self.combine_references(result['ask'], ref['ask'], 'max')
                    return result
                elif op == '/':
                    result['bid'] = self.combine_references(result['bid'], ref['ask'], op)
                    result['ask'] = self.combine_references(result['ask'], ref['bid'], op)
                    for key in result:
                        if (key != 'bid') and (key != 'ask'):
                            result[key] = self.combine_references(result[key], ref[key], op)
                    return result
                elif op == '*':
                    for key in result:
                        result[key] = self.combine_references(result[key], ref[key], op)
                    return result
            elif isinstance(ref, dict) and isinstance(result, numbers.Real):
                for key in ref:
                    ref[key] = self.combine_references(result, ref[key], op)
                return ref
            elif isinstance(ref, numbers.Real) and isinstance(result, dict):
                for key in result:
                    result[key] = self.combine_references(result[key], ref, op)
                return result
            elif isinstance(ref, list) and isinstance(result, list):
                if len(ref) == len(result):
                    for i in range(len(ref)):
                        result[i] = self.combine_references(result[i], ref[i], op)
                return result
            elif isinstance(ref, list) and isinstance(result, numbers.Real):
                for i in range(len(ref)):
                    ref[i] = self.combine_references(result, ref[i], op)
                return ref
            elif isinstance(ref, numbers.Real) and isinstance(result, list):
                for i in range(len(result)):
                    result[i] = self.combine_references(result[i], ref, op)
                return result
        return None

    def get_reference(self, modules=None, compass=None, relationship=None):
        modules = self.modules if modules is None else modules
        relationship = self.relationship if relationship is None else relationship
        first_call = True
        result = None
        if '+' in relationship:
            for x in relationship.split('+'):
                ref = self.get_reference(modules=modules, relationship=x)
                result = self.combine_references(result, ref, '+', first_call=first_call)
                first_call = False
        elif '/' in relationship:
            for x in relationship.split('/'):
                ref = self.get_reference(modules=modules, relationship=x)
                result = self.combine_references(result, ref, '/', first_call=first_call)
                first_call = False
        elif '*' in relationship:
            for x in relationship.split('*'):
                ref = self.get_reference(modules=modules, relationship=x)
                result = self.combine_references(result, ref, '*', first_call=first_call)
                first_call = False
        elif relationship in modules:
            result = modules[relationship].get(self.get_method.get(relationship))
        return result

    def get(self, method=None):
        result = None
        try:
            result = self.group_window(self.get_reference())
        except Exception as e:
            dc = {}
            for x in self.relationship.split('/').split('*').split('+'):
                dc[x] = self.modules[x].get(self.get_method.get(x))
            print(datetime.now(), '- [ Reference ]', str(e), str(dc))
        return result

    def probe(self):
        try:
            p = self.get()
        except Exception as e:
            p = str(e)
        print(datetime.now(), '- [ Reference ] Probe - Data:', p)
