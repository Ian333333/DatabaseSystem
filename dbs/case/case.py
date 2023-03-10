from dbs import TYPE_MAP
from dbs.case import SYMBOL_MAP


class BaseCase:
    def __init__(self, condition, symbol):
        self.condition = condition
        self.symbol = symbol

    def __call__(self, data, data_type):
        self.condition = TYPE_MAP[data_type.value](self.condition)

        if isinstance(self.condition, str):
            self.condition = self.condition.replace("'", '').replace('"', '')

        return SYMBOL_MAP[self.symbol](data, self.condition)


class BaseListCase(BaseCase):
    def __call__(self, data, data_type):
        if not isinstance(self.condition, list):
            raise TypeError('condition type error, value must be %s' % data_type)

        conditions = []

        for value in self.condition:
            value = TYPE_MAP[data_type.value](value)

            if isinstance(value, str):
                value = value.replace("'", '').replace('"', '')

            conditions.append(value)
        return SYMBOL_MAP[self.symbol](data, conditions)


class IsCase(BaseCase):
    def __init__(self, condition):
        super().__init__(condition, symbol='=')


class IsNotCase(BaseCase):
    def __init__(self, condition):
        super().__init__(condition, symbol='!=')


class InCase(BaseListCase):
    def __init__(self, condition):
        super().__init__(condition, symbol='IN')


class NotInCase(BaseListCase):
    def __init__(self, condition):
        super().__init__(condition, symbol='Not_IN')


class GreaterCase(BaseCase):
    def __init__(self, condition):
        super().__init__(condition, symbol='>')


class LessCase(BaseCase):
    def __init__(self, condition):
        super().__init__(condition, symbol='<')


class GreaterOrEqualCase(BaseCase):
    def __init__(self, condition):
        super().__init__(condition, symbol='>=')


class LessOrEqualCase(BaseCase):
    def __init__(self, condition):
        super().__init__(condition, symbol='<=')


class LikeCase(BaseCase):
    def __init__(self, condition):
        super().__init__(condition, symbol='LIKE')

    def __call__(self, data, data_type):
        self.condition = TYPE_MAP[data_type.value](self.condition)
        return SYMBOL_MAP[self.symbol](str(data), self.condition)


class RangeCase(BaseCase):
    def __init__(self, start, end):
        super().__init__((int(start), int(end)), symbol='RANGE')

    def __call__(self, data, data_type):
        if not isinstance(self.condition, tuple):
            raise TypeError('condition must be a tuple')

        return SYMBOL_MAP[self.symbol](data, self.condition)
