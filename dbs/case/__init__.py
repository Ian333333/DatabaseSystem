from dbs.core import TYPE_MAP

LIKE_SYMBOL = '%'


def __is(data, condition):
    return data == condition


def __is_not(data, condition):
    return data != condition


def __in(data, condition):
    return data in condition


def __not_in(data, condition):
    return data not in condition


def __greater(data, condition):
    return data > condition


def __less(data, condition):
    return data < condition


def __greater_or_equal(data, condition):
    return data >= condition


def __less_or_equal(data, condition):
    return data <= condition


def __like(data, condition):
    tmp = condition.split(LIKE_SYMBOL)
    length = len(tmp)
    if length == 3:
        condition = tmp[1]
    elif length == 2:
        raise Exception('Syntax Error')
    elif length == 1:
        condition = tmp[0]
    return condition in data


def __range(data, condition):
    return condition[0] <= data <= condition[1]


SYMBOL_MAP = {
    'IN': __in,
    'NOT_IN': __not_in,
    '>': __greater,
    '<': __less,
    '=': __is,
    '!=': __is_not,
    '>=': __greater_or_equal,
    '<=': __less_or_equal,
    'LIKE': __like,
    'RANGE': __range
}