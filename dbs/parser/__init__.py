import re

from dbs.case.case import *


class SQLParser:
    def __init__(self):
        self.__pattern_map = {
            'SELECT': r'(SELECT|select) (.*) (FROM|from) (.*)',
            'UPDATE': r'(UPDATE|update) (.*) (SET|set) (.*)',
            'INSERT': r'(INSERT|insert) (INTO|into) (.*)\((.*)\) (VALUES|values)\((.*)\)'
        }

        self.__action_map = {
            'SELECT': self.__select,
            'UPDATE': self.__update,
            'DELETE': self.__delete,
            'INSERT': self.__insert,
            'USE': self.__use,
            'EXIT': self.__exit,
            'QUIT': self.__exit,
            'SHOW': self.__show,
            'DROP': self.__drop
        }

        self.SYMBOL_MAP = {
            'IN': InCase,
            'NOT_IN': NotInCase,
            '>': GreaterCase,
            '<': LessCase,
            '=': IsCase,
            '!=': IsNotCase,
            '>=': GreaterOrEqualCase,
            '<=': LessOrEqualCase,
            'LIKE': LikeCase,
            'RANGE': RangeCase
        }

    def __filter_space(self, obj):
        res = []
        for x in obj:
            if x.strip() == '' or x.strip() == 'AND' or x.strip() == 'OR':
                continue
            res.append(x)

        return res

    def parse(self, statement):
        statement_tmp = statement

        if 'where' in statement:
            statement = statement.split('where')
        else:
            statement = statement.split('WHERE')

        base_statement = self.__filter_space(statement[0].split(' '))

        if len(base_statement) < 2 and base_statement[0] not in ['exit', 'quit']:
            raise Exception('Syntax Error for: %s' % statement_tmp)

        action_type = base_statement[0].upper()

        if action_type not in self.__action_map:
            raise Exception('Syntax Error for: %s' % statement_tmp)

        action = self.__action_map[action_type](base_statement)

        if action is None or 'type' not in action:
            raise Exception('Syntax Error for: %s' % statement_tmp)

        action['conditions'] = {}

        conditions = None

        if len(statement) == 2:

            conditions = self.__filter_space(statement[1].split(' '))

        if conditions:
            for index in range(0, len(conditions), 3):
                field = conditions[index]
                symbol = conditions[index + 1].upper()
                condition = conditions[index + 2]

                if symbol == 'RANGE':
                    condition_tmp = condition.replace('(', '').replace(')', '').split(',')
                    start = condition_tmp[0]
                    end = condition_tmp[1]
                    case = self.SYMBOL_MAP[symbol](start, end)
                elif symbol == 'IN' or symbol == 'NOT_IN':
                    condition_tmp = condition.replace('(', '').replace(')', '').replace(' ', '').split(',')
                    condition = condition_tmp
                    case = self.SYMBOL_MAP[symbol](condition)
                else:
                    case = self.SYMBOL_MAP[symbol](condition)

                action['conditions'][field] = case
        return action

    def __get_comp(self, action):
        return re.compile(self.__pattern_map[action])

    def __select(self, statement):
        comp = self.__get_comp('SELECT')
        ret = comp.findall(" ".join(statement))

        if ret and len(ret[0]) == 4:
            fields = ret[0][1]
            table = ret[0][3]

            if fields != '*':
                fields = [field.strip() for field in fields.split(",")]

            return {
                'type': 'search',
                'fields': fields,
                'table': table
            }

        return None

    def __update(self, statement):
        statement = ' '.join(statement)
        comp = self.__get_comp('UPDATE')
        res = comp.findall(statement)
        print(res)
        if res and len(res[0]) == 4:
            data = {
                'type': 'update',
                'table': res[0][1],
                'data': {}
            }
            set_statement = res[0][3].split(',')
            for s in set_statement:
                s = s.split('=')
                field = s[0].strip()
                value = s[1].strip()
                if "'" in value or '"' in value:
                    value = value.replace('"', '').replace("'", '').strip()
                else:
                    try:
                        value = int(value.strip())
                    except:
                        return None
                data['data'][field] = value

            return data
        return None

    def __delete(self, statement):
        print(statement)
        return {
            'type': 'delete',
            'table': statement[2]
        }

    def __insert(self, statement):
        statement = ' '.join(statement)
        comp = self.__get_comp('INSERT')
        res = comp.findall(statement)

        if res and len(res[0]) == 6:
            res_tmp = res[0]
            data = {
                'type': 'insert',
                'table': res_tmp[2],
                'data': {}
            }
            fields = res_tmp[3].split(',')
            values = res_tmp[5].split(',')

            for (field, value) in (fields, values):
                if "'" in value or '"' in value:
                    value = value.replace('"', '').replace("'", '').strip()
                else:
                    try:
                        value = int(value.strip())
                    except:
                        return None
                data['data'][field] = value
            return data
        return None

    def __use(self, statement):
        return {
            'type': 'use',
            'database': statement[1]
        }

    def __exit(self, _):
        return {
            'type': 'exit'
        }

    # database list or table list
    def __show(self, statement):
        level = statement[1]

        if level.upper() == 'DATABASES':
            return {
                'type': 'show',
                'level': 'databases'
            }
        if level.upper() == 'TABLES':
            return {
                'type': 'show',
                'level': 'tables'
            }

    def __drop(self, statement):
        level = statement[1]

        if level.upper() == 'DATABASE':
            return {
                'type': 'drop',
                'level': 'database',
                'name': statement[2]
            }
        if level.upper() == 'TABLE':
            return {
                'type': 'drop',
                'level': 'table',
                'name': statement[2]
            }
