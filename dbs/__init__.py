from dbs.core import *
from dbs.core.database import Database

import prettytable

import base64

import os

from dbs.parser import SQLParser


def _decode_db(content):
    content = base64.decodebytes(content)
    return content.decode()[::-1]


def _encode_db(content):
    content = content[::-1].encode()
    return base64.encodebytes(content)


class Engine:
    def __init__(self, db_name=None, format_type='dict', path='db.data'):
        self.__database_names = []
        self.__database_objs = {}

        self.__current_db = None

        if db_name is not None:
            self.select_db(db_name)

        self.__format_type = format_type
        self.__path = path

        self.__load_databases()

        self.__action_map = {
            'insert': self.__insert,
            'update': self.__update,
            'search': self.__search,
            'delete': self.__delete,
            'drop': self.__drop,
            'show': self.__show,
            'use': self.__use,
            'exit': self.__exit
        }

    def __insert(self, action):
        table = action['table']
        data = action['data']

        return self.insert(table, data=data)

    def __update(self, action):
        table = action['table']
        data = action['data']
        conditions = action['conditions']

        return self.update(table, data, conditions=conditions)

    def __delete(self, action):
        table = action['table']
        conditions = action['conditions']

        return self.delete(table, conditions=conditions)

    def __search(self, action):
        table = action['table']
        fields = action['fields']
        conditions = action['condition']

        return self.search(table, fields=fields, conditions=conditions)

    def __drop(self, action):
        if action['level'] == 'database':
            return self.drop_database(action['name'])
        return self.drop_table(action['name'])

    def __show(self, action):
        if action['level'] == 'databases':
            return self.get_database(format_type='dict')
        return self.get_table(format_type='dict')

    def __use(self, action):
        return self.select_db(action['database'])

    def __exit(self, _):
        return 'exit'

    def execute(self, statement):
        action = SQLParser().parse(statement)

        res = None

        if action['type'] in self.__action_map:
            res = self.__action_map.get(action['type'])(action)

            if action['type'] in ['insert', 'update', 'delete', 'create', 'drop']:
                self.commit()

        return res

    def run(self):
        while True:
            statement = input('\033[00;37misadb>')
            try:
                res = self.execute(statement)
                if res in ['exit', 'quit']:
                    print('Goodbye!')
                    return

                if res:
                    pt = prettytable.PrettyTable(res[0].keys())
                    pt.align = 'l'
                    for line in res:
                        pt.align = 'r'
                        pt.add_row(line.values())
                    print(pt)
            except Exception as e:
                print('\033[00;31m' + str(e))

    def select_db(self, db_name):
        if db_name not in self.__database_names:
            raise Exception('Database not exists')
        self.__current_db = self.__database_objs[db_name]

    def create_database(self, database_name):
        if database_name in self.__database_names:
            raise Exception('Database exist')

        self.__database_names.append(database_name)
        self.__database_objs[database_name] = Database(database_name)

    def drop_database(self, database_name):
        if database_name not in self.__database_names:
            raise Exception('Database not exists')

        self.__database_names.remove(database_name)
        self.__database_objs.pop(database_name, True)

    def serialize(self):
        return SerializationInterface.json.dumps([
            database.serialize() for database in self.__database_objs.values()
        ])

    def __dump_databases(self):
        with open(self.__path, 'wb') as f:
            content = _encode_db(self.serialize())

            f.write(content)

    # store database into file
    def deserialize(self, content):
        json_data = SerializationInterface.json.loads(content)

        for obj in json_data:
            database = Database.deserialize(obj)
            db_name = database.get_name()

            if db_name not in self.__database_names:
                self.__database_names.append(db_name)
                self.__database_objs[db_name] = database

    # load database from file
    def __load_databases(self):
        if not os.path.exists(self.__path):
            return
        with open(self.__path, 'rb') as f:
            content = f.read()
        if content:
            self.deserialize(_decode_db(content))

    def commit(self):
        self.__dump_databases()

    def rollback(self):
        self.__load_databases()

    def search(self, table_name, fields='*', sort='ASC', **conditions):
        return self.__get_table(table_name).search(fields=fields, sort=sort,
                                                   format_type=self.__format_type, **conditions)

    def insert(self, table_name, **data):
        return self.__get_table(table_name).insert(**data)

    def update(self, table_name, data, **condition):
        return self.__get_table(table_name).update(data, **condition)

    def delete(self, table_name, **condition):
        return self.__get_table(table_name).delete(**condition)

    def create_table(self, table_name, **options):
        self.__check_current_db()
        self.__current_db.create_table(table_name, **options)

    def get_database(self, format_type='list'):
        databases = self.__database_names

        if format_type == 'dict':
            tmp = []
            for database in databases:
                tmp.append({'name': database})

            databases = tmp
        return databases

    def get_table(self, format_type='list'):
        self.__check_current_db()
        tables = self.__current_db.get_table()

        if format_type == 'dict':
            tmp = []
            for table in tables:
                tmp.append({'name': table})
            tables = tmp

        return tables

    def __get_table(self, table_name):
        self.__check_current_db()

        table = self.__current_db.get_table_obj(table_name)

        if table is None:
            raise Exception('not table %s' % table_name)

        return table

    def __check_current_db(self):
        if not self.__current_db or not isinstance(self.__current_db, Database):
            raise Exception('No database selected')
