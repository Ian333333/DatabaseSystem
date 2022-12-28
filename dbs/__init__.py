from dbs.core import *
from dbs.core.database import Database

import base64

import os


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
