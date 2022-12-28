from dbs.core import *
from dbs.core.table import Table

class Database(SerializationInterface):
    def __init__(self, name):
        self.__name = name
        self.__table_names = []
        self.__table_objs = {}

    def create_table(self, table_name, **options):
        if table_name in self.__table_names:
            raise Exception('table exists')

        self.__table_names.append(table_name)

        self.__table_objs[table_name] = Table(**options)

    def drop_table(self, table_name):
        if table_name not in self.__table_names:
            raise Exception('table not exists')

        self.__table_names.remove(table_name)
        self.__table_objs.pop(table_name, True)

    def get_table_obj(self, name):
        return self.__table_objs.get(name, None)

    def get_name(self):
        return self.__name

    def serialize(self):
        data = {'name': self.__name, 'tables': []}

        for table_name, table_data in self.__table_objs.items():
            data['tables'].append([table_name, table_data.serialize()])

        return SerializationInterface.json.dumps(data)

    @staticmethod
    def deserialize(data):
        json_data = SerializationInterface.json.loads(data)
        obj_tmp = Database(json_data['name'])
        for table_name, table_obj in json_data['tables']:
            obj_tmp.add_table(table_name, Table.deserialize(table_obj))
        return obj_tmp

    def add_table(self, table_name, table):
        if table_name not in self.__table_names:
            self.__table_names.append(table_name)
            self.__table_objs[table_name] = table

    def get_table(self, index=None):
        length = len(self.__table_names)
        if isinstance(index, int) and 0 <= index < length:
            return self.__table_names[index]
        return self.__table_names
