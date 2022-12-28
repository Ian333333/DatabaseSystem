from dbs.case.case import BaseCase
from dbs.core import *
from dbs.core.field import Field


class Table(SerializationInterface):
    def __init__(self, **options):
        self.__field_names = []  # all the filed names in the table
        self.__field_objs = {}  # mapping between field names and objects
        self.__rows = 0  # number of entries

        for field_name, field_obj in options.items():
            self.add_field(field_name, field_obj)

    def add_field(self, field_name, field_obj, value=None):
        if field_name in self.__field_names:
            raise Exception('Field Exists')

        if not isinstance(field_obj, Field):
            raise TypeError('type error, value must be %s' % Field)

        self.__field_names.append(field_name)
        self.__field_objs[field_name] = field_obj

        # align field data
        if len(self.__field_names) > 1:
            length = self.__rows

            field_obj_length = field_obj.length()
            if field_obj_length != 0:
                if field_obj_length == length:
                    return

                raise Exception('Field data length not aligned')

            # if no field data, fill it with value
            for index in range(0, length):
                self.__get_field(field_name).add(value)
        else:
            self.__rows = field_obj.length()

    def search(self, fields, sort, format_type, **conditions):
        if fields == '*':
            fields = self.__field_names
        else:
            for field in fields:
                if field not in self.__field_names:
                    raise Exception('%s field not exists' % field)

        # initialize return value as an empty list
        rows = []

        match_index = self.__parse_conditions(**conditions)

        for index in match_index:
            if format_type == 'list':
                row = [self.__get_field_data(field_name, index) for field_name in fields]
            elif format_type == 'dict':
                row = {}
                for field_name in fields:
                    row[field_name] = self.__get_field_data(field_name, index)
            else:
                raise Exception('format type invalid')
            rows.append(row)

        if sort == 'DESC':
            rows = rows[::-1]

        return rows

    def __get_field(self, field_name):
        if field_name not in self.__field_names:
            raise Exception('%s field not exists' % field_name)
        return self.__field_objs[field_name]

    def __get_field_data(self, field_name, index=None):
        field = self.__get_field(field_name)
        return field.get_data(index)

    def __get_field_type(self, field_name):
        field = self.__get_field(field_name)
        return field.get_type()

    def __parse_conditions(self, **conditions):
        if 'conditions' in conditions:
            conditions = conditions['conditions']

        if not conditions:
            match_index = range(0, self.__rows)
        else:
            name_tmp = self.__get_name_tmp(**conditions)

            match_tmp = []

            match_index = []

            is_first_loop = True

            for field_name in name_tmp:
                data = self.__get_field_data(field_name)

                data_type = self.__get_field_type(field_name)

                case = conditions[field_name]

                if not isinstance(case, BaseCase):
                    raise TypeError('Type error, value must be "Case" object')

                # find indexes that match the first condition in the first loop
                # then remove indexes that do not match other conditions
                if is_first_loop:
                    length = self.__get_field_length(field_name)

                    for index in range(0, length):
                        if case(data[index], data_type):
                            match_tmp.append(index)
                            match_index.append(index)

                    is_first_loop = False

                    continue

                for index in match_tmp:
                    if not case(data[index], data_type):
                        match_index.remove(index)

                match_tmp = match_index

        return match_index

    def delete(self, **conditions):

        match_index = self.__parse_conditions(**conditions)
        match_index.sort()
        match_index = match_index[::-1]

        for field_name in self.__field_names:
            for index in match_index:
                self.__get_field(field_name).delete(index)

        self.__rows = self.__get_field_length(self.__field_names[0])

    def __get_field_length(self, field_name):
        field = self.__get_field(field_name)
        return field.length()

    def update(self, data, **conditions):
        match_index = self.__parse_conditions(**conditions)

        name_tmp = self.__get_name_tmp(**data)

        for field_name in name_tmp:
            for index in match_index:
                self.__get_field(field_name).modify(index, data[field_name])

    # resolve field name in the parameters
    def __get_name_tmp(self, **options):
        # return list
        name_tmp = []

        for field_name in options.keys():
            if field_name not in self.__field_names:
                raise Exception('%s Field Not Exists' % field_name)

            name_tmp.append(field_name)

        return name_tmp

    def insert(self, **data):
        if 'data' in data:
            data = data['data']

        name_tmp = self.__get_name_tmp(**data)

        for field_name in self.__field_names:

            value = None

            if field_name in name_tmp:
                value = data[field_name]

            # don't we need to insert data into all the fields?
            try:
                self.__get_field(field_name).add(value)
            except Exception as e:
                raise Exception(field_name, str(e))

        self.__rows += 1

    def serialize(self):
        data = {}
        for field in self.__field_names:
            data[field] = self.__field_objs[field].serialize()

        return SerializationInterface.json.dumps(data)

    def deserialize(data):
        json_data = SerializationInterface.json.loads(data)

        table_obj = Table()

        field_names = [field_name for field_name in json_data.keys()]

        for field_name in field_names:
            field_obj = Field.deserialize(json_data[field_name])
            table_obj.add_field(field_name, field_obj)

        return table_obj
