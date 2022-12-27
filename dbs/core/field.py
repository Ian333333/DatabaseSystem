from dbs.core import *


class Field(SerializationInterface):
    def __init__(self, data_type, keys=FieldKey.NULL, default=None):
        self.__type = data_type
        self.__keys = keys
        self.__default = default
        self.__values = []
        self.__rows = 0

        # if only one key, and it is not list, then transform it to list
        if not isinstance(self.__keys, list):
            self.__keys = [self.__keys]

        # if it is not in the FiledType, then throw error
        if not isinstance(self.__type, FieldType):
            raise TypeError('Data-Type require type of "FieldType"')

        for key in self.__keys:
            if not isinstance(key, FieldKey):
                raise TypeError('Data-Type require type of "FieldType"')

        if FieldKey.INCREMENT in self.__keys:
            # increment data type must be integer
            if self.__type != FieldType.INT:
                raise TypeError('Increment key require Data-Type is integer')

            if FieldKey.PRIMARY not in self.__keys:
                raise Exception('Increment key require primary key')

        if self.__default is not None and FieldKey.UNIQUE in self.__keys:
            raise Exception('Default value not allowed in unique key')

    # data type match or not
    def __check_type(self, value):
        if value is not None and not isinstance((value, TYPE_MAP[self.__type.value])):
            raise TypeError('data type error, value must be %s' % self.__type)

    #
    def __check_index(self, index):
        if not isinstance(index, int) or index > self.__rows or index < 0:
            raise Exception('Not this element')
        return True

    def __check_keys(self, value):
        # increment field
        if FieldKey.INCREMENT in self.__keys:
            if value is None:
                value = self.__rows + 1

            if value in self.__values:
                raise Exception('value %s exists' % value)

        if FieldKey.PRIMARY in self.__keys or FieldKey.UNIQUE in self.__keys:
            # value already exists
            if value in self.__values:
                raise Exception('value %s exists' % value)

        if (FieldKey.PRIMARY in self.__keys or FieldKey.NOT_NULL in self.__keys) and value is None:
            raise Exception('Field cannot be Null')

        return value

    def length(self):
        return self.__rows

    def get_data(self, index=None):
        if index is not None and self.__check_index(index):
            return self.__values[index]

        return self.__values

    def add(self, value):
        if value is None:
            value = self.__default

        value = self.__check_keys(value)
        self.__check_type(value)
        self.__values.append(value)
        self.__rows += 1

    def delete(self, index):
        self.__check_index(index)
        self.__values.pop(index)
        self.__rows -= 1

    def modify(self, index, value):
        self.__check_index(index)
        value = self.__check_keys(value)
        self.__check_type(value)
        self.__values[index] = value

    def get_keys(self):
        return self.__keys

    def get_type(self):
        return self.__type

    def length(self):
        return self.__rows

    def serialize(self):
        return SerializationInterface.json.dumps({
            'key': [key.value for key in self.__keys],
            'type': self.__type.value,
            'values': self.__values,
            'default': self.__default
        })

    @staticmethod
    def deserialize(data):
        json_data = SerializationInterface.json.loads(data)
        keys = [FieldKey(key) for key in json_data['key']]
        obj = Field(FieldType(json_data['type']), keys, default=json_data['default'])

        for value in json_data['value']:
            obj.add(value)

        return obj
