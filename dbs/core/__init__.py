import json
from enum import Enum


# Database Core Module
# Serialization Interface
class SerializationInterface:
    json = json  # define a json object

    # static method
    @staticmethod
    def deserialize(obj):
        raise NotImplementedError

    def serialize(self):
        raise NotImplementedError


class FieldType(Enum):
    INT = int = 'int'
    VARCHAR = varchar = 'str'
    FLOAT = float = 'float'


TYPE_MAP = {
    'int': int,
    'float': float,
    'str': str,
    'Int': int,
    'FLOAT': float,
    'VARCHAR': str
}


class FieldKey(Enum):
    PRIMARY = 'PRIMARY KEY'
    INCREMENT = 'AUTO_INCREMENT'
    UNIQUE = 'UNIQUE'
    NOT_NULL = 'NOT NULL'
    NULL = 'NULL'




