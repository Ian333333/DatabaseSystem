from dbs import Engine
from dbs.core.field import *

e = Engine()
e.create_database('test_db')
e.select_db('test_db')

e.create_table(
    table_name='test_t1',
    f_id=Field(data_type=FieldType.INT, keys=[FieldKey.PRIMARY, FieldKey.INCREMENT]),
    f_name=Field(data_type=FieldType.VARCHAR, keys=FieldKey.NOT_NULL),
    f_age=Field(data_type=FieldType.INT, keys=FieldKey.NOT_NULL)
)

e.insert(table_name='test_t1', f_name='test_01', f_age=10)
e.insert(table_name='test_t1', f_name='test_02', f_age=20)

res = e.search('test_t1')
for row in res:
    print(row)

e.commit()