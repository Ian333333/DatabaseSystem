from dbs import Engine
from dbs.case import *
from dbs.case.case import GreaterCase

e = Engine()
e.select_db('test_db')


e.insert(table_name='test_t1', f_name='test_03', f_age=30)
e.insert(table_name='test_t1', f_name='test_04', f_age=40)
e.insert(table_name='test_t1', f_name='test_05', f_age=50)
e.insert(table_name='test_t1', f_name='test_06', f_age=60)


print("-" * 5, "This is all the data in test_t1", "-" * 5)
all_data = e.search("test_t1")
for data in all_data:
    print(data)
print("-" * 30)


print("-" * 5, "Search for all users over 30", "-" * 5)
test_data_01 = e.search(table_name="test_t1", f_age=GreaterCase(30))
for data in test_data_01:
    print(data)
print("-" * 30)


e.commit()