import os

from pyiceberg.types import StringType, LongType

from eisberg.core import Catalog

warehouse_path = '_data/wh'
namespace = 'public'

if not os.path.exists(warehouse_path):
    os.makedirs(warehouse_path, exist_ok=True)

catalog = Catalog(
    'sandbox',
    uri=f'sqlite:///{warehouse_path}/catalog.db',
    warehouse=f'file://{warehouse_path}',
)
print(catalog.get_namespaces())
print(catalog.get_tables(namespace))

table = catalog.table(namespace, 'test_table') \
    .define("id", StringType()) \
    .define("name", StringType()) \
    .define("level", LongType())

from uuid import uuid4
from time import time
table.overwrite([
    {'id': uuid4().hex, 'name': f'uo.{time():.3f}', 'level': (i + 1) * 10}
    for i in range(5)
])

table.append([
    {'id': uuid4().hex, 'name': f'ua.{time():.3f}', 'level': (i + 1) * 10}
    for i in range(5)
])

print(table.query())