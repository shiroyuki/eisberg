import os
import shutil
from time import time
from unittest import TestCase
from uuid import uuid4

from pyiceberg.types import StringType, LongType

from eisberg.core import Catalog


class CoreTest(TestCase):
    def test_core(self):
        warehouse_path = '_data/wh'
        namespace = 'public'

        if not os.path.exists(warehouse_path):
            os.makedirs(warehouse_path, exist_ok=True)

        self.addCleanup(lambda: shutil.rmtree(warehouse_path))

        catalog = Catalog(
            'sandbox',
            uri=f'sqlite:///{warehouse_path}/catalog.db',
            warehouse=f'file://{warehouse_path}',
        )

        namespace = catalog.namespace(namespace)

        table = namespace.table('test_table') \
            .define("id", StringType()) \
            .define("name", StringType()) \
            .define("level", LongType())

        table.overwrite([
            {'id': uuid4().hex, 'name': f'uo.{time():.3f}', 'level': (i + 1) * 10}
            for i in range(5)
        ])

        table.append([
            {'id': uuid4().hex, 'name': f'ua.{time():.3f}', 'level': (i + 1) * 10}
            for i in range(5)
        ])

        self.assertEqual(['public'], [ns.name for ns in catalog.list_namespaces()])
        self.assertEqual(['test_table'], [t.name for t in namespace.list_tables()])

        result = table.query()

        self.assertEqual(10, len(result))
