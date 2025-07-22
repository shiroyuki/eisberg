"""
Prototype Iceberg Wrapper
Copyright Juti Noppornpitak
Licensed under Apache 2
"""
from collections import defaultdict
from typing import Any
from pandas import DataFrame
import pyarrow
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import IcebergType


# TODO Implement the DBAPI 2 to use with SQLAlchemy 2

class UndefinedSchemaError(RuntimeError):
    pass


class Table:
    def __init__(self, catalog: "Catalog", namespace: str, name: str):
        self._catalog = catalog
        self._namespace = namespace
        self._name = name
        self._fields: dict[str, NestedField] = dict()
        self._schema: Schema | None = None

    def define(self, name: str, kind: IcebergType, required: bool = False) -> "Table":
        self._fields[name] = NestedField(len(self._fields) + 1, name, kind, required=required)
        return self

    def create_if_not_exists(self):
        if not self._fields:
            raise UndefinedSchemaError('No schema defined')

        if not self._schema:
            self._schema = Schema(*[
                nf
                for nf in sorted(self._fields.values(), key=lambda nf: nf.field_id)
            ])
            self._catalog._api.create_table_if_not_exists(
                f'{self._namespace}.{self._name}',
                self._schema
            )
        else:
            pass  # NO NOOP

    def _convert_rows_to_table(self, rows: list[dict[str, Any]]) -> pyarrow.Table:
        fields = [
            nf.name
            for nf in sorted(self._fields.values(), key=lambda nf: nf.field_id)
        ]
        series = defaultdict(list)
        for row in rows:
            for field in fields:
                series[field].append(row[field])
        return pyarrow.table(series)

    def append(self, rows: list[dict[str, Any]]):
        self.create_if_not_exists()
        self._get_api().append(self._convert_rows_to_table(rows))

    def overwrite(self, rows: list[dict[str, Any]]):
        self.create_if_not_exists()
        self._get_api().overwrite(self._convert_rows_to_table(rows))

    def query(self) -> DataFrame:
        # TODO Implement row_filter
        return self._get_api().scan().to_pandas()

    def delete(self):
        self._catalog._api.drop_table(f'{self._namespace}.{self._name}')

    def _get_api(self):
        return self._catalog._api.load_table(f'{self._namespace}.{self._name}')


class Catalog:
    def __init__(self, name: str, **config):
        self._name = name
        self._iceberg_config = config
        self._api = load_catalog(self._name, **self._iceberg_config)

    def get_namespaces(self) -> list[str]:
        return [i[0] for i in self._api.list_namespaces()]

    def get_tables(self, namespace: str):
        self._api.create_namespace_if_not_exists(namespace)
        return [i[1] for i in self._api.list_tables(namespace=namespace)]

    def table(self, namespace: str, name: str) -> Table:
        """ Get a table by namespace and table name. """
        return Table(catalog=self, namespace=namespace, name=name)
