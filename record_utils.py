from __future__ import annotations
"""
Contains definitions and utilities to create and modifies Records.
Records are data containing objects that conform to a schema.
"""
from typing import List, Optional, Union

from dataexchange import Response
from schema import Schema


class Record:
    """
    Represents a record from table.
    This always corresponds to a given schema.
    """
    def __init__(self, values: dict = None, schema: Schema = None):
        # unordered mapping from: column-name -> column-value
        self.values = values
        # schema is needed for serializing record
        self.schema = schema

    def __str__(self):
        if self.values is None:
            return "Record(-)"
        body = ", ".join([f"{k}: {v}" for k, v in self.values.items()])
        return f"Record({body})"

    def __repr__(self):
        return str(self)

    def to_dict(self) -> dict:
        """
        Create a copy of internal record dict and return
        :return:
        """
        # return a copy of internal dict
        return {key: value for key, value in self.values.items()}

    def get(self, column: str):
        """
        column names are internally represented as lowercase versions
        of their names; thus the column must be lowercased for the lookup
        :param column:
        :return:
        """
        return self.values[column.lower()]

    def contains(self, column: str) -> bool:
        """
        check whether record has column

        :param column:
        :return:
        """
        return column.lower() in self.values


class MultiRecord:
    """
    Represents multiple records from multiple sources
    """
    def __init__(self):
        # keeps mapping of table alias -> record
        self.record_map = {}

    def __str__(self):
        if self.record_map is None:
            return "MRecord(-)"
        body = []
        for table_alias, record in self.record_map.items():
            for k, v in record.values.items():
                child = f"{table_alias}.{k}: {v}"
                body.append(child)
        joined = ", ".join(body)
        return f"MRecord({joined})"

    def to_dict(self) -> dict:
        """
        Convert record to dict
        :return:
        """
        dictionary = {}
        for table_alias, record in self.record_map.items():
            for key, value in record.to_dict().items():
                scoped_key = f"{table_alias}.{key}"
                dictionary[scoped_key] = value
        return dictionary

    def add_record(self, alias: str, record: Record):
        """

        :param alias:
        :param record:
        :return:
        """
        assert alias not in self.record_map
        self.record_map[alias] = record

    def add_multi_record(self, record: MultiRecord):
        """

        :param record:
        :return:
        """
        # add each child of arg `record` to this self
        for name, child_record in record.record_map.items():
            assert name not in self.record_map
            self.record_map[name] = child_record

    def contains(self, table_alias: str, column_name: str):
        return table_alias in self.record_map and self.record_map[table_alias].contains(column_name)

    def get(self, table_alias: str, column_name: str):
        assert self.contains(table_alias, column_name), f"column with reference [{table_alias}.{column_name}] does not exist"
        return self.record_map[table_alias].get(column_name)


# section: utilities


def join_records(left_record: Union[Record, MultiRecord], right_record: Union[Record, MultiRecord],
                 left_alias: Optional[str], right_alias: Optional[str]):
    """
    join records and return a multi-record
    left_, right_empty are used to handle left, right outer joined records
    :return:
    """

    joined = MultiRecord()
    if isinstance(left_record, Record):
        joined.add_record(left_alias, left_record)
    else:
        assert isinstance(left_record, MultiRecord)
        joined.add_multi_record(left_record)

    if isinstance(right_record, Record):
        joined.add_record(right_alias, right_record)
    else:
        assert isinstance(right_record, MultiRecord)
        joined.add_multi_record(right_record)

    return joined


def create_null_record(schema: Schema) -> Record:
    """
    given a `schema` return a record with the given
    schema and all fields set to null
    :param schema:
    :return:
    """
    values = {column.name: None for column in schema.columns}
    return Record(values, schema)


def validate_record(record) -> Response:
    """
    Validate whether record data types are as expected
    and primary key and non-nullable columns are set

    :param record:
    :return:
    """
    has_primary_key = False
    for column in record.schema.columns:
        value = record.values.get(column.name)
        # check if value must be set
        if value is None and not column.is_nullable:
            return Response(False, error_message=f'non-nullable field [{column.name}] is unset')
        # check if literals have valid value
        if value is not None and not column.datatype.is_valid_term(value):
            return Response(False, error_message=f'Column [{column.name}, type: {column.datatype}] has invalid term [{value}]')
        # check if column is primary key
        if column.is_primary_key:
            has_primary_key = True

    if not has_primary_key:
        return Response(False, error_message='missing primary key')

    return Response(True)


def create_record(column_name_list: List, value_list: List, schema: Schema) -> Response:
    """
    Create record. Note if the operation is successful, a valid record was read.
    :return:
    """
    if len(column_name_list) != len(value_list):
        return Response(False, error_message=f'Number of column names [{len(column_name_list)}] '
                                             f'does not equal number of values[{len(value_list)}]')

    # create record
    values = {}
    for idx, col_name in enumerate(column_name_list):
        value = value_list[idx]

        # todo: verify this
        extracted = getattr(value, 'literal', value)
        values[col_name] = extracted

    record = Record(values, schema)

    # validate record
    resp = validate_record(record)
    if not resp.success:
        return Response(False, error_message=f'Record failed schema validation: [{resp.error_message}]')

    return Response(True, body=record)


def create_catalog_record(pkey: int, table_name: str, root_page_num: int, sql_text: str, catalog_schema: CatalogSchema):
    """
    Create a catalog record
    :param pkey:
    :param table_name:
    :param root_page_num:
    :param catalog_schema:
    :return:
    """

    return create_record(['pkey', 'name', 'root_pagenum', 'sql_text'],
                         [pkey, table_name, root_page_num, sql_text],
                         catalog_schema)
