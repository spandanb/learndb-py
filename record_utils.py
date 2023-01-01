from __future__ import annotations
"""
Contains definitions and utilities to create and modifies Records.
Records are data containing objects that conform to a schema.
# TODO: should this module be called `record.py`
"""
from typing import Any, List, Optional, Union, Tuple
from lark import Token

from lang_parser.symbols3 import ColumnName, ColumnNameList, ValueList, Literal
from dataexchange import Response
from schema import SimpleSchema, ScopedSchema, GroupedSchema


class UnaggregatedGetOnUngroupedColumn(Exception):
    """
    Trying to access an ungrouped column without a reducing aggregate function
    """
    pass


class AbstractRecord:
    """
    Interface for Record.
    NOTE: this doesn't enforce that implementation classes implement all interface methods.
    TODO: Consider formalizing the interface using abc.ABCMeta; see: https://realpython.com/python-interface/
    """
    def get(self, column: str):
        raise NotImplementedError

    def has_columns(self, column: str) -> bool:
        raise NotImplementedError


class SimpleRecord(AbstractRecord):
    """
    Represents a record from table.
    This always corresponds to a given schema.
    # TODO make this a readonly type  - since these are shallow copied,
    # if the returned pipe allows
    """
    def __init__(self, values: dict = None, schema: SimpleSchema = None):
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

    def has_columns(self, column: str) -> bool:
        """
        check whether record has column

        :param column:
        :return:
        """
        return column.lower() in self.values

    def get_primary_key(self):
        pkey_col = self.schema.get_primary_key_column()
        return self.get(pkey_col)


# section: utilities

class ScopedRecord(AbstractRecord):
    """
    Represents scoped collection of record.
    These could be use generated from a single record, or a joining of multiple records.
    """
    # TODO: this should handle init records as two simple records, or a simple and JoinedRecord
    # for a joinedRecord, it should determine be able to flatten and store it
    # think through how this will generalize
    def __init__(self, name_to_records: dict, schema: ScopedSchema):
        self.names = name_to_records
        self.schema = schema

    @classmethod
    def from_records(cls, left_rec: Union[SimpleRecord, ScopedRecord], right_rec: SimpleRecord, left_alias: Optional[str],
                     right_alias: str, schema: ScopedSchema):
        if isinstance(left_rec, SimpleRecord):
            assert left_alias is not None
            return cls.from_simple_records(left_rec, right_rec, left_alias, right_alias, schema)
        else:
            # this mimics arguments to invoking method in vm
            assert isinstance(left_rec, ScopedRecord)
            # assert that the left alias is defined
            assert left_alias in left_rec.names
            return cls.from_joined_and_simple_record(left_rec, right_rec, right_alias, schema)

    @classmethod
    def from_simple_records(cls, left_rec: SimpleRecord, right_rec: SimpleRecord, left_alias, right_alias, schema: ScopedSchema):
        """
        Construct a JoinedRecord for 2 simple records
        """
        names = {left_alias: left_rec, right_alias: right_rec}
        return cls(names, schema)

    @classmethod
    def from_single_simple_record(cls, record, alias, schema: ScopedSchema):
        return cls({alias: record}, schema)

    @classmethod
    def from_joined_and_simple_record(cls, joined_rec: ScopedRecord, right_rec: SimpleRecord, right_alias, schema: ScopedSchema):
        names = joined_rec.names.copy()
        # make a shallow copy - don't need to copy records, since those are read-only
        assert right_alias not in names, f"Create failed: {right_alias} already exists in [{names.keys()}]"
        names[right_alias] = right_rec
        return cls(names, schema)

    def get(self, fqname):
        """
        Given a fq column name, e.g. f.cola
        """
        parts = fqname.split(".")
        assert len(parts) == 2
        table, column = parts
        if table not in self.names:
            raise ValueError(f"Uknown table alias [{table}]")
        record = self.names[table]
        return record.get(column)

    def has_columns(self, *args):
        """
        Intended to mimick simple
        """

    def __repr__(self):
        return f"JRec[{self.names}]"

    def __str__(self):
        return f"JRecord[{self.names}]"


class GroupedRecord(AbstractRecord):
    """
    Provides encapsulation over a record group.

    NOTE: Other Record types, contain concrete values; however, this
    potentially contains a recordset, corresponding to a group, which
    would need to be evaluated by a function to give a value. This is named with `Record`
    suffix since it implements the Record interface.
    """

    def __init__(self, schema: GroupedSchema, group_key: Tuple, group_recordset: List[Union[SimpleRecord, ScopedRecord]]):
        self.schema = schema
        self.group_key = group_key
        self.group_recordset = group_recordset

    def has_columns(self, column: str) -> bool:
        return self.schema.has_column()

    def get(self, column: str) -> Any:
        """
        Return value of grouped `column` from record
        """
        # determine if `column` is a grouping column
        column = column.lower()
        for idx, group_column in enumerate(self.schema.group_by_columns):
            if group_column.name == column:
                return self.group_key[idx]

        if self.schema.has_column(column):
            raise UnaggregatedGetOnUngroupedColumn(f"Expected grouping column, received non-grouping column: [{column}]")

        return None

    def recordset_to_values(self, column_name: str) -> List[Any]:
        """
        Generate list of column values from group_recordset.
        """
        return [record.get(column_name) for record in self.group_recordset]



def join_records(left_record: Union[SimpleRecord, ScopedRecord], right_record: Union[SimpleRecord, ScopedRecord],
                 left_alias: Optional[str], right_alias: Optional[str]):
    """
    TODO: remove if unused
    join records and return a multi-record
    left_, right_empty are used to handle left, right outer joined records
    :return:
    """

    joined = ScopedRecord()
    if isinstance(left_record, SimpleRecord):
        joined.add_record(left_alias, left_record)
    else:
        assert isinstance(left_record, ScopedRecord)
        joined.add_multi_record(left_record)

    if isinstance(right_record, SimpleRecord):
        joined.add_record(right_alias, right_record)
    else:
        assert isinstance(right_record, ScopedRecord)
        joined.add_multi_record(right_record)

    return joined


def create_null_record(schema: SimpleSchema) -> SimpleRecord:
    """
    given a `schema` return a record with the given
    schema and all fields set to null
    :param schema:
    :return:
    """
    values = {column.name: None for column in schema.columns}
    # TODO: when should this generate JoinedRecord
    # this will need a joined schema
    return SimpleRecord(values, schema)


def validate_record(record) -> Response:
    """
    Validate record based on schema:
        - literals are valid
        - if columns are primary key or non-nullable columns are set

    :param record:
    :return:
    """
    has_primary_key = False
    for column in record.schema.columns:
        # TODO: distinguish null from unset field
        value = record.values.get(column.name)
        # check if value must be set
        if value is None and not column.is_nullable:
            return Response(False, error_message=f'non-nullable field [{column.name}] is unset/null')
        elif value is None and column.is_primary_key:
            # TODO: should I assert primary key is int?
            return Response(False, error_message=f'primary-key field [{column.name}] is unset/null')

        # check if literals have valid value
        if value is not None and not column.datatype.is_valid_term(value):
            return Response(False, error_message=f'Column [{column.name}, type: {column.datatype}] has invalid term [{value}] [term type: {type(value)}]')

    return Response(True)


def create_record(column_name_list: ColumnNameList, value_list: ValueList, schema: SimpleSchema) -> Response:
    """
    Create record. Note if the operation is successful, a valid record was read.
    :return:
    """
    if len(column_name_list.names) != len(value_list.values):
        return Response(False, error_message=f'Number of column names [{len(column_name_list.names)}] '
                                             f'does not equal number of values[{len(value_list.values)}]')

    # create record
    values = {}
    for idx, col_name in enumerate(column_name_list.names):
        value = value_list.values[idx]
        values[col_name.name] = value.value if isinstance(value, Literal) else value

    record = SimpleRecord(values, schema)

    # validate record; i.e. is consistent with schema
    resp = validate_record(record)
    if not resp.success:
        return Response(False, error_message=f'Record failed schema validation: [{resp.error_message}]')

    return Response(True, body=record)


def create_record_from_raw_values(column_names: List[str], value_list: List[str], schema: SimpleSchema) -> Response:
    """
    Needed for creating final output recordset;
    Uses raw values, i.e. unboxed values
    # TODO: refactor to remove `column_names` which can be derived from schema, like: [col.name for col schema.columns]
    """

    if len(column_names) != len(value_list):
        return Response(False, error_message=f'Number of column names [{len(column_names)}] '
                                             f'does not equal number of values[{len(value_list)}]')

    # create record
    values = {}
    for idx, col_name in enumerate(column_names):
        value = value_list[idx]
        values[col_name] = value.value if isinstance(value, Literal) else value

    record = SimpleRecord(values, schema)

    # validate record; i.e. is consistent with schema
    resp = validate_record(record)
    if not resp.success:
        return Response(False, error_message=f'Record failed schema validation: [{resp.error_message}]')

    return Response(True, body=record)


def create_catalog_record(pkey: int, table_name: str, root_page_num: int, sql_text: str, catalog_schema: CatalogSchema):
    """
    Create a catalog record.

    NOTE: This must produce a type identical output to parser
    :param pkey:
    :param table_name:
    :param root_page_num:
    :param catalog_schema:
    :return:
    """

    return create_record(ColumnNameList([ColumnName('pkey'), ColumnName('name'), ColumnName('root_pagenum'),
                                         ColumnName('sql_text')]),
                         ValueList([pkey, table_name, root_page_num, sql_text]),
                         catalog_schema)

