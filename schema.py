from __future__ import annotations
"""
This contain structures to generate and manipulate
schema- including the logical schema (column name, column type),
and the physical representation (number of bytes,
length of encoding) thereof.

This should be split into logical (schema.py) and physical schema (serde.py)

The physical encoding is the file format.
"""


from typing import List, Optional, Union

from datatypes import DataType, Integer, Text, Blob, Float
from lang_parser.tokens import TokenType
from dataexchange import Response


class Column:
    """
    Represents a column in a schema
    """
    def __init__(self, name: str, datatype, is_primary_key: bool = False, is_nullable: bool = True):
        self.name = name
        self.datatype = datatype
        self.is_primary_key = is_primary_key
        self.is_nullable = is_nullable

    def __str__(self):
        return f'Column[{self.name}, {self.datatype}, is_primary: {self.is_primary_key}, is_nullable: {self.is_nullable}]'

    def __repr__(self):
        return self.__str__()


class Schema:
    """
    Represents a schema. This includes
    logical aspects (name) and physical aspects
    (number of bytes of storage, fixed vs. variable length encoding)

    Note a schema must be valid. If the schema is invalid, this
    should be raised prior to creating.

    NOTE: once constructed a schema should be treated as read-only
    """
    def __init__(self, name: str = None, columns: List[Column] = []):
        # name of object/entity defined
        self.name = name
        # list of column objects ordered by definition order
        self.columns = columns

    def __str__(self):
        body = ' '.join([col.name for col in self.columns])
        return f'Schema({str(self.name)}, {str(body)})'

    def __repr__(self):
        return str(self)

    def get_primary_key_column(self) -> str:
        """
        return column name of primary key column
        :return:
        """
        for column in self.columns:
            if column.is_primary_key:
                return column.name

        return None


class CatalogSchema(Schema):
    """
    Hardcoded schema object for the catalog table.

    This corresponds to the following table definition:
    create table catalog (
        type  text,
        name text,
        tbl_name text,
        rootpage integer,
        sql text
    )

    NOTE: This could be bootstrapped by parsing the above schema
    definition text- as all other schemas will be. But this
    will be easier. Yet, even doing that will require special
    handling of the catalog schema. Further, having a hardcoded
    schema will provide an easy validation on the parser.
    """

    def __init__(self):
        super().__init__('catalog')
        self.columns = [
            Column('pkey', Integer, is_primary_key=True),
            Column('name', Text),
            Column('root_pagenum', Integer),
            Column('sql_text', Text)
        ]


def schema_to_ddl(schema: Schema) -> str:
    """
    convert a schema to canonical ddl

    parser rule:
        create_stmnt -> "create" "table" table_name "(" column_def_list ")"

    e.g. ddl
    create table catalog (
        pkey int primary key
        type  text,
        name text,
        tbl_name text,
        rootpage integer,
        sql text
    )

    :return:
    """
    column_defs = []
    for column in schema.columns:
        if column.is_primary_key:
            # key is the first column in ddl
            # primary key implies not null
            column_defs.insert(0, f'{column.name} {column.datatype.typename} PRIMARY KEY')
        else:
            null_cond = "" if column.is_nullable else "NOT NULL"
            column_defs.append(f'{column.name} {column.datatype.typename} {null_cond}')
    column_def_body = ", ".join(column_defs)
    return f'CREATE TABLE {schema.name} ( {column_def_body} )'


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


def join_records(left_record: Union[Record, MultiRecord], right_record: Union[Record, MultiRecord],
                 left_alias: Optional[str], right_alias: Optional[str],
                 left_empty: bool = False, right_empty: bool = False):
    """
    join records and return a multi-record
    left_, right_empty are used to handle left, right outer joined records

    TODO: handle `left_empty`
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


def validate_schema(schema: Schema) -> Response:
    """
    Ensure schema is valid.
    A valid schema must have:
        - integer primary key (this can be handled automatically later)
        - unique column names
        - valid column names
        - valid datatypes

    :param schema:
    :return:
    """
    # validate - single column primary key
    if len([col for col in schema.columns if col.is_primary_key]) != 1:
        return Response(False, body='missing primary key')

    # validate - primary key is integer
    pkey = None
    for col in schema.columns:
        if col.is_primary_key:
            pkey = col
            break
    if pkey.datatype != Integer:
        return Response(False, body='primary key must be of integer type')

    # validate column names are unique
    names = set()
    for col in schema.columns:
        if col.name in names:
            return Response(False, body=f'duplicate column name [{col.name}]')
        names.add(col.name)

    # validate column types are valid
    for col in schema.columns:
        if not issubclass(col.datatype, DataType):
            return Response(False, body=f'invalid datatype for [{col.name}]')

    return Response(True)


def token_to_datatype(datatype_token: 'Token') -> Response:
    """
    parse datatype token into DataType
    :param datatype_token:
    :return:
    """
    token_type = datatype_token.token_type
    if token_type == TokenType.INTEGER:
        return Response(True, body=Integer)
    elif token_type == TokenType.TEXT:
        return Response(True, body=Text)
    elif token_type == TokenType.BLOB:
        return Response(True, body=Blob)
    elif token_type == TokenType.REAL:
        return Response(True, body=Float)
    return Response(False, error_message=f'Unrecognized datatype: [{datatype_token}]')


def generate_schema(create_stmnt: 'CreateStmnt') -> Response:
    """
    construct schema corresponding to schema object.
    Note if the operation is successful, a valid schema was read.
    :param create_stmnt:
    :return:
    """
    # construct schema
    table_name = create_stmnt.table_name.literal
    schema = Schema(name=table_name)
    columns = []
    for coldef in create_stmnt.column_def_list:
        resp = token_to_datatype(coldef.datatype)
        if not resp.success:
            return Response(False, error_message=f'Unable to parse datatype [{coldef.datatype}]')
        datatype = resp.body
        column_name = coldef.column_name.literal.lower()
        column = Column(column_name, datatype, is_primary_key=coldef.is_primary_key, is_nullable=coldef.is_nullable)
        columns.append(column)
    schema.columns = columns

    # validate schema
    resp = validate_schema(schema)
    if not resp.success:
        return Response(False, error_message=f'schema validation due to [{resp.error_message}]')
    return Response(True, body=schema)


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


