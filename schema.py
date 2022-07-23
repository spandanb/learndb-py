from __future__ import annotations
"""
This contain structures to generate and manipulate
logical schema- (column name, column type).

The physical representation (number of bytes,
length of encoding) of the schema is contained in serde.py.

While, records- i.e. data-containing objects with the structure
specified by the schema-, and related utilities are contained in record_utils.py
"""


from typing import List, Optional, Union

from datatypes import DataType, Integer, Text, Blob, Float
# from lang_parser.tokens import TokenType, Token
from lang_parser.symbols3 import TableName, DataType as SymbolDataType  # renaming to avoid ambiguity with actual datatypes
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

    Note: a schema must be valid. If the schema is invalid, this
    should be raised prior to creating. This is particularly important,
    since schemas will correspond: 1) to a real data sources,
    2) computed schema for output resultset. For (1) we would have
    constraints like primary key; but for (2) we would not; and hence
    these constraints should be external to the schema definition

    NOTE: once constructed a schema should be treated as read-only
    """
    def __init__(self, name: str = None, columns: List[Column] = None):
        # name of object/entity defined
        self.name = name
        # list of column objects ordered by definition order
        self.cols = columns

    @property
    def columns(self):
        return self.cols

    def __str__(self):
        body = ' '.join([col.name for col in self.cols])
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


class MultiSchema:
    """
    Represents a scoped (by table_alias) collection of schema
    TODO: rename to ScopedSchema
    """
    def __init__(self, schemas: dict):
        self.schemas = schemas  # table_name -> Schema

    def get_table_names(self):
        return self.schemas.keys()

    @classmethod
    def from_single_schema(cls, schema: Schema, alias: str):
        return cls({alias: schema})

    @classmethod
    def from_schemas(cls, left_schema: Union[Schema, MultiSchema], right_schema: Schema, left_alias: Optional[str],
                     right_alias: str):
        if isinstance(left_schema, Schema):
            assert left_alias is not None
            return cls({left_alias: left_schema, right_alias: right_schema})
        else:
            assert isinstance(left_schema, MultiSchema) and left_alias is None
            schemas = left_schema.schemas.copy()
            schemas[right_alias] = right_schema
            return cls(schemas)

    @property
    def columns(self):
        return [f"{table_alias}.{col}" for table_alias, schema in self.schemas.items() for col in schema.columns]


# create name alias, to ease deprecation
#
ScopedSchema = MultiSchema


class GroupedSchema:
    """
    Represents a grouped multi or simple schema
    """
    def __init__(self, schema: Union[Schema, MultiSchema], group_by_columns):
        self.schema = schema
        self.group_by_columns = group_by_columns

    @property
    def columns(self):
        return self.schema.columns


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
        super().__init__('catalog', [
            Column('pkey', Integer, is_primary_key=True),
            Column('name', Text),
            Column('root_pagenum', Integer),
            Column('sql_text', Text)
        ])


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
    assert isinstance(schema.name, TableName)
    return f'CREATE TABLE {schema.name.table_name} ( {column_def_body} )'


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


def token_to_datatype(datatype: DataType) -> Response:
    """
    parse datatype token into DataType
    :param datatype_token:
    :return:
    """
    if datatype == SymbolDataType.Integer:
        return Response(True, body=Integer)
    elif datatype == SymbolDataType.Text:
        return Response(True, body=Text)
    elif datatype == SymbolDataType.Blob:
        return Response(True, body=Blob)
    elif datatype == SymbolDataType.Real:
        return Response(True, body=Float)
    return Response(False, error_message=f'Unrecognized datatype: [{datatype}]')


def generate_schema(create_stmnt) -> Response:
    """
    Generate schema from a create stmnt. There is a very thin
    layer of translation between the stmnt and the schema object.
    But I want to distinguish the (create) stmnt from the schema.
    Note if the operation is successful, a valid schema was read.
    :param create_stmnt:
    :return:
    """

    columns = []
    for coldef in create_stmnt.columns:
        resp = token_to_datatype(coldef.datatype)
        if not resp.success:
            return Response(False, error_message=f'Unable to parse datatype [{coldef.datatype}]')
        datatype = resp.body
        column_name = coldef.column_name.name.lower()
        column = Column(column_name, datatype, is_primary_key=coldef.is_primary_key, is_nullable=coldef.is_nullable)
        columns.append(column)
    schema = Schema(name=create_stmnt.table_name, columns=columns)

    # validate schema
    resp = validate_schema(schema)
    if not resp.success:
        return Response(False, error_message=f'schema validation due to [{resp.error_message}]')
    return Response(True, body=schema)


def make_grouped_schema(schema, group_by_columns: List) -> Response:
    """
    Generate a grouped schema from a non-grouped schema. How
    will this handle both simple, and multi-schema
    """
    return Response(True, body=GroupedSchema(schema, group_by_columns))
