import logging

from lark import Token

from .dataexchange import Response
from .lang_parser.symbols import ColumnName
from .record_utils import InvalidNameException


class NameRegistry:
    """
    This entity is responsible for registering and resolving column name and types
    from records and schemas.
    TODO: split this into SchemaReader, and RecordReader
    """

    def __init__(self):
        # record used to resolve values
        self.record = None
        # schema to resolve names from
        self.schema = None

    def set_record(self, record):
        self.record = record

    def set_schema(self, schema):
        self.schema = schema

    def is_name(self, operand) -> bool:
        """
        Return true if operand is a name, i.e. IDENTIFIER or SCOPED_IDENTIFIER
        """
        if isinstance(operand, Token) and (operand.type == "IDENTIFIER" or operand.type == "SCOPED_IDENTIFIER"):
            return True
        elif isinstance(operand, ColumnName):
            return True
        else:
            return False

    def resolve_name(self, operand) -> Response:
        """
        This is only valid if called on a name, i.e. is_name(operand) == True.
        Note: This returns Response to distinguish resolve failed, from resolved to None
        """
        if isinstance(operand, ColumnName):
            try:
                val = self.record.get(operand.name)
                return Response(True, body=val)
            except InvalidNameException as e:
                logging.error(f"Attempted lookup on unknown column [{operand.name}]")
                logging.error(f"Valid column choices are [{self.record.columns}]")
                return Response(False, error_message=e.args[0])

        # NOTE: this was adapated from vm.check_resolve_name
        raise NotImplementedError

    def resolve_column_name_type(self, operand: str) -> Response:
        """
        Determine type of column name
        """
        if self.schema.has_column(operand):
            column = self.schema.get_column_by_name(operand)
            return Response(True, body=column.datatype)
        return Response(False, error_message=f"Unable to resolve column [{operand}]")
