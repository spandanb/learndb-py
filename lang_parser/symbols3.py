from lark import Lark, Transformer, Tree, v_args
from enum import Enum, auto
from typing import Any, List, Union
from dataclasses import dataclass

from .symbols import (
    _Symbol
)

class JoinType(Enum):
    Inner = auto()
    LeftOuter = auto()
    RightOuter = auto()
    FullOuter = auto()
    Cross = auto()


class ColumnModifier(Enum):
    PrimaryKey = auto()
    NotNull = auto()
    Nil = auto()  # no modifier - likely not needed



class CreateStmnt(_Symbol):
    def __init__(self, table_name: Tree = None, column_def_list: Tree = None):
        self.table_name = table_name
        self.columns = column_def_list
        self.validate()

    def validate(self):
        """
        Ensure one and only one primary key
        """
        pkey_count = len([col for col in self.columns if col.is_primary_key])
        if pkey_count != 1:
            raise ValueError(f"Expected 1 primary key received {pkey_count}")

    def __repr__(self):
        return str(self)

    def __str__(self):
        return f'{self.__class__.__name__}({self.__dict__})'


class ColumnDef(_Symbol):

    def __init__(self, column_name: Tree = None, datatype: Tree = None, column_modifier=None):
        self.column_name = column_name
        self.datatype = self._datatype_to_type(datatype)
        self.is_primary_key = primary_key is not None
        self.is_nullable = not_null is None

    @staticmethod
    def _datatype_to_type(datatype: str):
        datatype = datatype.lower()
        if datatype == "integer":
            return DataType.Integer
        elif datatype == "real":
            return DataType.Real
        elif datatype == "text":
            return DataType.Text
        elif datatype == "blob":
            return DataType.Blob
        else:
            raise ValueError(f"Unrecognized datatype [{datatype}]")

    def __repr__(self):
        return str(self)

    def __str__(self):
        return self.prettystr()
        #return f'{self.__class__.__name__}({self.__dict__})'


# simple classes


@dataclass
class TableName(_Symbol):
    table_name: Any


@dataclass
class ColumnName(_Symbol):
    column_name: Any


@v_args(tree=True)
class ToAst2(Transformer):
    """
    Convert parse tree to AST.
    Handles rules with optionals at tail
    and optionals in body.

    NOTE: another decision point here
    is where I wrap every rule in a dummy symbol class.
    - I could wrap each class, and a parent node can unwrap a child.
    - however, for boolean like fields, e.g. column_def__is_primary_key, it might be better
      to return an enum
    """
    # helpers

    def rules_to_kwargs(self, args) -> dict:
        kwargs = {arg.data: arg for arg in args}
        return kwargs

    # simple classes

    def program(self, arg):
        breakpoint()
        pass

    def create_stmnt(self, arg):
        breakpoint()

    def table_name(self, arg: Tree):
        assert len(arg.children) == 1
        val = TableName(arg.children[0])
        # breakpoint()
        return val

    def column_def_list(self, arg):
        breakpoint()

    def column_name(self, arg):
        assert len(arg.children) == 1
        val = TableName(arg.children[0])
        # breakpoint()
        return val

    def primary_key(self, arg):
        # this rule doesn't have any children nodes
        assert len(arg.children) == 0, f"Expected 0 children; received {len(arg.children)}"
        return ColumnModifier.PrimaryKey

    def not_null(self, arg):
        # this rule doesn't have any children nodes
        assert len(arg.children) == 0
        return ColumnModifier.NotNull
        # breakpoint()

    # kw mapped classes

    def column_def_kw_approach(self, tree):
        """
        there's two ways to handle these scenarios
        1) rule _to _kw; the problem here is that this mapping still doesn't have
            every case, e.g. if the rule appears multiple times, the rule_name_map would
            be wrong. Approach 2, i.e. explicit coding depending on the need seems better.
            Also, there aren't too many rules (right now) that woukd benefit from this.
            Albeit, for select  stmnt, there are many clauses, all of which except the select clause
            are optional. This takes me back to the whole cycle, of automating away parse-tree -> AST
            generation. Maybe I should write the select stmnt handler here, before adding a layer of abstratcion

        2) check with if, else conds, i.e. explicit/primitive checking
        """
        params = self.rules_to_kwargs(tree.children)
        val = ColumnDef(**params)
        breakpoint()
        return val

    def column_def(self, tree):
        """
        ?column_def       : column_name datatype primary_key? not_null?

        check with if, else conds
        """
        args = tree.children
        column_name = args[0]
        datatype = args[1]
        # any remaining args are column modifiers
        modifier = ColumnModifier.Nil
        if len(args) >= 3:
            # the logic here is that if the primary key modifier is used
            # not null is redudanct; and the parser ensures/requires primary
            # key mod must be specified before not null
            # todo: this more cleanly, e.g. primary key implies not null, uniqueness
            # modifiers could be a flag enum, which can be or'ed
            modifier = args[2]
        print("COLUMN_DEF", column_name, datatype, modifier)
        breakpoint()
        return val
