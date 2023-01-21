"""
class representing a logical-database
support API to read/write data via Table
and creating tables etc.
"""
import random
import string
from collections import defaultdict, UserList, UserDict
from typing import Optional, List

from .btree import Tree
from .dataexchange import Response
from .pager import Pager
from .schema import AbstractSchema, SimpleSchema, CatalogSchema, GroupedSchema
from .record_utils import GroupedRecord


class RecordSet(UserList):
    """
    Maintains a list of records
    """
    pass

class GroupedRecordSet(UserDict):
    """
    Maintains a dictionary of lists of records, where the dict is
    indexed by the group key
    """
    pass



class Scope:
    """
    A scope is a logical environment, within which names and objects are contained/defined.
    A passive entity that exposes add, remove, has_ {recordset, groupedrecordset,
    """
    def __init__(self):
        self.aliased_source = {}
        # NOTE: previously this was a list, but now since TableName is alias
        self.unaliased_source = set()
        # record identifier -> recordset
        self.record_sets = {}
        self.group_rsets = {}

    def register_aliased_source(self, source: str, alias: str):
        raise NotImplementedError

    def register_unaliased_source(self, source: str):
        raise NotImplementedError

    def get_recordset(self, name: str) -> Optional[RecordSet]:
        return self.record_sets.get(name)

    def add_recordset(self, name: str, recordset: RecordSet) -> None:
        self.record_sets[name] = recordset

    def get_grouped_recordset(self, name: str) -> Optional[GroupedRecordSet]:
        return self.group_rsets.get(name)

    def add_grouped_recordset(self, name, recordset: GroupedRecordSet) -> None:
        self.group_rsets[name] = recordset

    def cleanup(self):
        """
        Should recycle any objects
        """


class StateManager:
    """
    This entity is responsible for management of all state. State includes tables and functions,
    but all local recordsets, scopes, and materialized sources.


    ---

    This manages access to all state of the database (contained a single
    file). This includes
    data (i.e. tables and indices) and metadata (serde, schema).

    All state for user-defined tables/indices are contained in Tree
    (provides read/write access to storage) and Table (logical schema,
    physical layout, deser, i.e. read/write )

    This class is responsible for creating Tree and Table objects.

    This is responsible for creating/managing the catalog (a special table).

    The class is intimately tied to catalog definition, i.e. has magic
    constants for manipulating catalog.

    This class is a catch-all. I should refactor it if needed.
    """
    def __init__(self, filename: str):
        self.db_filename = filename
        self.pager = None
        # the catalog root is hardcoded to page 0
        self.catalog_root_page_num = 0
        self.catalog_schema = None
        self.catalog_tree = None

        # mapping from table_name to schema object
        # schema should be singletons
        self.schemas = {}
        self.trees = {}
        # initialize
        self.init()
        self.scopes : List[Scope] = []

    def init(self):
        """

        NOTE: no special handling is needed if this is a new db. This method
        will create the catalog tree- which will allocate the root page num.

        :return:
        """

        # initialize pager; this will create the file
        # file create functionality can be moved elsewhere if better suited
        self.pager = Pager.pager_open(self.db_filename)
        # keep ref to catalog schema
        # schema are treated as standalone read-only data
        self.catalog_schema = CatalogSchema()
        # create catalog tree
        self.catalog_tree = Tree(self.pager, self.catalog_root_page_num)

    def close(self):
        """
        this calls the pager `close`
        """
        self.pager.close()

    def get_pager(self):
        return self.pager

    def allocate_tree(self):
        """
        Allocate tree, by requesting an unused from pager, i.e.
        as a root page for new tree.
        :return:
        """
        return self.pager.get_unused_page_num()

    def table_exists(self, table_name: str) -> bool:
        return table_name in self.trees

    def register_tree(self, table_name: str, tree: Tree):
        self.trees[table_name] = tree

    def register_schema(self, table_name: str, schema: SimpleSchema):
        self.schemas[table_name] = schema

    def get_catalog_schema(self):
        return self.catalog_schema

    def has_schema(self, table_name: str):
        return table_name in self.schemas

    def get_schema(self, table_name: str):
        # todo: this should do a scoped lookup
        return self.schemas[table_name]

    def get_catalog_tree(self):
        return self.catalog_tree

    def get_tree(self, table_name):
        return self.trees[table_name]

    def print_tree(self, table_name: str):
        """
        This method prints the tree
        Putting this here, since the database encapsulates tree
        TODO: move this elsewhere
        :return:
        """
        self.get_tree(table_name).print_tree()

    def validate_tree(self, table_name: str):
        self.get_tree(table_name).validate()

    # section: record

    @staticmethod
    def gen_randkey(size=10, prefix=""):
        return prefix + "".join(random.choice(string.ascii_letters) for i in range(size))

    # section: scope management

    def create_scope(self, ):
        self.scopes.append(Scope())

    def end_scope(self):
        scope = self.scopes.pop()
        scope.cleanup()

    # recordset management

    def unique_recordset_name(self) -> str:
        """
        Generate a recordset name unique across all scopes
        """
        name = self.gen_randkey(prefix="r")
        scope = self.scopes[-1]
        while scope.get_recordset(name) is not None:
            # generate while name is non-unique
            name = self.gen_randkey(prefix="r")


    def init_recordset(self, schema) -> Response:
        """
        Creates a new recordset with the associated `schema`, and
        stores it in the current scope.
        Recordset name should be unique across all scopes
        """
        #
        name = self.gen_randkey(prefix="r")
        scope = self.scopes[-1]
        while scope.get_recordset(name) is not None:
            # generate while name is non-unique
            name = self.gen_randkey(prefix="r")
        return Response(True, body=name)

    def init_grouped_recordset(self, schema: GroupedSchema):
        """
        init a grouped recordset.
        NOTE: A grouped record set is internally stored like
        {group_key_tuple -> list_of_records}
        """
        name = self.gen_randkey(prefix="g")
        while name in self.grouprsets:
            # generate while non-unique
            name = self.gen_randkey(prefix="g")
        self.grouprsets[name] = defaultdict(list)
        self.schemas[name] = schema
        return Response(True, body=name)

    def get_recordset_schema(self, name: str) -> AbstractSchema:
        return self.name_registry.get_schema(name)
        # return self.schemas[name]

    def append_recordset(self, name: str, record):
        assert name in self.rsets
        self.rsets[name].append(record)

    def append_grouped_recordset(self, name: str, group_key: tuple, record):
        self.grouprsets[name][group_key].append(record)

    def drop_recordset(self, name: str):
        del self.rsets[name]

    def recordset_iter(self, name: str):
        """Return an iterator over recordset
        NOTE: The iterator will be consumed after one iteration
        """
        return iter(self.rsets[name])

    def grouped_recordset_iter(self, name) -> List[GroupedRecord]:
        """
        return a pair of (group_key, group_recordset_iterator)
        """
        # NOTE: cloning the group_rset, since it may need to be iterated multiple times
        ret = [GroupedRecord(self.schemas[name], group_key, list(group_rset))
                 for group_key, group_rset in self.grouprsets[name].items()]
        return ret

