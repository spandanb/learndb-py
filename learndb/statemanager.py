"""
class representing a logical-database
support API to read/write data via Table
and creating tables etc.
"""
import random
import string
from collections import UserList, UserDict
from typing import Optional, List, Union, Tuple

from .btree import Tree
from .constants import CATALOG_ROOT_PAGE_NUM
from .dataexchange import Response
from .pager import Pager
from .record_utils import GroupedRecord
from .schema import (
    SimpleSchema,
    ScopedSchema,
    CatalogSchema,
    GroupedSchema,
    NonGroupedSchema,
)


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

    def __getitem__(self, key):
        if key not in self.data:
            self.data[key] = []
        return self.data[key]

    def __setitem__(self, key, value):
        if key not in self.data:
            self.data[key] = []
        self.data[key].append(value)


class Scope:
    """
    A scope is a logical environment, within which names and objects are contained/defined.
    A passive entity that exposes add, remove, has_ {recordset, groupedrecordset,
    """

    def __init__(self):
        self.aliased_source = {}
        # NOTE: previously this was a list, but now since TableName is alias
        self.unaliased_source = set()
        # recordset name -> recordset
        self.record_sets = {}
        self.group_rsets = {}
        # recordset name -> schema
        self.rsets_schemas = {}
        self.group_rsets_schemas = {}

    def register_aliased_source(self, source: str, alias: str):
        raise NotImplementedError

    def register_unaliased_source(self, source: str):
        raise NotImplementedError

    def get_recordset(self, name: str) -> Optional[RecordSet]:
        return self.record_sets.get(name)

    def add_recordset(
        self, name: str, schema: NonGroupedSchema, recordset: RecordSet
    ) -> None:
        """
        Upsert a new recordset with `name`
        """
        self.rsets_schemas[name] = schema
        self.record_sets[name] = recordset

    def drop_recordset(self, name: str):
        del self.record_sets[name]

    def get_recordset_schema(self, name: str) -> Optional[NonGroupedSchema]:
        return self.rsets_schemas.get(name)

    def add_grouped_recordset(
        self, name, schema: GroupedSchema, recordset: GroupedRecordSet
    ) -> None:
        self.group_rsets_schemas[name] = schema
        self.group_rsets[name] = recordset

    def get_grouped_recordset(self, name: str) -> Optional[GroupedRecordSet]:
        return self.group_rsets.get(name)

    def get_grouped_recordset_schema(self, name: str) -> Optional[GroupedSchema]:
        return self.group_rsets_schemas.get(name)

    def drop_grouped_recordset(self, name: str):
        raise NotImplementedError

    def cleanup(self):
        """
        TODO: recycle any objects
        """


class StateManager:
    """
    This entity is responsible for management of all state of the database
    (contained a single file).

    State can be broadly divided into: 1) persisted tables (btree and schema),
    that live in an implicit global scope.
    2) all objects that live and die with a session, e.g. local recordsets,
    scopes, and materialized sources.

    There is a third category- objects like functions that logically/from the user's
    perspective live in the same assumed global scope as table names. But these,
    are managed separately.

    This class is responsible for creating Tree and Table objects.
    This is responsible for creating/managing the catalog (a special table).

    The class is intimately tied to catalog definition, i.e. has magic
    constants for manipulating catalog.
    """

    def __init__(self, filename: str):
        # database file
        self.db_filename = filename
        # initialize pager; this will create the file
        # file create functionality can be moved elsewhere if better suited
        self.pager = Pager.pager_open(self.db_filename)
        # the catalog root pagenum is hardcoded
        self.catalog_root_page_num = CATALOG_ROOT_PAGE_NUM
        # catalog schema
        self.catalog_schema = CatalogSchema()
        # catalog tree
        self.catalog_tree = Tree(self.pager, self.catalog_root_page_num)
        # mapping from table_name to schema object
        self.schemas = {}
        self.trees = {}
        # scope stack
        self.scopes: List[Scope] = []

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

    def unregister_table(self, table_name: str):
        """
        Remove table_name entry from both trees and schemas cache
        """
        del self.trees[table_name]
        del self.schemas[table_name]

    def get_catalog_schema(self):
        return self.catalog_schema

    def has_schema(self, table_name: str):
        return table_name in self.schemas

    def get_schema(self, table_name: str):
        return self.schemas[table_name]

    def get_catalog_tree(self):
        return self.catalog_tree

    def get_tree(self, table_name):
        return self.trees[table_name]

    def print_tree(self, table_name: str):
        """
        This method prints the tree
        Putting this here, since the datastore encapsulates tree
        :return:
        """
        self.get_tree(table_name).print_tree()

    def validate_tree(self, table_name: str):
        self.get_tree(table_name).validate()

    # section: scope management

    def begin_scope(
        self,
    ):
        self.scopes.append(Scope())

    def end_scope(self):
        scope = self.scopes.pop()
        scope.cleanup()

    # recordset management

    @staticmethod
    def gen_randkey(size=10, prefix=""):
        return prefix + "".join(
            random.choice(string.ascii_letters) for i in range(size)
        )

    def unique_recordset_name(self) -> str:
        """
        Generate a recordset name unique across all scopes
        """
        is_unique = False
        name = None
        while not is_unique:
            name = self.gen_randkey(prefix="r")
            # name must be unique across all scopes
            for scope in self.scopes:
                if scope.get_recordset(name) is not None:
                    break
            else:
                is_unique = True
        return name

    def unique_grouped_recordset_name(self) -> str:
        """
        Generate a recordset name unique across all scopes
        """
        is_unique = False
        name = None
        while not is_unique:
            name = self.gen_randkey(prefix="g")
            # name must be unique across all scopes
            for scope in self.scopes:
                if scope.get_grouped_recordset(name) is not None:
                    break
            else:
                is_unique = True
        return name

    def init_recordset(self, schema: Union[SimpleSchema, ScopedSchema]) -> Response:
        """
        Creates a new recordset with the associated `schema`, and
        stores it in the current scope.
        Recordset name should be unique across all scopes
        """
        name = self.unique_recordset_name()
        scope = self.scopes[-1]
        scope.add_recordset(name, schema, RecordSet())
        return Response(True, body=name)

    def init_grouped_recordset(self, schema: GroupedSchema):
        """
        init a grouped recordset.
        NOTE: A grouped record set is internally stored like
        {group_key_tuple -> list_of_records}
        """
        name = self.unique_grouped_recordset_name()
        scope = self.scopes[-1]
        scope.add_grouped_recordset(name, schema, GroupedRecordSet())
        return Response(True, body=name)

    def find_recordset_scope(self, name: str) -> Optional[Scope]:
        """
        Find and return scope, where scope contains recordset with `name`
        """
        for scope in reversed(self.scopes):
            rset = scope.get_recordset(name)
            if rset is not None:
                return scope

    def find_grouped_recordset_scope(self, name: str) -> Optional[Scope]:
        """
        Find and return scope, where scope contains grouped recordset with `name`
        """
        for scope in reversed(self.scopes):
            rset = scope.get_grouped_recordset(name)
            if rset is not None:
                return scope

    def get_recordset_schema(self, name: str) -> Optional[NonGroupedSchema]:
        scope = self.find_recordset_scope(name)
        if scope:
            return scope.get_recordset_schema(name)

    def get_grouped_recordset_schema(self, name: str) -> Optional[NonGroupedSchema]:
        scope = self.find_grouped_recordset_scope(name)
        if scope:
            return scope.get_grouped_recordset_schema(name)

    def append_recordset(self, name: str, record):
        """
        find the correct recordset across all scopes;
        then add record to it
        """
        scope = self.find_recordset_scope(name)
        assert scope is not None
        recordset = scope.get_recordset(name)
        recordset.append(record)

    def append_grouped_recordset(self, name: str, group_key: Tuple, record):
        """
        Add record to a group
        """
        scope = self.find_grouped_recordset_scope(name)
        assert scope is not None
        recordset = scope.get_grouped_recordset(name)
        recordset[group_key].append(record)

    def add_group_grouped_recordset(self, name: str, group_key: Tuple, group_recordset):
        """
        Add a new group, with a given set of records for group_recordset
        """
        scope = self.find_grouped_recordset_scope(name)
        assert scope is not None
        recordset = scope.get_grouped_recordset(name)
        assert group_key not in recordset
        recordset[group_key] = group_recordset

    def drop_recordset(self, name: str):
        scope = self.find_recordset_scope(name)
        assert scope is not None
        scope.drop_recordset(name)

    def drop_grouped_recordset(self, name: str):
        raise NotImplementedError

    def recordset_iter(self, name: str):
        """Return an iterator over recordset
        NOTE: The iterator will be consumed after one iteration
        """
        scope = self.find_recordset_scope(name)
        assert scope is not None
        return iter(scope.get_recordset(name))

    def grouped_recordset_iter(self, name) -> List[GroupedRecord]:
        """
        return an iterator over a groups from a grouped recordset
        """
        scope = self.find_grouped_recordset_scope(name)
        assert scope is not None
        recordset = scope.get_grouped_recordset(name)
        schema = scope.get_grouped_recordset_schema(name)
        # NOTE: cloning the group_rset, since it may need to be iterated multiple times
        # A group is represented by a GroupedRecord
        return [
            GroupedRecord(schema, group_key, group_rset)
            for group_key, group_rset in recordset.items()
        ]
