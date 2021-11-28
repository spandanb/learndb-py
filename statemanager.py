"""
class representing a logical-database
support API to read/write data via Table
and creating tables etc.
"""
import os.path

from table import Table
from btree import Tree
from pager import Pager
from schema import Schema, CatalogSchema, generate_schema, Record, create_record

from dataexchange import Response


class StateManager:
    """
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

    def register_tree(self, table_name: str, tree: Tree):
        self.trees[table_name] = tree

    def register_schema(self, table_name: str, schema: Schema):
        self.schemas[table_name] = schema

    def get_catalog_schema(self):
        return self.catalog_schema

    def get_schema(self, table_name: str):
        return self.schemas.get(table_name)

    def get_catalog_tree(self):
        return self.catalog_tree

    def get_tree(self, table_name):
        return self.trees.get(table_name)



class Database:
    """
    This provides a high-level interface to various
    database operations.
    This currently works for a single table.
    TODO: handle multiple tables
    TODO: rename to StorageManager or Storage since this class acts as interface
        to storage layer

    this class should contain the state of the database

    sqllite:
        tree
        btshared

    """
    def __init__(self, filename: str):
        self.filename = filename
        self.pager = None
        # metadata catalog
        self.catalog = None
        self.table = None

    def db_open(self):
        """
        opens connection to db, i.e. initializes
        table and pager.

        The relationships are: `tree` abstracts the pages into a tree
        and maps 1-1 with the logical entity `table`. The table.root_page_num
        is a reference to first

        """
        self.pager = Pager.pager_open(self.filename)
        # with one table the root page is hard coded to 0, but
        # with multiple tables I will need a mapping: table_name -> root_page_num
        # this mapping itself could be placed on the root_page like sqlite
        self.table = Table(self.pager, root_page_num=0)

    def db_close(self, table: 'Table'):
        """
        this calls the pager `close`
        """
        self.table.pager.close()

    def print_tree(self):
        """
        This method prints the tree
        Putting this here, since the database encapsulates tree
        :return:
        """
        self.table.tree.print_tree()

    def validate_tree(self):
        self.table.tree.validate()

