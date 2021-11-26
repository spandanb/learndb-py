"""
class representing a logical-database
support API to read/write data via Table
and creating tables etc.
"""
import os.path

from table import Table
from btree import Tree
from pager import Pager
from schema import Schema


class SchemaManager:
    """
    Manage access to schema and utils
    """
    def __init__(self):
        pass

    def generate_schema(self) -> Schema:
        pass




class SerdeManager:
    pass


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
    """
    def __init__(self, filename: str):
        self.db_filename = filename
        self.pager = None
        # the catalog root is hardcoded to page 0
        self.catalog_root_page_num = 0
        self.catalog_tree = None
        self.catalog_table = None
        # mapping from table_name to object
        # todo: do I need tables or schemas
        self.tables = {}
        self.trees = {}

    def init(self):
        """

        NOTE: no special handling is needed if this is a new db. This method
        will create the catalog tree- which will allocate the root page num.

        :return:
        """

        # initialize pager; this will create the file
        # file create functionality can be moved elsewhere if better suited
        self.pager = Pager.pager_open(self.db_filename)

        self.catalog_tree = Tree(self.pager, self.catalog_root_page_num)

    def close(self):
        """
        this calls the pager `close`
        """
        self.pager.close()

    def generate_schema(self):
        """
        NOTE: this should just invoke schema.py::construct_schema and perhaps cache it
        :return:
        """

    def create_table(self, table_def):
        """
        this should create an entry in the catalog table.
        catalog is a metadata table. there may be a in-memory class
        to simply ops

        :return:
        """
        # write new table to catalog table
        # create a btree corresponding to new table

        # create a table (logical schema + serde)

    def get_table(self):
        """
        this would return a reference to tree for table
        """


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

    def db_close(self, table: Table):
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

