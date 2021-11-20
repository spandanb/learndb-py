"""
class representing a logical-database
support API to read/write data via Table
and creating tables etc.
"""

from table import Table
from btree import Tree
from pager import Pager


class StateManager:
    """
    This manages access to all state of the database. This includes
    data (i.e. tables and indices) and metadata (serde, schema).

    The

    contains the state of the database (corresponding to a single file)
    and exposes interface for manipulating it.

    sqllite structure (btshared) contains:
    - pager
    - pagesize (int)
    - cursors (list of open cursors on db)


    This will replace: Database
    """
    def __init__(self, filename: str):
        self.filename = filename
        self.pager = None

        self.page_size = 0
        # catalog contains root page num of tables and indices
        self.catalog_root_page_num = 0
        self.catalog = None

    def open(self):
        """
        opens connection to db, i.e. initializes
        pager.

        The relationships are: `tree` abstracts the pages into a tree
        and maps 1-1 with the logical entity `table`. The table.root_page_num
        is a reference to first
        """
        self.pager = Pager.pager_open(self.filename)

        # create catalog
        # catalog holds mapping from table_name -> root_page_num
        # self.catalog = Table(self.pager, root_page_num=self.catalog_root_page_num)

    def close(self):
        """
        this calls the pager `close`
        """
        self.pager.close()

    def create_catalog_table(self):
        pass

    def create_table(self, table_def):
        """
        this should create an entry in the catalog table.
        catalog is a metadata table. there may be a in-memory class
        to simply ops

        this should w

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

