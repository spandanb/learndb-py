"""
class representing a logical-database
support API to read/write data via Table
and creating tables etc.
"""

from table import Table
from pager import Pager


class Database:
    """
    This provides a high-level interface to various
    database operations.
    This currently works for a single table.
    TODO: handle multiple tables
    TODO: rename to StorageManager or Storage since this class acts as interface
        to storage layer
    """
    def __init__(self, filename: str):
        self.filename = filename
        self.pager = None
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

