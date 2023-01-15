"""
class representing a logical-database
support API to read/write data via Table
and creating tables etc.
"""

from .btree import Tree
from .pager import Pager
from .schema import SimpleSchema, CatalogSchema


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

