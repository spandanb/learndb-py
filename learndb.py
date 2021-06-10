from __future__ import annotations
"""
Python prototype/reference implementation
"""
import os.path
import sys

from typing import Union
from dataclasses import dataclass
from enum import Enum, auto
from random import randint  # for testing
import traceback # for testing

# section: constants

EXIT_SUCCESS = 0
EXIT_FAILURE = 1

PAGE_SIZE = 4096
WORD = 32

TABLE_MAX_PAGES = 100

DB_FILE = 'db.file'

# serialized data layout (row)
ID_SIZE = 6 # length in bytes
BODY_SIZE = 58
ROW_SIZE = ID_SIZE + BODY_SIZE
ID_OFFSET = 0
BODY_OFFSET = ID_OFFSET + ID_SIZE
ROWS_PER_PAGE = PAGE_SIZE // ROW_SIZE

# serialized data layout (tree nodes)
# common node header layout
NODE_TYPE_SIZE = 8
NODE_TYPE_OFFSET = 0
IS_ROOT_SIZE = 8
IS_ROOT_OFFSET = NODE_TYPE_SIZE
# NOTE: this should be defined based on width of system register
PARENT_POINTER_SIZE = WORD
PARENT_POINTER_OFFSET = NODE_TYPE_SIZE + IS_ROOT_SIZE
COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE

# leaf node header layout
# layout:
# nodetype .. is_root .. parent_pointer
# num_keys .. key 0 .. val 0 .. key N-1 val N-1
LEAF_NODE_NUM_CELLS_SIZE = WORD
LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE
LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE

# Leaf node body layout
LEAF_NODE_KEY_SIZE = WORD
LEAF_NODE_KEY_OFFSET = 0
# NOTE: nodes should not cross the page boundary; thus ROW_SIZE is upper
# bounded by remaining space in page
LEAF_NODE_VALUE_SIZE = ROW_SIZE
LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
# LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE
# todo: nuke after testing
LEAF_NODE_MAX_CELLS = 2

# when a node is split, off number of cells, left will get one more
LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) // 2
LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT

# Internal node body layout
# layout:
# nodetype .. is_root .. parent_pointer
# num_keys .. right-child-ptr
# ptr 0 .. key 0 .. ptr N-1 key N-1
INTERNAL_NODE_NUM_KEYS_SIZE = WORD
INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE
INTERNAL_NODE_RIGHT_CHILD_SIZE  = WORD
INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE
INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE

INTERNAL_NODE_KEY_SIZE = WORD
# Ptr to child re
INTERNAL_NODE_CHILD_SIZE = WORD
INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE
INTERNAL_NODE_SPACE_FOR_CELLS = PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE
# INTERNAL_NODE_MAX_CELLS =  INTERNAL_NODE_SPACE_FOR_CELLS / INTERNAL_NODE_CELL_SIZE
# todo: nuke after testing
# cells, i.e. key, child ptr in the body
INTERNAL_NODE_MAX_CELLS = 2
# the +1 is for the right child
INTERNAL_NODE_MAX_CHILDREN = INTERNAL_NODE_MAX_CELLS + 1
INTERNAL_NODE_RIGHT_SPLIT_CHILD_COUNT = (INTERNAL_NODE_MAX_CHILDREN + 1) // 2
INTERNAL_NODE_LEFT_SPLIT_CHILD_COUNT = (INTERNAL_NODE_MAX_CHILDREN + 1) - INTERNAL_NODE_RIGHT_SPLIT_CHILD_COUNT

# section: enums

class MetaCommandResult(Enum):
    Success = auto()
    UnrecognizedCommand = auto()


class StatementType(Enum):
    Uninitialized = auto()
    Insert = auto()
    Select = auto()


class PrepareResult(Enum):
    Success = auto()
    UnrecognizedStatement = auto()


class ExecuteResult(Enum):
    Success = auto()
    TableFull = auto()

class NodeType(Enum):
    NodeInternal =1
    NodeLeaf = 2


class TreeInsertResult(Enum):
    Success = auto()
    DuplicateKey = auto()


# section: classes/structs
@dataclass
class Row:
    """
    NOTE: this assumes a fixed table definition. Fixing the
    table definition, like in the tutorial to bootstrap the
    (de)serialize logic.
    Later when I can handle generic schemas this will need to be
    made generic
    """
    identifier : int
    body: str


@dataclass
class Statement:
    statement_type: StatementType
    row_to_insert: Row

# section: helpers


# section : helper objects/functions, e.g. table, pager

def db_open(filename: str) -> Table:
    """
    opens connection to db, i.e. initializes
    table and pager.

    The relationships are: `tree` is a abstracts the pages into a tree
    and maps 1-1 with the logical entity `table`. The table.root_page_num
    is a reference to first

    """
    pager = Pager.pager_open(filename)
    # with one table the root page is hard coded to 0, but
    # with multiple tables I will need a mapping: table_name -> root_page_num
    table = Table(pager, root_page_num=0)
    return table


def db_close(table: Table):
    """
    this calls the pager `close`
    """
    table.pager.close()


class Pager:
    """
    manager of pages in memory (cache)
    and on file
    """
    def __init__(self, filename):
        """
        filename is handled differently from tutorial
        since it passes a fileptr; here I'll manage the file
        with the `Pager` class
        """
        self.pages = [None for _ in range(TABLE_MAX_PAGES)]
        self.filename = filename
        self.fileptr = None
        self.file_length = 0
        self.num_pages = 0
        self.open_file()

    def open_file(self):
        """
        open database file
        """
        # open binary file such that: it is readable, not truncated(random),
        # create if not exists, writable(random)
        # a+b (and more generally any "a") mode can only write to end
        # of file; seeks only applies to read ops
        # r+b allows read and write, without truncation, but errors if
        # the file does not exist
        # NB: this sets the file ptr location to the end of the file
        try:
            self.fileptr = open(self.filename, "r+b")
        except FileNotFoundError:
            self.fileptr = open(self.filename, "w+b")
        self.file_length = os.path.getsize(self.filename)

        if self.file_length % PAGE_SIZE != 0:
            # avoiding exceptions since I want this to be closer to Rust, i.e panic or enum
            print("Db file is not a whole number of pages. Corrupt file.")
            sys.exit(EXIT_FAILURE)

        self.num_pages = self.file_length // PAGE_SIZE

        # warm up page cache, i.e. load data into memory
        # to load data, seek to beginning of file
        self.fileptr.seek(0)
        for page_num in range(self.num_pages):
            self.get_page(page_num)

    @classmethod
    def pager_open(cls, filename):
        """
        this does nothing - keeping it so code is aligned.
        C works with fd (ints), so you can
        open files and pass around an int. For python, I need to
        pass the file ref around.
        """
        return cls(filename)

    def get_unused_page_num(self) -> int:
        """
        NOTE: this depends on num_pages being updated when a new page is requested
        :return:
        """
        return self.num_pages

    def page_exists(self, page_num: int) -> bool:
        """

        :param page_num: does this page exist/ has been allocated
        :return:
        """
        # num_pages counts whole pages
        return page_num < self.num_pages

    def get_page(self, page_num: int) -> bytearray:
        """
        get `page` given `page_num`
        """
        if page_num > TABLE_MAX_PAGES:
            print(f"Tried to fetch page out of bounds (requested page = {page_num}, max pages = {TABLE_MAX_PAGES})")
            traceback.print_stack()
            sys.exit(EXIT_FAILURE)

        if self.pages[page_num] is None:
            # cache miss. Allocate memory and load from file.
            page = bytearray(PAGE_SIZE)

            # determine number of pages in file; there should only be complete pages
            num_pages = self.file_length // PAGE_SIZE
            if page_num < num_pages:
                # this page exists on file, load from file
                # into `page`
                self.fileptr.seek(page_num * PAGE_SIZE)
                read_page = self.fileptr.read(PAGE_SIZE)
                assert len(read_page) == PAGE_SIZE, "corrupt file: read page returned byte array smaller than page"
                page[:PAGE_SIZE] = read_page
            else:
                pass

            self.pages[page_num] = page

            if page_num >= self.num_pages:
                self.num_pages += 1

        return self.pages[page_num]

    def close(self):
        """
        close the connection i.e. flush pages to file
        """
        # this is 0-based
        # NOTE: not sure about this +1;
        for page_num in range(self.num_pages):
            if self.pages[page_num] is None:
                continue
            self.flush_page(page_num)

    def flush_page(self, page_num: int):
        """
        flush/write page to file
        page_num is the page to write
        size is the number of bytes to write
        """
        if self.pages[page_num] is None:
            print("Tried to flush null page")
            sys.exit(EXIT_FAILURE)

        byte_offset = page_num * PAGE_SIZE
        self.fileptr.seek(byte_offset)
        to_write = self.pages[page_num]
        self.fileptr.write(to_write)



class Cursor:
    """
    Represents a cursor. A cursor understands
    how to traverse the table and how to insert, and remove
    rows from a table.
    """
    def __init__(self, table: Table, page_num: int = 0):
        self.table = table
        self.tree = table.tree
        self.page_num = page_num
        self.cell_num = 0
        self.end_of_table = False
        self.first_leaf()

    def first_leaf(self):
        """
        set cursor location to left-most/first leaf
        """
        # start with root and descend until we hit left most leaf
        node = self.table.pager.get_page(self.page_num)
        while Tree.get_node_type(node) == NodeType.NodeInternal:
            assert Tree.internal_node_num_keys(node) > 0, "invalid tree with zero keys internal node"
            child_page_num = Tree.internal_node_child(node, 0)
            self.page_num = child_page_num
            node = self.table.pager.get_page(child_page_num)

        self.cell_num = 0
        # node must be leaf node
        self.end_of_table = (Tree.leaf_node_num_cells(node) == 0)

    @classmethod
    def table_start(cls, table: Table) -> Cursor:
        """
        cursor pointing to beginning of table
        """
        return cls(table, 0)

    def get_row(self) -> Row:
        """
        return row pointed by cursor
        :return:
        """
        node = self.table.pager.get_page(self.page_num)
        serialized = Tree.leaf_node_value(node, self.cell_num)
        return Table.deserialize(serialized)

    def insert_row(self, row: Row):
        """
        insert row to location pointed by cursor
        :return:
        """
        serialized = Table.serialize(row)
        self.tree.insert(row.identifier, serialized)

    def advance_old(self):
        """
        advance the cursor
        :return:
        """
        node = self.table.pager.get_page(self.page_num)
        self.cell_num += 1
        # consider caching RHS value
        if self.cell_num >= Tree.leaf_node_num_cells(node):
            self.end_of_table = True

    def next_leaf(self):
        """
        move self.page_num and self.cell_num to next leaf and next cell
        this method requires the self.page_num start at a leaf node.

        NOTE: if starting from an internal node, to get to a leaf use `first_leaf` method
        :return:
        """
        # starting point
        node = self.table.pager.get_page(self.page_num)
        if Tree.is_node_root(node) is True:
            # there is nothing
            self.end_of_table = True
            return

        node_max_value = self.tree.get_node_max_key(node)
        parent_page_num = Tree.get_parent_page_num(node)
        # check if current page, i.e. self.page_num is right most child of it's parent
        parent = self.table.pager.get_page(parent_page_num)
        child_num = self.tree.internal_node_find(parent_page_num, node_max_value)
        if child_num == INTERNAL_NODE_MAX_CELLS:
            # this is the right child; thus all children have been consumed
            # go up another level
            self.page_num = parent_page_num
            self.next_leaf()
        else:
            # there is at least one child to be consumed
            # find the next child
            if child_num == Tree.internal_node_num_keys(parent) - 1:
                # next child is the right child
                next_child = Tree.internal_node_right_child(parent)
            else:
                next_child = Tree.internal_node_child(parent, child_num + 1)
            self.page_num = next_child
            # now find first leaf in next child
            self.first_leaf()

    def advance(self):
        """
        advance the cursor, from left most leaf node to right most leaf node
        :return:
        """
        # advance always start at leaf node and ends at a leaf node;
        # starting at or ending at an internal node means the cursor is inconsistent
        node = self.table.pager.get_page(self.page_num)
        # we are currently on the last cell in the node
        # go to the next node if it exists
        if self.cell_num >= Tree.leaf_node_num_cells(node) - 1:
            self.next_leaf()
        else:
            self.cell_num += 1

class Tree:
    """
    Manages read/writes from/to pages corresponding
    to a specific table. In the future 1) the btree could
    also be used as a secondary index and 2) other data
    structures (e.g. SSTable) could replace Tree.


    collections of methods related to BTree
    Right now, this exposes a very low level API of reading/writing from bytes
    And other actors call the relevant methods.

    note: For now, making methods that don't need access to pager static
    Also consider a higher order abstractions over bytearray of different length and
    offset; this would be possible, if the tree understood, the node layout
    """

    def __init__(self, pager: Pager, root_page_num: int):
        """

        :param pager:
        :param root_page_num: of the table this Tree represents
        """
        self.pager = pager
        self.root_page_num = root_page_num
        self.check_create_leaf_root()

    def check_create_leaf_root(self):
        """
        check whether root node exists, if not create it as a leaf node

        :return:
        """
        # root page does not exist, i.e. tree does not exist
        # initialize tree as a a single
        if not self.pager.page_exists(self.root_page_num):
            root_node = self.pager.get_page(self.root_page_num)
            self.initialize_leaf_node(root_node)
            self.set_node_is_root(root_node, True)

    def insert(self, key: int, value: bytes) -> TreeInsertResult:
        """

        :param key: the
        :return:
        """
        page_num, cell_num = self.find(key)
        node = self.pager.get_page(page_num)
        if self.leaf_node_key(node, cell_num) == key:
            return TreeInsertResult.DuplicateKey

        # print(f"inserting key: {key}, page_num: {page_num}, cell_num: {cell_num} ")
        self.leaf_node_insert(page_num, cell_num, key, value)
        return TreeInsertResult.Success

    def find(self, key: int, page_num: int = None) -> tuple:
        """
        find where key exists or should go

        :param page_num:
        :param key:
        :return: (page_num, cell_num): (int, int)
        """
        if page_num is None:
            page_num = self.root_page_num

        node = self.pager.get_page(page_num)
        if self.get_node_type(node) == NodeType.NodeInternal:
            # relative position of this child amongst all children
            child_num = self.internal_node_find(page_num, key)
            if child_num <= self.internal_node_num_keys(node) - 1:
                child_page = self.internal_node_child(node, child_num)
            else:
                child_page = self.internal_node_right_child(node)
            # recurse call
            return self.find(key, child_page)
        else:  # leaf node
            cell_num = self.leaf_node_find(page_num, key)
            return page_num, cell_num

    def delete(self, key):
        raise NotImplemented

    def internal_node_find(self, page_num: int, key: int) -> int:
        """
        implement a binary search to find child location where key should be inserted
        NOTE: this will return a child_pos in range [0, num_children], with index at
        position num_children corresponding to the right child.

        NOTE: This will return special value `INTERNAL_NODE_MAX_CELLS` to indicate
        the position of the key is the right child. All callers must handle this.

        :param page_num:
        :param key:
        :return: child_position where key is to be inserted
        """
        node = self.pager.get_page(page_num)
        if self.get_node_max_key(node) < key:
            # handle special case: key corresponds to right child
            return INTERNAL_NODE_MAX_CELLS

        # do a binary search
        left_closed_index = 0
        right_open_index = self.internal_node_num_keys(node)
        while left_closed_index != right_open_index:
            index =  left_closed_index + (right_open_index - left_closed_index ) // 2
            key_at_index = self.internal_node_key(node, index)
            if key == key_at_index:
                return index
            if key < key_at_index:
                right_open_index = index
            else:
                left_closed_index = index + 1

        return left_closed_index

    def leaf_node_find(self, page_num: int, key: int) -> int:
        """
        find `key` on leaf node ref'ed by `page_num` via binary search
        :param page_num:
        :param key:
        :return: cell_number corresponding to insert location
        """
        node = self.pager.get_page(page_num)
        num_cells = self.leaf_node_num_cells(node)
        left_closed_index = 0
        # 'open' since it's one past right index
        right_open_index = num_cells
        while left_closed_index != right_open_index:
            # this avoid the overflow issue with
            # index = (right_open_index + left_closed_index) // 2
            index =  left_closed_index + (right_open_index - left_closed_index ) // 2
            key_at_index = self.leaf_node_key(node, index)
            if key == key_at_index:
                # key found
                return index
            if key < key_at_index:
                right_open_index = index
            else:
                left_closed_index = index + 1

        return left_closed_index

    def internal_node_insert(self, old_child_page_num: int, new_child_page_num: int):
        """
        Invoked after children at `old_child_page_num` and `new_child_page_num`,
        are created after a new entry causes the node to split. The split can cause either
        sibling to contain the new entry and thus, insert must update the correct child key.

        NOTE: The `new_child_page_num` is the new/right/upper child. However, the old key
        can belong to either old or new child. This is crucial, the new child refers to a newly
        allocated node; but implies nothing about the keys on the new node, and specifically,
        that the newly inserted key (in leaf that precipitated this call) landed on the new child.
        Further, the split can be such that neither child has the previous max key.  Thus the
        parent's references (page num and max key) for both, or one child may need to be updated.

        Further, the key corresponding to both split may need to propagated up to ancestors.

        If the parent is at capacity, the parent must be recursively split/inserted into.
        Thus this method can be called at any level of the splitting, the arguments
        can be either leaf or internal node siblings.
        """
        # left child is old child (page already exists) and right is new;
        left_child = self.pager.get_page(old_child_page_num)
        parent_page_num = self.get_parent_page_num(left_child)
        parent = self.pager.get_page(parent_page_num)

        num_keys = self.internal_node_num_keys(parent)
        if num_keys >= INTERNAL_NODE_MAX_CELLS:
            self.internal_node_split_and_insert(parent_page_num, new_child_page_num, old_child_page_num)
            return

        # determine which child(ren) need to be updated
        left_child_max_key = self.get_node_max_key(left_child)
        right_child = self.pager.get_page(new_child_page_num)
        right_child_max_key = self.get_node_max_key(right_child)
        right_child_updated = False

        # old child is left of new child; we may need to update key for both
        # we have to be careful, since find depends on the state of the node
        # first update old node
        left_child_child_num = self.internal_node_find(parent_page_num, left_child_max_key)
        if left_child_child_num < INTERNAL_NODE_MAX_CELLS:
            # nothing to do if left child is still right child
            # the subsequent insertion of right child will move this
            # later we'll have to update its ancestors
            key_at_left_child_child_num = self.internal_node_key(parent, left_child_child_num)
            if left_child_max_key != key_at_left_child_child_num:

                self.set_internal_node_key(parent, left_child_child_num, left_child_max_key)

                # NOTE: this is only updating the child key, but the ptr should refer to the same page
                # but the below assertion leads to failure in an otherwise successful case? - perhaps that's
                # masking a different error
                # TODO: Check if it is possible that some children need to be moved?
                # assert  child_at_left_child_child_num == old_child_page_num, \
                # f"child_at_left_child_child_num={child_at_left_child_child_num}, old_child_page_num = {old_child_page_num}"

        # insert new node
        right_child_child_num = self.internal_node_find(parent_page_num, right_child_max_key)
        if right_child_child_num == INTERNAL_NODE_MAX_CELLS:
            # move current right child to tail of main body
            current_right_child_page_num = self.internal_node_right_child(parent)
            current_right_child = self.pager.get_page(current_right_child_page_num)
            current_right_child_max_key = self.get_node_max_key(current_right_child)

            self.set_internal_node_child(parent, num_keys, current_right_child_page_num)
            self.set_internal_node_key(parent, num_keys, current_right_child_max_key)

            # make new child, right child, and move the old child into the body
            self.set_internal_node_right_child(parent, new_child_page_num)
            right_child_updated = True
        else:
            # right child is in internal of node
            # move children right of insertion point right by +1
            children = self.internal_node_children_starting_at(parent, right_child_child_num)
            self.set_internal_node_children_starting_at(parent, children, right_child_child_num + 1)

            self.set_internal_node_key(parent, right_child_child_num, right_child_max_key)
            self.set_internal_node_child(parent, right_child_child_num, new_child_page_num)

        # update count
        self.set_internal_node_num_keys(parent, num_keys + 1)

        # right child was split; but we don't know whether a new max key was inserted
        if right_child_updated:
            self.check_update_parent_on_new_right(parent_page_num)

    def internal_node_split_and_insert(self, page_num: int, new_child_page_num: int, old_child_page_num: int):
        """
        Invoked when a new child (internal node, referred to by `new_child_page_num`) is to be inserted into an
        parent (internal node, referred to by `page_num`), and the parent node is full. Here the parent
        are split, and the new child is inserted into the parent.

        TODO: update description

        There is an error in the code, typified by the reasoning below, namely that only
        old child's key could not have changed.


        Next, the other parent split must be inserted into the parent's parent.
        If the parent's parent has space, the new split is added, and the op
        ends. However, if the parent's parent is full, the splitting op proceeds
        recursively up the chain of ancestors, until we hit the root. At which point
        we split the root, and create a new root, which is the parent of the root split.


        An internal node is organized like:
        ch 0, key 0, ... ch N-1, key N-1, ch N
        i.e. N+1 children pointers and N keys

        First, conceptually complete this array, like:
        ch 0, key 0, ... ch new , key new , ... ch N-1, key N-1, ch N, key N

        Then split  num_keys + 2 over left and right splits
        +2 corresponds to new key, and right key
        """

        parent = self.pager.get_page(page_num)

        # first check if old node key needs to be updated; page num should be unchanged
        old_child = self.pager.get_page(old_child_page_num)
        old_child_key = self.get_node_max_key(old_child)
        old_child_insert_pos = self.internal_node_find(page_num, old_child_key)
        if old_child_insert_pos < INTERNAL_NODE_MAX_CELLS:
            assert old_child_page_num == self.internal_node_child(parent,
                                                                  old_child_insert_pos), "old child page num and position should be unchanged"
            if self.internal_node_key(parent, old_child_insert_pos) != old_child_key:
                # key has changed, update
                self.set_internal_node_key(parent, old_child_insert_pos, old_child_key)

        # create a new node, i.e. corresponding to the right split
        new_page_num = self.pager.get_unused_page_num()
        new_node = self.pager.get_page(new_page_num)
        self.initialize_internal_node(new_node)
        self.set_node_is_root(new_node, False)

        # `num_keys` is the number of keys stored in body of internal node
        num_keys = self.internal_node_num_keys(parent)
        # `total_keys` contains total number of keys
        # including 1 extra key for the new child
        total_keys = num_keys + 1

        # determine insertion location of `new_child_page_num`, using key (child-node-max-key)
        new_child = self.pager.get_page(new_child_page_num)
        new_child_key = self.get_node_max_key(new_child)
        new_child_insert_pos = self.internal_node_find(page_num, new_child_key)
        if new_child_insert_pos == INTERNAL_NODE_MAX_CELLS:
            # the logic below expects this to +1 shifted
            new_child_insert_pos += 1

        # divide keys evenly between old (left child) and new (right child)
        split_left_count = 0
        split_right_count = 0
        # `shifted_index` iterates over [num_keys+1,...0]; can be conceptualized
        # as index of each child as if the internal node could hold new child
        for shifted_index in range(total_keys, -1, -1):

            # determine destination node from the shifted position
            if shifted_index >= INTERNAL_NODE_LEFT_SPLIT_CHILD_COUNT:
                dest_node = new_node
                split_right_count += 1
            else:
                dest_node = parent
                split_left_count += 1

            # determine location for cell on it's respective node, after partition operation
            post_partition_index = shifted_index % INTERNAL_NODE_LEFT_SPLIT_CHILD_COUNT

            # we're iterating from higher to lower keyed children,
            # so if this is the first child in the given split, it must be the right child
            is_right_child_after_split = split_left_count == 1 or split_right_count == 1

            # dest_name = "old/left" if dest_node == old_node else "new/right"
            # print(f"In loop shifted_index: {shifted_index}, current_idx: undetermined, destination: {dest_name} post-part-idx: {post_partition_index}")

            # insert new child
            if shifted_index == new_child_insert_pos:
                # print(f"inserting new child into {dest_name} at {new_child_insert_idx}; key: {new_child_key}: is_right_after_split: {is_right_child_after_split}",
                #      f"new_child_page_num: {new_child_page_num}")
                if is_right_child_after_split:
                    self.set_internal_node_right_child(dest_node, new_child_page_num)
                else:
                    self.set_internal_node_key(dest_node, post_partition_index, new_child_key)
                    self.set_internal_node_child(dest_node, post_partition_index, new_child_page_num)
                # below logic is for copying existing children; skip
                continue

            # handle existing entries
            # determine which child the `shifted_index` is referring to
            current_idx = shifted_index
            current_child_page_num = 0
            if shifted_index > new_child_insert_pos:
                current_idx -= 1

            is_current_right_child = current_idx == num_keys

            # print(f"In loop shifted_index: {shifted_index}, current_idx: {current_idx}, destination: {dest_name} post-part-idx: {post_partition_index}")
            # print("-")
            # copy existing child cell to new location
            if is_current_right_child and is_right_child_after_split:
                # current right child, remains a right child
                current_child_page_num = self.internal_node_right_child(parent)
                self.set_internal_node_right_child(dest_node, current_child_page_num)
                # print(f"inserting into {dest_name}; right to right : current child is {current_child_page_num}",
                #      f"current_idx: {current_idx}, post_part_idx: {post_partition_index}")
            elif is_current_right_child:
                # current right child, becomes an internal cell
                # lookup key from right child page num
                current_child_page_num = self.internal_node_right_child(parent)
                current_child = self.pager.get_page(current_child_page_num)
                current_child_key = self.get_node_max_key(current_child)
                self.set_internal_node_key(dest_node, post_partition_index, current_child_key)
                self.set_internal_node_child(dest_node, post_partition_index, current_child_page_num)
                # debug
                # print(f"inserting into {dest_name}; moving current right child to internal. current_child_page_num:{current_child_page_num}, key: {current_child_key}",
                #      f"current_idx: {current_idx}, post_part_idx: {post_partition_index}, current_child_key: {current_child_key}")
            elif is_right_child_after_split:
                # internal cell becomes right child
                current_child_page_num = self.internal_node_child(parent, current_idx)
                self.set_internal_node_right_child(dest_node, current_child_page_num)

                # debugging
                # current_child = self.pager.get_page(current_child_page_num)
                # current_child_key = self.get_node_max_key(current_child)
                # print(f"inserting into {dest_name}; moving internal to right child. page_num: {current_child_page_num}",
                #      f"current_idx: {current_idx}, post_part_idx: {post_partition_index}, current_child_key: {current_child_key}")
            else:  # never a right child
                assert current_idx < num_keys, f"Internal node set cell location [{current_idx}] must be less than num_cells [{num_keys}]"

                cell_to_copy = self.internal_node_cell(parent, current_idx)
                self.set_internal_node_cell(dest_node, post_partition_index, cell_to_copy)

                # debugging
                # print(f"inserting into {dest_name}; moving internal to internal",
                #      f"current_idx: {current_idx}, post_part_idx: {post_partition_index}")

                # print(f"node type of old node is: {self.get_node_type(old_node)}")

                # current_child_page_num = self.internal_node_child(old_node, current_idx) # for error the internal cell contains leaf value
                # current_child = self.pager.get_page(current_child_page_num)
                # current_child_key = self.get_node_max_key(current_child)
                # print(f"same ^ inserting into {dest_name}; moving internal to internal",
                #      f"current_idx: {current_idx}, post_part_idx: {post_partition_index}, current_child_page_num: {current_child_page_num}")

                # print('.'* 40)
                # self.print_internal_node(old_node, recurse=False)
                # print('`'* 40)

        # set left and right split counts
        # -1 since the number of keys excludes right child's key
        self.set_internal_node_num_keys(parent, split_left_count - 1)
        self.set_internal_node_num_keys(new_node, split_right_count - 1)

        # for testing
        # print("In internal_node_split_and_insert")
        # print("print old/left internal node after split")
        # self.print_internal_node(old_node, recurse=False)
        # print("print new/right internal node after split")
        # self.print_internal_node(new_node, recurse=False)

        # update parent
        if self.is_node_root(parent):
            self.create_new_root(new_page_num)
        else:
            self.internal_node_insert(page_num, new_page_num)

    def leaf_node_insert(self, page_num: int, cell_num: int, key: int, value: bytes):
        """
        If there is space on the referred leaf, then the cell will be inserted,
        and the operation will terminate.
        If there is not, the node will have to be split.

        :param page_num:
        :param cell_num: the current cell that the cursor is pointing to
        :param key:
        :param value:
        :return:
        """
        node = self.pager.get_page(page_num)
        num_cells = Tree.leaf_node_num_cells(node)
        if num_cells >= LEAF_NODE_MAX_CELLS:
            # node full - split node and insert
            self.leaf_node_split_and_insert(page_num, cell_num, key, value)
            return

        if cell_num < num_cells:
            # the new cell is left of some an cells
            # move all those cells right by 1 unit
            cells = self.leaf_node_cells_starting_at(node, cell_num)
            self.set_leaf_node_cells_starting_at(node, cell_num + 1, cells)

        Tree.set_leaf_node_key(node, cell_num, key)
        Tree.set_leaf_node_value(node, cell_num, value)
        Tree.set_leaf_node_num_cells(node, num_cells + 1)

        # TODO: verify this logic is correct
        # new key was inserted at largest index, i.e. new max-key - update parent
        if cell_num == num_cells:
            # get previous max key, so we can determine what index to update in parent
            prev_max_key = Tree.leaf_node_key(node, num_cells - 1)
            self.update_parent_on_new_max_child(page_num, prev_max_key, key)

    def leaf_node_split_and_insert(self, page_num: int, new_key_cell_num: int, key: int, value: bytes):
        """
        Split node, i.e. create a new node and move half the cells over to the new node
        NB: after split keys on the upper half (right) must be strictly greater than lower (left) half

        If the node being split is a non-root node, split the node and add the new
        child to the parent. If the parent/ancestor is full, repeat split op until every ancestor is
        within capacity.

        If node being split is root, will need to create a new root. Root page must remain at `root_page_num`

        Args:
            new_key_cell_num: where the new entry would be inserted

        example: insert 2, into leaf node: [1,3,4,5], with max node keys: 4
            [1,2,3] [4,5]
            note:
                - all cells after the new cell to be inserted must be shifted right by 1
                - cells left of new cell are unchanged
        """
        old_node = self.pager.get_page(page_num)
        # create a new node, corresponding to the right split
        new_page_num = self.pager.get_unused_page_num()
        new_node = self.pager.get_page(new_page_num)

        self.initialize_leaf_node(new_node)
        self.set_node_is_root(new_node, False)

        # print("printing new leaf node after init")
        # self.print_leaf_node(new_node)

        # keys must be divided between old (left child) and new (right child)
        # start from right, move each key (cell) to correct location
        split_left_count = 0
        split_right_count = 0
        # `shifted_cell_num` iterates from [n..0], i.e. an entry for all existing cells
        # and the new cell to be inserted
        for shifted_cell_num in range(LEAF_NODE_MAX_CELLS, -1, -1):

            # determine destination node from it's shifted position
            if shifted_cell_num >= LEAF_NODE_LEFT_SPLIT_COUNT:
                dest_node = new_node
                split_right_count += 1
            else:
                dest_node = old_node
                split_left_count += 1

            # determine new cell, i.e. post-splitting location
            # cell should be left aligned on its node
            new_cell_num = shifted_cell_num % LEAF_NODE_LEFT_SPLIT_COUNT

            # insert new entry
            if shifted_cell_num == new_key_cell_num:
                self.set_leaf_node_key_value(dest_node, new_cell_num, key, value)
                continue

            # handle existing cells
            # determine cell's current location
            current_cell_num = shifted_cell_num
            if shifted_cell_num > new_key_cell_num:
                current_cell_num = shifted_cell_num - 1

            # copy existing cell
            cell_to_copy = self.leaf_node_cell(old_node, current_cell_num)
            self.set_leaf_node_cell(dest_node, new_cell_num, cell_to_copy)

            #key_to_move = self.leaf_node_key(old_node, current_cell_num)
            #node_name = "old_node" if dest_node is old_node else "new_node"
            #print(f"key_to_move: {key_to_move} from cell_idx: {current_cell_num} to [{node_name}, {new_cell_num}]")

        # update counts on child nodes
        # NOTE: the constants `LEAF_NODE_LEFT_SPLIT_COUNT`, `LEAF_NODE_RIGHT_SPLIT_COUNT`
        # don't account for insertion position of new key
        # TODO: verify this is indeed the case^
        self.set_leaf_node_num_cells(old_node, split_left_count)
        self.set_leaf_node_num_cells(new_node, split_right_count)

        #print("printing old leaf node after split-insert")
        #self.print_leaf_node(old_node)
        #print("printing new leaf node after split-insert")
        #self.print_leaf_node(new_node)

        # add new node as child to parent of split node
        # if split node was root, create new root and add split as children
        if self.is_node_root(old_node):
            self.create_new_root(new_page_num)
        else:
            self.internal_node_insert(page_num, new_page_num)

    def create_new_root(self, right_child_page_num: int):
        """
        Create a root.
        Here the old root's content is copied to new left child.
        The reason for doing it thus, rather than allocating a new root node,
        is because the root node needs to be the first page in the database file.

        A internal node maintains a array like:
        [ptr[0], key[0], ptr[1], key[1],...ptr[n-1]key[n-1][ptr[n]]
        NOTE: (from book)
        All of the keys on the left most child subtree that Ptr(0) points to have
         values less than or equal to Key(0)
        All of the keys on the child subtree that Ptr(1) points to have values greater
         than Key(0) and less than or equal to Key(1), and so forth
        All of the keys on the right most child subtree that Ptr(n) points to have values greater than Key(n - 1).

        :param right_child_page_num:
        :return:
        """

        root = self.pager.get_page(self.root_page_num)
        # create a new left node
        # root-page must be the first "page" in the file
        left_child_page_num = self.pager.get_unused_page_num()
        left_child = self.pager.get_page(left_child_page_num)
        right_child = self.pager.get_page(right_child_page_num)

        # copy root contents to left child
        left_child[:PAGE_SIZE] = root
        self.set_node_is_root(left_child, False)
        self.set_node_is_root(right_child, False)

        # set up root node; root node will be internal node
        # with one key and two children
        self.initialize_internal_node(root)
        self.set_node_is_root(root, True)

        self.set_internal_node_num_keys(root, 1)
        self.set_internal_node_child(root, 0, left_child_page_num)
        left_child_max_key = self.get_node_max_key(left_child)
        self.set_internal_node_key(root, 0, left_child_max_key)
        self.set_internal_node_right_child(root, right_child_page_num)

    # section: utility methods

    @staticmethod
    def initialize_internal_node(node: bytearray):
        Tree.set_node_type(node, NodeType.NodeInternal)
        Tree.set_internal_node_num_keys(node, 0)

    @staticmethod
    def initialize_leaf_node(node: bytearray):
        Tree.set_node_type(node, NodeType.NodeLeaf)
        Tree.set_leaf_node_num_cells(node, 0)

    @staticmethod
    def internal_node_cell_offset(cell_num: int) -> int:
        return INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE

    @staticmethod
    def internal_node_key_offset(cell_num: int) -> int:
        return Tree.internal_node_cell_offset(cell_num) + INTERNAL_NODE_CHILD_SIZE

    @staticmethod
    def internal_node_child_offset(cell_num: int) -> int:
        return Tree.internal_node_cell_offset(cell_num)

    @staticmethod
    def leaf_node_cell_offset(cell_num: int) -> int:
        """
        helper to calculate cell offset; this is the
        offset to the key for the given cell
        """
        return LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE

    @staticmethod
    def leaf_node_key_offset(cell_num: int) -> int:
        """
        synonym's with cell offset; defining seperate key-value offset
        methods since internal nodes have key/values in reverse order
        """
        return Tree.leaf_node_cell_offset(cell_num)

    @staticmethod
    def leaf_node_value_offset(cell_num: int) -> int:
        """
        returns offset to value
        """
        return Tree.leaf_node_cell_offset(cell_num) + LEAF_NODE_KEY_SIZE

    @staticmethod
    def depth_to_indent(depth:int) -> str:
        """
        maybe this should go in its own dedicated class
        :param depth:
        :return:
        """
        return " " * (depth * 4)

    def print_tree(self, root_page_num: int = None, depth: int = 0):
        """
        print entire tree node by node, starting at an optional node
        """
        if root_page_num is None:
            # start from tree root
            root_page_num = self.root_page_num

        # self.print_tree_constants()
        indent = self.depth_to_indent(depth)
        root = self.pager.get_page(root_page_num)
        if self.get_node_type(root) == NodeType.NodeLeaf:
            print(f"{indent}printing leaf node at page num: {root_page_num}")
            self.print_leaf_node(root, depth = depth)
            print(f"{indent}{'.'*100}")
        else:
            print(f"{indent}printing internal node at page num: {root_page_num}")
            self.print_internal_node(root, recurse = True, depth = depth)
            print(f"{indent}{'.'*100}")

    @staticmethod
    def print_tree_constants():
        print(f"consts: internal_node_max_keys: {INTERNAL_NODE_MAX_CHILDREN}, ")
        print(f"consts: leaf_node_max_cells: {LEAF_NODE_MAX_CELLS}, "
              f"leaf_node_left_split_count: {LEAF_NODE_LEFT_SPLIT_COUNT},"
              f"leaf_node_right_split_count: {LEAF_NODE_RIGHT_SPLIT_COUNT}")

    def print_internal_node(self, node: bytes, recurse: bool = True, depth: int = 0):
        """
        :param node:
        :param recurse:
        :param depth: determine level of indentation
        :return:
        """
        num_cells = self.internal_node_num_keys(node)
        children = []
        indent = self.depth_to_indent(depth)
        print(f"{indent}internal (size {num_cells})")
        for i in range(num_cells):
            key = Tree.internal_node_key(node, i)
            ptr = Tree.internal_node_child(node, i)
            children.append(ptr)
            print(f"{indent}{i}-key: {key}, child: {ptr}")

        # print right child key as well
        right_child_page_num = self.internal_node_right_child(node)
        right_child = self.pager.get_page(right_child_page_num)
        right_child_key = self.get_node_max_key(right_child)
        print(f"{indent}right-key: {right_child_key}, child: {right_child_page_num}")

        if not recurse:
            return

        # append right child
        children.append(self.internal_node_right_child(node))
        for child_page_num in children:
            self.print_tree(child_page_num, depth = depth + 1)

    @staticmethod
    def print_leaf_node(node: bytes, depth: int = 0):
        num_cells = Tree.leaf_node_num_cells(node)
        indent = Tree.depth_to_indent(depth)
        print(f"{indent}leaf (size {num_cells})")
        for i in range(num_cells):
            key = Tree.leaf_node_key(node, i)
            print(f"{indent}{i} - {key}")

    def validate_existence(self, keys: list) -> bool:
        """
        checks whether all keys exist in tree, i.e. no missing keys
        :param keys:
        :return:
        """

    def validate(self) -> bool:
        """
        traverse the tree, starting at root, and ensure values are ordered as expected

        :return:
            raises AssertionError on failure
            True on success
        """
        stack = [(self.root_page_num, float('-inf'), float('inf'))]
        while stack:
            node_page_num, lower_bound, upper_bound = stack.pop()
            node = self.pager.get_page(node_page_num)
            if self.get_node_type(node) == NodeType.NodeInternal:
                print(f"validating internal node on page_num: {node_page_num}")
                self.print_internal_node(node, recurse=False)
                for child_num in range(self.internal_node_num_keys(node)):
                    key = self.internal_node_key(node, child_num)
                    # TODO: should these checks be inclusive?
                    assert lower_bound < key, f"validation: global lower bound [{lower_bound}] constraint violated [{key}]"
                    assert upper_bound >= key, f"validation: global upper bound [{upper_bound}] constraint violated [{key}]"

                    if child_num > 0:
                        prev_key = self.internal_node_key(node, child_num - 1)
                        # validation: check if all of node's key are ordered
                        assert key > prev_key, f"validation: internal node siblings must be strictly greater key: {key}. prev_key:{prev_key}"

                    # todo: check right child is consistent, i.e. max?

                    # add children to stack
                    child_page_num = self.internal_node_child(node, child_num)
                    # lower bound is prev child for non-zero child, and parent's lower bound for 0-child
                    child_lower_bound = self.internal_node_key(node, child_num - 1) if child_num > 0 else lower_bound
                    # upper bound is key value for non-right children
                    child_upper_bound = self.internal_node_key(node, child_num)
                    stack.append((child_page_num, child_lower_bound, child_upper_bound))

                # add right child
                child_page_num = self.internal_node_right_child(node)
                # lower bound is last key
                child_lower_bound = self.internal_node_key(node, self.internal_node_num_keys(node) - 1)
                stack.append((child_page_num, child_lower_bound, upper_bound))

            else:  # leaf node
                print(f"validating leaf node on page_num: {node_page_num}")
                self.print_leaf_node(node)
                for cell_num in range(self.leaf_node_num_cells(node)):
                    if cell_num > 0:
                        key = self.leaf_node_key(node, cell_num)
                        prev_key = self.leaf_node_key(node, cell_num - 1)
                        # validation: check if all of node's key are ordered
                        assert key > prev_key, "validation-0: leaf node siblings must be strictly greater"

        return True

    # section : node getter/setters: common, internal, leaf

    @staticmethod
    def get_parent_page_num(node: bytes) -> int:
        """
        return pointer to parent page_num
        """
        value = node[PARENT_POINTER_OFFSET: PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE]
        return int.from_bytes(value, sys.byteorder)

    @staticmethod
    def get_node_type(node: bytes) -> NodeType:
        value = int.from_bytes(node[NODE_TYPE_OFFSET:NODE_TYPE_OFFSET+NODE_TYPE_SIZE], sys.byteorder)
        return NodeType(value)

    def get_node_max_key(self, node: bytes) -> int:
        if self.get_node_type(node) == NodeType.NodeInternal:
            # max key is right child's max key, so will need to fetch right child
            right_child_page_num = self.internal_node_right_child(node)
            right_child = self.pager.get_page(right_child_page_num)
            # this call will recurse until it hits the right most leaf cell
            right_child_key = self.get_node_max_key(right_child)
            return right_child_key
        else:
            return self.leaf_node_key(node, self.leaf_node_num_cells(node) - 1)

    @staticmethod
    def is_node_root(node: bytes) -> bool:
        value = node[IS_ROOT_OFFSET: IS_ROOT_OFFSET + IS_ROOT_SIZE]
        int_val = int.from_bytes(value, sys.byteorder)
        return bool(int_val)

    @staticmethod
    def internal_node_num_keys(node: bytes) -> int:
        value = node[INTERNAL_NODE_NUM_KEYS_OFFSET: INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE]
        return int.from_bytes(value, sys.byteorder)

    @staticmethod
    def internal_node_child(node: bytes, child_num: int) -> int:
        """return child ptr, i.e. page number"""
        offset = Tree.internal_node_child_offset(child_num)
        value = node[offset: offset + INTERNAL_NODE_CHILD_SIZE]
        return int.from_bytes(value, sys.byteorder)

    @staticmethod
    def internal_node_right_child(node: bytes) -> int:
        value = node[INTERNAL_NODE_RIGHT_CHILD_OFFSET: INTERNAL_NODE_RIGHT_CHILD_OFFSET + INTERNAL_NODE_RIGHT_CHILD_SIZE]
        return int.from_bytes(value, sys.byteorder)

    @staticmethod
    def internal_node_cell(node: bytes, key_num: int) -> bytes:
        """return entire cell containing key and child ptr
        this does not work for right child"""
        offset = Tree.internal_node_cell_offset(key_num)
        return node[offset: offset + INTERNAL_NODE_CELL_SIZE]

    @staticmethod
    def internal_node_key(node: bytes, key_num: int) -> int:
        offset = Tree.internal_node_key_offset(key_num)
        bin_num = node[offset: offset + INTERNAL_NODE_KEY_SIZE]
        return int.from_bytes(bin_num, sys.byteorder)

    @staticmethod
    def internal_node_children_starting_at(node: bytes, child_num: int) -> bytes:
        """
        return bytes corresponding to all children including at `child_num`

        :param node:
        :param child_num:
        :return:
        """
        offset = Tree.internal_node_cell_offset(child_num)
        num_keys = Tree.internal_node_num_keys(node)
        num_keys_to_shift = num_keys - child_num
        return node[offset: offset + num_keys_to_shift * INTERNAL_NODE_CELL_SIZE]

    @staticmethod
    def leaf_node_cell(node: bytes, cell_num: int) -> bytes:
        """
        returns entire cell consisting of key and value
        :param node:
        :param bytes:
        :param cell_num:
        :return:
        """
        offset = Tree.leaf_node_cell_offset(cell_num)
        return node[offset: offset + LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE]

    @staticmethod
    def leaf_node_num_cells(node: bytes) -> int:
        """
        `node` is exactly equal to a `page`. However,`node` is in the domain
        of the tree, while page is in the domain of storage.
        Using the same naming convention of `prop_name` for getter and `set_prop_name` for setter
        """
        bin_num = node[LEAF_NODE_NUM_CELLS_OFFSET: LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE]
        return int.from_bytes(bin_num, sys.byteorder)

    @staticmethod
    def leaf_node_key(node: bytes, cell_num: int) -> int:
        """
        return the leaf node key (int)
        :param node:
        :param cell_num:
        :return:
        """
        offset = Tree.leaf_node_key_offset(cell_num)
        bin_num = node[offset: offset + LEAF_NODE_KEY_SIZE]
        return int.from_bytes(bin_num, sys.byteorder)

    @staticmethod
    def leaf_node_cells_starting_at(node: bytes, cell_num: int) -> bytes:
        """
        return bytes corresponding to all children including at `child_num`

        :param node:
        :param child_num:
        :return:
        """
        offset = Tree.leaf_node_cell_offset(cell_num)
        num_keys = Tree.leaf_node_num_cells(node)
        num_keys_to_shift = num_keys - cell_num
        return node[offset: offset + num_keys_to_shift * LEAF_NODE_CELL_SIZE]

    @staticmethod
    def leaf_node_value(node: bytes, cell_num: int) -> bytes:
        """
        :param node:
        :param cell_num: determines offset
        :return:
        """
        offset = Tree.leaf_node_value_offset(cell_num)
        return node[offset: offset + LEAF_NODE_VALUE_SIZE]

    @staticmethod
    def set_node_is_root(node: bytes, is_root: bool):
        value = is_root.to_bytes(IS_ROOT_SIZE, sys.byteorder)
        node[IS_ROOT_OFFSET: IS_ROOT_OFFSET + IS_ROOT_SIZE] = value

    @staticmethod
    def set_node_type(node: bytes, node_type: NodeType):
        bits = node_type.value.to_bytes(NODE_TYPE_SIZE, sys.byteorder)
        node[NODE_TYPE_OFFSET: NODE_TYPE_OFFSET + NODE_TYPE_SIZE] = bits

    def check_update_parent_on_new_right(self, node_page_num: int):
        """
        similar to updating parent on new max child, however, invoked when
        the right child is updated; this first checks whether the new right child
        indeed has a max key, and then invokes foo

        :param node_page_num:
        :return:
        """
        node = self.pager.get_page(node_page_num)
        if self.is_node_root(node):
            # nothing to do
            return

        parent_page_num = self.get_parent_page_num(node)
        parent = self.pager.get_page(parent_page_num)

        node_max_key = self.get_node_max_key(node)
        # check node position in parent
        node_child_num = self.internal_node_find(parent_page_num, node_max_key)
        if node_child_num == INTERNAL_NODE_MAX_CELLS:
            # the node is parent's right child
            # this can be conceptually viewed as this as parent receiving a new child
            # since get internal node max child recursively gets the rightmost leaf descendant
            self.check_update_parent_on_new_right(parent_page_num)
        else:
            # node is a non-right child of it's parent
            old_child_key = self.internal_node_key(parent, node_child_num)
            if old_child_key < node_max_key:
                # we indeed have a new max key
                # update the node
                self.set_internal_node_key(parent, node_child_num, node_max_key)
                self.update_parent_on_new_max_child(parent_page_num, old_child_key, node_max_key)
            else:
                # logical error in the invocation, since if old is less than node, then it must be max
                assert old_child_key == node_max_key
                # parent does not need to be updated


    def update_parent_on_new_max_child(self, node_page_num: int, prev_max_key: int, new_max_key: int):
        """
        Invoked when node at `node_page_num` has max key `prev_max_key` replaced with
        `new_max_key`.

        Thus, if the node was a non-right child, update the parent's key referring
        to this child, and the op terminates. If the node was a right-child, the parent does
        not need to be updated. (since the parent doesn't hold the right key). However,
        now update the grandparent, since the max key of it's child (the parent) has changed. This
        op recurses until, we update a non-right child or reach the root.

        NOTE: This must be invoked after the node has been updated and the new key has been inserted
        (into the child, i.e. `node_page_num`). This is needed, since this determines,
        how index of the previous max child is fetched

        consider: renaming update_parent, update_parents_on_new_key, update_parent_on_new_max_child

        :param child_page_num:
        :param prev_max_key:
        :param new_max_key:
        :return:0
        """

        assert prev_max_key < new_max_key, f"updating parent on new max child key [{new_max_key}] requires new key be greater than previous max key [{prev_max_key}]"

        node = self.pager.get_page(node_page_num)
        if self.is_node_root(node):
            # nothing to do
            return

        # find node's parent
        parent_page_num = self.get_parent_page_num(node)
        parent = self.pager.get_page(parent_page_num)
        assert self.get_node_type(parent) == NodeType.NodeInternal, "update parent invoked on leaf node"

        # determine whether node is an internal child of it's parent or right child
        prev_max_key_child_idx = self.internal_node_find(parent_page_num, prev_max_key)

        print(f"In update_parent_child_key; node_page_num: {node_page_num}, prev_max_key: {prev_max_key}, new_max_key: {new_max_key}",
              f"parent_page_num: {parent_page_num}, prev_max_key_child_idx: {prev_max_key_child_idx}, INTERNAL_NODE_MAX_CELLS: {INTERNAL_NODE_MAX_CELLS}")

        if prev_max_key_child_idx < INTERNAL_NODE_MAX_CELLS:
            # key is not parent's right child; update parent at child idx and terminate op
            self.set_internal_node_key(parent, prev_max_key_child_idx, new_max_key)
        else:
            # key is parent's right child; recurse to parent' parent
            self.update_parent_on_new_max_child(parent_page_num, prev_max_key, new_max_key)

    @staticmethod
    def set_internal_node_cell(node: bytes, key_num: int, cell: bytes) -> bytes:
        """
        write entire cell
        this won't work for right child
        """
        offset = Tree.internal_node_cell_offset(key_num)
        assert len(cell) == INTERNAL_NODE_CELL_SIZE, "bytes written to internal cell less than INTERNAL_NODE_CELL_SIZE"
        node[offset: offset + len(cell)] = cell

    @staticmethod
    def set_internal_node_child(node: bytes, child_num: int, child_page_num: int):
        """
        set the nth child
        """
        assert child_page_num < 100, f"attempting to set very large page num {child_page_num}"
        offset = Tree.internal_node_child_offset(child_num)
        value = child_page_num.to_bytes(INTERNAL_NODE_CHILD_SIZE, sys.byteorder)
        node[offset: offset + INTERNAL_NODE_CHILD_SIZE] = value

    @staticmethod
    def set_internal_node_children_starting_at(node: bytes, children: bytes, child_num: int ):
        """
        bulk set multiple cells

        :param node:
        :param children:
        :param child_num:
        :return:
        """
        assert len(children) % INTERNAL_NODE_CELL_SIZE == 0, "error: children are not an integer multiple of cell size"
        offset = Tree.internal_node_cell_offset(child_num)
        node[offset: offset + len(children)] = children

    @staticmethod
    def set_internal_node_key(node: bytes, child_num: int, key: int):
        offset = Tree.internal_node_key_offset(child_num)
        value = key.to_bytes(INTERNAL_NODE_CHILD_SIZE, sys.byteorder)
        node[offset: offset + INTERNAL_NODE_NUM_KEYS_SIZE] = value

    @staticmethod
    def set_internal_node_num_keys(node: bytes, num_keys: int):
        value = num_keys.to_bytes(INTERNAL_NODE_NUM_KEYS_SIZE, sys.byteorder)
        node[INTERNAL_NODE_NUM_KEYS_OFFSET: INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE] = value

    @staticmethod
    def set_internal_node_right_child(node: bytes, right_child_page_num: int):
        assert right_child_page_num < 100, f"attempting to set very large page num {right_child_page_num}"
        value = right_child_page_num.to_bytes(INTERNAL_NODE_RIGHT_CHILD_SIZE, sys.byteorder)
        node[INTERNAL_NODE_RIGHT_CHILD_OFFSET:INTERNAL_NODE_RIGHT_CHILD_OFFSET + INTERNAL_NODE_RIGHT_CHILD_SIZE] = value

    @staticmethod
    def set_leaf_node_key(node: bytes, cell_num: int, key: int):
        offset = Tree.leaf_node_key_offset(cell_num)
        value = key.to_bytes(LEAF_NODE_KEY_SIZE, sys.byteorder)
        node[offset: offset + LEAF_NODE_KEY_SIZE] = value

    @staticmethod
    def set_leaf_node_cells_starting_at(node: bytes, cell_num: int, cells: bytes):
        offset = Tree.leaf_node_cell_offset(cell_num)
        node[offset: offset + len(cells)] = cells

    @staticmethod
    def set_leaf_node_num_cells(node: bytearray, num_cells: int):
        """
        write num of node cells: encode to int
        """
        value = num_cells.to_bytes(LEAF_NODE_NUM_CELLS_SIZE, sys.byteorder)
        node[LEAF_NODE_NUM_CELLS_OFFSET: LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE] = value

    @staticmethod
    def set_leaf_node_value(node: bytes, cell_num: int, value: bytes):
        """
        :param node:
        :param cell_num:
        :param value:
        :return:
        """
        offset = Tree.leaf_node_value_offset(cell_num)
        node[offset: offset + LEAF_NODE_VALUE_SIZE] = value

    @staticmethod
    def set_leaf_node_cell(node: bytes, cell_num: int, cell: bytes):
        """
        write both key and value for a given cell
        """
        offset = Tree.leaf_node_cell_offset(cell_num)
        assert len(cell) == LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE, "cell length not equal to key len plus value length"
        node[offset: offset + LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE] = cell

    @staticmethod
    def set_leaf_node_key_value(node: bytes, cell_num: int, key: int, value: bytes):
        """
        write both key and value for a given cell
        """
        Tree.set_leaf_node_key(node, cell_num, key)
        Tree.set_leaf_node_value(node, cell_num, value)


class Table:
    """
    Currently `Table` interface is around (de)ser given a row number.
    Ultimately, the table should
    represent the logical-relation-entity, and access to the pager, i.e. the storage
    layer should be done via an Engine, that acts as the storage layer access for
    all tables.
    """
    def __init__(self, pager: Pager, root_page_num: int = 0):
        self.pager = pager
        self.root_page_num = root_page_num
        self.tree = Tree(pager, root_page_num)

    @staticmethod
    def serialize(row: Row) -> bytearray:
        """
        turn row (object) into bytes
        """

        serialized = bytearray(ROW_SIZE)
        ser_id = row.identifier.to_bytes(ID_SIZE, sys.byteorder)
        # strings needs to be encoded
        ser_body = bytes(str(row.body), "utf-8")
        if len(ser_body) > BODY_SIZE:
            raise ValueError("row serialization failed; body too long")

        serialized[ID_OFFSET: ID_OFFSET + ID_SIZE] = ser_id
        serialized[BODY_OFFSET: BODY_OFFSET + len(ser_body)] = ser_body
        return serialized

    @staticmethod
    def deserialize(row_bytes: bytes):
        """

        :param byte_offset:
        :return:
        """
        # read bytes corresponding to columns
        id_bstr = row_bytes[ID_OFFSET: ID_OFFSET + ID_SIZE]
        body_bstr = row_bytes[BODY_OFFSET: BODY_OFFSET + BODY_SIZE]

        # this will need to be revisited when handling other data types
        id_val = int.from_bytes(id_bstr, sys.byteorder)
        # not sure if stripping nulls is valid (for other datatypes)
        body_val = body_bstr.rstrip(b'\x00')
        body_val = body_val.decode('utf-8')
        return Row(id_val, body_val)

# section: core execution/user-interface logic

def is_meta_command(command: str) -> bool:
    return command[0] == '.'


def do_meta_command(command: str, table: Table) -> MetaCommandResult:
    if command == ".quit":
        db_close(table)
        sys.exit(EXIT_SUCCESS)
    elif command == ".btree":
        print("Printing tree" + "-"*50)
        table.tree.print_tree()
        print("Finished printing tree" + "-"*50)
        return MetaCommandResult.Success
    elif command == ".validate":
        print("Validating tree" + "-"*50)
        table.tree.validate()
        print("Validation succeeded" + "-"*50)
        return MetaCommandResult.Success
    elif command == ".nuke":
        os.remove(DB_FILE)
    return MetaCommandResult.UnrecognizedCommand


def prepare_statement(command: str, statement: Statement) -> PrepareResult:
    """
    prepare a statement
    :param command:
    :param statement: modify in-place to be similar to rust impl

    :return:
    """
    if command.startswith("insert"):
        statement.statement_type = StatementType.Insert
        return PrepareResult.Success
    elif command.startswith("select"):
        statement.statement_type = StatementType.Select
        return PrepareResult.Success
    return PrepareResult.UnrecognizedStatement


def execute_insert(statement: Statement, table: Table) -> ExecuteResult:
    print("executing insert...")
    cursor = Cursor.table_start(table)

    row_to_insert = statement.row_to_insert
    if row_to_insert is None:
        # TODO: nuke me
        row_to_insert = next_row()
        print(f"inserting row with id: {row_to_insert.identifier}")

    cursor.insert_row(row_to_insert)

    return ExecuteResult.Success


def execute_select(table: Table):
    # get cursor to start of table
    print("executing select...")

    cursor = Cursor.table_start(table)
    while cursor.end_of_table is False:
        print(cursor.get_row())
        cursor.advance()


def execute_statement(statement: Statement, table: Table):
    """
    execute statement
    """
    match statement.statement_type:
        case StatementType.Select:
            execute_select(table)
        case StatementType.Insert:
            execute_insert(statement, table)


def input_handler(input_buffer: str, table: Table):
    """
    handle input buffer; could contain command or meta command
    """
    if is_meta_command(input_buffer):
        match do_meta_command(input_buffer, table):
            case MetaCommandResult.Success:
                return
            case MetaCommandResult.UnrecognizedCommand:
                print("Unrecognized meta command")
                return

    statement = Statement(StatementType.Uninitialized, None)
    match prepare_statement(input_buffer, statement):
        case PrepareResult.Success:
            # will execute below
            pass
        case PrepareResult.UnrecognizedStatement:
            print(f"Unrecognized keyword at start of '{input_buffer}'")
            return

    # handle non-meta command
    execute_statement(statement, table)
    print(f"Executed command '{input_buffer}'")


def next_value(index):


    # return randint(1, 1000)

    # vals = [64, 5, 13, 82]
    # vals = [82, 13, 5, 2, 0]
    # vals = [10, 20, 30, 40, 50, 60, 70]
    # vals = [1,2,3,4]
    # vals = [72, 79, 96, 38, 47]
    # vals = [i for i in range(1, 100)]
    # vals = [432, 507, 311, 35, 246, 950, 956, 929, 769, 744, 994, 438]
    # vals = [159, 597, 520, 189, 822, 725, 504, 397, 218, 134, 516]
    # vals = [159, 597, 520, 189, 822, 725, 504, 397]
    # vals = [960, 267, 947, 400, 795, 327, 464, 884, 667, 870, 92]
    # vals = [793, 651, 165, 282, 177] #, 439, 593, ]
    vals = [229, 653, 248, 298, 801, 947, 63, 619, 475, 422, 856, 57, 38]

    if index >= len(vals):
        return vals[-1]
    return vals[index]



def insert_helper(table, key):
    """
    helper to invoke insert for debugging
    """
    statement = Statement(StatementType.Insert, Row(key, "hello database"))
    execute_statement(statement, table)


def repl():
    """
    repl
    """
    table = db_open(DB_FILE)
    while True:
        input_buffer = input("db > ")
        input_handler(input_buffer, table)


def test():
    os.remove(DB_FILE)
    table = db_open(DB_FILE)

    Tree.print_tree_constants()

    values = []
    for i in range(20):
        value = next_value(i)
        values.append(value)
        try:
            insert_helper(table, value)
        except AssertionError as e:
            print(f"Caught assertion error; values: {values}")
            raise
        # input_handler('insert', table)

        input_handler('.btree', table)
        #input_handler('.validate', table)
        print(" ")

    # input_handler('select', table)
    # input_handler('.btree', table)


    input_handler('.quit', table)


if __name__ == '__main__':
    # repl()
    test()
