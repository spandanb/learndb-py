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


# section: constants

EXIT_SUCCESS = 0
EXIT_FAILURE = 1

PAGE_SIZE = 4096
WORD = 32

TABLE_MAX_PAGES = 100

DB_FILE = 'db.file'

NEXT_ROW_INDEX = 0 # for testing

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
# todo: nuke me; only for testing
LEAF_NODE_MAX_CELLS = 3


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
# keeping it small for testing
INTERNAL_NODE_MAX_CELLS = 3

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

def next_row():
    """
    helper method - creates a simple `Row`
    should be nuked when I can handle generic row definitions
    """
    # vals = [64, 5, 13, 82]
    # vals = [82, 13, 5, 2]
    vals = [10, 20, 30, 40, 50, 60, 70]
    # vals = [1,2,3,4]
    global NEXT_ROW_INDEX
    row = Row(vals[NEXT_ROW_INDEX], "hello database")
    NEXT_ROW_INDEX += 1
    if NEXT_ROW_INDEX >= len(vals):
        NEXT_ROW_INDEX = len(vals) - 1
    return row
    # return Row(randint(1, 100), "hello database")


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
            print(f"Tried to fetch page out of bounds (max pages = {TABLE_MAX_PAGES})")
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
        :return: cursor pointing to beginning of table
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
        this method can handle both self.page_num starting at an internal node
        or at a leaf node
        :return:
        """
        # starting point
        node = self.table.pager.get_page(self.page_num)
        if Tree.is_node_root(node) is True:
            # there is nothing
            self.end_of_table = True
            return

        node_max_value = Tree.get_node_max_key(node)
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

    def find(self, key) -> tuple:
        """
        find where key exists or should go

        :param key:
        :return: (page_num, cell_num): (int, int)
        """
        root_node = self.pager.get_page(self.root_page_num)
        if self.get_node_type(root_node) == NodeType.NodeInternal:
            print("Can't handle internal nodes")
            sys.exit(EXIT_FAILURE)
        else:  # leaf node
            page_num = self.root_page_num
            cell_num = self.leaf_node_find(page_num, key)
            return page_num, cell_num

    def delete(self, key):
        raise NotImplemented

    def internal_node_find(self, page_num: int, key: int) -> int:
        """
        implement a binary search to find child location where key should be inserted
        NOTE: this will return a child_pos in range [0, num_children], with index at
        position num_children corresponding to the right child.
        :param page_num:
        :param key:
        :return:
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
        implement a binary search
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
        Add new child/key pair to parent.
        If parent is at capacity, recursively apply splitting to parent

        NOTE: new_child is right of old_child
        NOTE: this is only invoked when a leaf overflows.
        """

        old_child = self.pager.get_page(old_child_page_num)
        parent_page_num = self.get_parent_page_num(old_child)
        parent = self.pager.get_page(parent_page_num)

        num_keys = self.internal_node_num_keys(parent)
        if num_keys >= INTERNAL_NODE_MAX_CELLS:
            self.internal_node_split_and_insert()
            return

        # insert new child into parent
        new_child = self.pager.get_page(new_child_page_num)
        new_child_max_key = self.get_node_max_key(new_child)

        right_child_page_num = self.internal_node_right_child(parent)
        right_child = self.pager.get_page(right_child_page_num)
        right_child_max_key = self.get_node_max_key(right_child)

        # the new child is right of the old child.
        # If the old child was the right, (i.e. largest) child of the parent
        # move it to a cell, and make new child the right child, i.e. the largest
        if new_child_max_key < right_child_max_key:
            # add `new_child` to correct location in children cells
            # find correct location for new child
            new_child_cell_num = self.internal_node_find(parent_page_num, new_child_max_key)
            # move all cells right of `new_child_cell_idx`, right by 1
            children = self.internal_node_children_starting_at(new_child_cell_num)
            self.set_internal_node_children_starting_at(new_child_cell_num + 1, children)
            # set new child
            self.set_internal_node_child(parent, new_child_cell_num, new_child_page_num)
        else:
            # make new child, right child
            self.set_internal_node_right_child(new_child)
            # make right child, last child cell
            self.set_internal_node_child(num_keys, right_child_page_num)
            self.set_internal_node_key(num_keys, right_child_max_key)

        # update count
        self.set_internal_node_num_keys(num_keys + 1)

    def internal_node_split_and_insert(self, page_num: int, child_page_num: int):
        """
        parent page, i.e. page at `page_num` is full.
        split the page and insert child_page.
        Then if the parent's parent has capacity, update it with the new node.
        Otherwise, recurse up chain of ancestors until all nodes have no more than max

        `child_page_num` is to be inserted

        """
        old_node = self.pager.get_page(page_num)
        # create a new node
        new_page_num = self.pager.get_unused_page_num()
        new_node = self.pager.get_page(new_page_num)
        self.initialize_internal_node(new_node)

        child_node = self.pager.get_page(child_page_num)
        child_key = self.get_node_max_key(child_node)

        # determine key insertion location via binary search
        num_keys = self.internal_node_num_keys(old_node)
        left_closed_index = 0
        right_open_index = num_keys
        while left_closed_index != right_open_index:
            # break when condition is false, that is the insertion point
            index =  left_closed_index + (right_open_index - left_closed_index ) // 2
            key_at_index = self.internal_node_key(index)
            if child_key == key_at_index:
                # this seems like an duplicate key error
                print("Duplicate key detected")
                sys.exit(EXIT_FAILURE)

            if child_key < key_at_index:
                right_open_index = index
            else:
                left_closed_index = index + 1

        # insert child at this position
        child_position = left_closed_index

        # keys must be divided between old (left child) and new (right child)
        # start from right, move each key (cell) to correct location
        for cell_idx in range(INTERNAL_NODE_MAX_CELLS, -1, -1):
            dest_node = new_node if cell_idx >= LEAF_NODE_LEFT_SPLIT_COUNT else old_node
            new_cell_idx = -1
            if cell_idx > child_position:
                # this is cell that is greater than the cell to insert
                # so it should be shifted to the right
                # verify this logic
                new_cell_idx = (cell_idx + 1) % LEAF_NODE_LEFT_SPLIT_COUNT
            else:
                new_cell_idx = cell_idx % LEAF_NODE_LEFT_SPLIT_COUNT

            if cell_idx == child_position:
                # insert new cell
                self.set_internal_node_key(dest_node, child_position, child_key)
                self.set_internal_node_child(dest_node, child_position, child_page_num)
            else:  # copy an existing cell
                cell_to_copy = self.internal_node_cell(old_node, cell_idx)
                self.set_internal_node_cell(dest_node, new_cell_idx, cell_to_copy)

        # update parent
        if self.is_node_root(old_node):
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
        # create a new node
        new_page_num = self.pager.get_unused_page_num()
        new_node = self.pager.get_page(new_page_num)

        self.initialize_leaf_node(new_node)

        # print("printing new leaf node after init")
        # self.print_leaf_node(new_node)

        # keys must be divided between old (left child) and new (right child)
        # start from right, move each key (cell) to correct location
        split_left_count = 0
        split_right_count = 0
        # `shifted_cell_num` iterates from [n..0], i.e. an entry for all existing cells
        # and the new cell to be inserted
        for shifted_cell_num in range(LEAF_NODE_MAX_CELLS, -1, -1):

            # determine cell's current location
            current_cell_num = shifted_cell_num
            if shifted_cell_num > new_key_cell_num:
                current_cell_num = shifted_cell_num - 1

            # determine destination node from it's shifted position
            if shifted_cell_num >= LEAF_NODE_LEFT_SPLIT_COUNT:
                dest_node = new_node
                split_right_count += 1
                # print("new node")
            else:
                dest_node = old_node
                split_left_count += 1
                # print("old node")

            # determine new cell; cell should be left aligned on its node
            new_cell_num = shifted_cell_num % LEAF_NODE_LEFT_SPLIT_COUNT

            # key_to_move = self.leaf_node_key(old_node, current_cell_num)
            # node_name = "old_node" if dest_node is old_node else "new_node"
            # print(f"key_to_move: {key_to_move} from cell_idx: {current_cell_num} to [{node_name}, {new_cell_num}]")

            # copy/insert cell
            if shifted_cell_num == new_key_cell_num:
                # insert new cell
                self.set_leaf_node_key_value(dest_node, new_cell_num, key, value)
            else:
                # copy existing cell
                cell_to_copy = self.leaf_node_cell(old_node, current_cell_num)
                self.set_leaf_node_cell(dest_node, new_cell_num, cell_to_copy)

        # update counts on child nodes
        # NOTE: the constants `LEAF_NODE_LEFT_SPLIT_COUNT`, `LEAF_NODE_RIGHT_SPLIT_COUNT`
        # don't account for insertion position of new key
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

        # set up root code; root code will be internal node
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

    def print_tree(self, root_page_num=None):
        if root_page_num is None:
            # start from tree root
            root_page_num = self.root_page_num

        root = self.pager.get_page(root_page_num)

        if self.get_node_type(root) == NodeType.NodeLeaf:
            self.print_leaf_node(root)
        else:
            self.print_internal_node(root)

    @staticmethod
    def print_node_header(node: bytes):
        pass

    def print_internal_node(self, node: bytes):
        num_cells = self.internal_node_num_keys(node)
        children = []
        print(f"internal (size {num_cells})")
        for i in range(num_cells):
            key = Tree.internal_node_key(node, i)
            ptr = Tree.internal_node_child(node, i)
            children.append(ptr)
            print(f"{i}-{key}")
        # append right child
        children.append(self.internal_node_right_child(node))
        for child_page_num in children:
            self.print_tree(child_page_num)

    @staticmethod
    def print_leaf_node(node: bytes):
        num_cells = Tree.leaf_node_num_cells(node)
        print(f"leaf (size {num_cells})")
        for i in range(num_cells):
            key = Tree.leaf_node_key(node, i)
            print(f"{i} - {key}")

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

    @staticmethod
    def get_node_max_key(node: bytes) -> int:
        if Tree.get_node_type(node) == NodeType.NodeInternal:
            return Tree.internal_node_key(node, Tree.internal_node_num_keys(node) - 1)
        else:
            return Tree.leaf_node_key(node, Tree.leaf_node_num_cells(node) - 1)

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
        value = node[INTERNAL_NODE_RIGHT_CHILD_OFFSET:INTERNAL_NODE_RIGHT_CHILD_OFFSET + INTERNAL_NODE_RIGHT_CHILD_SIZE]
        return int.from_bytes(value, sys.byteorder)

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

    @staticmethod
    def set_internal_node_child(node: bytes, child_num: int, child_page_num: int):
        """
        set the nth child
        """
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
        table.tree.print_tree()
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


def main():
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
    input_handler('insert', table)
    input_handler('insert', table)
    input_handler('insert', table)
    input_handler('insert', table)
    input_handler('.btree', table)
    input_handler('select', table)
    input_handler('.quit', table)


if __name__ == '__main__':
    # main()
    test()
