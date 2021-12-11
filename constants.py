EXIT_SUCCESS = 0
EXIT_FAILURE = 1

PAGE_SIZE = 4096
WORD = 4

TABLE_MAX_PAGES = 100

DB_FILE = 'db.file'

# TODO: nuke consts for a fixed schema
# serialized data layout (row)
ID_SIZE = 6  # length in bytes
BODY_SIZE = 58
ROW_SIZE = ID_SIZE + BODY_SIZE
ID_OFFSET = 0
BODY_OFFSET = ID_OFFSET + ID_SIZE
ROWS_PER_PAGE = PAGE_SIZE // ROW_SIZE

USAGE = '''
Supported commands:
-------------------
insert 3 into tree
> insert 3

select and output all rows (no filtering support for now)
> select

delete 3 from tree
> delete 3

Supported meta-commands:
------------------------
print usage
.help

quit REPl
> .quit

print btree
> .btree

performs internal consistency checks on tree
> .validate
'''

# btree constants

# serialized data layout (tree nodes)
# common node header layout
NODE_TYPE_SIZE = WORD
NODE_TYPE_OFFSET = 0
IS_ROOT_SIZE = WORD
IS_ROOT_OFFSET = NODE_TYPE_SIZE
# NOTE: this should be defined based on width of system register
PARENT_POINTER_SIZE = WORD
PARENT_POINTER_OFFSET = NODE_TYPE_SIZE + IS_ROOT_SIZE
COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE

# Internal node body layout
# layout:
# nodetype .. is_root .. parent_pointer
# num_keys .. right-child-ptr
# ptr 0 .. key 0 .. ptr N-1 key N-1
INTERNAL_NODE_NUM_KEYS_SIZE = WORD
INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE
INTERNAL_NODE_RIGHT_CHILD_SIZE = WORD
INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE
INTERNAL_NODE_HAS_RIGHT_CHILD_SIZE = WORD
INTERNAL_NODE_HAS_RIGHT_CHILD_OFFSET = INTERNAL_NODE_RIGHT_CHILD_OFFSET + INTERNAL_NODE_HAS_RIGHT_CHILD_SIZE
INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE + INTERNAL_NODE_HAS_RIGHT_CHILD_SIZE

INTERNAL_NODE_KEY_SIZE = WORD
# Ptr to child re
INTERNAL_NODE_CHILD_SIZE = WORD
INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE
INTERNAL_NODE_SPACE_FOR_CELLS = PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE
# INTERNAL_NODE_MAX_CELLS =  INTERNAL_NODE_SPACE_FOR_CELLS / INTERNAL_NODE_CELL_SIZE
# todo: nuke after testing
# NOTE: this should not dip below 3 due to the constraint of unary trees
# cells, i.e. key, child ptr in the body
INTERNAL_NODE_MAX_CELLS = 3
# the +1 is for the right child
INTERNAL_NODE_MAX_CHILDREN = INTERNAL_NODE_MAX_CELLS + 1
INTERNAL_NODE_RIGHT_SPLIT_CHILD_COUNT = (INTERNAL_NODE_MAX_CHILDREN + 1) // 2
INTERNAL_NODE_LEFT_SPLIT_CHILD_COUNT = (INTERNAL_NODE_MAX_CHILDREN + 1) - INTERNAL_NODE_RIGHT_SPLIT_CHILD_COUNT

# leaf node header layout
# old layout:
# nodetype .. is_root .. parent_pointer
# num_keys .. key 0 .. val 0 .. key N-1 val N-1
# key 0 .. val 0 .. key N-1 val N-1

# new layout
# nodetype .. is_root .. parent_pointer
# num_cells .. alloc_ptr .. free_list_head_ptr .. total_free_bytes
# cellptr_0 .. cellptr_1 ... cellptr_N-1
LEAF_NODE_NUM_CELLS_SIZE = WORD
LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE
LEAF_NODE_ALLOC_POINTER_SIZE = WORD
LEAF_NODE_ALLOC_POINTER_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_ALLOC_POINTER_SIZE
LEAF_NODE_FREE_LIST_HEAD_POINTER_SIZE = WORD
LEAF_NODE_FREE_LIST_HEAD_POINTER_OFFSET = LEAF_NODE_ALLOC_POINTER_OFFSET + LEAF_NODE_FREE_LIST_HEAD_POINTER_SIZE
LEAF_NODE_TOTAL_FREE_BYTES_SIZE = WORD
LEAF_NODE_TOTAL_FREE_BYTES_OFFSET = LEAF_NODE_FREE_LIST_HEAD_POINTER_OFFSET + LEAF_NODE_TOTAL_FREE_BYTES_SIZE

LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_ALLOC_POINTER_SIZE + \
    LEAF_NODE_FREE_LIST_HEAD_POINTER_SIZE + LEAF_NODE_TOTAL_FREE_BYTES_SIZE

# location where cell point start
LEAF_NODE_CELL_POINTER_START = LEAF_NODE_HEADER_SIZE
LEAF_NODE_CELL_POINTER_SIZE = WORD

# cell constants

# NOTE: this is intended to support APIs that expect fixed size key
# this is the older api; likely can be removed once btree older api is pruned
LEAF_NODE_KEY_SIZE = WORD
# NOTE: these are relative to beginning of cell
CELL_KEY_SIZE_OFFSET = 0
# the size of the key-size field
CELL_KEY_SIZE_SIZE = WORD
CELL_DATA_SIZE_OFFSET = CELL_KEY_SIZE_OFFSET + CELL_KEY_SIZE_SIZE
CELL_DATA_SIZE_SIZE = WORD
CELL_KEY_PAYLOAD_OFFSET = CELL_DATA_SIZE_OFFSET + CELL_DATA_SIZE_SIZE
# max cell that can fit on page
LEAF_NODE_MAX_CELL_SIZE = PAGE_SIZE - LEAF_NODE_HEADER_SIZE - LEAF_NODE_CELL_POINTER_SIZE


# limit for debugging
LEAF_NODE_MAX_CELLS = 3


# TODO: most of the leaf_node constants below are unused and should be removed
# commenting out below
"""
# Leaf node body layout
LEAF_NODE_KEY_SIZE = WORD
LEAF_NODE_KEY_OFFSET = 0

# NOTE: nodes should not cross the page boundary; thus ROW_SIZE is upper
# bounded by remaining space in page
# NOTE: ROW_SIZE includes the key
LEAF_NODE_VALUE_SIZE = ROW_SIZE
LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
# LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE

# when a node is split, off number of cells, left will get one more
LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) // 2
LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT
"""


# this initialize the catalog/metadata table
# todo: nuke if unused
INIT_CATALOG_SQL = '''
create table catalog (
        type  text,
        name text,
        tbl_name text,
        rootpage integer,
        sql text
    )
'''


# serde constants
# length of encoded bytes
INTEGER_SIZE = WORD
FLOAT_SIZE = WORD
