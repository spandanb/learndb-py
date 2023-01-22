# operational constants
EXIT_SUCCESS = 0
EXIT_FAILURE = 1

DB_FILE = 'db.file'
# TODO: nuke here
#TEST_DB_FILE = 'testdb.file'

# storage constants
# NOTE: storage and btree constants that affect how the
# db file is written, should not be changed once a db file is created.
PAGE_SIZE = 4096
WORD = 4

# file header constants
FILE_HEADER_OFFSET = 0
FILE_HEADER_SIZE = 100
FILE_PAGE_AREA_OFFSET = FILE_HEADER_SIZE
FILE_HEADER_VERSION_FIELD_OFFSET = 0
FILE_HEADER_VERSION_FIELD_SIZE = 16
# NOTE: The diff between size and len(FILE_HEADER_VERSION_VALUE) should be padding
FILE_HEADER_VERSION_VALUE = b'learndb v1'
FILE_HEADER_NEXT_FREE_PAGE_HEAD_OFFSET = FILE_HEADER_VERSION_FIELD_OFFSET + FILE_HEADER_VERSION_FIELD_SIZE
FILE_HEADER_NEXT_FREE_PAGE_HEAD_SIZE = WORD
FILE_HEADER_PADDING = FILE_HEADER_SIZE - FILE_HEADER_VERSION_FIELD_SIZE - FILE_HEADER_NEXT_FREE_PAGE_HEAD_SIZE
# pager constants
FREE_PAGE_NEXT_FREE_PAGE_HEAD_OFFSET = 0
FREE_PAGE_NEXT_FREE_PAGE_HEAD_SIZE = WORD

# btree constants
TABLE_MAX_PAGES = 100

# represents a null value in header
NULLPTR = 0

# serialized data layout (tree nodes)
# common node header layout
NODE_TYPE_SIZE = WORD
NODE_TYPE_OFFSET = 0
IS_ROOT_SIZE = WORD
IS_ROOT_OFFSET = NODE_TYPE_SIZE
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

# NOTE: this is limited for debugging/dev
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
# num_cells .. alloc_ptr .. free_list_head_ptr .. total_free_list_space
# cellptr_0 .. cellptr_1 ... cellptr_N-1
LEAF_NODE_NUM_CELLS_SIZE = WORD
LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE
LEAF_NODE_ALLOC_POINTER_SIZE = WORD
LEAF_NODE_ALLOC_POINTER_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_ALLOC_POINTER_SIZE
LEAF_NODE_FREE_LIST_HEAD_POINTER_SIZE = WORD
LEAF_NODE_FREE_LIST_HEAD_POINTER_OFFSET = LEAF_NODE_ALLOC_POINTER_OFFSET + LEAF_NODE_FREE_LIST_HEAD_POINTER_SIZE
LEAF_NODE_TOTAL_FREE_LIST_SPACE_SIZE = WORD
LEAF_NODE_TOTAL_FREE_LIST_SPACE_OFFSET = LEAF_NODE_FREE_LIST_HEAD_POINTER_OFFSET + LEAF_NODE_TOTAL_FREE_LIST_SPACE_SIZE

LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_ALLOC_POINTER_SIZE + \
    LEAF_NODE_FREE_LIST_HEAD_POINTER_SIZE + LEAF_NODE_TOTAL_FREE_LIST_SPACE_SIZE

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
# space excluding headers, i.e. only space for cells and cellptr
LEAF_NODE_NON_HEADER_SPACE = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
# max cell that can fit on page is non-header space and 1 cell ptr
LEAF_NODE_MAX_CELL_SIZE = LEAF_NODE_NON_HEADER_SPACE - LEAF_NODE_CELL_POINTER_SIZE

# free-block constants
# NOTE: these are relative to start of a free block
FREE_BLOCK_SIZE_SIZE = WORD
FREE_BLOCK_SIZE_OFFSET = 0
FREE_BLOCK_NEXT_BLOCK_SIZE = WORD
FREE_BLOCK_NEXT_BLOCK_OFFSET = FREE_BLOCK_SIZE_OFFSET + FREE_BLOCK_SIZE_SIZE
FREE_BLOCK_HEADER_SIZE = FREE_BLOCK_SIZE_SIZE + FREE_BLOCK_NEXT_BLOCK_SIZE

# NOTE: this is limited for debugging/dev
LEAF_NODE_MAX_CELLS = 3


# serde constants
# length of encoded bytes
INTEGER_SIZE = WORD
REAL_SIZE = WORD
# when real numbers are stored there is some rounding error
# hence two real numbers where the abs difference is less than `REAL_EPSILON`, are considered equal
# NOTE: I just ballparked this epsilon; in actuality the diff will likely depend on the absolute
# value of the real number
REAL_EPSILON = 0.00001

# Higher-level constanst
# name of catalog
CATALOG = 'catalog'
CATALOG_ROOT_PAGE_NUM = 0

USAGE = '''
Supported meta-commands:
------------------------
print usage
.help

quit REPl
> .quit

print btree for table <table-name>
> .btree <table-name>

performs internal consistency checks on table <table-name>
> .validate <table-name>

Supported commands:
-------------------
The following lists supported commands, and an example. For a complete grammar see docs/sql-lang.txt

Create table
> create table customers ( cust_id integer primary key, cust_name text, cust_height float)

Insert records
> insert into customers ( cust_id, cust_name, cust_height) values (1, 'Bob Maharaj', 162.5 )

Select some rows, only supports equality predicate
> select cust_name, cust_height from customers

Delete (only single equality predicate supported)
> delete from customers where cust_name = "Bob Maharaj"
'''


