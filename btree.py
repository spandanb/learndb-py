from __future__ import annotations
"""
Contains the implementation of the btree
"""
import sys
from enum import Enum, auto

from utils import debug

# NOTE: the following are duplicated from the main module
WORD = 32
ROW_SIZE = 64
PAGE_SIZE = 4096

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
INTERNAL_NODE_MAX_CELLS = 3
# the +1 is for the right child
INTERNAL_NODE_MAX_CHILDREN = INTERNAL_NODE_MAX_CELLS + 1
INTERNAL_NODE_RIGHT_SPLIT_CHILD_COUNT = (INTERNAL_NODE_MAX_CHILDREN + 1) // 2
INTERNAL_NODE_LEFT_SPLIT_CHILD_COUNT = (INTERNAL_NODE_MAX_CHILDREN + 1) - INTERNAL_NODE_RIGHT_SPLIT_CHILD_COUNT

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
# NOTE: ROW_SIZE includes the key
LEAF_NODE_VALUE_SIZE = ROW_SIZE
LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
# LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE
# todo: nuke after testing
LEAF_NODE_MAX_CELLS = 3

# when a node is split, off number of cells, left will get one more
LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) // 2
LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT


class TreeInsertResult(Enum):
    Success = auto()
    DuplicateKey = auto()


class TreeDeleteResult(Enum):
    Success = auto()


class NodeType(Enum):
    NodeInternal =1
    NodeLeaf = 2


class Tree:
    """
    Manages read/writes from/to pages corresponding
    to a specific table. In the future 1) the btree could
    also be used as a secondary index and 2) other data
    structures (e.g. SSTable) could replace Tree.

    The public facing interface consists of `find`, `insert`
    and `delete`, and validators. The remaining methods should not
    be invoked by external actors.

    The tree functionality can be divided into methods that
    operate on page sized `bytes`, e.g. (set_)leaf_node_key. And higher
    level helpers that support find, insert, and delete.
    """

    def __init__(self, pager: 'Pager', root_page_num: int):
        """

        :param pager:
        :param root_page_num: of the table this Tree represents
        """
        self.pager = pager
        self.root_page_num = root_page_num
        self.check_create_leaf_root()

    # section : public interface: find, search, and delete

    def insert(self, key: int, value: bytes) -> TreeInsertResult:
        """
        insert a `key` into the tree

        Algorithm:
            find the key location i.e. leaf page num, and cell num. If key
            at location is same as key to insert, raise/return failure since
            duplicate keys are not supported.

            Check whether the leaf node has capacity to insert the new entry.
            If it does, move all entries, right of insert point, right by 1.
            If it does not, split the leaf into lower (aka left) and upper (right) splits.
            The convention is that after a split, the left is the older node, while
            the right a newly created split.

            Check if the parent, has capacity to add new/right/upper child.
            If it does, add the child and the operation terminates.
            Otherwise, the parent must be split. This splitting op proceeds recursively
            until an ancestor has enough room.

            If we reach the root and it too does not have enough capacity, create a new root.
            NOTE: The root page num must not change. Thus when creating a new root, it must
            occupy the root page, which will require the left split to be copied to a new node.

            Finally, if a the right(-most) child is updated, then it's ancestors
            keys for the children may need to be updated.

        :param key: key to insert
        :param value: value associated with key
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

        :param page_num: start search here
        :param key: key being seeked
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

    def delete(self, key: int):
        """
        delete `key`

        Algorithm:
            find the key, i.e. leaf page num, and cell num
             if the key does not exist, op terminates

            if the key exist, delete the key, i.e. reduce
            num_keys - 1 and move all cells from key position left by 1

            if deleted key was max key, then the parent's key for
             the child will have to be updated. If the child was the parent right
             child, then the parent is unchanged, but the grandparent may
             need to be updated. This chain of updates may go all the way upto the root.

            Next, we check whether the node, and specifically, the node plus it's left
            and right adjacent sibling have total number of elements less than some threshold.
            i.e. L + R + N <= LEAF_NODE_MAX_KEYS * 2 - buffer

            If the parent falls below the threshold, apply restructing recursively. If the
            root needs to be restructured, delete the root, i.e. the tree reduces in height by 1.
            NOTE: when the root is deleted, the tree must be such that the new root is at the same
            root page num.
        """
        # find
        page_num, cell_num = self.find(key)
        node = self.pager.get_page(page_num)
        if self.leaf_node_key(node, cell_num) != key:
            return TreeDeleteResult.Success

        self.leaf_node_delete(page_num, cell_num)
        return TreeDeleteResult.Success

    # section: core logic helpers

    def leaf_node_delete(self, page_num: int, cell_num: int):
        """
        delete key at `cell_num`

        :param self:
        :param page_num:
        :param cell_num:
        :param key:
        :return:
        """
        node = self.pager.get_page(page_num)
        num_cells = Tree.leaf_node_num_cells(node)

        # move cells left by 1
        # TODO: ensure `leaf_node_cells_starting_at` can handle when cell_num + 1 is greater than num_cells
        cells = self.leaf_node_cells_starting_at(node, cell_num + 1)
        self.set_leaf_node_cells_starting_at(node, cell_num, cells)

        # reduce cell count
        self.set_leaf_node_num_cells(node, num_cells - 1)

        # right-most child was deleted; parent key may
        # need to be updated
        if cell_num == num_cells - 1:
            self.check_update_parent_key(page_num)

        # check whether we need to restructure
        # if the current node, and it's left and right siblings
        # have less than

    def restructure_leaf_node(self, page_num: int):
        """
        check whether node at `page_num` and it's left and right siblings
        have a combined number of keys/children below the restructuring threshold.
        If so, the nodes are restructured into
        :param page_num:
        :return:
        """

        node = self.pager.get_page(page_num)
        if self.is_root(node):
            # nothing to do
            return

        parent_page_num = self.get_parent_page_num(node)
        parent = self.pager.get_page(parent_page_num)

        # find node's location in parent
        node_max_key = self.get_node_max_key(node)
        node_child_num = self.internal_node_find(parent_page_num, node_max_key)

        # check if sum of cells in left and right sibling and node
        # is below threshold, if so restructure the nodes.
        total_count = self.leaf_node_num_cells(node)
        if node_child_num > 0:
            # left sibling exists, add number of cells in left to total
            left_page_num = self.internal_node_child(parent, node_child_num - 1)
            left = self.pager.get_page(left_page_num)
            total_count += self.leaf_node_num_cells(left)

        # Todo: complete

    def internal_node_find(self, page_num: int, key: int) -> int:
        """
        implement a binary search to find child location where key should be inserted
        NOTE: this will return a child_pos in range [0, num_children], with index at
        position num_children corresponding to the right child.

        NOTE: If the key exists, then the return index should refer to the exact location
        of the key. If it doesn't then it should refer to the insertion location
        where the key should go. And here, any keys on returned index must be greater.

        NOTE: This will return special value `INTERNAL_NODE_MAX_CELLS` to indicate
        the position of the key is the right child. All callers must handle this.

        :param page_num:
        :param key:
        :return: child_position where key is to be inserted
        """
        node = self.pager.get_page(page_num)
        if self.get_node_max_key(node) <= key:
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

        debug(f"old_child_page_num is {old_child_page_num}, parent_page_num: {parent_page_num}")

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

                # it's possible that the left child num may point to an unoccupied but non-right cell
                if left_child_child_num <= self.internal_node_num_keys(parent) - 1:
                    # NOTE: this is only updating the child key, but the ptr should refer to the same page
                    child_at_left_child_child_num = self.internal_node_child(parent, left_child_child_num)
                    assert child_at_left_child_child_num == old_child_page_num, \
                        f"child_at_left_child_child_num={child_at_left_child_child_num}, old_child_page_num = {old_child_page_num}"
                else:
                    # unoccupied cell- set child ref
                    self.set_internal_node_child(parent, left_child_child_num, old_child_page_num)

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
            self.check_update_parent_key(parent_page_num)

    def internal_node_split_and_insert(self, page_num: int, new_child_page_num: int, old_child_page_num: int):
        """
        Invoked when a new child (internal node, referred to by `new_child_page_num`)
        is to be inserted into an parent (internal node, referred to by `page_num`),
        and the parent node is full. Here the parent node is split, and the
        new child is inserted into the parent.
        """

        parent = self.pager.get_page(page_num)

        # first check if old child node key needs to be updated; page num should be unchanged
        old_child = self.pager.get_page(old_child_page_num)
        old_child_key = self.get_node_max_key(old_child)
        old_child_insert_pos = self.internal_node_find(page_num, old_child_key)
        if old_child_insert_pos < INTERNAL_NODE_MAX_CELLS:
            page_num_at_old_child_insert_pos = self.internal_node_child(parent, old_child_insert_pos)
            # if this assertion fails, that means the old page is not landing quiet where it should
            # perhaps, then something needs to be moved, or there is another error
            assert old_child_page_num == page_num_at_old_child_insert_pos, \
                f"old child page num [{old_child_page_num}] did not match page num at child pos [{page_num_at_old_child_insert_pos}]"
            if self.internal_node_key(parent, old_child_insert_pos) != old_child_key:
                # key has changed, update
                self.set_internal_node_key(parent, old_child_insert_pos, old_child_key)

        # create a new node, i.e. corresponding to the right split of `parent`, i.e. `new_parent`
        new_parent_page_num = self.pager.get_unused_page_num()
        new_parent = self.pager.get_page(new_parent_page_num)
        self.initialize_internal_node(new_parent)
        self.set_node_is_root(new_parent, False)
        self.set_parent_page_num(new_parent, self.get_parent_page_num(parent))

        # `num_keys` is the number of keys stored in body of internal node
        num_keys = self.internal_node_num_keys(parent)
        # `total_keys` contains total number of keys
        # including 1 extra key for the new child
        total_keys = num_keys + 1

        # determine insertion location of `new_child_page_num`, using key (child-node-max-key)
        new_child = self.pager.get_page(new_child_page_num)
        new_child_key = self.get_node_max_key(new_child)
        # NOTE: this find op assumes node has capacity for new child
        # specifically if it returns `INTERNAL_NODE_MAX_CELLS` that may represent either
        # right child insert point or last cell; this must be handled before using `new_child_insert_pos`
        new_child_insert_pos = self.internal_node_find(page_num, new_child_key)

        if new_child_insert_pos == INTERNAL_NODE_MAX_CELLS and new_child_key > self.get_node_max_key(parent):
            # if `new_child_insert_pos`value is INTERNAL_NODE_MAX_CELLS
            # it's post split position is ambiguous since it could refer
            # to last inner child or right child
            new_child_insert_pos += 1

        # divide keys evenly between old (left child) and new (right child)
        split_left_count = 0
        split_right_count = 0
        # `shifted_index` iterates over [num_keys+1,...0]; can be conceptualized
        # as index of each child as if the internal node could hold new child
        for shifted_index in range(total_keys, -1, -1):

            # determine destination node from the shifted position
            if shifted_index >= INTERNAL_NODE_LEFT_SPLIT_CHILD_COUNT:
                # NOTE: `new_parent` is the split of parent
                dest_node = new_parent
                dest_page_num = new_parent_page_num
                split_right_count += 1
            else:
                dest_node = parent
                dest_page_num = page_num
                split_left_count += 1

            # determine location for cell on it's respective node, after partition operation
            post_partition_index = shifted_index % INTERNAL_NODE_LEFT_SPLIT_CHILD_COUNT

            # we're iterating from higher to lower keyed children,
            # so if this is the first child in the given split, it must be the right child
            is_right_child_after_split = split_left_count == 1 or split_right_count == 1

            # debugging info
            dest_name = "old/left" if dest_node == parent else "new/right"

            # insert new child
            if shifted_index == new_child_insert_pos:
                # set new node's parent: i.e. parent or new_parent
                self.set_parent_page_num(new_child, dest_page_num)

                debug(f"In loop shifted_index: {shifted_index} post-part-idx: {post_partition_index}"
                      f" inserting new child into {dest_name} at {new_child_insert_pos}; key: {new_child_key}: "
                      f" is_right_after_split: {is_right_child_after_split}, new_child_page_num: {new_child_page_num}")
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
            if shifted_index > new_child_insert_pos:
                current_idx -= 1

            is_current_right_child = current_idx == num_keys

            debug(f"In loop shifted_index: {shifted_index} post-part-idx: {post_partition_index}"
                  f" inserting existing child into {dest_name} is_right_after_split: {is_right_child_after_split}"
                  f" is_current_right_child: {is_current_right_child}")

            # copy existing child cell to new location
            if is_current_right_child and is_right_child_after_split:
                # current right child, remains a right child
                current_child_page_num = self.internal_node_right_child(parent)
                self.set_internal_node_right_child(dest_node, current_child_page_num)
                debug(f"In loop; right to right; current_child_page_num: {current_child_page_num}")
            elif is_current_right_child:
                # current right child, becomes an internal cell
                # lookup key from right child page num
                current_child_page_num = self.internal_node_right_child(parent)
                current_child = self.pager.get_page(current_child_page_num)
                current_child_key = self.get_node_max_key(current_child)
                self.set_internal_node_key(dest_node, post_partition_index, current_child_key)
                self.set_internal_node_child(dest_node, post_partition_index, current_child_page_num)
                debug(f"In loop; is_current_right; current_child_page_num: {current_child_page_num}, "
                      f"current_child_key: {current_child_key} ")
            elif is_right_child_after_split:
                # internal cell becomes right child
                current_child_page_num = self.internal_node_child(parent, current_idx)
                self.set_internal_node_right_child(dest_node, current_child_page_num)
                debug(f"In loop; to_right; current_child_page_num: {current_child_page_num}")
            else:  # never a right child
                assert current_idx < num_keys, \
                    f"Internal node set cell location [{current_idx}] must be less than num_cells [{num_keys}]"

                cell_to_copy = self.internal_node_cell(parent, current_idx)
                self.set_internal_node_cell(dest_node, post_partition_index, cell_to_copy)
                debug(f"non-right to non-right")
            print(" ")

        # set left and right split counts
        # -1 since the number of keys excludes right child's key
        self.set_internal_node_num_keys(parent, split_left_count - 1)
        self.set_internal_node_num_keys(new_parent, split_right_count - 1)

        # some children got moved to the new/right node; update that nodes' children
        self.check_update_parent_ref_in_children(new_parent_page_num)

        # for testing
        debug("In internal_node_split_and_insert")
        debug("print old/left internal node after split")
        # self.print_internal_node(parent, recurse=False)
        debug("print new/right internal node after split")
        # self.print_internal_node(new_parent, recurse=False)

        # update parent
        if self.is_node_root(parent):
            self.create_new_root(new_parent_page_num)
        else:
            self.internal_node_insert(page_num, new_parent_page_num)

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

        # new key was inserted at largest index, i.e. new max-key - update parent
        if cell_num == num_cells:
            # update the parent's key
            self.check_update_parent_key(page_num)

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
        # get parent page num from left child
        self.set_parent_page_num(new_node, self.get_parent_page_num(old_node))

        # keys must be divided between old (left child) and new (right child)
        # start from right, move each key (cell) to correct location

        # `shifted_cell_num` iterates from [n..0], i.e. an entry for all existing cells
        # and the new cell to be inserted
        for shifted_cell_num in range(LEAF_NODE_MAX_CELLS, -1, -1):

            # determine destination node from it's shifted position
            if shifted_cell_num >= LEAF_NODE_LEFT_SPLIT_COUNT:
                dest_node = new_node
            else:
                dest_node = old_node

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

            # key_to_move = self.leaf_node_key(old_node, current_cell_num)
            # node_name = "old_node" if dest_node is old_node else "new_node"
            # print(f"key_to_move: {key_to_move} from cell_idx: {current_cell_num} to [{node_name}, {new_cell_num}]")

        # update counts on child nodes
        self.set_leaf_node_num_cells(old_node, LEAF_NODE_LEFT_SPLIT_COUNT)
        self.set_leaf_node_num_cells(new_node, LEAF_NODE_RIGHT_SPLIT_COUNT)

        debug("printing old leaf node after split-insert")
        # self.print_leaf_node(old_node)
        debug("printing new leaf node after split-insert")
        # self.print_leaf_node(new_node)

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
        # root points to itself
        self.set_parent_page_num(root, self.root_page_num)
        # update childrens' parent reference
        self.set_parent_page_num(left_child, self.root_page_num)
        self.set_parent_page_num(right_child, self.root_page_num)

        self.set_internal_node_num_keys(root, 1)
        self.set_internal_node_child(root, 0, left_child_page_num)
        left_child_max_key = self.get_node_max_key(left_child)
        self.set_internal_node_key(root, 0, left_child_max_key)
        self.set_internal_node_right_child(root, right_child_page_num)

        # update all of left node's children to refer to new left page
        self.check_update_parent_ref_in_children(left_child_page_num)

    def check_update_parent_ref_in_children(self, node_page_num: int):
        """
        invoked when node is moved to `node_page_num`, e.g. on new root
        after split. Check if node was internal node, if so, update
        it's childrens' parent pointer to new page num
        """
        node = self.pager.get_page(node_page_num)
        if self.get_node_type(node) == NodeType.NodeInternal:
            for child_num in range(self.internal_node_num_keys(node)):
                child = self.pager.get_page(self.internal_node_child(node, child_num))
                self.set_parent_page_num(child, node_page_num)

            right_child = self.pager.get_page(self.internal_node_right_child(node))
            self.set_parent_page_num(right_child, node_page_num)

    def check_update_parent_key(self, node_page_num: int):
        """
        Checks whether the parent of the node (`node_page_num`) has a different key, than
        the right-most value of node. If it does, it updates it to the correct value.
        This should be invoked when the right-most child of (either internal or leaf) node
        is updated. If parent's key does not need to be updated, this is a no-op. However,
        unnecessary calls should be avoided, as it is expensive.

        :param self:
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
        # NOTE: the node position in parent should be unchanged after
        # insertion/deletion of key, i.e. which may have changed the `node_max_key`
        # since the added\removed key's value is still be bounded by the prev
        # and subsequent key values in parent
        node_child_num = self.internal_node_find(parent_page_num, node_max_key)
        if node_child_num == INTERNAL_NODE_MAX_CELLS:
            # the node is parent's right child; thus parent is not
            # updated; but it's grandparent might need to.
            # this can be conceptually viewed as this parent potentially receiving a new child
            # since get internal node max child recursively gets the rightmost leaf descendant
            self.check_update_parent_key(parent_page_num)
        else:
            # node is a non-right child of it's parent
            old_child_key = self.internal_node_key(parent, node_child_num)
            if old_child_key != node_max_key:
                # we indeed have a new max key
                # update the parent node
                self.set_internal_node_key(parent, node_child_num, node_max_key)
                # as this is a non-right node, the operation
                # terminates here

    # section: initialization helpers

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
            # root is own parent
            self.set_parent_page_num(root_node, self.root_page_num)

    @staticmethod
    def initialize_internal_node(node: bytes):
        Tree.set_node_type(node, NodeType.NodeInternal)
        Tree.set_internal_node_num_keys(node, 0)

    @staticmethod
    def initialize_leaf_node(node: bytes):
        Tree.set_node_type(node, NodeType.NodeLeaf)
        Tree.set_leaf_node_num_cells(node, 0)

    # section: utility methods

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
        # root of invocation; not necessarily global root
        root = self.pager.get_page(root_page_num)
        parent_num = "NOPAR" if self.is_node_root(root) else self.get_parent_page_num(root)
        if self.get_node_type(root) == NodeType.NodeLeaf:
            body = f"printing leaf node at page num: [{root_page_num}]. parent: [{parent_num}]"
            divider = f"{indent}{len(body) * '.'}"
            print(f"{indent}{body}")
            print(divider)
            self.print_leaf_node(root, depth = depth)
            print(divider)
        else:
            body = f"printing internal node at page num: [{root_page_num}]. parent: [{parent_num}]"
            divider = f"{indent}{len(body) * '.'}"
            print(f"{indent}{body}")
            print(divider)
            self.print_internal_node(root, recurse = True, depth = depth)
            print(divider)

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

    def validate(self):
        """
        invoke all sub-validators
        :return:
        """
        self.validate_ordering()
        self.validate_parent_keys()

    def validate_parent_keys(self) -> bool:
        """
        validate:
            1) that each parent key value (referring to child) is max value in right child
            2) child contains correct parent page ref
        """
        stack = [self.root_page_num]
        while stack:
            node_page_num = stack.pop()
            node = self.pager.get_page(node_page_num)
            if self.get_node_type(node) == NodeType.NodeInternal:
                for child_num in range(self.internal_node_num_keys(node)):
                    child_key = self.internal_node_key(node, child_num)
                    child_page_num = self.internal_node_child(node, child_num)
                    child_node = self.pager.get_page(child_page_num)
                    child_max_key = self.get_node_max_key(child_node)
                    #  validate that each parent key value (referring to child) is max value in right child
                    assert child_key == child_max_key, \
                        f"Expected at pos [{child_num}] key [{child_key}]; child-max-key: {child_max_key}; parent_page_num: " \
                        f"{node_page_num}, child_page_num: {child_page_num}"

                    # validate that child's ref to parent page num is correct
                    child_parent_ref = self.get_parent_page_num(child_node)
                    assert child_parent_ref == node_page_num, \
                        f"child ref to parent [{child_parent_ref}], does not match parent page num: [{node_page_num}] " \
                        f"child page num is {child_page_num}"

    def validate_ordering(self) -> bool:
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
                # print(f"validating internal node on page_num: {node_page_num}")
                # self.print_internal_node(node, recurse=False)

                # validate inner keys are ordered
                for child_num in range(self.internal_node_num_keys(node)):
                    key = self.internal_node_key(node, child_num)
                    assert lower_bound < key, f"validation: global lower bound [{lower_bound}] constraint violated [{key}]"
                    assert upper_bound >= key, f"validation: global upper bound [{upper_bound}] constraint violated [{key}]"

                    if child_num > 0:
                        prev_key = self.internal_node_key(node, child_num - 1)
                        # validation: check if all of node's key are ordered
                        assert key > prev_key, f"validation: internal node siblings must be strictly greater key: {key}. " \
                                               f"prev_key:{prev_key}"

                    # add children to stack
                    child_page_num = self.internal_node_child(node, child_num)
                    # lower bound is prev child for non-zero child, and parent's lower bound for 0-child
                    child_lower_bound = self.internal_node_key(node, child_num - 1) if child_num > 0 else lower_bound
                    # upper bound is key value for non-right children
                    child_upper_bound = self.internal_node_key(node, child_num)
                    stack.append((child_page_num, child_lower_bound, child_upper_bound))

                # validate right is the max key
                inner_max_key = self.internal_node_key(node, self.internal_node_num_keys(node)-1)
                right_key = self.get_node_max_key(self.pager.get_page(self.internal_node_right_child(node)))
                assert inner_max_key < right_key, f"Expected right child key [{right_key}] to be strictly greater than max-inner-key: {inner_max_key}"

                # add right child
                child_page_num = self.internal_node_right_child(node)
                # lower bound is last key
                child_lower_bound = self.internal_node_key(node, self.internal_node_num_keys(node) - 1)
                stack.append((child_page_num, child_lower_bound, upper_bound))

            else:  # leaf node
                # print(f"validating leaf node on page_num: {node_page_num}")
                # self.print_leaf_node(node)
                for cell_num in range(self.leaf_node_num_cells(node)):
                    if cell_num > 0:
                        key = self.leaf_node_key(node, cell_num)
                        prev_key = self.leaf_node_key(node, cell_num - 1)
                        # validation: check if all of node's key are ordered
                        assert key > prev_key, "validation: leaf node siblings must be strictly greater"

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
        assert key_num != INTERNAL_NODE_MAX_CELLS
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
    def set_parent_page_num(node: bytes, page_num: int):
        value = page_num.to_bytes(PARENT_POINTER_SIZE, sys.byteorder)
        node[PARENT_POINTER_OFFSET: PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE] = value

    @staticmethod
    def set_node_is_root(node: bytes, is_root: bool):
        value = is_root.to_bytes(IS_ROOT_SIZE, sys.byteorder)
        node[IS_ROOT_OFFSET: IS_ROOT_OFFSET + IS_ROOT_SIZE] = value

    @staticmethod
    def set_node_type(node: bytes, node_type: NodeType):
        bits = node_type.value.to_bytes(NODE_TYPE_SIZE, sys.byteorder)
        node[NODE_TYPE_OFFSET: NODE_TYPE_OFFSET + NODE_TYPE_SIZE] = bits

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

