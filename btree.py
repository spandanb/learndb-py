from __future__ import annotations
"""
Contains the implementation of the btree
"""
import sys

from collections import namedtuple, deque
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, List
from itertools import chain

from serde import get_cell_key, get_cell_key_in_page, get_cell_size
from utils import debug

from constants import (WORD,
                       # ROW_SIZE,
                       NULLPTR,
                       PAGE_SIZE,
                       # common
                       NODE_TYPE_SIZE,
                       NODE_TYPE_OFFSET,
                       IS_ROOT_SIZE,
                       IS_ROOT_OFFSET,
                       PARENT_POINTER_SIZE,
                       PARENT_POINTER_OFFSET,
                       COMMON_NODE_HEADER_SIZE,
                       # internal node
                       INTERNAL_NODE_NUM_KEYS_SIZE,
                       INTERNAL_NODE_NUM_KEYS_OFFSET,
                       INTERNAL_NODE_RIGHT_CHILD_SIZE,
                       INTERNAL_NODE_RIGHT_CHILD_OFFSET,
                       INTERNAL_NODE_HAS_RIGHT_CHILD_SIZE,
                       INTERNAL_NODE_HAS_RIGHT_CHILD_OFFSET,
                       INTERNAL_NODE_HEADER_SIZE,
                       INTERNAL_NODE_KEY_SIZE,
                       INTERNAL_NODE_CHILD_SIZE,
                       INTERNAL_NODE_CELL_SIZE,
                       INTERNAL_NODE_SPACE_FOR_CELLS,
                       INTERNAL_NODE_MAX_CELLS,
                       INTERNAL_NODE_MAX_CHILDREN,
                       INTERNAL_NODE_RIGHT_SPLIT_CHILD_COUNT,
                       INTERNAL_NODE_LEFT_SPLIT_CHILD_COUNT,
                       # leaf node header layout
                       LEAF_NODE_NUM_CELLS_SIZE,
                       LEAF_NODE_NUM_CELLS_OFFSET,
                       LEAF_NODE_HEADER_SIZE,
                       LEAF_NODE_KEY_SIZE,

                       LEAF_NODE_MAX_CELL_SIZE,
                       LEAF_NODE_MAX_CELLS,  # for debugging
                       # todo: nuke these
                       #LEAF_NODE_KEY_OFFSET,
                       #LEAF_NODE_VALUE_SIZE,
                       #LEAF_NODE_VALUE_OFFSET,
                       #LEAF_NODE_CELL_SIZE,
                       #LEAF_NODE_SPACE_FOR_CELLS,
                       #LEAF_NODE_RIGHT_SPLIT_COUNT,
                       #LEAF_NODE_LEFT_SPLIT_COUNT,

                       # below are newly defined consts
                       LEAF_NODE_CELL_POINTER_START,
                       LEAF_NODE_CELL_POINTER_SIZE,

                       LEAF_NODE_ALLOC_POINTER_OFFSET,
                       LEAF_NODE_ALLOC_POINTER_SIZE,
                       LEAF_NODE_FREE_LIST_HEAD_POINTER_SIZE ,
                       LEAF_NODE_FREE_LIST_HEAD_POINTER_OFFSET,
                       LEAF_NODE_TOTAL_FREE_LIST_SPACE_SIZE,
                       LEAF_NODE_TOTAL_FREE_LIST_SPACE_OFFSET,

                       CELL_KEY_SIZE_OFFSET,
                       CELL_KEY_SIZE_SIZE,
                       CELL_KEY_PAYLOAD_OFFSET,

                       FREE_BLOCK_SIZE_SIZE,
                       FREE_BLOCK_SIZE_OFFSET,
                       FREE_BLOCK_NEXT_BLOCK_SIZE,
                       FREE_BLOCK_NEXT_BLOCK_OFFSET,
                       FREE_BLOCK_HEADER_SIZE
                       )


class TreeInsertResult(Enum):
    Success = auto()
    DuplicateKey = auto()


class TreeDeleteResult(Enum):
    Success = auto()


class NodeType(Enum):
    NodeInternal = 1
    NodeLeaf = 2


@dataclass
class NodeInfo:
    page_num: int
    parent_pos: int  # parent's position of this node
    # give these default values; since these depend on type
    # of node and hence may need to be populated after `page_num` and `parent_pos`
    # are determined
    node: bytearray = None
    count: int = 0


class Tree:
    """
    Manages read/writes from/to pages corresponding
    to a specific table/index.

    NOTE: There is a one btree per table- or more generally,
    per one materialized index. The root tree, will hold
    the mapping of table_name -> btree_root_page_num. However,
    the tree is agnostic to how the data is interpreted, and
    is only interested in its interface, which operates on sorted
    bytes strings.

    The public interface consists of `find`, `insert`
    and `delete`, and validators. The remaining methods should not
    be invoked by external actors. In principle, any other structure,
    e.g. SSTable, implementing this interface, could replace this.

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

    # section : public interface: find, insert, and delete
    # NB: the helper methods are clustered along these 3 methods

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

    def insert(self, cell: bytes) -> TreeInsertResult:
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

        :param cell: cell to insert (contains key)
        :return:
        """
        key = get_cell_key(cell)
        page_num, cell_num = self.find(key)
        node = self.pager.get_page(page_num)
        if self.leaf_node_key(node, cell_num) == key:
            return TreeInsertResult.DuplicateKey

        self.leaf_node_insert(page_num, cell_num, key, cell)
        return TreeInsertResult.Success

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

    # section: logic helpers - find

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
        num_cells = self.internal_node_num_keys(node)
        node_max_key = self.get_node_max_key(node)

        # node_max_key None is ambiguous; could mean that either parent is
        # empty of right child is empty
        node_empty = False
        node_right_child_empty = False
        if node_max_key is None:
            # node is empty if it has no right child
            node_empty = not self.internal_node_has_right_child(node)
            # node's right child must empty if node is not empty
            node_right_child_empty = not node_empty

        # handle corner cases
        # node is empty - new child goes at position 0
        if node_empty:
            return 0
        elif node_right_child_empty:
            # not entirely sure about this; should child go to greatest
            # inner or right child
            return INTERNAL_NODE_MAX_CELLS
        elif node_max_key <= key:
            # key corresponds to right child
            return INTERNAL_NODE_MAX_CELLS
        elif num_cells == 0:
            # node is unary- it's single child is right child
            # current key is less than right node's key; hence
            # it's the first inner child
            return 0

        # do a binary search
        left_closed_index = 0
        right_open_index = self.internal_node_num_keys(node)
        while left_closed_index != right_open_index:
            index = left_closed_index + (right_open_index - left_closed_index ) // 2
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
            index = left_closed_index + (right_open_index - left_closed_index) // 2
            key_at_index = self.leaf_node_key(node, index)
            if key == key_at_index:
                # key found
                return index
            if key < key_at_index:
                right_open_index = index
            else:
                left_closed_index = index + 1

        return left_closed_index

    # section: logic helpers - insert

    def internal_node_insert(self, old_child_page_num: int, left_child_page_num: int,
                             right_child_page_num: int, middle_child_page_num: Optional[int] = None):
        """
        Invoked after a child node (`old_child_page_num`) is split into left_ right_ and optionally
        middle_child_page_num. Add new children to old child's parent and remove old child.

        To find the insertion location, find the location of old_child in parent.
        - If old_child was right child, right split becomes new right child,
        and left and middle are added to tail of existing children.
        - If old child was an inner child, then start by adding the left child in old's spot.
        Then move any siblings to the right over, more to the right.
        Insert middle and right to the right of that.

        ---

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

        :param old_child_page_num: child that was split
        :param new_child_page_num: remove this
        :param left_child_page_num: left split
        :param right_child_page_num: right split
        :param middle_child_page_num: optional middle split
        """
        # old child is the child that was split in left, middle and right child
        old_child = self.pager.get_page(old_child_page_num)
        parent_page_num = self.get_parent_page_num(old_child)
        parent = self.pager.get_page(parent_page_num)

        num_keys = self.internal_node_num_keys(parent)
        # number of nodes to add; one node is already included
        num_new_nodes = 1 if middle_child_page_num is None else 2
        if num_keys + num_new_nodes > INTERNAL_NODE_MAX_CELLS:
            # todo: update internal_node_split_and_insert
            raise Exception("Inner node split not implemented")
            self.internal_node_split_and_insert(parent_page_num, old_child_page_num, left_child_page_num,
                                                right_child_page_num, middle_child_page_num)
            return

        # get children
        old_node = self.pager.get_page(old_child_page_num)
        left_child = self.pager.get_page(left_child_page_num)
        middle_child = None
        right_child = self.pager.get_page(right_child_page_num)

        old_child_max_key = self.get_node_max_key(old_node)
        left_child_max_key = self.get_node_max_key(left_child)
        middle_child_max_key = None
        right_child_max_key = self.get_node_max_key(right_child)
        right_child_updated = False

        if middle_child_page_num:
            middle_child = self.pager.get_page(middle_child_page_num)
            middle_child_max_key = self.get_node_max_key(middle_child)

        # determine old_node's location
        old_child_num = self.internal_node_find(parent_page_num, old_child_max_key)
        if old_child_num == INTERNAL_NODE_MAX_CELLS:
            # old child is the right child, the splits must all be right of all other children
            # set right split as new right child
            self.set_internal_node_right_child(parent, right_child_page_num)
            right_child_updated = True

            # insert left child at tail of existing children
            self.set_internal_node_child(parent, num_keys, left_child_page_num)
            self.set_internal_node_key(parent, num_keys, left_child_max_key)
            num_keys += 1

            if middle_child_page_num:
                self.set_internal_node_child(parent, num_keys, middle_child_page_num)
                self.set_internal_node_key(parent, num_keys, middle_child_max_key)

        else:
            # old node was inner child; insert left most split at its location
            # all splits must be inner children
            self.set_internal_node_key(parent, old_child_num, left_child_max_key)
            self.set_internal_node_child(parent, old_child_num, left_child_page_num)

            # to insert other splits, move siblings over
            sibling_src_num = old_child_num + 1
            sibling_dest_num = sibling_src_num + (2 if middle_child_page_num else 1)
            children = self.internal_node_children_starting_at(parent, sibling_src_num)
            self.set_internal_node_children_starting_at(parent, children, sibling_dest_num)

            # insert middle
            next_child_num = old_child_num + 1
            if middle_child_page_num:
                self.set_internal_node_key(parent, next_child_num, middle_child_max_key)
                self.set_internal_node_child(parent, next_child_num, middle_child_page_num)
                next_child_num += 1
                num_keys += 1

            # insert right
            self.set_internal_node_key(parent, next_child_num, right_child_max_key)
            self.set_internal_node_child(parent, next_child_num, right_child_page_num)
            num_keys += 1

        self.set_internal_node_num_keys(parent, num_keys)

        # recycle old node
        self.pager.return_page(old_child_page_num)

        # right child was split; but we don't know whether a new max key was inserted
        if right_child_updated:
            self.check_update_parent_key(parent_page_num)

    def internal_node_split_and_insert(self, old_child_page_num: int, left_child_page_num: int,
                                       right_child_page_num: int, middle_child_page_num: Optional[int] = None):
        """
        Find where the old child is located in the parent. The left, middle, and
        right splits will go where the old child was. Additionally, all nodes must
        be

        :param old_child_page_num:
        :param left_child_page_num:
        :param right_child_page_num:
        :param middle_child_page_num:
        :return:
        """
        old_child = self.pager.get_page(old_child_page_num)
        parent_page_num = self.get_parent_page_num(old_child)
        # the `parent` node will be split into left and right parents
        left_parent = self.pager.get_page(parent_page_num)

        # create a new node for the right split of `parent`
        right_parent_page_num = self.pager.get_unused_page_num()
        right_parent = self.pager.get_page(right_parent_page_num)
        self.initialize_internal_node(right_parent, node_is_root=False)
        grandparent_page_num = self.get_parent_page_num(left_parent)
        self.set_parent_page_num(right_parent, grandparent_page_num)

        old_child_max_key = self.get_node_max_key(old_child)
        left_child = self.pager.get_page(left_child_page_num)
        middle_child = None
        right_child = self.pager.get_page(right_child_page_num)

        # the new children will go where old child was
        old_child_num = self.internal_node_find(parent_page_num, old_child_max_key)
        if old_child_num == INTERNAL_NODE_MAX_CELLS:
            pass

        # todo: recycle old_child_num

        # determine how many total children to distribute across splits
        num_keys = Tree.internal_node_num_keys(left_parent)
        total_children = num_keys + (1 if middle_child_page_num is None else 2)
        right_split_count = total_children // 2
        left_split_count = total_children - right_split_count

        # Todo: iterate backwards so I can do this in-place
        # actually, doing it in-place break the API?
        child_num = 0
        # distribute children on left_parent and three splits
        # evenly onto left_ and right_parent.
        while child_num < num_keys:
            # get child_num
            # set child on node if needed
            if child_num == old_child_num:
                # this is where new nodes go

            if current_split_count > left_split_count:
                # switch to current_node to right_split_count
                pass

            # ensure both splits have right child set

        # handle old right child



        # update parent
        if self.is_node_root(left_parent):
            self.create_new_root(parent_page_num, right_parent_page_num)
        else:
            self.internal_node_insert(parent_page_num, right_parent_page_num)

    def internal_node_split_and_insert_old(self, page_num: int, new_child_page_num: int, old_child_page_num: int):
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

    @staticmethod
    def leaf_node_alloc_ptr(node: bytes) -> int:
        """
        :param node:
        :return: the value of the alloc ptr, i.e. the abs offset of the first byte that can be allocated
        """
        bstring = node[LEAF_NODE_ALLOC_POINTER_OFFSET: LEAF_NODE_ALLOC_POINTER_OFFSET + LEAF_NODE_ALLOC_POINTER_SIZE]
        return int.from_bytes(bstring, sys.byteorder)

    @staticmethod
    def leaf_node_unallocated_offset(node: bytes) -> int:
        """
        return first bytes of unallocated space- i.e. first byte past the cell ptr array
        :param node:
        :return:
        """
        array_size = Tree.leaf_node_num_cells(node) * LEAF_NODE_CELL_POINTER_SIZE
        offset = LEAF_NODE_HEADER_SIZE + array_size
        return offset

    @staticmethod
    def leaf_node_alloc_block_space(node: bytes) -> int:
        """
        space (bytes) available on alloc block

        TODO: move static methods with rest of static methods

        :param node:
        :return:
        """
        # space available is from tail of cell ptr array to alloc_ptr
        unalloc_head = Tree.leaf_node_unallocated_offset(node)
        alloc_ptr = Tree.leaf_node_alloc_ptr(node)
        space_available = alloc_ptr - unalloc_head
        return space_available

    @staticmethod
    def leaf_node_total_free_list_space(node: bytes) -> int:
        """
        total/combined space in free list
        :param node:
        :return:
        """
        binvalue = node[LEAF_NODE_TOTAL_FREE_LIST_SPACE_OFFSET:
                        LEAF_NODE_TOTAL_FREE_LIST_SPACE_OFFSET + LEAF_NODE_TOTAL_FREE_LIST_SPACE_SIZE]
        return int.from_bytes(binvalue, sys.byteorder)

    @staticmethod
    def set_leaf_node_total_free_list_space(node: bytes, total_free_space: int) -> int:
        value = total_free_space.to_bytes(LEAF_NODE_TOTAL_FREE_LIST_SPACE_SIZE, sys.byteorder)
        node[LEAF_NODE_TOTAL_FREE_LIST_SPACE_OFFSET:
             LEAF_NODE_TOTAL_FREE_LIST_SPACE_OFFSET + LEAF_NODE_TOTAL_FREE_LIST_SPACE_SIZE] = value

    @staticmethod
    def leaf_node_free_list_head(node: bytes) -> int:
        """
        return head of free list. May be null, i.e. 0 if
        list is empty.
        :param node:
        :return:
        """
        binval = node[LEAF_NODE_FREE_LIST_HEAD_POINTER_OFFSET:
                      LEAF_NODE_FREE_LIST_HEAD_POINTER_OFFSET + LEAF_NODE_FREE_LIST_HEAD_POINTER_SIZE]
        return int.from_bytes(binval, sys.byteorder)

    @staticmethod
    def set_leaf_node_free_list_head(node: bytes, head: int):
        value = head.to_bytes(LEAF_NODE_FREE_LIST_HEAD_POINTER_SIZE, sys.byteorder)
        node[LEAF_NODE_FREE_LIST_HEAD_POINTER_OFFSET:
             LEAF_NODE_FREE_LIST_HEAD_POINTER_OFFSET + LEAF_NODE_FREE_LIST_HEAD_POINTER_SIZE] = value

    @staticmethod
    def free_block_size(node: bytes, free_block_offset: int) -> int:
        """

        todo: these should be grouped with other static accessors

        get size of free block pointed to by `free_block_offset`
        :param node:
        :param free_block_offset:
        :return:
        """
        offset = free_block_offset + FREE_BLOCK_SIZE_OFFSET
        binval = node[offset: offset + FREE_BLOCK_SIZE_SIZE]
        return int.from_bytes(binval, sys.byteorder)

    @staticmethod
    def free_block_next_free(node: bytes, free_block_offset: int) -> int:
        """
        get next free block pointed to by block at `free_block_offset`
        """
        offset = free_block_offset + FREE_BLOCK_NEXT_BLOCK_OFFSET
        binval = node[offset: offset + FREE_BLOCK_NEXT_BLOCK_SIZE]
        return int.from_bytes(binval, sys.byteorder)

    @staticmethod
    def find_free_block(node: bytes, space_needed: int):
        """
        find first free block in free list that is at least as big as `space_needed`

        :param node:
        :param space_needed:
        :return: (bool, int, int): (free_block_found, prev_block, next_block)
            prev_ and next_block are int offsets
        """
        # start at head (offset to first free block)
        head = Tree.leaf_node_free_list_head(node)
        prev_block = NULLPTR
        while head != NULLPTR:
            # check current block size
            block_size = Tree.free_block_size(node, head)
            next_block = Tree.free_block_next_free(node, head)
            if block_size >= space_needed:
                return True, prev_block, next_block

            # go to next block
            prev_block = head
            head = next_block

        # no matching block found
        return False, NULLPTR, NULLPTR

    def leaf_node_insert(self, page_num: int, cell_num: int, key: int, cell: bytes):
        """
        If there is space on the referred leaf, then the cell will be inserted,
        and the operation will terminate.

        If there is not, the node will be split.

        Allocation can happen from: 1) allocation block or 2) free list.

        The allocation strategy is:
            - first check if there is a block in free list that satisfies cell
                - if so, fragment the block (if-needed)
                - copy cell onto block
                - update total_free_bytes
            - next, check if allocation block has enough space
                - if so, copy cell
                - update alloc_ptr
            - next, check if total free space in free list and alloc block
              can satisfy the cell
                 - if so, compact the node
                 - copy cell
            - else,
                - split node and copy cells

        NOTE: for now ignoring free-list- allocating from allocation block

        nodetype .. is_root .. parent_pointer
        num_cells .. alloc_ptr .. free_list_head_ptr .. total_bytes_in_free_list
        ...
        cellptr_0, cellptr_1,... cellptr_N
        ...
        unallocated-space
        ...
        cells

        e.g.

        consider a page of size 32, word: 8
        the indexable bytes are [0, 31]
        the alloc_ptr starts at value 32
        if the header takes 8 bytes, then the cellptr would start at 8
        and say that takes a word, then the allocated space starts at 16,
        so the total space available is 2 words, i.e. 32 - 16 i.e. alloc_ptr - len(cell)
        next, alloc_ptr is updated to 16, i.e.

        0
        8
        16
        24....31


        :param page_num:
        :param cell_num: the current cell that the cursor is pointing to
        :param key: key in cell
        :param cell: to be inserted
        :return:
        """

        node = self.pager.get_page(page_num)
        num_cells = Tree.leaf_node_num_cells(node)

        # determine space needed
        space_needed = len(cell)
        assert space_needed <= LEAF_NODE_MAX_CELL_SIZE, "cell exceeds max size"
        # space available in allocation block
        alloc_block_space = Tree.leaf_node_alloc_block_space(node)
        # space available in free list
        total_space_free_list = Tree.leaf_node_total_free_list_space(node)

        debug(f'alloc_block_space= {alloc_block_space}, total_space_free_list={total_space_free_list}')

        # determine where to place the cell
        # NOTE: the condition on num_cells is only for debugging/developing
        if total_space_free_list + alloc_block_space < space_needed or num_cells >= LEAF_NODE_MAX_CELLS:
            # node is full - split node and insert
            # raise Exception("no way leaf is full")
            self.leaf_node_split_and_insert(page_num, cell_num, cell)
            return

        # check if a free block will satisfy
        has_free_block, prev_node, next_node = Tree.find_free_block(node, space_needed)
        assert has_free_block is False, "unexpected free block"
        if has_free_block:
            # update free list nodes, and total_
            # copy cell onto block
            # todo: complete me
            pass
        # check if allocation block will satisfy
        elif alloc_block_space >= space_needed:
            # copy cell onto block
            # update alloc_ptr

            if cell_num < num_cells:
                # the new cell is left of some existing cell(s)
                # move these cell ptrs right by 1 unit
                # NB: cells never move except during defragmentation or splitting
                cellptrs = self.leaf_node_cellptrs_starting_at(node, cell_num)
                self.set_leaf_node_cellptrs_starting_at(node, cell_num + 1, cellptrs)

            # allocate cell on alloc block
            self.leaf_node_allocate_alloc_block_cell(node, cell_num, cell)

        # check if combined alloc + free blocks will satisfy
        else:
            assert alloc_block_space + total_space_free_list >= space_needed
            # perform compaction on node
            # todo: complete me

        # new key was inserted at largest index, i.e. new max-key - update parent
        if cell_num == num_cells:
            # update the parent's key
            self.check_update_parent_key(page_num)

    @staticmethod
    def leaf_node_allocate_alloc_block_cell(node: bytes, cell_num: int, cell: bytes):
        """
        allocate a cell and cell_num;
        update alloc_ptr

        :param node:
        :param cell_num:
        :param cell:
        :return:
        """
        alloc_ptr = Tree.leaf_node_alloc_ptr(node)
        # update alloc ptr
        # determine the new value of alloc ptr
        # alloc_ptr points past the first allocatable byte
        # alloc pointer grows up, i.e. towards lower addresses
        # new alloc ptr is also the location of the new cell
        new_alloc_ptr = alloc_ptr - len(cell)

        # write cellptr
        # ensure cellptr value is in the right type
        Tree.set_leaf_node_cellptr(node, cell_num, new_alloc_ptr)

        # copy cell contents onto node
        # the cell will be copied starting at the new alloc_ptr location
        # print(f'writing cell to {new_alloc_ptr}')
        Tree.set_leaf_node_cell(node, new_alloc_ptr, cell)

        # update the alloc ptr
        Tree.set_leaf_node_alloc_ptr(node, new_alloc_ptr)

        # update cell count
        num_cells = Tree.leaf_node_num_cells(node)
        Tree.set_leaf_node_num_cells(node, num_cells + 1)

    def leaf_node_split_and_insert(self, page_num: int, new_cell_cell_num: int, new_cell: bytes):
        """

        Split node and insert new_cell. After the insert, the nodes must be such that
        their cells' keys are ordered. Thus, in some cases, the node may be split into 3.

        Currently, the original node's content is copied onto a new node. The alternative
        is to in-place split the node. This may require less copying, but is quiet tricky to implement for
        variable cells.

        If the node being split is a non-root node, split the node and add the new
        child to the parent. If the parent/ancestor is full, repeat split op until every ancestor is
        within capacity.

        If node being split is root, will need to create a new root. Root page must remain at `root_page_num`
        """
        # old node is the original node
        old_node = self.pager.get_page(page_num)

        # create a new node to write to
        # this is the first split, there can be 2, or 3 (rare) splits
        new_page_num = self.pager.get_unused_page_num()
        new_node_cell_num = 0
        new_node = self.pager.get_page(new_page_num)
        new_node_page_nums = [new_page_num]

        num_cells = Tree.leaf_node_num_cells(old_node)

        self.initialize_leaf_node(new_node, node_is_root=False)
        # get parent page num from old node
        self.set_parent_page_num(new_node, self.get_parent_page_num(old_node))

        # iterate over cells and place them on splits
        # such that cells on splits are ordered
        # manually control iteration var, since both new_cell and an existing
        # cell correspond to same `cell_num`
        cell_num = 0
        new_cell_placed = False
        while cell_num < num_cells:
            # check if current node can handle this cell
            # assume nodes have been compacted, i.e. only consider alloc block
            available_space = Tree.leaf_node_alloc_block_space(new_node)

            # this handles picking the correct cell, i.e. cells must be
            # placed in the correct order, including the new cell
            if cell_num == new_cell_cell_num and not new_cell_placed:
                # place the new cell first
                cell = new_cell
                # don't incr cell_num
                new_cell_placed = True
            else:
                cell = self.leaf_node_cell(old_node, cell_num)
                cell_num += 1

            space_needed = len(cell)

            # we want to respect LEAF_NODE_MAX_CELLS since a split may have been
            # called because of this and not inadequate space on node
            if available_space < space_needed or cell_num >= LEAF_NODE_MAX_CELLS:
                # finalize node
                Tree.set_leaf_node_num_cells(new_node, new_node_cell_num)
                # create new node
                new_page_num = self.pager.get_unused_page_num()
                # update write node ref
                new_node = self.pager.get_page(new_page_num)
                self.initialize_leaf_node(new_node, node_is_root=False)
                new_node_page_nums.append(new_page_num)
                new_node_cell_num = 0

            # provision cell
            self.leaf_node_allocate_alloc_block_cell(new_node, new_node_cell_num, cell)
            new_node_cell_num += 1

        new_node_count = len(new_node_page_nums)
        assert new_node_count == 2 or new_node_count == 3, f"Expected 2 or 3 new nodes; received {new_node_count}"
        left_child_page_num = new_node_page_nums[0]
        right_child_page_num = new_node_page_nums[-1]
        middle_child_page_num = new_node_page_nums[1] if len(new_node_page_nums) == 3 else None

        # add new node as child to parent of split node
        # if split node was root, create new root and add split as children
        if self.is_node_root(old_node):
            self.create_new_root(left_child_page_num, right_child_page_num, middle_child_page_num=middle_child_page_num)
        else:
            # todo: ensure the tail invocation recycles old node
            # for create_new_root, the old node doesn't get recycled
            # and I dont' wan this method to be responsible for anything once the below has been called
            self.internal_node_insert(page_num, left_child_page_num, right_child_page_num,
                                      middle_child_page_num=middle_child_page_num)

    def create_new_root(self, left_child_page_num: int, right_child_page_num: int,
                        middle_child_page_num: Optional[int] = None):
        """
        Create a root. The arguments are children page_nums that must be added
        to root.

        The root page num never changes because the catalog tracks tree
        based on the root page num.



        A internal node maintains a array like:
        [ptr[0], key[0], ptr[1], key[1],...ptr[n-1]key[n-1][ptr[n]]
        NOTE: (from book)
        All of the keys on the left most child subtree that Ptr(0) points to have
         values less than or equal to Key(0)
        All of the keys on the child subtree that Ptr(1) points to have values greater
         than Key(0) and less than or equal to Key(1), and so forth
        All of the keys on the right most child subtree that Ptr(n) points to have values greater than Key(n - 1).

        :param left_child_page_num
        :param right_child_page_num:
        :param middle_child_page_num: optional page num corresponding to optional 3rd child,
            i.e. rightmost child
        :return:
        """

        # set root node
        # root node is the old node that was split- and whose contents
        # were splilled onto left, right and optional middle child
        root = self.pager.get_page(self.root_page_num)
        self.initialize_internal_node(root)
        self.set_node_is_root(root, True)
        # root points to itself
        self.set_parent_page_num(root, self.root_page_num)

        left_child = self.pager.get_page(left_child_page_num)
        right_child = self.pager.get_page(right_child_page_num)
        middle_child = None  # optional

        self.set_node_is_root(left_child, False)
        self.set_node_is_root(right_child, False)

        self.set_parent_page_num(left_child, self.root_page_num)
        self.set_parent_page_num(right_child, self.root_page_num)

        if middle_child_page_num is not None:
            middle_child = self.pager.get_page(middle_child_page_num)
            self.set_node_is_root(middle_child, False)
            self.set_parent_page_num(middle_child, self.root_page_num)

        # set left child in root
        self.set_internal_node_child(root, 0, left_child_page_num)
        left_child_max_key = self.get_node_max_key(left_child)
        self.set_internal_node_key(root, 0, left_child_max_key)

        # set middle child
        if middle_child_page_num is not None:
            self.set_internal_node_child(root, 0, middle_child_page_num)
            middle_child_max_key = self.get_node_max_key(middle_child)
            self.set_internal_node_key(root, 1, middle_child_max_key)

        # set right child
        self.set_internal_node_right_child(root, right_child_page_num)

        # set num_keys
        num_keys = 1 if middle_child is None else 2
        self.set_internal_node_num_keys(root, num_keys)

        # update all of left node's children to refer to new left page
        self.check_update_parent_ref_in_children(left_child_page_num)

    # section: logic helpers - delete

    def leaf_node_delete(self, page_num: int, cell_num: int):
        """
        delete key located at `page_num` at `cell_num`
        :param page_num:
        :param cell_num:
        :return:
        """
        node = self.pager.get_page(page_num)

        del_key = self.leaf_node_key(node, cell_num)

        # 1. delete cell
        num_cells = self.leaf_node_num_cells(node)
        # 1.1. move cells over left by 1
        # NOTE: last valid cell is at index num_cells - 1;
        # for anything to exist cell_num must be less than num_cells by 2
        if cell_num <= num_cells - 2:
            cells = self.leaf_node_cells_starting_at(node, cell_num + 1)
            self.set_leaf_node_cells_starting_at(node, cell_num, cells)

        # 1.2. reduce cell count
        new_num_cells = num_cells - 1
        self.set_leaf_node_num_cells(node, new_num_cells)

        # 2 - exit
        if self.is_node_root(node):
            # root node- nothing to do
            return

        # 4. update ancestors
        if cell_num == num_cells - 1 and new_num_cells > 0:
            self.check_update_parent_key(page_num)

        # debug("printing leaf node post del........")
        # self.print_leaf_node(node)
        # debug("printing tree node post del........")
        # self.print_tree()

        self.check_restructure_leaf(page_num, del_key)

    def check_restructure_leaf(self, page_num: int, del_key: int):
        """
        this should see if node at `page_num` and it's siblings
        can be restructured; if so, handles entire restructuring

        :param page_num:
        :param del_key: key that was deleted
        :return:
        """
        # 0. setup
        node = self.pager.get_page(page_num)
        if self.is_node_root(node):
            # root node- nothing to do
            return

        # 1. check if any restructuring is needed
        # get_leaf_siblings will have to be updated to additionally use del_key, e.g. if node is empty
        siblings = self.get_leaf_siblings(page_num, del_key)
        total_sib_count = sum([s.count for s in siblings])
        can_fit_on_fewer_nodes = total_sib_count <= (len(siblings) - 1) * LEAF_NODE_MAX_CELLS
        if not can_fit_on_fewer_nodes:
            # no compaction is possible
            return

        # 2. compact siblings
        # debug(f"compacting siblings [{len(siblings)}]  " + ", ".join([f"count={s.count} page_num={s.page_num}" for s in siblings]))
        post_compact_num_nodes = self.compact_leaf_nodes(siblings)
        # debug(f"num used nodes for compaction: {post_compact_num_nodes}; total sib: {len(siblings)}")

        updated = deque((siblings[idx] for idx in range(post_compact_num_nodes)))
        deleted = deque((siblings[idx] for idx in range(post_compact_num_nodes, len(siblings))))

        # 3. update parent and children are consistent, after siblings are compacted
        # consistent means all references are correct
        parent_page_num = self.get_parent_page_num(node)
        parent = self.pager.get_page(parent_page_num)

        self. post_compaction_helper(parent_page_num, updated, deleted)
        # the parent node must now be consistent

        # debug("printing leaf node post compaction leaf........")
        # self.print_leaf_node(node)
        # debug("printing tree node post compaction leaf........")
        # self.print_tree()

        # determine if the root needs to be deleted;
        if self.is_node_root(parent):
            self.check_delete_root(parent_page_num)
        else:
            self.check_restructure_internal(parent_page_num, del_key)

    def check_restructure_internal(self, page_num: int, del_key: int):
        """

        :param page_num:
        :return:
        """
        node = self.pager.get_page(page_num)
        if self.is_node_root(node):
            # root node- nothing to do
            return

        # get_leaf_siblings will have to be updated to additionally use del_key, e.g. if node is empty
        siblings = self.get_internal_siblings(page_num, del_key)
        total_sib_count = sum([s.count for s in siblings])
        can_fit_on_fewer_nodes = total_sib_count <= (len(siblings) - 1) * INTERNAL_NODE_MAX_CHILDREN
        if not can_fit_on_fewer_nodes:
            # no compaction is possible
            return

        # debug(f"compacting siblings [{len(siblings)}]  " + ", ".join([f"count={s.count} page_num={s.page_num}" for s in siblings]))
        post_compact_num_nodes = self.compact_internal_nodes(siblings)
        # debug(f"num used nodes for compaction: {post_compact_num_nodes}; total sib: {len(siblings)}")

        updated = deque((siblings[idx] for idx in range(post_compact_num_nodes)))
        deleted = deque((siblings[idx] for idx in range(post_compact_num_nodes, len(siblings))))

        # update parent
        parent_page_num = self.get_parent_page_num(node)
        parent = self.pager.get_page(parent_page_num)

        # otherwise, children refs are not updated
        for node_info in updated:
            self.check_update_parent_ref_in_children(node_info.page_num)

        self.post_compaction_helper(parent_page_num, updated, deleted)
        # the parent node must now be consistent

        # determine if the root needs to be deleted;
        if self.is_node_root(parent):
            self.check_delete_root(parent_page_num)
        else:
            self.check_restructure_internal(parent_page_num, del_key)

    def post_compaction_helper(self, page_num: int, updated_children: deque[NodeInfo], deleted_children: deque[NodeInfo]):
        """
        helper method to update parent at `page_num` after children
         siblings are compacted.

        Performs the following tasks:
        - updates parent key ref for inner children
        - attaches a new right child, if siblings include right
        - move children right of deleted gap, so there is a gap
        :return:
        """
        # 0. setup
        node = self.pager.get_page(page_num)
        assert self.get_node_type(node) is NodeType.NodeInternal

        # children must be sorted; as this is used to determine
        # how to fill the gap caused by deleted nodes
        # assert list(ni.parent_pos for ni in chain(updated_children, deleted_children)) == list(ni.parent_pos for ni in sorted(chain(updated_children, deleted_children)))
        assert len(deleted_children) > 0

        # since children are sorted, the last node must be the greatest child
        right_child_updated = deleted_children[-1].parent_pos == INTERNAL_NODE_MAX_CELLS
        # whether a right child has been attached
        right_child_attached = False

        # 1. ensure node is consistent w.r.t. to updated children
        # 1.1. attach right child, if one was taken
        if right_child_updated and updated_children:
            # a right child was taken, attach right-most child as parent's right child
            child = updated_children.pop()
            self.set_internal_node_right_child(node, child.page_num)
            right_child_attached = True

        # 1.2. handle updating keys for (inner) children (1 or 2)
        # NB: there is a difference between attaching right child and updating inner children
        # since the new right may be a different node; whereas an inner node does not change
        # whereas it's key may need to be updated
        while updated_children:
            child = updated_children.popleft()
            assert child.parent_pos != INTERNAL_NODE_MAX_CELLS
            # debug(f"child parent page num: {self.internal_node_child(node, child.parent_pos)}")
            max_key = self.get_node_max_key(child.node)
            self.set_internal_node_key(node, child.parent_pos, max_key)

        # 2. move any siblings greater than siblings that were compacted
        # such that inner children are laid contiguously
        prev_num_keys = self.internal_node_num_keys(node)
        right_most_sib = deleted_children[-1]
        greater_inner_siblings_exist = right_most_sib.parent_pos <= prev_num_keys - 2
        if not right_child_updated and greater_inner_siblings_exist:
            # consider the left and right boundary's of the gap of the deleted
            left_boundary_child_num = deleted_children[0].parent_pos
            right_boundary_child_num = deleted_children[-1].parent_pos + 1
            greater_siblings = self.internal_node_children_starting_at(node, right_boundary_child_num)
            self.set_internal_node_children_starting_at(node, greater_siblings, left_boundary_child_num)

        # 3. update num_keys
        new_num_keys = prev_num_keys - len(deleted_children)
        self.set_internal_node_num_keys(node, new_num_keys)

        # 4. Possible unary, or zero-ary tree
        # a right child was taken, but none were attached
        # the right child must always be set; unless the node has become zero-ary
        # in which case, the flag for that must be set
        if right_child_updated and not right_child_attached:
            if new_num_keys > 1:
                largest_child_page_num = self.internal_node_child(node, new_num_keys - 1)
                self.set_internal_node_right_child(node, largest_child_page_num)
                # decrement count
                self.set_internal_node_num_keys(node, new_num_keys - 1)
                right_child_attached = True
            else:
                # zero-ary
                self.set_internal_node_has_right_child(node, False)

        # 5. update ancestors
        if self.internal_node_has_right_child(node):
            self.check_update_parent_key(page_num)

        # 6. update children's refs to parent
        # NB: the following is not recursive
        self.check_update_parent_ref_in_children(page_num)

        # 7. recycle nodes
        while deleted_children:
            self.pager.return_page(deleted_children.pop().page_num)

    def check_delete_root(self, page_num: int):
        """
        Delete the root at `page_num`

        Note that root_page_num must not change. So we must
        copy the contents of the new root, onto the page that corresponds to the old
        root.

        :param page_num: the node to delete
        :return:
        """
        root = self.pager.get_page(page_num)
        assert self.is_node_root(root)

        if self.internal_node_num_keys(root) > 0:
            # the tree has at least two node; nothing to do
            return

        if not self.internal_node_has_right_child(root):
            # nothing is left in the tree; reset root to empty leaf node
            self.initialize_leaf_node(root)
        else:
            # tree is unary; delete root and set child to be new root
            child_page_num = self.internal_node_right_child(root)
            child = self.pager.get_page(child_page_num)
            # copy child onto root page
            root[:PAGE_SIZE] = child
            self.set_node_is_root(root, True)
            # update children of root; since parent page_num has changed
            self.check_update_parent_ref_in_children(page_num)
            self.pager.return_page(child_page_num)

    def compact_internal_nodes(self, siblings: list) -> int:
        """
        compact siblings i.e. their children onto the fewest siblings as possible.
        We have to be careful that nodes' right children are handled properly

        :param siblings:
        :return:
        """
        assert len(siblings) > 0
        if len(siblings) == 1:
            return 1

        # source of copying is second node, first cell
        src_idx = 1
        src_cell = 0
        # destination of copying is first node, after the last child
        dest_idx = 0
        dest_cell = siblings[0].count

        # before copying; the dest_nodes' right child must be moved to
        # greatest inner child; this is needed for the compaction algorithm
        first = siblings[0]
        if first.count < INTERNAL_NODE_MAX_CHILDREN:
            # get right child
            right_child_page_num = self.internal_node_right_child(first.node)
            # set as inner child
            right_child = self.pager.get_page(right_child_page_num)
            right_child_max_key = self.get_node_max_key(right_child)
            greatest_unused_child_num = siblings[0].count - 1
            self.set_internal_node_child(first.node, greatest_unused_child_num, right_child_page_num)
            self.set_internal_node_key(first.node, greatest_unused_child_num, right_child_max_key)
            # set has no right child
            self.set_internal_node_has_right_child(first.node, False)

        while True:
            # check if copying is complete; i.e. src is at last node, last cell
            if src_idx == len(siblings) - 1 and src_cell == INTERNAL_NODE_MAX_CHILDREN:
                # set number of keys on dest
                # NOTE: `dest_cell` is the new cell idx to write to == count of cells written
                if dest_cell < INTERNAL_NODE_MAX_CHILDREN:
                    # ensure dest node has a right child
                    # get last child to set as right child
                    greatest_child = self.internal_node_child(siblings[dest_idx].node, dest_cell - 1)
                    self.set_internal_node_right_child(siblings[dest_idx].node, greatest_child)
                    self.set_internal_node_num_keys(siblings[dest_idx].node, dest_cell - 1)
                else:
                    self.set_internal_node_num_keys(siblings[dest_idx].node, INTERNAL_NODE_MAX_CELLS)
                break

            # if src or dest is at boundary, move it to next node
            # NB: src_cell takes on the following values:
            # [0, num_cells), INTERNAL_NODE_MAX_CELLS, INTERNAL_NODE_MAX_CHILDREN
            if src_cell == INTERNAL_NODE_MAX_CHILDREN:
                src_idx += 1
                src_cell = 0

            # if src is pointing to a non-existent inner child
            # move it to the right child
            if src_cell == siblings[src_idx].count - 1:
                src_cell = INTERNAL_NODE_MAX_CELLS

            if dest_cell == INTERNAL_NODE_MAX_CHILDREN:
                # set count
                self.set_internal_node_num_keys(siblings[dest_idx].node, INTERNAL_NODE_MAX_CELLS)
                # move to next destination node
                dest_idx += 1
                dest_cell = 0

            src_node = siblings[src_idx].node
            dest_node = siblings[dest_idx].node

            # copy if src and dest locations are different
            # NB: src and dest can point to the same node
            if src_idx != dest_idx or src_cell != dest_cell:
                # determine src
                if src_cell == INTERNAL_NODE_MAX_CELLS:
                    to_copy_page_num = self.internal_node_right_child(src_node)
                else:
                    to_copy_page_num = self.internal_node_child(src_node, src_cell)

                to_copy_key = self.get_node_max_key(self.pager.get_page(to_copy_page_num))

                # determine destination
                if dest_cell == INTERNAL_NODE_MAX_CELLS:
                    self.set_internal_node_right_child(dest_node, to_copy_page_num)
                else:
                    self.set_internal_node_child(dest_node, dest_cell, to_copy_page_num)
                    self.set_internal_node_key(dest_node, dest_cell, to_copy_key)

                debug(f"to_copy_page_num={to_copy_page_num}, to_copy_key={to_copy_key}, "
                      f"dest_idx:{dest_idx} dest_cell={dest_cell} dest_node_page: {siblings[dest_idx].page_num}"
                      f" src_idx:{src_idx} src_cell={src_cell} src_node_page: {siblings[src_idx].page_num}")

            # always increment
            src_cell += 1
            dest_cell += 1

        # mark all to-recycled nodes with 0 count
        for i in range(dest_idx + 1, len(siblings)):
            self.set_internal_node_num_keys(siblings[i].node, 0)
            self.set_internal_node_has_right_child(siblings[i].node, False)

        # return number of used nodes after compaction
        return dest_idx + 1

    def compact_leaf_nodes(self, siblings: Iterable) -> int:
        """
        left-align cells on leaf node siblings
        update each sibling with the updated cell count

        :param siblings:
        :return: number of siblings needed to compress original cells into
        """
        # not sure what to do for a zeroary tree
        assert len(siblings) > 0

        if len(siblings) == 1:
            return 1

        # pack cells leftwards, starting at left most sibling
        # start packing contents of sibling
        src_idx = 1
        src_cell = 0
        dest_idx = 0
        dest_cell = siblings[0].count  # after the last child
        while True:

            # check if src is at last node, last cell, i.e. all cells
            # have been copied; break
            if src_idx == len(siblings) - 1 and src_cell == siblings[src_idx].count:
                # set number of leaves on dest
                # only needed for dest, since unused src nodes will be recycled
                # NOTE: `dest_cell` is the new cell idx to write to == count of cells written
                self.set_leaf_node_num_cells(siblings[dest_idx].node, dest_cell)
                break

            # if src or dest is at boundary, move it to next node
            if src_cell == siblings[src_idx].count:
                src_idx += 1
                src_cell = 0

            if dest_cell == LEAF_NODE_MAX_CELLS:
                # set number of leaves
                self.set_leaf_node_num_cells(siblings[dest_idx].node, LEAF_NODE_MAX_CELLS)
                dest_idx += 1
                dest_cell = 0

            src_node = siblings[src_idx].node
            dest_node = siblings[dest_idx].node

            src_key = self.leaf_node_key(src_node, src_cell)  # used for debugging
            debug(f'before copy: src_idx: {src_idx}, src_key: {src_key} pg:{siblings[src_idx].page_num}, src_cell: {src_cell}; '
                  f'dest_idx: {dest_idx}, pg:{siblings[dest_idx].page_num} dest_cell: {dest_cell}')

            # copy if src and dest are different
            # otherwise, just increment the pointers
            if src_idx != dest_idx or src_cell != dest_cell:
                debug(f'about to copy: src_idx: {src_idx}, src_key: {src_key} pg:{siblings[src_idx].page_num}, src_cell: {src_cell}; '
                      f'dest_idx: {dest_idx}, pg:{siblings[dest_idx].page_num} dest_cell: {dest_cell}')
                to_copy = self.leaf_node_cell(src_node, src_cell)
                self.set_leaf_node_cell(dest_node, dest_cell, to_copy)
            src_cell += 1
            dest_cell += 1

        # mark all to-recycled nodes with 0 count
        for i in range(dest_idx + 1, len(siblings)):
            self.set_leaf_node_num_cells(siblings[i].node, 0)

        # return number of used nodes after compaction
        return dest_idx + 1

    def get_leaf_siblings(self, page_num: int, removed_key: int) -> Iterable(NodeInfo):
        """
        get leaf siblings
        """
        siblings = self.get_siblings(page_num, removed_key)
        for sibling in siblings:
            sibling.node = self.pager.get_page(sibling.page_num)
            sibling.count = self.leaf_node_num_cells(sibling.node)
        return siblings

    def get_internal_siblings(self, page_num: int, removed_key: int) -> Iterable(NodeInfo):
        """
        get internal siblings
        """
        siblings = self.get_siblings(page_num, removed_key)
        for sibling in siblings:
            sibling.node = self.pager.get_page(sibling.page_num)
            # NB: +1 for right child
            sibling.count = self.internal_node_num_keys(sibling.node) + 1
        return siblings

    def get_siblings(self, page_num: int, removed_key) -> Iterable(NodeInfo):
        """
        get a deque of left, right siblings and node at `page_num`
        generic accessor- independent of type of node at `page_num`
        :param page_num:
        :param removed_key: in case node's last key was
        :return:
        """
        node = self.pager.get_page(page_num)
        if self.is_node_root(node):
            return

        siblings = deque()

        parent_page_num = self.get_parent_page_num(node)
        parent = self.pager.get_page(parent_page_num)

        node_max_key = self.get_node_max_key(node)
        empty_node = node_max_key is None
        # if this is None, i.e. node is empty, use the removed_key
        # to locate the node
        if empty_node:
            node_max_key = removed_key

        # 1. find node's location in parent
        # even though children of node may be deleted, it's position in parent
        node_child_num = self.internal_node_find(parent_page_num, node_max_key)
        debug(f"removed key is {removed_key}; node_child_num is: {node_child_num}; node max key: {node_max_key}; "
              f"parent_page_num: {parent_page_num}")
        if node_child_num == INTERNAL_NODE_MAX_CELLS:
            parent_ref_child_page_num = self.internal_node_right_child(parent)
        else:
            parent_ref_child_page_num = self.internal_node_child(parent, node_child_num)

        # 2. ensure tree is in consistent state; parent's ref to child must point to child node
        assert page_num == parent_ref_child_page_num, f"page_num [{page_num}] != parent_ref_child_page_num [{parent_ref_child_page_num}]"

        # 3. get left sibling if if exists
        if node_child_num > 0:
            if node_child_num == INTERNAL_NODE_MAX_CELLS:
                # left sibling of right node is the last inner cell
                parent_pos = self.internal_node_num_keys(parent) - 1
                left_page_num = self.internal_node_child(parent, parent_pos)
            else:
                # left sibling is the left sibling for an inner node
                parent_pos = node_child_num - 1
                left_page_num = self.internal_node_child(parent, parent_pos)

            siblings.append(NodeInfo(left_page_num, parent_pos))

        # 4. add node
        siblings.append(NodeInfo(page_num, node_child_num))

        # 5. check if right sibling exists
        # node is inner child and greater inner child exists
        if node_child_num < INTERNAL_NODE_MAX_CELLS and node_child_num < self.internal_node_num_keys(parent) - 1:
            right_page_num = self.internal_node_child(parent, node_child_num + 1)
            siblings.append(NodeInfo(right_page_num, node_child_num + 1))
        # node is inner child, and greater is right child
        elif node_child_num < INTERNAL_NODE_MAX_CELLS:
            assert node_child_num == self.internal_node_num_keys(parent) - 1
            right_page_num = self.internal_node_right_child(parent)
            siblings.append(NodeInfo(right_page_num, INTERNAL_NODE_MAX_CELLS))
        # node is right child
        else:
            assert node_child_num == INTERNAL_NODE_MAX_CELLS

        return siblings

    # section : common update utilities

    def check_update_parent_ref_in_children(self, page_num: int):
        """
        invoked when node is moved to `page_num`, e.g. on new root
        after split. Check if node was internal node, if so, update
        it's childrens' parent pointer to new page num
        """
        node = self.pager.get_page(page_num)
        if self.get_node_type(node) == NodeType.NodeInternal:
            for child_num in range(self.internal_node_num_keys(node)):
                child = self.pager.get_page(self.internal_node_child(node, child_num))
                self.set_parent_page_num(child, page_num)

            right_child = self.pager.get_page(self.internal_node_right_child(node))
            self.set_parent_page_num(right_child, page_num)

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

        # assert node is not empty
        if self.get_node_type(node) == NodeType.NodeLeaf:
            assert self.leaf_node_num_cells(node) > 0
        else:
            assert self.internal_node_has_right_child(node) is True

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
    def initialize_internal_node(node: bytes, node_is_root=False):
        Tree.set_node_type(node, NodeType.NodeInternal)
        Tree.set_internal_node_num_keys(node, 0)
        Tree.set_node_is_root(node, node_is_root)

    @staticmethod
    def initialize_leaf_node(node: bytes, node_is_root=False):
        Tree.set_node_type(node, NodeType.NodeLeaf)
        Tree.set_leaf_node_num_cells(node, 0)
        Tree.set_leaf_node_alloc_ptr(node, PAGE_SIZE)
        Tree.set_leaf_node_free_list_head(node, NULLPTR)
        Tree.set_leaf_node_total_free_list_space(node, 0)
        Tree.set_node_is_root(node, node_is_root)

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
    def leaf_node_cell_offset_old(cell_num: int) -> int:
        """
        helper to calculate cell offset; this is the
        offset to the key for the given cell

        TODO: nuke me - this is the unsupported; old API
        """
        return LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_POINTER_SIZE

    @staticmethod
    def leaf_node_cell_offset(node: bytes, cell_num: int):
        """
        Get offset (absolute position) to cell at `cell_num`
        :param node:
        :param cell_num:
        :return:
        """
        # read cellptr
        offset = LEAF_NODE_CELL_POINTER_START + (cell_num * LEAF_NODE_CELL_POINTER_SIZE)
        cellptr = node[offset: offset + LEAF_NODE_CELL_POINTER_SIZE]
        # cell_offset is the absolute offset on the page
        cell_offset = int.from_bytes(cellptr, sys.byteorder)
        return cell_offset

    @staticmethod
    def leaf_node_cell_ptr_offset(cell_num: int):
        """
        offset to cell ptr at `cell_num`

        :param cell_num: 0-based position
        :return:
        """
        offset = LEAF_NODE_CELL_POINTER_START + (cell_num * LEAF_NODE_CELL_POINTER_SIZE)
        return offset


    @staticmethod
    def leaf_node_key_offset(cell_num: int) -> int:
        """
        synonym's with cell offset; defining seperate key-value offset
        methods since internal nodes have key/values in reverse order
        """
        return Tree.leaf_node_cell_offset_old(cell_num)

    @staticmethod
    def leaf_node_value_offset(cell_num: int) -> int:
        """
        returns offset to value
        """
        return Tree.leaf_node_cell_offset_old(cell_num) + LEAF_NODE_KEY_SIZE

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
            self.print_leaf_node(root, depth=depth)
            print(divider)
        else:
            body = f"printing internal node at page num: [{root_page_num}]. parent: [{parent_num}]"
            divider = f"{indent}{len(body) * '.'}"
            print(f"{indent}{body}")
            print(divider)
            self.print_internal_node(root, recurse=True, depth=depth)
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
        has_right = self.internal_node_has_right_child(node)
        children = []
        indent = self.depth_to_indent(depth)
        print(f"{indent}internal (size: {num_cells}, has_right: {has_right})")
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
        alloc_ptr = Tree.leaf_node_alloc_ptr(node)
        total_free_list_space = Tree.leaf_node_total_free_list_space(node)
        print(f"{indent}leaf (size: {num_cells}, alloc_ptr: {alloc_ptr}, free_list_space: {total_free_list_space})")
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

    def get_node_max_key(self, node: bytes) -> Optional[int]:
        if self.get_node_type(node) == NodeType.NodeInternal:
            # check if node is empty
            if self.internal_node_has_right_child(node) is False:
                return None
            # max key is right child's max key, so will need to fetch right child
            right_child_page_num = self.internal_node_right_child(node)
            right_child = self.pager.get_page(right_child_page_num)
            # this call will recurse until it hits the right most leaf cell
            right_child_key = self.get_node_max_key(right_child)
            return right_child_key
        else:
            # check if node is empty
            if self.leaf_node_num_cells(node) == 0:
                return None
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
    def internal_node_has_right_child(node: bytes) -> bool:
        value = node[INTERNAL_NODE_HAS_RIGHT_CHILD_OFFSET: INTERNAL_NODE_HAS_RIGHT_CHILD_OFFSET + INTERNAL_NODE_HAS_RIGHT_CHILD_SIZE]
        int_val = int.from_bytes(value, sys.byteorder)
        return bool(int_val)

    @staticmethod
    def leaf_node_cell(node: bytes, cell_num: int) -> bytes:
        """
        returns entire cell
        :param node:
        :param bytes:
        :param cell_num:
        :return:
        """
        # get cellptr from cell num
        cellptr = Tree.leaf_node_cellptr(node, cell_num)
        # get cell size
        cell_size = get_cell_size(node, cellptr)
        return node[cellptr: cellptr + cell_size]

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
    def leaf_node_key_old(node: bytes, cell_num: int) -> int:
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
    def leaf_node_key(node: bytes, cell_num: int) -> int:
        """
        get key in leaf node at position `cell_num`

        :param node:
        :param cell_num: a contiguous integer (0-based), indicating the relative position
        :return:
        """
        cell_offset = Tree.leaf_node_cell_offset(node, cell_num)
        return get_cell_key_in_page(node, cell_offset)

    @staticmethod
    def leaf_node_cellptr(node: bytes, cell_num: int) -> int:
        """
        returns cellptr, i.e. offset to cell
        :param node:
        :param cell_num:
        :return:
        """
        offset = LEAF_NODE_CELL_POINTER_START + cell_num * LEAF_NODE_CELL_POINTER_SIZE
        binstr = node[offset: offset + LEAF_NODE_CELL_POINTER_SIZE]
        return int.from_bytes(binstr, sys.byteorder)

    @staticmethod
    def leaf_node_cellptrs_starting_at(node: bytes, cell_num: int) -> bytes:
        """
        return bytes corresponding to all cell ptrs including at position `cell_num`

        :param node:
        :param cell_num: 0-based relative position
        :return:
        """
        assert cell_num <= Tree.leaf_node_num_cells(node) - 1, \
            f"out of bounds cell [{cell_num}] lookup [total: {Tree.leaf_node_num_cells(node)}]"
        offset = Tree.leaf_node_cell_offset(node, cell_num)
        num_cells = Tree.leaf_node_num_cells(node)
        num_cellptrs_after_cell_num = num_cells - cell_num
        return node[offset: offset + num_cellptrs_after_cell_num * LEAF_NODE_CELL_POINTER_SIZE]

    @staticmethod
    def leaf_node_cells_starting_at(node: bytes, cell_num: int) -> bytes:
        """
        TODO: nuke me; replaced by leaf_node_cellptrs_starting_at


        return bytes corresponding to all children including at `child_num`

        :param node:
        :param child_num:
        :return:
        """
        assert cell_num <= Tree.leaf_node_num_cells(node) - 1, f"out of bounds cell [{cell_num}] lookup [total: {Tree.leaf_node_num_cells(node)}]"
        offset = Tree.leaf_node_cell_offset_old(cell_num)
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
    def set_internal_node_has_right_child(node: bytes, has_right_child: bool):
        value = has_right_child.to_bytes(INTERNAL_NODE_HAS_RIGHT_CHILD_SIZE, sys.byteorder)
        node[INTERNAL_NODE_HAS_RIGHT_CHILD_OFFSET: INTERNAL_NODE_HAS_RIGHT_CHILD_OFFSET + INTERNAL_NODE_HAS_RIGHT_CHILD_SIZE] = value

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
        Tree.set_internal_node_has_right_child(node, True)
        assert right_child_page_num < 100, f"attempting to set very large page num {right_child_page_num}"
        value = right_child_page_num.to_bytes(INTERNAL_NODE_RIGHT_CHILD_SIZE, sys.byteorder)
        node[INTERNAL_NODE_RIGHT_CHILD_OFFSET:INTERNAL_NODE_RIGHT_CHILD_OFFSET + INTERNAL_NODE_RIGHT_CHILD_SIZE] = value

    @staticmethod
    def set_leaf_node_key(node: bytes, cell_num: int, key: int):
        offset = Tree.leaf_node_key_offset(cell_num)
        value = key.to_bytes(LEAF_NODE_KEY_SIZE, sys.byteorder)
        node[offset: offset + LEAF_NODE_KEY_SIZE] = value

    @staticmethod
    def set_leaf_node_alloc_ptr(node: bytes, alloc_ptr: int):
        """

        :param node:
        :param alloc_ptr:
        :return:
        """
        value = alloc_ptr.to_bytes(LEAF_NODE_ALLOC_POINTER_SIZE, sys.byteorder)
        node[LEAF_NODE_ALLOC_POINTER_OFFSET: LEAF_NODE_ALLOC_POINTER_OFFSET + LEAF_NODE_ALLOC_POINTER_SIZE] = value

    @staticmethod
    def set_leaf_node_cellptr(node: bytes, cell_num: int, cellptr: int):
        """
        This should set the actual cellptr value, i.e. the offset.
        :param node:
        :param cell_num: 0-based position
        :param cellptr: int corresponding to offset value
        :return:
        """
        offset = LEAF_NODE_CELL_POINTER_START + LEAF_NODE_CELL_POINTER_SIZE * cell_num
        assert cellptr > 0, "cellptr must be a positive offset"
        cbytes = cellptr.to_bytes(LEAF_NODE_CELL_POINTER_SIZE, sys.byteorder)
        node[offset: offset + len(cbytes)] = cbytes

    @staticmethod
    def set_leaf_node_cellptrs_starting_at(node: bytes, cell_num: int, cellptrs: bytes):
        """
        set a sub-array of cellptrs at position cell_num

        :return:
        """
        offset = Tree.leaf_node_cell_ptr_offset(cell_num)
        node[offset: offset + len(cellptrs)] = cellptrs

    @staticmethod
    def set_leaf_node_cells_starting_at(node: bytes, cell_num: int, cells: bytes):
        """
        todo: nuke me; replaced by set_leaf_node_cellptrs_starting_at
        :param node:
        :param cell_num:
        :param cells:
        :return:
        """
        offset = Tree.leaf_node_cell_offset_old(cell_num)
        node[offset: offset + len(cells)] = cells

    @staticmethod
    def set_leaf_node_num_cells(node: bytes, num_cells: int):
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
    def set_leaf_node_cell(node: bytes, cell_offset: int, cell: bytes):
        """
        write cell to given offset

        :param cell_offset: abs offset on node
        :param cell: cell to write
        """
        node[cell_offset: cell_offset + len(cell)] = cell

    @staticmethod
    def set_leaf_node_key_value(node: bytes, cell_num: int, key: int, value: bytes):
        """
        write both key and value for a given cell
        """
        Tree.set_leaf_node_key(node, cell_num, key)
        Tree.set_leaf_node_value(node, cell_num, value)

