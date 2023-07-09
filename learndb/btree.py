from __future__ import annotations

"""
Contains the implementation of the btree
"""
import sys
import logging

from collections import deque
from enum import Enum, auto
from typing import Optional

from .constants import (
    NULLPTR,
    PAGE_SIZE,
    # common
    NODE_TYPE_SIZE,
    NODE_TYPE_OFFSET,
    IS_ROOT_SIZE,
    IS_ROOT_OFFSET,
    PARENT_POINTER_SIZE,
    PARENT_POINTER_OFFSET,
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
    INTERNAL_NODE_MAX_CELLS,  # for debugging
    INTERNAL_NODE_MAX_CHILDREN,
    # leaf node header layout
    LEAF_NODE_NUM_CELLS_SIZE,
    LEAF_NODE_NUM_CELLS_OFFSET,
    LEAF_NODE_HEADER_SIZE,
    LEAF_NODE_KEY_SIZE,
    LEAF_NODE_MAX_CELL_SIZE,
    LEAF_NODE_MAX_CELLS,  # for debugging
    LEAF_NODE_CELL_POINTER_START,
    LEAF_NODE_CELL_POINTER_SIZE,
    LEAF_NODE_NON_HEADER_SPACE,
    LEAF_NODE_ALLOC_POINTER_OFFSET,
    LEAF_NODE_ALLOC_POINTER_SIZE,
    LEAF_NODE_FREE_LIST_HEAD_POINTER_SIZE,
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
    FREE_BLOCK_HEADER_SIZE,
)
from .serde import get_cell_key, get_cell_key_in_page, get_cell_size


class TreeInsertResult(Enum):
    Success = auto()
    DuplicateKey = auto()


class TreeDeleteResult(Enum):
    Success = auto()


class NodeType(Enum):
    NodeInternal = 1
    NodeLeaf = 2


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

    def __init__(self, pager: "Pager", root_page_num: int):
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
        # if insertion is an occupied cell, check for duplicate key
        if (
            self.leaf_node_num_cells(node) > cell_num
            and self.leaf_node_key(node, cell_num) == key
        ):
            return TreeInsertResult.DuplicateKey

        self.leaf_node_insert(page_num, cell_num, cell)
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
            index = left_closed_index + (right_open_index - left_closed_index) // 2
            key_at_index = self.internal_node_key(node, index)
            if key == key_at_index:
                return index
            if key < key_at_index:
                right_open_index = index
            else:
                left_closed_index = index + 1

        return left_closed_index

    # section: logic helpers - insert core

    def leaf_node_insert(self, page_num: int, cell_num: int, cell: bytes):
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

        # logging.debug(f'alloc_block_space= {alloc_block_space}, total_space_free_list={total_space_free_list}')

        # determine where to place the cell
        # NOTE: the condition on num_cells is only for debugging/developing
        # determine whether we need to split the cell
        if (
            total_space_free_list + alloc_block_space < space_needed
            or num_cells >= LEAF_NODE_MAX_CELLS
        ):
            # node is full - split node and insert
            # raise Exception("no way leaf is full")
            self.leaf_node_split_and_insert(page_num, cell_num, cell)
            return

        # check if a free block will satisfy
        has_free_block, prev_node, next_node = Tree.find_free_block(node, space_needed)
        has_free_block = False  # todo: remove after testing freelist
        assert has_free_block is False, "unexpected free block"
        if has_free_block:
            # update free list nodes, and total_
            # copy cell onto block
            # todo: complete me
            raise NotImplementedError
        # check if allocation block will satisfy
        elif alloc_block_space >= space_needed:
            # copy cell onto block
            # update alloc_ptr

            if cell_num < num_cells:
                # NOTE: cell ptrs are sorted by cell key
                # the new cell is left of some existing cell(s)
                # move these cell ptrs right by 1 unit
                cellptrs = self.leaf_node_cellptrs_starting_at(node, cell_num)
                self.set_leaf_node_cellptrs_starting_at(node, cell_num + 1, cellptrs)

            # NOTE: cells are unordered
            # allocate cell on (top of) alloc block
            self.leaf_node_allocate_alloc_block_cell(node, cell_num, cell)

        # check if combined alloc + free blocks will satisfy
        else:
            assert alloc_block_space + total_space_free_list >= space_needed
            # perform compaction on node
            # todo: complete me
            # todo: this could be done with a check above allocate; whether node should be compacted before alloc
            raise NotImplementedError

        # new key was inserted at largest index, i.e. new max-key - update parent
        if cell_num == num_cells and cell_num != 0:
            # update the parent's key
            old_max_key = Tree.leaf_node_key(node, cell_num - 1)
            new_max_key = get_cell_key(cell)
            self.update_parent_on_new_right_child(page_num, old_max_key, new_max_key)

    def leaf_node_split_and_insert(
        self, page_num: int, new_cell_num: int, new_cell: bytes
    ):
        """
        Split node at `page_num` and insert `new_cell`. After the insert, the nodes must be such that
        their cells' keys are ordered. Thus, in some cases, the node may be split into 3.

        Currently, the original node's content is copied onto a new node,
        i.e. split is out-of-place.

        If the node being split is a non-root node, split the node and add the new
        child to the parent. If the parent/ancestor is full, repeat split op until every ancestor is
        within capacity.

        If node being split is root, will need to create a new root. Root page must remain at `root_page_num`

        :param page_num: the original node where new cell should be placed; but must be split
            due to capacity
        :param new_cell_num: location of new cell
        :param new_cell: contents of new cell
        """
        # 1. get old node
        old_node = self.pager.get_page(page_num)
        num_cells = Tree.leaf_node_num_cells(old_node)

        # 2. create first split; there can be 2, or 3 splits
        # these are dest(ination) nodes
        new_page_num = self.pager.get_unused_page_num()
        dest_node = self.pager.get_page(new_page_num)
        dest_cell_num = 0
        self.initialize_leaf_node(
            dest_node,
            node_is_root=False,
            parent_page_num=self.get_parent_page_num(old_node),
        )

        # 3. track all splits
        new_node_page_nums = [new_page_num]

        # 4. place all children cells of old node into new splits
        # manually control iteration var. There are two scenarios:
        # 4.1) new cell is an inner cell, then we need to account for new and existing cell having same cell_num
        src_cell_num = 0
        new_cell_placed = False
        while src_cell_num < num_cells:
            # check if current node can handle this cell
            # for a new node we only need to consider the alloc block
            available_space = Tree.leaf_node_alloc_block_space(dest_node)

            # this handles picking the correct cell, i.e. cells must be
            # placed in the correct order, including the new cell
            # which must be placed before existing cell at cell_num
            if src_cell_num == new_cell_num and not new_cell_placed:
                # place the new cell first
                cell = new_cell
                # don't incr src_cell_num
                new_cell_placed = True
            else:
                cell = self.leaf_node_cell(old_node, src_cell_num)
                src_cell_num += 1

            space_needed = len(cell)

            # we want to respect LEAF_NODE_MAX_CELLS since a split may have been
            # invoked because of count and not space constraint on node
            if available_space < space_needed or src_cell_num >= LEAF_NODE_MAX_CELLS:
                # finalize previous split
                Tree.set_leaf_node_num_cells(dest_node, dest_cell_num)

                # create new node
                new_page_num = self.pager.get_unused_page_num()
                dest_node = self.pager.get_page(new_page_num)
                dest_cell_num = 0
                self.initialize_leaf_node(dest_node, node_is_root=False)
                self.set_parent_page_num(dest_node, self.get_parent_page_num(old_node))
                new_node_page_nums.append(new_page_num)

            # provision cell
            self.leaf_node_allocate_alloc_block_cell(dest_node, dest_cell_num, cell)
            dest_cell_num += 1

        # 4.2) new cell is rightmost cell, then we add it to tail
        if num_cells == new_cell_num:
            assert new_cell_placed is False, "expected new cell to not have been placed"
            self.leaf_node_allocate_alloc_block_cell(dest_node, dest_cell_num, new_cell)

        # 5.1 expand args for tail call
        new_node_count = len(new_node_page_nums)
        assert (
            new_node_count == 2 or new_node_count == 3
        ), f"Expected 2 or 3 new nodes; received {new_node_count}"
        left_child_page_num = new_node_page_nums[0]
        right_child_page_num = new_node_page_nums[-1]
        middle_child_page_num = (
            new_node_page_nums[1] if len(new_node_page_nums) == 3 else None
        )

        # add new node as child to parent of split node
        # if split node was root, create new root and add split as children
        if self.is_node_root(old_node):
            self.create_new_root(
                left_child_page_num,
                right_child_page_num,
                middle_child_page_num=middle_child_page_num,
            )
        else:
            # for create_new_root, the old node doesn't get recycled
            # and I dont' wan this method to be responsible for anything once the below has been called
            self.internal_node_insert(
                page_num,
                left_child_page_num,
                right_child_page_num,
                middle_child_page_num=middle_child_page_num,
            )

    def internal_node_insert(
        self,
        old_child_page_num: int,
        left_child_page_num: int,
        right_child_page_num: int,
        middle_child_page_num: Optional[int] = None,
    ):
        """
        Invoked after a child node `old_child_page_num` is split into left_ right_ and potentially
        middle_child_page_num.
        This adds new children to old child's parent at old child's location (child_num
        in parent) and remove old child.

        To find the insertion location, find the location of old_child in parent.
        - If old_child was right child, right split becomes new right child,
        and left and middle are added to tail of existing children.
        - If old child was an inner child, then start by adding the left child in old's spot.
        Then move any siblings to the right over, more to the right.
        Insert middle and right to the right of that.

        If the parent is at capacity, the parent must be recursively split/inserted into.
        Thus this method can be called at any level of the splitting, the arguments
        can be either leaf or internal node siblings.

        :param old_child_page_num: child that was split
        :param left_child_page_num: left split
        :param right_child_page_num: right split
        :param middle_child_page_num: optional middle split
        """
        # 1. get old node and parent
        old_child = self.pager.get_page(old_child_page_num)
        parent_page_num = self.get_parent_page_num(old_child)
        parent = self.pager.get_page(parent_page_num)
        num_keys = self.internal_node_num_keys(parent)
        # number of nodes to add; one (old) node is already included
        num_new_nodes = 1 if middle_child_page_num is None else 2

        # 2. check if we need to split node
        if num_keys + num_new_nodes > INTERNAL_NODE_MAX_CELLS:
            # raise Exception("Inner node split not implemented")
            self.internal_node_split_and_insert(
                old_child_page_num,
                left_child_page_num,
                right_child_page_num,
                middle_child_page_num,
            )
            return

        # 3. prepare children for insertion
        # 3.1. materialize children and get their keys
        old_node = self.pager.get_page(old_child_page_num)
        left_child = self.pager.get_page(left_child_page_num)
        middle_child = None
        right_child = self.pager.get_page(right_child_page_num)
        old_child_max_key = self.get_node_max_key(old_node)
        left_child_max_key = self.get_node_max_key(left_child)
        middle_child_max_key = None
        right_child_max_key = self.get_node_max_key(right_child)

        if middle_child_page_num:
            middle_child = self.pager.get_page(middle_child_page_num)
            middle_child_max_key = self.get_node_max_key(middle_child)

        # 4. determine old_node's location
        old_child_num = self.internal_node_find(parent_page_num, old_child_max_key)
        right_child_updated = (
            False  # this is needed for old way of updating parent's ref key to child
        )
        # 5. insert new node's at old's location
        if old_child_num == INTERNAL_NODE_MAX_CELLS:
            # old child is the right child, the splits must all be right of all other children
            # set right split as new right child
            self.set_internal_node_right_child(parent, right_child_page_num)
            right_child_updated = True

            # insert left child at tail of existing children
            self.set_internal_node_child(parent, num_keys, left_child_page_num)
            self.set_internal_node_key(parent, num_keys, left_child_max_key)
            num_keys += 1

            # insert middle child to the right of left child
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
            self.set_internal_node_children_starting_at(
                parent, children, sibling_dest_num
            )

            # insert middle
            next_child_num = old_child_num + 1
            if middle_child_page_num:
                self.set_internal_node_key(parent, next_child_num, middle_child_max_key)
                self.set_internal_node_child(
                    parent, next_child_num, middle_child_page_num
                )
                next_child_num += 1
                num_keys += 1

            # insert right
            self.set_internal_node_key(parent, next_child_num, right_child_max_key)
            self.set_internal_node_child(parent, next_child_num, right_child_page_num)
            num_keys += 1

        # 6. update parent's num keys
        self.set_internal_node_num_keys(parent, num_keys)

        # recycle old node
        self.pager.return_page(old_child_page_num)

        # 7. update parent's ref key to child
        # note: keys ref, i.e. the max key value may need
        # to be updated upto root node, i.e. arbitrary depth; unlike
        # the child page nums refs, which are only held by the parent

        # 7.1. recursively update parent key for children
        # untested
        # if old child was right child and we have a new max key
        if (
            old_child_num == INTERNAL_NODE_MAX_CELLS
            and old_child_max_key < right_child_max_key
        ):
            # update ancestor(s) as there is a new max key
            self.update_parent_on_new_right_child(
                parent_page_num, old_child_max_key, right_child_max_key
            )

    def internal_node_split_and_insert(
        self,
        old_child_page_num: int,
        left_child_page_num: int,
        right_child_page_num: int,
        middle_child_page_num: Optional[int] = None,
    ):
        """
        Invoked when node at `old_child_page_num` is split into left_,  right_,
        and optionally `middle_` child, and the splits cannot fit onto the parent.
        This will split the parent, and attempt to place the split parent, into it's
        parent recursively.

        Find where the old child is located in the parent. The left, middle, and
        right splits will go where the old child was.

        This performs an out-of-place split.

        :param old_child_page_num:
        :param left_child_page_num:
        :param right_child_page_num:
        :param middle_child_page_num:
        :return:
        """

        # 1. setup
        old_child = self.pager.get_page(old_child_page_num)
        parent_page_num = self.get_parent_page_num(old_child)
        parent = self.pager.get_page(parent_page_num)
        grandparent_page_num = self.get_parent_page_num(parent)

        # 1.1. initialize new parents
        # parent node will be split out-of-place into left_ and right_ parent
        # create new nodes for the left and right split of `parent`
        left_parent_page_num = self.pager.get_unused_page_num()
        left_parent = self.pager.get_page(left_parent_page_num)
        self.initialize_internal_node(
            left_parent, node_is_root=False, parent_page_num=grandparent_page_num
        )
        right_parent_page_num = self.pager.get_unused_page_num()
        right_parent = self.pager.get_page(right_parent_page_num)
        self.initialize_internal_node(
            right_parent, node_is_root=False, parent_page_num=grandparent_page_num
        )

        # 1.2. prepare new children to be inserted
        old_child_max_key = self.get_node_max_key(old_child)

        # 1.3. determine how many total children to distribute across splits
        num_keys = Tree.internal_node_num_keys(parent)
        # total_children is num of children to place, i.e. inner children + right child + additional children
        total_children = num_keys + 1 + (1 if middle_child_page_num is None else 2)
        right_split_count = total_children // 2
        left_split_count = total_children - right_split_count

        # 1.4. track which children have to be inserted
        new_children = deque()
        new_children.append(left_child_page_num)
        if middle_child_page_num:
            new_children.append(middle_child_page_num)
        new_children.append(right_child_page_num)

        # 2. place existing children- internal and right, and new
        # children onto the parent splits

        # 2.1. prepare iteration
        old_child_num = self.internal_node_find(parent_page_num, old_child_max_key)
        # distribute children from old parent, and new splits
        # evenly onto left_ and right_parent.
        # src child num
        src_child_num = 0
        dest_child_num = 0
        # flag to determine whether to write to left or right split
        write_to_left = True

        while src_child_num <= num_keys:
            # 2.2. determine src child page num
            # src child is picked based on src_child_num in parent
            # if src_child_num is where old_child was, then we insert the
            # new children starting there, and until all nodes are placed
            src_child_page_num = None
            if src_child_num == num_keys:
                # src is right child
                if old_child_num == INTERNAL_NODE_MAX_CELLS:
                    # src are new splits
                    src_child_page_num = new_children.popleft()
                    # no more new children, incr
                    if len(new_children) == 0:
                        src_child_num += 1
                else:
                    src_child_page_num = Tree.internal_node_right_child(parent)
                    src_child_num += 1
            else:
                # inner child
                if src_child_num == old_child_num:
                    # place new child(ren) at old child's location instead of old location
                    src_child_page_num = new_children.popleft()
                    # no more new children, incr
                    if len(new_children) == 0:
                        src_child_num += 1
                else:
                    # place existing child
                    src_child_page_num = Tree.internal_node_child(parent, src_child_num)
                    src_child_num += 1

            # 2.3. materialize src child
            child_node = self.pager.get_page(src_child_page_num)
            child_key = self.get_node_max_key(child_node)

            # 2.4. determine destination node and position
            # copy src to destination
            if write_to_left:
                # destination is left node
                if dest_child_num == left_split_count - 1:
                    # src is the right child of left parent
                    Tree.set_internal_node_right_child(left_parent, src_child_page_num)
                    # write to node
                    # left node is full; subsequent writes will go to right nodes
                    write_to_left = False
                    dest_child_num = 0
                else:
                    # src is an inner child
                    Tree.set_internal_node_key(left_parent, dest_child_num, child_key)
                    Tree.set_internal_node_child(
                        left_parent, dest_child_num, src_child_page_num
                    )
                    dest_child_num += 1
                # update child's parent ref to new parent
                Tree.set_parent_page_num(child_node, left_parent_page_num)
            else:
                # destination is right node
                if dest_child_num == right_split_count - 1:
                    Tree.set_internal_node_right_child(right_parent, src_child_page_num)
                else:
                    # src is an inner child
                    Tree.set_internal_node_child(
                        right_parent, dest_child_num, src_child_page_num
                    )
                    Tree.set_internal_node_key(right_parent, dest_child_num, child_key)
                    dest_child_num += 1
                # update child's parent ref to new parent
                Tree.set_parent_page_num(child_node, right_parent_page_num)

        # 3. update counts
        Tree.set_internal_node_num_keys(left_parent, left_split_count - 1)
        Tree.set_internal_node_num_keys(right_parent, right_split_count - 1)

        # 4. recycle old_child_num
        self.pager.return_page(old_child_page_num)

        # 5. update parent
        if self.is_node_root(parent):
            self.create_new_root(left_parent_page_num, right_parent_page_num)
        else:
            self.internal_node_insert(
                parent_page_num, left_parent_page_num, right_parent_page_num
            )

    def create_new_root(
        self,
        left_child_page_num: int,
        right_child_page_num: int,
        middle_child_page_num: Optional[int] = None,
    ):
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
        # were splilled onto left, right and potentially middle child
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

    # section: logic helpers - insert helpers

    @staticmethod
    def find_free_block(node: bytes, space_needed: int):
        """
        find first free block in free list that is at least as big as `space_needed`
        free list is unsorted

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
        Tree.set_leaf_node_cellptr(node, cell_num, new_alloc_ptr)

        # copy cell contents onto node
        # the cell will be copied starting at the new alloc_ptr location
        Tree.set_leaf_node_cell(node, new_alloc_ptr, cell)

        # update the alloc ptr
        Tree.set_leaf_node_alloc_ptr(node, new_alloc_ptr)

        # update cell count
        num_cells = Tree.leaf_node_num_cells(node)
        Tree.set_leaf_node_num_cells(node, num_cells + 1)

    # section: logic helpers - delete core

    def leaf_node_delete(self, page_num: int, cell_num: int):
        """
        delete key located at `page_num` at `cell_num`

        eagerly performs compaction if possible

        :param page_num:
        :param cell_num:
        :return:
        """
        # 1. setup
        node = self.pager.get_page(page_num)
        num_cells = Tree.leaf_node_num_cells(node)
        del_key = self.leaf_node_key(node, cell_num)

        # 2. check if compaction is possible
        if not Tree.is_node_root(node):
            left_sib_page_num = self.get_left_sibling(page_num)
            right_sib_page_num = self.get_right_sibling(page_num)

            # this indicates an inconsistency in code
            assert left_sib_page_num != page_num
            assert right_sib_page_num != page_num

            num_sibs = 1
            num_children = num_cells - 1  # -1 for deleted
            cell = Tree.leaf_node_cell(node, cell_num)
            total_space_needed = (
                Tree.leaf_node_cell_cellptr_space(node)
                - len(cell)
                - LEAF_NODE_CELL_POINTER_SIZE
            )

            if left_sib_page_num:
                left_sib = self.pager.get_page(left_sib_page_num)
                num_sibs += 1
                num_children += Tree.leaf_node_num_cells(left_sib)
                total_space_needed = Tree.leaf_node_cell_cellptr_space(left_sib)
            if right_sib_page_num:
                right_sib = self.pager.get_page(right_sib_page_num)
                num_sibs += 1
                num_children += Tree.leaf_node_num_cells(right_sib)
                total_space_needed = Tree.leaf_node_cell_cellptr_space(right_sib)

            # 2.1. compaction is possible if: 1) node is non-root, 2)  num of children and 3) space can
            # fit on one at least 1 fewer node
            if (
                num_children <= (num_sibs - 1) * LEAF_NODE_MAX_CELLS
                and total_space_needed <= (num_sibs - 1) * LEAF_NODE_NON_HEADER_SPACE
            ):
                return self.leaf_node_compact_and_delete(page_num, cell_num)

        # 3. handle deletion
        # 3.1. deallocate cell (must be done before deleting cellptr)
        Tree.leaf_node_deallocate_cell(node, cell_num)
        # 3.2. move cellptr left over deleted cellptr, if there is anything right of deleted
        if cell_num < num_cells - 1:
            Tree.set_leaf_node_cellptrs_starting_at(
                node, cell_num, Tree.leaf_node_cellptrs_starting_at(node, cell_num + 1)
            )
        # 3.3. decrement count
        Tree.set_leaf_node_num_cells(node, num_cells - 1)

        # 4. update parent
        if cell_num == num_cells - 1 and cell_num != 0:
            # deleted was the rightmost child, i.e. now there is a new max key
            # propagate this up the ancestor chain
            new_right_key = self.leaf_node_key(node, cell_num - 1)
            self.update_parent_on_new_right_child(page_num, del_key, new_right_key)

    def leaf_node_compact_and_delete(self, page_num: int, cell_num: int):
        """
        compact nodes and delete child at `child_num`

        :param page_num:
        :param cell_num: location of cell to be deleted
        :return:
        """
        # 1. setup
        # 1.1. get all src nodes
        node = self.pager.get_page(page_num)
        assert self.is_node_root(node) is False, "Expected non-root for compaction"
        cell_to_delete = Tree.leaf_node_cell(node, cell_num)

        left_sib_page_num = self.get_left_sibling(page_num)
        right_sib_page_num = self.get_right_sibling(page_num)
        left_sib = left_sib_page_num and self.pager.get_page(left_sib_page_num)
        right_sib = right_sib_page_num and self.pager.get_page(right_sib_page_num)

        # 1.2. prepare destination nodes
        new_page_num = self.pager.get_unused_page_num()
        new_page_nums = [new_page_num]  # track new pages
        dest_node = self.pager.get_page(new_page_num)
        dest_cell_num = 0
        self.initialize_leaf_node(
            dest_node,
            node_is_root=False,
            parent_page_num=self.get_parent_page_num(node),
        )

        # 1.3 setup src_nodes
        src_nodes = deque()
        if left_sib:
            src_nodes.append(left_sib)
        src_nodes.append(node)
        if right_sib:
            src_nodes.append(right_sib)

        # 1.4. determine how many dest nodes we need and number of cells on each node
        # attempt to spread cell count evenly on fewest number of nodes possible
        total_space_needed = Tree.leaf_node_cell_cellptr_space(node) - len(
            cell_to_delete
        )
        total_cells = Tree.leaf_node_num_cells(node) - 1
        if left_sib_page_num:
            total_cells += Tree.leaf_node_num_cells(left_sib)
            total_space_needed = Tree.leaf_node_cell_cellptr_space(left_sib)
        if right_sib_page_num:
            total_cells += Tree.leaf_node_num_cells(right_sib)
            total_space_needed = Tree.leaf_node_cell_cellptr_space(right_sib)

        quot, rem = divmod(total_space_needed, LEAF_NODE_NON_HEADER_SPACE)
        # number of splits based on space
        space_split_count = quot + (1 if rem != 0 else 0)
        quot, rem = divmod(total_cells, LEAF_NODE_MAX_CELLS)
        # number of splits based on cell count limit
        count_split_count = quot + (1 if rem != 0 else 0)
        # bounded by space and count
        num_dest_nodes = max(space_split_count, count_split_count)

        # determine number of cells for each dest_node
        # each dest_node will get min_num_dest_cells, and num_extra_cells will get 1 extra
        min_num_dest_cells, extra_dest_cell_count = divmod(total_cells, num_dest_nodes)

        # 2. perform compaction
        # iterate over all siblings, left to right, and for each
        # node, iterate over each child and place it on the current dest node
        # dest nodes
        # if the current node can't fit it; provision a new node

        while src_nodes:
            # 2.1. place all cells from src_node onto dest_node, except cell to delete
            src_node = src_nodes.popleft()
            # whether to skip the cell_num to be deleted
            skip_del_cell = src_node == node
            for src_cell_num in range(Tree.leaf_node_num_cells(src_node)):
                if skip_del_cell and src_cell_num == cell_num:
                    # skip copying the cell to delete, effectively deleting it
                    continue

                # 2.1.1. get src cell
                src_cell = Tree.leaf_node_cell(src_node, src_cell_num)

                # check if this should be placed on dest node
                # conditions are: 1) dest node has enough space,
                # 2) number of cells is equal to determined amount - to create near equal distribution
                # NOTE: if the cell length has large variance, count based splits will lead to more
                # split/compact ops than space based splits.

                # 2.2.2 determine destination node and cell_num
                available_space = Tree.leaf_node_alloc_block_space(dest_node)
                space_needed = len(src_cell)

                # 2.2.3. determine whether maximum number of cells have been placed on dest node
                # this depends on how many extra_dest cell there are
                # len of new_page_nums list is the 1-based count of dest nodes so far
                # compare with num of extra cells to determine whether this dest node get an extra cell
                if len(new_page_nums) <= extra_dest_cell_count:
                    dest_node_capacity_reached = dest_cell_num >= min_num_dest_cells + 1
                else:
                    dest_node_capacity_reached = dest_cell_num >= min_num_dest_cells

                # 2.2.4. check if we need to provision a new node
                if available_space < space_needed or dest_node_capacity_reached:
                    # finalize previous split
                    Tree.set_leaf_node_num_cells(dest_node, dest_cell_num)

                    # provision new dest_node
                    new_page_num = self.pager.get_unused_page_num()
                    new_page_nums.append(new_page_num)
                    dest_node = self.pager.get_page(new_page_num)
                    dest_cell_num = 0
                    self.initialize_leaf_node(
                        dest_node,
                        node_is_root=False,
                        parent_page_num=self.get_parent_page_num(node),
                    )

                # 2.2.5. provision src cell onto dest cell
                self.leaf_node_allocate_alloc_block_cell(
                    dest_node, dest_cell_num, src_cell
                )
                dest_cell_num += 1

        # set final leaf count
        Tree.set_leaf_node_num_cells(dest_node, dest_cell_num)

        # 3. update parent with new children
        new_left_sib_page_num = new_page_nums[0]
        new_right_sib_page_num = new_page_nums[1] if len(new_page_nums) > 1 else None

        # compaction is only called on non-root leafs
        # internal node op should handle deleting unnecessary tree levels
        self.internal_node_delete(
            left_sib_page_num,
            page_num,
            right_sib_page_num,
            new_left_sib_page_num,
            new_right_sib_page_num,
        )

    def internal_node_delete(
        self,
        old_left_child_page_num: Optional[int],
        old_middle_child_page_num: int,
        old_right_child_page_num: Optional[int],
        new_left_child_page_num: int,
        new_right_child_page_num: Optional[int],
    ):
        """
        Invoked when children nodes are compacted into fewer nodes (3 or 2 are compacted into 1 or 2)
        Removes references to old children, and update to new children.

        Then recycle old node- this is the consistent with insert where
        parent is responsible for recycling children

        :param old_left_child_page_num:
        :param old_middle_child_page_num: is located contiguously with left and right children in parent
        :param old_right_child_page_num:
        :param new_left_child_page_num:
        :param new_right_child_page_num:
        :return:
        """
        # 1. validate input
        num_old_nodes = (
            (1 if old_left_child_page_num else 0)
            + 1
            + (1 if old_right_child_page_num else 0)
        )
        num_new_nodes = 1 + (1 if new_right_child_page_num else 0)
        assert (
            num_old_nodes > num_new_nodes
        ), f"expected internal node delete to have fewer new [{num_new_nodes}] than old nodes [{num_old_nodes}]"
        assert (
            2 <= num_old_nodes <= 3
        ), f"expected 2 or 3 old nodes, got {num_old_nodes}"
        assert (
            1 <= num_new_nodes <= 2
        ), f"expected 1 or 2 old nodes, got {num_new_nodes}"

        # 2. setup
        old_middle_child = self.pager.get_page(old_middle_child_page_num)
        old_middle_child_key = self.get_node_max_key(old_middle_child)
        parent_page_num = self.get_parent_page_num(old_middle_child)
        parent = self.pager.get_page(parent_page_num)
        parent_num_keys = self.internal_node_num_keys(parent)
        parent_num_new_keys = parent_num_keys - (num_old_nodes - num_new_nodes)

        # 3. determine the start and end locations of src nodes
        # there can be 3 or 2 src nodes- this should be reflected in some args being None
        # and the center's position being inner or extremal
        old_child_num = self.internal_node_find(parent_page_num, old_middle_child_key)

        # 3.1. get left most child num (amongst compacted siblings)
        first_old_child_num = None
        if old_child_num == 0:
            first_old_child_num = 0
        elif old_child_num == INTERNAL_NODE_MAX_CELLS:
            # single right child
            assert Tree.internal_node_has_right_child(parent)
        else:
            first_old_child_num = old_child_num - 1

        # 3.2. get right most child num
        last_old_child_num = None
        if old_child_num == INTERNAL_NODE_MAX_CELLS:
            last_old_child_num = old_child_num
        elif old_child_num == parent_num_keys - 1:
            last_old_child_num = INTERNAL_NODE_MAX_CELLS
        else:
            last_old_child_num = old_child_num + 1

        # 3.3. prepare new children to be inserted
        new_child = self.pager.get_page(new_left_child_page_num)
        new_child_key = self.get_node_max_key(new_child)
        new_children = deque([(new_left_child_page_num, new_child_key)])
        Tree.set_parent_page_num(new_child, parent_page_num)
        # max key amongst all new children
        new_max_key = self.get_node_max_key(new_child)
        if new_right_child_page_num:
            new_child = self.pager.get_page(new_right_child_page_num)
            new_child_key = self.get_node_max_key(new_child)
            new_children.append((new_right_child_page_num, new_child_key))
            Tree.set_parent_page_num(new_child, parent_page_num)
            new_max_key = self.get_node_max_key(new_child)

        # 4. place new nodes where old_nodes were
        # 4.1. if right child was compacted
        if last_old_child_num == INTERNAL_NODE_MAX_CELLS:
            # place rightmost new child at right
            new_child_page_num, _ = new_children.pop()
            Tree.set_internal_node_right_child(parent, new_child_page_num)
            if new_children:
                new_child_page_num, new_child_key = new_children.popleft()
                # if there is another child, place it to the left most position of the compacted children
                Tree.set_internal_node_child(
                    parent, first_old_child_num, new_child_page_num
                )
                Tree.set_internal_node_key(parent, first_old_child_num, new_child_key)
        # 4.2. if inner children were compacted
        else:
            # place leftmost new child at leftmost old child's location
            new_child_page_num, new_child_key = new_children.popleft()
            Tree.set_internal_node_child(
                parent, first_old_child_num, new_child_page_num
            )
            Tree.set_internal_node_key(parent, first_old_child_num, new_child_key)
            # where right siblings will be move to
            right_bound = first_old_child_num + 1
            if new_children:
                # if there is another child, place it to the left of that
                new_child_page_num, new_child_key = new_children.popleft()
                Tree.set_internal_node_child(parent, right_bound, new_child_page_num)
                Tree.set_internal_node_key(parent, right_bound, new_child_key)
                right_bound += 1
            # move any inner children to the right over the empty cells
            if last_old_child_num < parent_num_keys - 1:
                right_children = self.internal_node_children_starting_at(
                    parent, last_old_child_num + 1
                )
                self.set_internal_node_children_starting_at(
                    parent, right_children, right_bound
                )

        # 5. update parent count
        Tree.set_internal_node_num_keys(parent, parent_num_new_keys)

        # 6. update ancestor(s) if there is a new max key on right child
        if last_old_child_num == INTERNAL_NODE_MAX_CELLS:
            old_rightmost = self.pager.get_page(
                old_right_child_page_num or old_middle_child_page_num
            )
            old_max_key = self.get_node_max_key(old_rightmost)
            if old_max_key != new_max_key:
                self.update_parent_on_new_right_child(
                    parent_page_num, old_max_key, new_max_key
                )

        # 7. recycle old nodes
        self.pager.return_page(old_middle_child_page_num)
        if old_left_child_page_num:
            self.pager.return_page(old_left_child_page_num)
        if old_right_child_page_num:
            self.pager.return_page(old_right_child_page_num)

        # 8. check if compaction is possible
        # only attempt compaction if parent is not root
        if not self.is_node_root(parent):
            left_sib_page_num = self.get_left_sibling(parent_page_num)
            right_sib_page_num = self.get_right_sibling(parent_page_num)
            sib_count = 1
            total_children_count = self.internal_node_num_children(parent)

            if left_sib_page_num:
                left_sib = self.pager.get_page(left_sib_page_num)
                sib_count += 1
                total_children_count += self.internal_node_num_children(left_sib)
            if right_sib_page_num:
                right_sib = self.pager.get_page(right_sib_page_num)
                sib_count += 1
                total_children_count += self.internal_node_num_children(right_sib)

            # compact if we can fit siblings' children on at least one fewer node
            if total_children_count <= (sib_count - 1) * INTERNAL_NODE_MAX_CHILDREN:
                return self.internal_node_compact(parent_page_num)

        # 9. check if tree depth can be reduced
        if self.is_node_root(parent):
            # if parent has only one child (right child), delete parent
            if parent_num_new_keys == 0:
                self.delete_root()

    def internal_node_compact(self, page_num: Optional[int]):
        """
        This is invoked after an internal node has some elements deleted, and
        node at `page_num` can be compacted with its siblings.

        NOTE: this is different from deletion/compaction for leaves- which
        is done in one operation. The main reason is that internal node deletion and compaction
        may have variable number of children compacted into a variable number of children.
        Thus doing compaction along with deletes is quiet tricky.

        Algo:
            - get parent of child (page_num is one of the compacted nodes)
            - get siblings of parent
            - determine how many total children there are
            - determine how many children each parent gets
            - distribute src children to dest
            - update parents

        :param self:
        :param page_num: one of the siblings to be compacted
        :return:
        """

        # 1.1. get parent of compacted nodes
        node = self.pager.get_page(page_num)
        parent_page_num = self.get_parent_page_num(node)
        parent = self.pager.get_page(parent_page_num)

        # 1.2. get siblings
        left_sib_page_num = self.get_left_sibling(page_num)
        right_sib_page_num = self.get_right_sibling(page_num)
        left_sib = left_sib_page_num and self.pager.get_page(left_sib_page_num)
        right_sib = right_sib_page_num and self.pager.get_page(right_sib_page_num)

        # 1.3. determine total children
        total_children = Tree.internal_node_num_children(node)
        if left_sib_page_num:
            total_children += Tree.internal_node_num_children(left_sib)
        if right_sib_page_num:
            total_children += Tree.internal_node_num_children(right_sib)

        # 1.4. determine distributions of children onto destinations
        quot, rem = divmod(total_children, INTERNAL_NODE_MAX_CHILDREN)
        num_parents = quot + (1 if rem != 0 else 0)
        # each parent split gets at least `min_num_dest_cells`
        # and `extra_dest_cell_count` get 1 extra
        min_num_dest_cells, extra_dest_cell_count = divmod(total_children, num_parents)

        # 1.5. setup src_nodes
        src_nodes = deque()
        if left_sib:
            src_nodes.append(left_sib_page_num)
        src_nodes.append(page_num)
        if right_sib:
            src_nodes.append(right_sib_page_num)

        # 1.6. prepare destination nodes
        dest_page_num = self.pager.get_unused_page_num()
        new_page_nums = [dest_page_num]  # track new pages
        dest_node = self.pager.get_page(dest_page_num)
        dest_cell_num = 0
        self.initialize_internal_node(
            dest_node, node_is_root=False, parent_page_num=parent_page_num
        )

        # 2. place all src children
        while src_nodes:
            # 2.1. place all children from src_node onto dest_node
            src_node_page_num = src_nodes.popleft()
            src_node = self.pager.get_page(src_node_page_num)
            src_node_num_children = Tree.internal_node_num_children(src_node)
            # copy each src child onto the dest
            for src_child_num in range(src_node_num_children):
                # determine child referenced is right or inner child
                if src_child_num == src_node_num_children - 1:
                    src_child_page_num = self.internal_node_right_child(src_node)
                else:
                    src_child_page_num = self.internal_node_child(
                        src_node, src_child_num
                    )

                src_child_node = self.pager.get_page(src_child_page_num)
                src_child_key = self.get_node_max_key(src_child_node)

                # num of new page nums is count of dest nodes
                # compare this count with num of extra dest cells to determine
                # if this node gets an extra cell or whether all extra cells have
                # been placed
                if len(new_page_nums) <= extra_dest_cell_count:
                    dest_node_capacity_reached = dest_cell_num >= min_num_dest_cells + 1
                    place_at_right = dest_cell_num == min_num_dest_cells
                else:
                    dest_node_capacity_reached = dest_cell_num >= min_num_dest_cells
                    place_at_right = dest_cell_num == min_num_dest_cells - 1

                # 2.2.4. check if we need to provision a new node
                if dest_node_capacity_reached:
                    # finalize previous split- set count
                    # dest_cell_num is the count since it increments by 1 for each child placed
                    Tree.set_internal_children_count(dest_node, dest_cell_num)
                    # Tree.set_internal_node_num_keys(dest_node, dest_cell_num)
                    # provision new node
                    dest_page_num = self.pager.get_unused_page_num()
                    new_page_nums.append(dest_page_num)
                    dest_node = self.pager.get_page(dest_page_num)
                    dest_cell_num = 0
                    self.initialize_internal_node(
                        dest_node, node_is_root=False, parent_page_num=parent_page_num
                    )

                if place_at_right:
                    self.set_internal_node_right_child(dest_node, src_child_page_num)
                    self.set_parent_page_num(src_child_node, dest_page_num)
                else:
                    self.set_internal_node_child(
                        dest_node, dest_cell_num, src_child_page_num
                    )
                    # assert src_child_key is not None
                    self.set_internal_node_key(dest_node, dest_cell_num, src_child_key)
                    self.set_parent_page_num(src_child_node, dest_page_num)

                # update bookkeeping vars
                dest_cell_num += 1

        # all src child node have been placed
        # ensure dest node has right number of children
        Tree.set_internal_children_count(dest_node, dest_cell_num)

        new_left_sib_page_num = new_page_nums[0]
        new_right_sib_page_num = new_page_nums[1] if len(new_page_nums) > 1 else None

        # 3. update ancestor
        # ancestor will recycle compacted nodes
        return self.internal_node_delete(
            left_sib_page_num,
            page_num,
            right_sib_page_num,
            new_left_sib_page_num,
            new_right_sib_page_num,
        )

    def delete_root(self):
        """
        this should be invoked when the root has a single child, and thus
        can be removed, and the tree's depth decreased by 1
        :return:
        """
        root = self.pager.get_page(self.root_page_num)
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
            self.set_parent_page_num(root, NULLPTR)
            # update children of root; since parent page_num has changed
            self.check_update_parent_ref_in_children(self.root_page_num)
            self.pager.return_page(child_page_num)

    # section: logic helpers - delete helpers

    @staticmethod
    def leaf_node_deallocate_cell(node: bytes, cell_num: int):
        """
        deallocate the cell referered to at cell_num.
        If the cell is at the boundary of the alloc ptr, i.e. the cell at the lowest
        address, then return cell to the alloc ptr, otherwise return to free list.

        ensure this is called before cellptr is deallocated
        :param node:
        :param cell_num:
        :return:
        """

        # 1. check if cell can be returned to alloc block
        cellptr = Tree.leaf_node_cellptr(node, cell_num)
        cell = Tree.leaf_node_cell(node, cell_num)
        alloc_ptr = Tree.leaf_node_alloc_ptr(node)

        if cellptr == alloc_ptr:
            # return cell to alloc block
            new_alloc_ptr = alloc_ptr + len(cell)
            Tree.set_leaf_node_alloc_ptr(node, new_alloc_ptr)
            return

        # 2. return cell to free list
        # 2.1. format cell as free list node
        offset = cellptr
        # 2.2. set block size
        block_size = len(cell)
        block_size_value = block_size.to_bytes(
            FREE_BLOCK_SIZE_SIZE, sys.byteorder
        )  # encoded value
        block_size_offset = offset + FREE_BLOCK_SIZE_OFFSET
        node[
            block_size_offset : block_size_offset + FREE_BLOCK_SIZE_SIZE
        ] = block_size_value

        # 2.3. insert node and set next ptr
        head = Tree.leaf_node_free_list_head(node)
        if head == NULLPTR:
            # set head to cell offset
            Tree.set_leaf_node_free_list_head(node, cellptr)
            next_ptr_offset = offset + FREE_BLOCK_NEXT_BLOCK_OFFSET
            # set next ptr to null
            node[
                next_ptr_offset : next_ptr_offset + FREE_BLOCK_NEXT_BLOCK_SIZE
            ] = NULLPTR.to_bytes(FREE_BLOCK_NEXT_BLOCK_SIZE, sys.byteorder)
        else:
            # insert to head of list
            # make current head, new cell's next
            next_ptr = head.to_bytes(FREE_BLOCK_NEXT_BLOCK_SIZE, sys.byteorder)
            next_ptr_offset = offset + FREE_BLOCK_NEXT_BLOCK_OFFSET
            node[
                next_ptr_offset : next_ptr_offset + FREE_BLOCK_NEXT_BLOCK_SIZE
            ] = next_ptr
            # set new node as head
            Tree.set_leaf_node_free_list_head(node, cellptr)

        # 2.4. set free list total space
        Tree.set_leaf_node_total_free_list_space(
            node, Tree.leaf_node_total_free_list_space(node) + len(cell)
        )

    def get_left_sibling(self, page_num: int) -> Optional[int]:
        """
        get left sibling if it exists
        :param page_num:
        :return: Optional[int] left sibling page_num if it exists
        """

        node = self.pager.get_page(page_num)
        if Tree.is_node_root(node):
            return None

        node_key = self.get_node_max_key(node)
        parent_page_num = Tree.get_parent_page_num(node)
        parent = self.pager.get_page(parent_page_num)

        child_num = self.internal_node_find(parent_page_num, node_key)
        sib_page_num = None
        if child_num == INTERNAL_NODE_MAX_CELLS:
            # child is right child, left sibling is last inner cell if it exists
            parent_num_keys = self.internal_node_num_keys(parent)
            if parent_num_keys == 0:
                # handle unary
                sib_page_num = None
            else:
                sib_page_num = self.internal_node_child(parent, parent_num_keys - 1)
        elif child_num > 0:
            sib_page_num = self.internal_node_child(parent, child_num - 1)
        else:
            # node is leftmost
            sib_page_num = None

        assert (
            sib_page_num != page_num
        ), f"expected left sibling [{sib_page_num}] to different from arg [{page_num}]"
        return sib_page_num

    def get_right_sibling(self, page_num: int) -> Optional[int]:
        """
        get right sibling
        :param page_num:
        :return: Optional[int] right sibling page_num if it exists
        """
        node = self.pager.get_page(page_num)
        if Tree.is_node_root(node):
            return None

        node_key = self.get_node_max_key(node)
        parent_page_num = Tree.get_parent_page_num(node)
        parent = self.pager.get_page(parent_page_num)

        child_num = self.internal_node_find(parent_page_num, node_key)
        sib_page_num = None
        if child_num == INTERNAL_NODE_MAX_CELLS:
            # node is right most
            sib_page_num = None
        elif child_num == self.internal_node_num_keys(parent) - 1:
            # node is right-most inner cell
            sib_page_num = self.internal_node_right_child(parent)
        else:
            sib_page_num = self.internal_node_child(parent, child_num + 1)

        assert (
            sib_page_num != page_num
        ), f"expected right sibling [{sib_page_num}] to different from arg [{page_num}]"
        return sib_page_num

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

    def update_parent_on_new_right_child(
        self, page_num: int, old_child_key: int, new_child_key: int
    ):
        """
        Invoked when node at `page_num` has `old_child_key` replaced with `new_child_key`

        If the replaced child was:
         - not the right child, then the parent's inner key must be updated.
         - was the right child, the keys must be propagated to parent's parent, and
         up the ancestor chain, until an inner key is updated or the root is reached

        :return:
        """
        node = self.pager.get_page(page_num)
        if self.is_node_root(node):
            # nothing to do
            return

        parent_page_num = self.get_parent_page_num(node)
        parent = self.pager.get_page(parent_page_num)

        old_child_num = self.internal_node_find(parent_page_num, old_child_key)
        if old_child_num == INTERNAL_NODE_MAX_CELLS:
            # the node is parent's right child; thus parent is not
            # updated; but it's grandparent might need to be- propagate up
            self.update_parent_on_new_right_child(
                parent_page_num, old_child_key, new_child_key
            )
        else:
            # node is a non-right child of it's parent, update key ref
            # and terminate op
            self.set_internal_node_key(parent, old_child_num, new_child_key)

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
    def initialize_internal_node(node: bytes, node_is_root=False, parent_page_num=0):
        Tree.set_node_type(node, NodeType.NodeInternal)
        Tree.set_internal_node_num_keys(node, 0)
        Tree.set_node_is_root(node, node_is_root)
        Tree.set_parent_page_num(node, parent_page_num)
        Tree.set_internal_node_num_keys(node, 0)
        Tree.set_internal_node_has_right_child(node, False)

    @staticmethod
    def initialize_leaf_node(node: bytes, node_is_root=False, parent_page_num=0):
        Tree.set_node_type(node, NodeType.NodeLeaf)
        Tree.set_leaf_node_num_cells(node, 0)
        Tree.set_leaf_node_alloc_ptr(node, PAGE_SIZE)
        Tree.set_leaf_node_free_list_head(node, NULLPTR)
        Tree.set_leaf_node_total_free_list_space(node, 0)
        Tree.set_node_is_root(node, node_is_root)
        Tree.set_parent_page_num(node, parent_page_num)

    # section: btree utility methods

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
    def leaf_node_cell_offset(node: bytes, cell_num: int):
        """
        Get offset (absolute position) to cell at `cell_num` by dereferencing the
        cellptr

        :param node:
        :param cell_num:
        :return:
        """
        # read cellptr
        offset = LEAF_NODE_CELL_POINTER_START + (cell_num * LEAF_NODE_CELL_POINTER_SIZE)
        cellptr = node[offset : offset + LEAF_NODE_CELL_POINTER_SIZE]
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

    # section : node getters setters: common, internal, leaf, leaf::free-list

    @staticmethod
    def get_parent_page_num(node: bytes) -> int:
        """
        return pointer to parent page_num
        """
        value = node[
            PARENT_POINTER_OFFSET : PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE
        ]
        return int.from_bytes(value, sys.byteorder)

    @staticmethod
    def get_node_type(node: bytes) -> NodeType:
        value = int.from_bytes(
            node[NODE_TYPE_OFFSET : NODE_TYPE_OFFSET + NODE_TYPE_SIZE], sys.byteorder
        )
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
        value = node[IS_ROOT_OFFSET : IS_ROOT_OFFSET + IS_ROOT_SIZE]
        int_val = int.from_bytes(value, sys.byteorder)
        return bool(int_val)

    @staticmethod
    def internal_node_num_keys(node: bytes) -> int:
        value = node[
            INTERNAL_NODE_NUM_KEYS_OFFSET : INTERNAL_NODE_NUM_KEYS_OFFSET
            + INTERNAL_NODE_NUM_KEYS_SIZE
        ]
        return int.from_bytes(value, sys.byteorder)

    @staticmethod
    def internal_node_num_children(node: bytes) -> int:
        """return total number of children in node"""
        return Tree.internal_node_num_keys(node) + (
            1 if Tree.internal_node_has_right_child(node) else 0
        )

    @staticmethod
    def internal_node_child(node: bytes, child_num: int) -> int:
        """return child ptr, i.e. page number"""
        offset = Tree.internal_node_child_offset(child_num)
        value = node[offset : offset + INTERNAL_NODE_CHILD_SIZE]
        return int.from_bytes(value, sys.byteorder)

    @staticmethod
    def internal_node_right_child(node: bytes) -> int:
        value = node[
            INTERNAL_NODE_RIGHT_CHILD_OFFSET : INTERNAL_NODE_RIGHT_CHILD_OFFSET
            + INTERNAL_NODE_RIGHT_CHILD_SIZE
        ]
        return int.from_bytes(value, sys.byteorder)

    @staticmethod
    def internal_node_cell(node: bytes, key_num: int) -> bytes:
        """return entire cell containing key and child ptr
        this does not work for right child"""
        assert key_num != INTERNAL_NODE_MAX_CELLS
        offset = Tree.internal_node_cell_offset(key_num)
        return node[offset : offset + INTERNAL_NODE_CELL_SIZE]

    @staticmethod
    def internal_node_key(node: bytes, key_num: int) -> int:
        offset = Tree.internal_node_key_offset(key_num)
        bin_num = node[offset : offset + INTERNAL_NODE_KEY_SIZE]
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
        return node[offset : offset + num_keys_to_shift * INTERNAL_NODE_CELL_SIZE]

    @staticmethod
    def internal_node_has_right_child(node: bytes) -> bool:
        value = node[
            INTERNAL_NODE_HAS_RIGHT_CHILD_OFFSET : INTERNAL_NODE_HAS_RIGHT_CHILD_OFFSET
            + INTERNAL_NODE_HAS_RIGHT_CHILD_SIZE
        ]
        return bool.from_bytes(value, sys.byteorder)

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
        return node[cellptr : cellptr + cell_size]

    @staticmethod
    def leaf_node_cell_size(node: bytes, cell_num: int) -> int:
        """
        Get size of cell at `cell_num`
        :param node:
        :param cell_num:
        :return:
        """
        cellptr = Tree.leaf_node_cellptr(node, cell_num)
        # get cell size
        return get_cell_size(node, cellptr)

    @staticmethod
    def leaf_node_cell_cellptr_space(node: bytes) -> int:
        """
        return the total space used by cells and cellptrs
        this seems fairly expensive, so I might want to cache the node value
        :return:
        """
        space = 0
        for cell_num in range(Tree.leaf_node_num_cells(node)):
            space += (
                Tree.leaf_node_cell_size(node, cell_num) + LEAF_NODE_CELL_POINTER_SIZE
            )
        return space

    @staticmethod
    def leaf_node_num_cells(node: bytes) -> int:
        """
        `node` is exactly equal to a `page`. However,`node` is in the domain
        of the tree, while page is in the domain of storage.
        Using the same naming convention of `prop_name` for getter and `set_prop_name` for setter
        """
        bin_num = node[
            LEAF_NODE_NUM_CELLS_OFFSET : LEAF_NODE_NUM_CELLS_OFFSET
            + LEAF_NODE_NUM_CELLS_SIZE
        ]
        return int.from_bytes(bin_num, sys.byteorder)

    @staticmethod
    def leaf_node_key(node: bytes, cell_num: int) -> int:
        """
        get key in leaf node at position `cell_num`

        :param node:
        :param cell_num: a contiguous integer (0-based), indicating the relative position
        :return:
        """
        assert cell_num < Tree.leaf_node_num_cells(
            node
        ), "cell at cell_num greater than number of cells requested"
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
        binstr = node[offset : offset + LEAF_NODE_CELL_POINTER_SIZE]
        return int.from_bytes(binstr, sys.byteorder)

    @staticmethod
    def leaf_node_cellptrs_starting_at(node: bytes, cell_num: int) -> bytes:
        """
        return bytes corresponding to all cell ptrs starting at position `cell_num`

        :param node:
        :param cell_num: 0-based relative position
        :return:
        """
        assert (
            cell_num <= Tree.leaf_node_num_cells(node) - 1
        ), f"out of bounds cell [{cell_num}] lookup [total: {Tree.leaf_node_num_cells(node)}]"
        start = LEAF_NODE_CELL_POINTER_START + cell_num * LEAF_NODE_CELL_POINTER_SIZE
        num_cells = Tree.leaf_node_num_cells(node)
        num_cellptrs_after_cell_num = num_cells - cell_num
        end = start + num_cellptrs_after_cell_num * LEAF_NODE_CELL_POINTER_SIZE
        return node[start:end]

    @staticmethod
    def leaf_node_alloc_ptr(node: bytes) -> int:
        """
        :param node:
        :return: the value of the alloc ptr, i.e. the abs offset of the first byte that can be allocated
        """
        bstring = node[
            LEAF_NODE_ALLOC_POINTER_OFFSET : LEAF_NODE_ALLOC_POINTER_OFFSET
            + LEAF_NODE_ALLOC_POINTER_SIZE
        ]
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
        binvalue = node[
            LEAF_NODE_TOTAL_FREE_LIST_SPACE_OFFSET : LEAF_NODE_TOTAL_FREE_LIST_SPACE_OFFSET
            + LEAF_NODE_TOTAL_FREE_LIST_SPACE_SIZE
        ]
        return int.from_bytes(binvalue, sys.byteorder)

    @staticmethod
    def leaf_node_free_list_head(node: bytes) -> int:
        """
        return head of free list. May be null, i.e. 0 if
        list is empty.
        :param node:
        :return:
        """
        binval = node[
            LEAF_NODE_FREE_LIST_HEAD_POINTER_OFFSET : LEAF_NODE_FREE_LIST_HEAD_POINTER_OFFSET
            + LEAF_NODE_FREE_LIST_HEAD_POINTER_SIZE
        ]
        return int.from_bytes(binval, sys.byteorder)

    @staticmethod
    def free_block_size(node: bytes, free_block_offset: int) -> int:
        """
        get size of free block pointed to by `free_block_offset`
        :param node:
        :param free_block_offset:
        :return:
        """
        offset = free_block_offset + FREE_BLOCK_SIZE_OFFSET
        binval = node[offset : offset + FREE_BLOCK_SIZE_SIZE]
        return int.from_bytes(binval, sys.byteorder)

    @staticmethod
    def free_block_next_free(node: bytes, free_block_offset: int) -> int:
        """
        get next free block pointed to by block at `free_block_offset`
        """
        offset = free_block_offset + FREE_BLOCK_NEXT_BLOCK_OFFSET
        binval = node[offset : offset + FREE_BLOCK_NEXT_BLOCK_SIZE]
        return int.from_bytes(binval, sys.byteorder)

    @staticmethod
    def set_parent_page_num(node: bytes, page_num: int):
        value = page_num.to_bytes(PARENT_POINTER_SIZE, sys.byteorder)
        node[
            PARENT_POINTER_OFFSET : PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE
        ] = value

    @staticmethod
    def set_node_is_root(node: bytes, is_root: bool):
        value = is_root.to_bytes(IS_ROOT_SIZE, sys.byteorder)
        node[IS_ROOT_OFFSET : IS_ROOT_OFFSET + IS_ROOT_SIZE] = value

    @staticmethod
    def set_internal_node_has_right_child(node: bytes, has_right_child: bool):
        value = has_right_child.to_bytes(
            INTERNAL_NODE_HAS_RIGHT_CHILD_SIZE, sys.byteorder
        )
        node[
            INTERNAL_NODE_HAS_RIGHT_CHILD_OFFSET : INTERNAL_NODE_HAS_RIGHT_CHILD_OFFSET
            + INTERNAL_NODE_HAS_RIGHT_CHILD_SIZE
        ] = value

    @staticmethod
    def set_node_type(node: bytes, node_type: NodeType):
        bits = node_type.value.to_bytes(NODE_TYPE_SIZE, sys.byteorder)
        node[NODE_TYPE_OFFSET : NODE_TYPE_OFFSET + NODE_TYPE_SIZE] = bits

    @staticmethod
    def set_internal_node_cell(node: bytes, key_num: int, cell: bytes) -> bytes:
        """
        write entire cell
        this won't work for right child
        """
        offset = Tree.internal_node_cell_offset(key_num)
        assert (
            len(cell) == INTERNAL_NODE_CELL_SIZE
        ), "bytes written to internal cell less than INTERNAL_NODE_CELL_SIZE"
        node[offset : offset + len(cell)] = cell

    @staticmethod
    def set_internal_node_child(node: bytes, child_num: int, child_page_num: int):
        """
        set the nth child
        """
        assert (
            child_page_num < 100
        ), f"attempting to set very large page num {child_page_num}"
        offset = Tree.internal_node_child_offset(child_num)
        value = child_page_num.to_bytes(INTERNAL_NODE_CHILD_SIZE, sys.byteorder)
        node[offset : offset + INTERNAL_NODE_CHILD_SIZE] = value

    @staticmethod
    def set_internal_node_children_starting_at(
        node: bytes, children: bytes, child_num: int
    ):
        """
        bulk set multiple cells

        :param node:
        :param children:
        :param child_num:
        :return:
        """
        assert (
            len(children) % INTERNAL_NODE_CELL_SIZE == 0
        ), "error: children are not an integer multiple of cell size"
        offset = Tree.internal_node_cell_offset(child_num)
        node[offset : offset + len(children)] = children

    @staticmethod
    def set_internal_node_key(node: bytes, child_num: int, key: int):
        offset = Tree.internal_node_key_offset(child_num)
        value = key.to_bytes(INTERNAL_NODE_CHILD_SIZE, sys.byteorder)
        node[offset : offset + INTERNAL_NODE_NUM_KEYS_SIZE] = value

    @staticmethod
    def set_internal_node_num_keys(node: bytes, num_keys: int):
        value = num_keys.to_bytes(INTERNAL_NODE_NUM_KEYS_SIZE, sys.byteorder)
        node[
            INTERNAL_NODE_NUM_KEYS_OFFSET : INTERNAL_NODE_NUM_KEYS_OFFSET
            + INTERNAL_NODE_NUM_KEYS_SIZE
        ] = value

    @staticmethod
    def set_internal_children_count(node: bytes, num_children: int):
        """
        Set internal children count, including:
            1) whether right child is set
            2) number of inner children

        :param node:
        :param num_children:
        :return:
        """
        if num_children > 0:
            Tree.set_internal_node_has_right_child(node, True)
            num_children -= 1
        Tree.set_internal_node_num_keys(node, num_children)

    @staticmethod
    def set_internal_node_right_child(node: bytes, right_child_page_num: int):
        Tree.set_internal_node_has_right_child(node, True)
        assert (
            right_child_page_num < 100
        ), f"attempting to set very large page num {right_child_page_num}"
        value = right_child_page_num.to_bytes(
            INTERNAL_NODE_RIGHT_CHILD_SIZE, sys.byteorder
        )
        node[
            INTERNAL_NODE_RIGHT_CHILD_OFFSET : INTERNAL_NODE_RIGHT_CHILD_OFFSET
            + INTERNAL_NODE_RIGHT_CHILD_SIZE
        ] = value

    @staticmethod
    def set_leaf_node_key(node: bytes, cell_num: int, key: int):
        offset = Tree.leaf_node_key_offset(cell_num)
        value = key.to_bytes(LEAF_NODE_KEY_SIZE, sys.byteorder)
        node[offset : offset + LEAF_NODE_KEY_SIZE] = value

    @staticmethod
    def set_leaf_node_alloc_ptr(node: bytes, alloc_ptr: int):
        """

        :param node:
        :param alloc_ptr:
        :return:
        """
        value = alloc_ptr.to_bytes(LEAF_NODE_ALLOC_POINTER_SIZE, sys.byteorder)
        node[
            LEAF_NODE_ALLOC_POINTER_OFFSET : LEAF_NODE_ALLOC_POINTER_OFFSET
            + LEAF_NODE_ALLOC_POINTER_SIZE
        ] = value

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
        node[offset : offset + len(cbytes)] = cbytes

    @staticmethod
    def set_leaf_node_cellptrs_starting_at(node: bytes, cell_num: int, cellptrs: bytes):
        """
        set a sub-array of cellptrs at position cell_num

        :return:
        """
        offset = Tree.leaf_node_cell_ptr_offset(cell_num)
        node[offset : offset + len(cellptrs)] = cellptrs

    @staticmethod
    def set_leaf_node_num_cells(node: bytes, num_cells: int):
        """
        write num of node cells: encode to int
        """
        value = num_cells.to_bytes(LEAF_NODE_NUM_CELLS_SIZE, sys.byteorder)
        node[
            LEAF_NODE_NUM_CELLS_OFFSET : LEAF_NODE_NUM_CELLS_OFFSET
            + LEAF_NODE_NUM_CELLS_SIZE
        ] = value

    @staticmethod
    def set_leaf_node_cell(node: bytes, cell_offset: int, cell: bytes):
        """
        write cell to given offset

        :param cell_offset: abs offset on node
        :param cell: cell to write
        """
        node[cell_offset : cell_offset + len(cell)] = cell

    @staticmethod
    def set_leaf_node_free_list_head(node: bytes, head: int):
        """

        :param node:
        :param head: offset to head of free list, i.e. free node location
        :return:
        """
        value = head.to_bytes(LEAF_NODE_FREE_LIST_HEAD_POINTER_SIZE, sys.byteorder)
        node[
            LEAF_NODE_FREE_LIST_HEAD_POINTER_OFFSET : LEAF_NODE_FREE_LIST_HEAD_POINTER_OFFSET
            + LEAF_NODE_FREE_LIST_HEAD_POINTER_SIZE
        ] = value

    @staticmethod
    def set_leaf_node_total_free_list_space(node: bytes, total_free_space: int) -> int:
        value = total_free_space.to_bytes(
            LEAF_NODE_TOTAL_FREE_LIST_SPACE_SIZE, sys.byteorder
        )
        node[
            LEAF_NODE_TOTAL_FREE_LIST_SPACE_OFFSET : LEAF_NODE_TOTAL_FREE_LIST_SPACE_OFFSET
            + LEAF_NODE_TOTAL_FREE_LIST_SPACE_SIZE
        ] = value

    # section: btree debugging utilities

    @staticmethod
    def depth_to_indent(depth: int) -> str:
        """
        maybe this should go in its own dedicated class
        :param depth:
        :return:
        """
        return " " * (depth * 4)

    def print_nodes(self, nodes: dict):
        """
        utility to print multiple nodes simultaneously
        :param nodes: a dict of node_name(str) -> node_page_num(int)
        :return:
        """
        assert isinstance(nodes, dict)
        for name, page_num in nodes.items():
            if page_num is None:
                continue
            logging.debug(f"printing node: {page_num}")
            self.print_tree(page_num)

    def print_tree(self, root_page_num: int = None, depth: int = 0):
        """
        print entire tree node by node, starting at an optional node
        :param root_page_num:
        :param depth: depth of current invocation (used for formatting indentation)
        """
        if root_page_num is None:
            # start from tree root
            root_page_num = self.root_page_num

        indent = self.depth_to_indent(depth)
        # root of invocation; not necessarily global root
        root = self.pager.get_page(root_page_num)
        parent_num = (
            "NULLPTR" if self.is_node_root(root) else self.get_parent_page_num(root)
        )
        if self.get_node_type(root) == NodeType.NodeLeaf:
            body = f"printing leaf node at page num: [{root_page_num}]. parent: [{parent_num}]"
            divider = f"{indent}{len(body) * '.'}"
            print(f"{indent}{body}")
            print(divider)
            self.print_leaf_node(root, depth=depth)
            print(divider)
        else:
            body = f".. printing internal node at page num: [{root_page_num}]. parent: [{parent_num}] .."
            divider = f"{indent}{len(body) * '.'}"

            print(divider)
            print(f"{indent}{body}")
            print(divider)
            self.print_internal_node(root, recurse=True, depth=depth)
            print(divider)

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
            self.print_tree(child_page_num, depth=depth + 1)

    @staticmethod
    def print_leaf_node(node: bytes, depth: int = 0):
        num_cells = Tree.leaf_node_num_cells(node)
        indent = Tree.depth_to_indent(depth)
        alloc_ptr = Tree.leaf_node_alloc_ptr(node)
        total_free_list_space = Tree.leaf_node_total_free_list_space(node)
        print(
            f"{indent}leaf (size: {num_cells}, alloc_ptr: {alloc_ptr}, free_list_space: {total_free_list_space})"
        )
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
                    assert child_key == child_max_key, (
                        f"Expected at pos [{child_num}] key [{child_key}]; child-max-key: {child_max_key}; parent_page_num: "
                        f"{node_page_num}, child_page_num: {child_page_num}"
                    )

                    # validate that child's ref to parent page num is correct
                    child_parent_ref = self.get_parent_page_num(child_node)
                    assert child_parent_ref == node_page_num, (
                        f"child ref to parent [{child_parent_ref}], does not match parent page num: [{node_page_num}] "
                        f"child page num is {child_page_num}"
                    )

    def validate_ordering(self) -> bool:
        """
        traverse the tree, starting at root, and ensure values are ordered as expected

        :return:
            raises AssertionError on failure
            True on success
        """
        stack = [(self.root_page_num, float("-inf"), float("inf"))]
        while stack:
            node_page_num, lower_bound, upper_bound = stack.pop()
            node = self.pager.get_page(node_page_num)
            if self.get_node_type(node) == NodeType.NodeInternal:
                # print(f"validating internal node on page_num: {node_page_num}")
                # self.print_internal_node(node, recurse=False)

                # validate inner keys are ordered
                for child_num in range(self.internal_node_num_keys(node)):
                    key = self.internal_node_key(node, child_num)
                    assert (
                        lower_bound < key
                    ), f"validation: global lower bound [{lower_bound}] constraint violated [{key}]"
                    assert (
                        upper_bound >= key
                    ), f"validation: global upper bound [{upper_bound}] constraint violated [{key}]"

                    if child_num > 0:
                        prev_key = self.internal_node_key(node, child_num - 1)
                        # validation: check if all of node's key are ordered
                        assert key > prev_key, (
                            f"validation: internal node siblings must be strictly greater key: {key}. "
                            f"prev_key:{prev_key}"
                        )

                    # add children to stack
                    child_page_num = self.internal_node_child(node, child_num)
                    # lower bound is prev child for non-zero child, and parent's lower bound for 0-child
                    child_lower_bound = (
                        self.internal_node_key(node, child_num - 1)
                        if child_num > 0
                        else lower_bound
                    )
                    # upper bound is key value for non-right children
                    child_upper_bound = self.internal_node_key(node, child_num)
                    stack.append((child_page_num, child_lower_bound, child_upper_bound))

                # validate right is the max key, if inner children exist
                num_keys = self.internal_node_num_keys(node)
                if num_keys > 0:
                    inner_max_key = self.internal_node_key(node, num_keys - 1)
                    if not self.internal_node_has_right_child(node):
                        # a 0-ary node indicates something wrong with some operation
                        raise ValueError(f"node(page_num: {node_page_num}) is 0-ary")

                    right_key = self.get_node_max_key(
                        self.pager.get_page(self.internal_node_right_child(node))
                    )
                    assert inner_max_key is not None
                    assert right_key is not None
                    assert (
                        inner_max_key < right_key
                    ), f"Expected right child key [{right_key}] to be strictly greater than max-inner-key: {inner_max_key}"

                # add right child
                child_page_num = self.internal_node_right_child(node)
                # lower bound is last key
                child_lower_bound = self.internal_node_key(
                    node, self.internal_node_num_keys(node) - 1
                )
                stack.append((child_page_num, child_lower_bound, upper_bound))

            else:  # leaf node
                # print(f"validating leaf node on page_num: {node_page_num}")
                # self.print_leaf_node(node)
                for cell_num in range(self.leaf_node_num_cells(node)):
                    if cell_num > 0:
                        key = self.leaf_node_key(node, cell_num)
                        prev_key = self.leaf_node_key(node, cell_num - 1)
                        # validation: check if all of node's key are ordered
                        assert (
                            key > prev_key
                        ), "validation: leaf node siblings must be strictly greater"

        return True
