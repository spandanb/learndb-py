from constants import INTERNAL_NODE_MAX_CELLS
from dataexchange import Response, Row
from btree import Tree, NodeType, TreeInsertResult, TreeDeleteResult
from table import Table


class Cursor:
    """
    Represents a cursor. A cursor understands how to navigate
    a database (on-disk) page, i.e. reading and understanding header values.
    A cursor exposes an interface to read, insert and delete rows.
    """
    def __init__(self, pager: 'Pager', tree: 'Tree'):
        self.tree = tree
        self.pager = pager
        self.page_num = tree.root_page_num
        self.cell_num = 0
        self.end_of_table = False
        self.first_leaf()

    def first_leaf(self):
        """
        set cursor location to left-most/first leaf
        """
        # start with root and descend until we hit left most leaf
        node = self.pager.get_page(self.page_num)
        while Tree.get_node_type(node) == NodeType.NodeInternal:
            assert Tree.internal_node_has_right_child(node), "invalid tree with no right child"
            if Tree.internal_node_num_keys(node) == 0:
                # get right child- unary tree
                child_page_num = Tree.internal_node_right_child(node)
            else:
                child_page_num = Tree.internal_node_child(node, 0)
            self.page_num = child_page_num
            node = self.pager.get_page(child_page_num)

        self.cell_num = 0
        # node must be leaf node
        self.end_of_table = (Tree.leaf_node_num_cells(node) == 0)

    def get_cell(self) -> bytes:
        """
        return cell pointed by cursor
        :return:
        """
        node = self.pager.get_page(self.page_num)
        cell = Tree.leaf_node_cell(node, self.cell_num)
        return cell

    def insert_row(self, row: Row) -> Response:
        """
        # TODO: nuke me; do this op by directly invoking tree method
        insert row
        :return:
        """
        serialized = Table.serialize(row)
        response = self.tree.insert(row.identifier, serialized)
        if response == TreeInsertResult.Success:
            return Response(True)
        else:
            assert response == TreeInsertResult.DuplicateKey
            return Response(False, status=TreeInsertResult.DuplicateKey)

    def delete_key(self, key: int) -> Response:
        """
        # TODO: nuke me; do this op by directly invoking tree method
        delete key from table

        :param key:
        :return:
        """
        response = self.tree.delete(key)
        if response == TreeDeleteResult.Success:
            return Response(True)

    def next_leaf(self):
        """
        move self.page_num and self.cell_num to next leaf and next cell
        this method requires the self.page_num start at a leaf node.

        NOTE: if starting from an internal node, to get to a leaf use `first_leaf` method
        :return:
        """
        # starting point
        node = self.pager.get_page(self.page_num)
        if Tree.is_node_root(node) is True:
            # there is nothing
            self.end_of_table = True
            return

        node_max_value = self.tree.get_node_max_key(node)
        assert node_max_value is not None

        parent_page_num = Tree.get_parent_page_num(node)
        # check if current page, i.e. self.page_num is right most child of it's parent
        parent = self.pager.get_page(parent_page_num)
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
        advance the cursor
         1) from left most leaf node to right most leaf node
         2) from leftmost cell to right most cell
        :return:
        """
        # advance always start at leaf node and ends at a leaf node;
        # starting at or ending at an internal node means the cursor is inconsistent
        node = self.pager.get_page(self.page_num)
        # we are currently on the last cell in the node
        # go to the next node if it exists
        if self.cell_num >= Tree.leaf_node_num_cells(node) - 1:
            self.next_leaf()
        else:
            self.cell_num += 1
