"""
Rewrite of virtual machine class.
This focuses on select execution- later merge
the rest of the logic from the old VM in here

"""
import logging
import random
import string

from dataexchange import Response

from lang_parser.visitor import Visitor
from lang_parser.symbols import (
    _Symbol as Symbol,
    Program,
    SelectStmnt,
    Joining,
    FromClause,
    #SingleSourceX as SingleSource,
    SingleSource,
    ConditionedJoinX as ConditionedJoin,
    UnconditionedJoin
)


logger = logging.getLogger(__name__)


class RecordSet:
    pass


class GroupedRecordSet(RecordSet):
    pass


class NullRecordSet(RecordSet):
    pass


class VirtualMachine(Visitor):
    """
    New virtual machine.
    Compared to the previous impl; this will add or improve:
        - api for creating, manipulating, and combining RecordSets

    There are 2 flavors of RecordSets: RecordSet, GroupedRecordSet, and possibly a EmptyRecordSet
    The RS api would consist of:
        init_recordset(name)
        append_recordset(name, record)
        drop_recordset(name)

        init_groupedrecordset(name, group_vec)
        append_groupedrecordset(name, record)
        drop_groupedrecordset(name)

    These will likely be backed by simple lists, and dicts of lists.
    The main reason for doing it this way; instead of making rich
    RecordSet types, is because that would require logic for interpreting
    symbols into RecordSet; and I want that logic to not be replicated outside vm.

    Should "state" be managed by the statemanager?

    """
    def __init__(self, statemanager, pipe):
        # should these part of the statemanager?
        self.rsets = {}
        self.grouprsets = {}
        # todo: init_catalog

    def init_catalog(self):
        pass

    def run(self, program):
        """
        run the virtual machine with program on state
        :param program:
        :return:
        """
        result = []
        for stmt in program.statements:
            try:
                result.append(self.execute(stmt))
            except Exception:
                logging.error(f"ERROR: virtual machine failed on: [{stmt}]")
                raise

    def execute(self, stmnt: Symbol):
        """
        execute statement
        :param stmnt:
        :return:
        """
        return stmnt.accept(self)

    # section : top-level statement handlers

    def visit_program(self, program: Program) -> Response:
        for stmt in program.statements:
            # note sure how to collect result
            self.execute(stmt)
        return Response(True)

    def visit_create_table_stmnt(self, stmnt) -> Response:
        pass

    def visit_select_stmnt(self, stmnt) -> Response:
        """

        """

        self.materialize(stmnt.from_clause)


    # section : statement helpers
    # general principles:
    # 1) helpers should be able to handle null types

    def materialize(self, from_clause: FromClause) -> Response:
        """
        this and other materialize methods should return Response(recordSetName: str);
        but this already shows a limitation of this

        """
        source = from_clause.source
        if isinstance(source, SingleSource):
            # NOTE: single source means a single physical table
            return self.materialize_single_source(source)

        elif isinstance(source, Joining):
            return self.materialize_joining(source)
        # case nestedSelect

    def materialize_single_source(self, source: SingleSource) -> Response:
        """
        Materialize single source and return
        """
        resp = self.init_recordset()
        assert resp.success
        rsname = resp.body

        # get schema for table, and cursor on tree corresponding to table
        schema, tree = self.get_schema_and_tree(source)
        cursor = Cursor(self.state_manager.get_pager(), tree)
        # iterate over entire table
        while self.cursor.end_of_table is False:
            cell = self.cursor.get_cell()
            resp = deserialize_cell(cell, schema)
            assert resp.success
            record = resp.body
            self.append_recordset(rsname, record)
            # advance cursor
            cursor.advance()
        return Response(True, body=rsname)

    def materialize_joining(self, source: Joining) -> Response:
        resp = self.init_recordset()
        assert resp.success
        rsname = resp.body

        stack = []
        ptr = source
        # while False:
        while True:
            # recurse down
            if isinstance(ptr, Joining):
                # not sure if this is how it'll work
                stack.append(ptr)
            else:
                pass



    # section: record set utilities

    def init_recordset(self) -> Response:
        """
        initialize recordset; this requires a unique name
        for each recordset
        """
        gen_randkey = lambda: tuple(random.choice(string.ascii_letters) for i in range(10))
        name = gen_randkey()
        while name in self.rsets:
            # generate while non-unique
            name = gen_randkey()

        return Response(True, body=name)

    def append_recordset(self, name: str, record):
        assert name in self.rsets
        self.rsets[name].append(record)

    def drop_recordset(self, name:str):
        pass
