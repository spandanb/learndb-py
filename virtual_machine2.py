"""
Rewrite of virtual machine class.
This focuses on select execution- later merge
the rest of the logic from the old VM in here

"""
import logging
import random
import string

from typing import List

from btree import Tree, TreeInsertResult, TreeDeleteResult
from cursor import Cursor
from dataexchange import Response
from schema import generate_schema, schema_to_ddl
from serde import serialize_record, deserialize_cell

from lark import Token
from lang_parser.visitor import Visitor
from lang_parser.symbols3 import (
    Symbol,
    Program,
    CreateStmnt,
    FromClause,
    SingleSource,
    UnconditionedJoin,
    JoinType,
    Joining,
    ConditionedJoin,
    Comparison,
    ComparisonOp

)

from lang_parser.sqlhandler import SqlFrontEnd

from record_utils import (
    create_catalog_record,
    join_records,
    create_record,
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

    """
    def __init__(self, state_manager, output_pipe):
        self.state_manager = state_manager
        self.output_pipe = output_pipe
        # NOTE: some state is managed by the state_manager, and rest via
        # vm's instance variable. Ultimately, there should be a cleaner,
        # unified management of state
        self.rsets = {}
        self.grouprsets = {}
        self.init_catalog()

    def init_catalog(self):
        """
        Initialize the catalog
        read the catalog, materialize table metadata and register with the statemanager
        :return:
        """
        # get/register all tables' metadata from catalog
        catalog_tree = self.state_manager.get_catalog_tree()
        catalog_schema = self.state_manager.get_catalog_schema()
        pager = self.state_manager.get_pager()
        cursor = Cursor(pager, catalog_tree)

        # need parser to parse schema definition
        parser = SqlFrontEnd()

        # iterate over table entries
        while cursor.end_of_table is False:
            cell = cursor.get_cell()
            resp = deserialize_cell(cell, catalog_schema)
            assert resp.success, "deserialize failed while bootstrapping catalog"
            table_record = resp.body

            # get schema by parsing sql_text
            sql_text = table_record.get("sql_text")
            logging.info(f"bootstrapping schema from [{sql_text}]")
            parser.parse(sql_text)
            assert parser.is_success(), "catalog sql parse failed"
            program = parser.get_parsed()
            assert len(program.statements) == 1
            stmnt = program.statements[0]
            assert isinstance(stmnt, CreateStmnt)
            resp = generate_schema(stmnt)
            assert resp.success, "schema generation failed"
            table_schema = resp.body

            # get tree
            # should vm be responsible for this
            tree = Tree(self.state_manager.get_pager(), table_record.get("root_pagenum"))

            # register schema
            self.state_manager.register_schema(table_record.get("name"), table_schema)
            self.state_manager.register_tree(table_record.get("name"), tree)

            cursor.advance()

    def run(self, program, stop_on_err=False) -> List:
        """
        run the virtual machine with program on state
        :param program:
        :param stop_on_err: stop program execution on first error
        :return:
        """
        result = []
        for stmt in program.statements:
            try:
                resp = self.execute(stmt)
                # todo: why isn't resp always Response
                if isinstance(resp, Response) and not resp.success:
                    logging.warning(f"Statement [{stmt}] failed with {resp}")
                result.append(resp)
                if not resp and stop_on_err:
                    return result
            except Exception as e:
                logging.error(f"ERROR: virtual machine errored on: [{stmt}] with [{e}|]")
                # ultimately this should not throw
                raise
        return result

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

    def visit_create_stmnt(self, stmnt: CreateStmnt) -> Response:
        """
        Handle create stmnt
        generate, validate, and persisted schema.
        """
        # 1.attempt to generate schema from create_stmnt
        response = generate_schema(stmnt)
        if not response.success:
            # schema generation failed
            return Response(False, error_message=f'schema generation failed due to [{response.error_message}]')
        table_schema = response.body
        table_name = table_schema.name
        assert isinstance(table_name, str), "table_name is not string"

        # 2. check whether table name is unique
        assert self.state_manager.table_exists(table_name) is False, f"table {table_name} exists"

        # 3. allocate tree for new table
        page_num = self.state_manager.allocate_tree()

        # 4. construct record for table
        # NOTE: for now using page_num as unique int key
        pkey = page_num
        sql_text = schema_to_ddl(table_schema)
        # logging.info(f'visit_create_stmnt: generated DDL: {sql_text}')
        catalog_schema = self.state_manager.get_catalog_schema()
        response = create_catalog_record(pkey, table_name, page_num, sql_text, catalog_schema)
        if not response.success:
            return Response(False, error_message=f'Failure due to {response.error_message}')

        # 5. serialize table record, i.e. record in catalog table for new user table
        table_record = response.body
        response = serialize_record(table_record)
        if not response.success:
            return Response(False, error_message=f'Serialization failed: [{response.error_message}]')

        # 6. insert entry into catalog tree
        cell = response.body
        catalog_tree = self.state_manager.get_catalog_tree()
        catalog_tree.insert(cell)

        # 7. register schema
        self.state_manager.register_schema(table_name, table_schema)
        # 8. register tree
        tree = Tree(self.state_manager.get_pager(), table_record.get("root_pagenum"))
        self.state_manager.register_tree(table_name, tree)

    def visit_select_stmnt(self, stmnt) -> Response:
        """
        handle select stmnt
        most clauses are optional
        """
        # 1. setup
        self.output_pipe.reset()

        # 2. check and handle from clause
        from_clause = stmnt.from_clause
        if from_clause:
            # materialize source in from clause
            resp = self.materialize(stmnt.from_clause.source.source)
            if not resp.success:
                return Response(False, error_message=f"[from_clause] source materialization failed due to {resp.error_message}")
            rsname = resp.body

            # 3. apply filter on source - where clause
            if from_clause.where_clause:
                resp = self.filter_results(from_clause.where_clause, rsname)
                if not resp.success:
                    return Response(False, error_message=f"[where_clause] filtering failed due to {resp.error_message}")
                # filtering produces a new resultset
                rsname = resp.body

            if from_clause.group_by_clause:
                pass
            if from_clause.having_clause:
                pass
            if from_clause.order_by_clause:
                pass
            if from_clause.limit_clause:
                pass

            for record in self.recordset_iter(rsname):
                # todo: create records with only selected columns
                self.output_pipe.write(record)

        # output pipe for sanity
        # for msg in self.output_pipe.store:
        #    logger.info(msg)


    def visit_insert_stmnt(self, stmnt) -> Response:
        """
        handle insert stmnt
        """
        # todo: error if table does not exist
        # get schema
        schema = self.state_manager.get_schema(stmnt.table_name)
        resp = create_record(stmnt.column_name_list, stmnt.value_list, schema)
        if not resp.success:
            return Response(False, error_message=f"Insert record failed due to [{resp.error_message}]")

        record = resp.body
        # get table's tree
        tree = self.state_manager.get_tree(stmnt.table_name)

        resp = serialize_record(record)
        assert resp.success, f"serialize record failed due to {resp.error_message}"

        cell = resp.body
        resp = tree.insert(cell)
        assert resp == TreeInsertResult.Success, f"Insert op failed with status: {resp}"

    # section : statement helpers
    # general principles:
    # 1) helpers should be able to handle null types

    def get_schema(self, table_name):
        if table_name.lower() == "catalog":
            return self.state_manager.get_catalog_schema()
        else:
            return self.state_manager.get_schema(table_name)

    def get_tree(self, table_name):
        if table_name.lower() == "catalog":
            return self.state_manager.get_catalog_tree()
        else:
            return self.state_manager.get_tree(table_name)

    def materialize(self, source) -> Response:
        """
        this and other materialize methods should return Response(recordSetName: str);
        but this already shows a limitation of this

        """
        if isinstance(source, SingleSource):
            # NOTE: single source means a single physical table
            return self.materialize_single_source(source)

        elif isinstance(source, Joining):
            return self.materialize_joining(source)

        else:
            raise ValueError(f"Unknown materialization source type {source}")
        # case nestedSelect

    def materialize_single_source(self, source: SingleSource) -> Response:
        """
        Materialize single source and return
        """
        resp = self.init_recordset()
        assert resp.success
        rsname = resp.body

        # get schema for table, and cursor on tree corresponding to table
        # do names need to be resolved?
        schema = self.get_schema(source.table_name)
        tree = self.get_tree(source.table_name)
        cursor = Cursor(self.state_manager.get_pager(), tree)
        # iterate over entire table
        while cursor.end_of_table is False:
            cell = cursor.get_cell()
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
        # parser places first table in a series of joins in the most nested
        # join; recursively traverse the join object(s) and construct a ordered list of
        # tables to materialize
        while True:
            # recurse down
            if isinstance(ptr.source, Joining):
                stack.append(ptr.source)
                ptr = ptr.source
            else:
                assert isinstance(ptr.source, SingleSource)
                # end of joining stack
                stack.append(ptr.source)
                break

        # now materialize joins
        first = stack.pop()
        resp = self.materialize(first)
        assert resp.success
        rsname = resp.body
        while stack:
            # join next source with existing rset
            next_join = stack.pop()
            assert isinstance(next_join, Joining)
            # NOTE: currently, materialize and join are separate steps
            # these could be combined; but for now keep them separate
            # to keep it simple
            resp = self.materialize_single_source(next_join.other_source)
            assert resp.success
            next_rsname = resp.body

            resp = self.join_recordset(next_join, rsname, next_rsname)
            assert resp.success
            rsname = resp.body

        return Response(True, body=rsname)

    def check_resolve_name(self, operand, record) -> Response:
        """
        Check if the `operand` is logical name, if so
        attempt to resolve it from record
        """
        if isinstance(operand, Token) and operand.type == "IDENTIFIER":
            # check if we can resolve this
            if operand in record.values:
                return Response(True, body=record.values[operand])
            return Response(False)
        else:
            # not name, return as is
            return Response(True, body=operand)

    def filter_results(self, where_clause, source_rsname: str) -> Response:
        """
        Apply where_clause on source_rsname and return
        """
        resp = self.init_recordset()
        assert resp.success
        # generate new result set
        rsname = resp.body

        for record in self.recordset_iter(source_rsname):
            or_result = False
            # or the result of each and_clause
            for and_clause in where_clause.condition.and_clauses:
                and_result = True
                for predicate in and_clause.predicates:
                    assert isinstance(predicate, Comparison), f"Expected Comparison received {predicate}"
                    # resolve any logical names
                    resp = self.check_resolve_name(predicate.left_op, record)
                    assert resp.success
                    left_value = resp.body
                    resp = self.check_resolve_name(predicate.right_op, record)
                    assert resp.success
                    right_value = resp.body
                    # value of evaluated predicate
                    pred_value = False
                    if predicate.operator == ComparisonOp.Greater:
                        pred_value = left_value > right_value
                    elif predicate.operator == ComparisonOp.Less:
                        pred_value = left_value < right_value
                    elif predicate.operator >= ComparisonOp.GreaterEqual:
                        pred_value = left_value >= right_value
                    elif predicate.operator == ComparisonOp.LessEqual:
                        pred_value = left_value <= right_value
                    elif predicate.operator == ComparisonOp.Equal:
                        pred_value = left_value == right_value
                    else:
                        assert predicate.operator == ComparisonOp.NotEqual
                        pred_value = left_value != right_value

                    and_result = and_result and pred_value
                    # todo: once an and_result is False, stop loop

                or_result = or_result or and_result

            if or_result:
                self.append_recordset(rsname, record)

        return Response(True, body=rsname)

    def join_recordset(self, join, left_rsname: str, right_rsname: str) -> Response:
        """
        join record based on record type and return joined recordset
        """
        resp = self.init_recordset()
        assert resp.success
        rsname = resp.body

        left_iter = self.recordset_iter(left_rsname)
        right_iter = self.recordset_iter(right_rsname)

        # inner join
        if join.join_type == JoinType.Inner:
            for left_rec in left_iter:
                for right_rec in right_iter:
                    if self.evaluate_on(join.condition, left_rec, right_rec):
                        joined = join_records(left_rec, right_rec)
                        self.append_recordset(rsname, joined)

        # todo: implement other joins

        return Response(True, body=rsname)

    def evaluate_on(self, condition, left_record, right_record):
        pass


    # section: record set utilities

    def gen_randkey(self, size=10):
        return "".join(random.choice(string.ascii_letters) for i in range(size))

    def init_recordset(self) -> Response:
        """
        initialize recordset; this requires a unique name
        for each recordset
        """
        name = self.gen_randkey()
        while name in self.rsets:
            # generate while non-unique
            name = self.gen_randkey()
        self.rsets[name] = []
        return Response(True, body=name)

    def append_recordset(self, name: str, record):
        assert name in self.rsets
        self.rsets[name].append(record)

    def drop_recordset(self, name: str):
        del self.rsets[name]

    def recordset_iter(self, name: str):
        """return an iterator over recordset"""
        return iter(self.rsets[name])
