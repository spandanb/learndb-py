from __future__ import annotations
import logging
from typing import Optional, Tuple, Union

from constants import CATALOG
from cursor import Cursor
from btree import Tree, TreeInsertResult, TreeDeleteResult
from statemanager import StateManager
from schema import generate_schema, Schema, schema_to_ddl
from serde import serialize_record, deserialize_cell
from dataexchange import Response

from lang_parser.visitor import Visitor
from lang_parser.tokens import TokenType
from lang_parser.symbols import (
    Symbol,
    Program,
    CreateStmnt,
    SelectExpr,
    DropStmnt,
    InsertStmnt,
    DeleteStmnt,
    UpdateStmnt,
    TruncateStmnt,
    Joining,
    JoinType,
    OnClause,
    AliasableSource,
    WhereClause
)
from lang_parser.sqlhandler import SqlFrontEnd
from record_utils import (
    create_record,
    create_null_record,
    create_catalog_record,
    join_records,
    MultiRecord,
    Record,
)

# section: exceptions


class NameResolutionError(Exception):
    """
    Unable to resolve name
    """
    pass


class ExecutionException(Exception):
    """
    Some error while VM was running
    """
    pass


# section: helper classes

class RecordSetIter:
    """
    This is an iterator over a RecordSet
    """
    def __init__(self, record_set: 'RecordSet'):
        self.record_set = record_set
        self.recordidx = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.recordidx >= len(self.record_set):
            raise StopIteration
        value = self.record_set[self.recordidx]
        self.recordidx += 1
        return value


class SerializedRecordIter:
    """
    This is an iterator of serialized records.
    This should encapsulate the entire logic around
    cursor advancing and yielding records.

    The reason for creating and naming this, is so that
    there can be uniform API around DeserializedRecordIter,
    which would contain in-memory record objects, e.g. from a join
    op.
    """
    def __init__(self, cursor, schema):
        self.cursor = cursor
        self.schema = schema

    def __iter__(self):
        return self

    def __next__(self):
        if self.cursor.end_of_table:
            raise StopIteration

        cell = self.cursor.get_cell()
        resp = deserialize_cell(cell, self.schema)
        assert resp.success
        record = resp.body
        self.cursor.advance()
        return record


class RecordSet:
    """
    A iterable set of records.
    """
    def __init__(self):
        self.records = []

    def __len__(self):
        return len(self.records)

    def __getitem__(self, idx: int):
        return self.records[idx]

    def get_record(self, idx: int):
        return self.records[idx]

    def append(self, record):
        self.records.append(record)


class RecordAccessor:
    """
    Transparently provides a uniform access to column values in Record
    and MultiRecord objects.
    This is primarily intended to simplify how values are accessed
    when evaluating select clause

    """
    def __init__(self):
        # map of table name -> record
        self.records = {}
        self.multi_records = []

    def add_record(self, alias: str, record: Record):
        assert alias not in self.records
        self.records[alias] = record

    def add_multi_record(self, mrecord: MultiRecord):
        self.multi_records.append(mrecord)

    def get(self, name: str):
        """
        resolve name and return value corresponding to name
        first attempt to resolve from records dict; then multi_records
        name is in format <table alias>.<column name>
        :param name:
        :return:
        """
        assert '.' in name
        name_parts = name.split('.')
        assert len(name_parts) == 2
        table_alias, column_name = name_parts
        if table_alias in self.records:
            return self.records[table_alias].get(column_name)

        for mrecord in self.multi_records:
            if mrecord.contains(table_alias, column_name):
                return mrecord.get(table_alias, column_name)

        raise NameResolutionError(f"Unable to resolve [{name}]")


# section: VM


class VirtualMachine(Visitor):
    """
    Execute prepared statements corresponding to some sql statements, on some
    state. The state is encoded as a catalog (which maps to the table
    containing information of all objects (tables + indices).

    The VM implements different top-level methods corresponding
    to different sql statement type, e.g. create, update, delete, truncate statements.
    Each of these ops will typically have some of the following phases:
        - name resolution
        - optimize
        - execute op

    """
    def __init__(self, state_manager: StateManager, output_pipe: 'Pipe'):
        self.state_manager = state_manager
        self.output_pipe = output_pipe
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

    # section : top-level handlers

    def visit_program(self, program: Program) -> Response:
        for stmt in program.statements:
            # note sure how to collect result
            self.execute(stmt)
        return Response(True)

    def visit_create_stmnt(self, stmnt: CreateStmnt) -> Response:
        """

        :param stmnt:
        :return:
        """
        # print(f"In vm: creating table [name={stmnt.table_name}, cols={stmnt.column_def_list}]")

        # 1. generate schema from stmnt
        response = generate_schema(stmnt)
        if response.success is False:
            # schema generation failed
            return Response(False, error_message=f'schema generation failed due to [{response.error_message}]')

        # if generation succeeded, then schema is valid
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

    def visit_select_expr(self, expr: SelectExpr, parent_context = None):
        """
        Handle select expr.

        NOTE: this will need to handle 2 things
            - other clauses, i.e.. join, group by, order by, having
            - nested select expr
                -- this will require rethinking how select is exec'ed and results outputted

        :param expr:
        :param parent_context: unused- would be needed for correlated queries
        :return:
        """
        self.output_pipe.reset()

        record_set = self.materialize_source(expr.from_location)
        record_set_iter = RecordSetIter(record_set)

        # iterate record set over all records
        for record in record_set_iter:
            # evaluate where condition and filter non-matching rows
            if self.evaluate_where_clause(expr.where_clause, record) is True:
                # write to output if matches condition
                # todo: piped record should only contain selected column
                self.output_pipe.write(record)

    def visit_insert_stmnt(self, stmnt: InsertStmnt):
        """
        Handle insert stmnt

        :param stmnt:
        :return:
        """
        table_name = self.resolve_name(stmnt)

        # get schema
        schema = self.state_manager.get_schema(table_name)

        # extract values from stmnt and construct record
        # extract literal from tokens
        column_names = [col_token.literal for col_token in stmnt.column_name_list]
        # cast value literals into the correct type
        # vm is responsible for glue between parser and execution
        value_list = [value_token.value.literal for value_token in stmnt.value_list]
        resp = create_record(column_names, value_list, schema)
        assert resp.success, f"create record failed due to {resp.error_message}"
        record = resp.body
        # get table's tree
        tree = self.state_manager.get_tree(table_name)

        resp = serialize_record(record)
        assert resp.success, f"serialize record failed due to {resp.error_message}"

        cell = resp.body
        resp = tree.insert(cell)
        assert resp == TreeInsertResult.Success, f"Insert op failed with status: {resp}"

    def visit_delete_stmnt(self, stmnt: DeleteStmnt):
        """
        Handle delete stmnt.

        NOTE: the delete condition can cover multiple rows
        for now where cond is restricted to equality condition

        :param stmnt:
        :return:
        """
        # identifier are case sensitive, so don't convert
        table_name = self.resolve_name(stmnt)
        # check table is not catalog
        assert table_name != CATALOG, "cannot delete table from catalog; use drop table"

        # get tree and schema
        schema, tree = self.get_schema_and_tree(stmnt)

        # scan table and determine which keys to delete, based on where condition
        cursor = Cursor(self.state_manager.get_pager(), tree)
        # keys to delete based on where condition
        del_keys = []

        # get primary key column name
        primary_key_col = schema.get_primary_key_column()

        # iterate cursor
        while cursor.end_of_table is False:
            cell = cursor.get_cell()
            # NOTE: if the condition is only on the primary key,
            # an optimization could be to only deserialize the key and not the entire record
            resp = deserialize_cell(cell, schema)
            assert resp.success
            record = resp.body

            if self.evaluate_where_clause(stmnt.where_clause, record):
                del_key = record.get(primary_key_col)
                del_keys.append(del_key)

            cursor.advance()
        
        # delete matching keys
        for del_key in del_keys:
            resp = tree.delete(del_key)
            assert resp == TreeDeleteResult.Success, f"delete failed for key {del_key}"

    def visit_drop_stmnt(self, stmnt: DropStmnt):
        """
        handle drop statement to drop a table from catalog
        :param stmnt:
        :return:
        """
        table_name = self.resolve_name(stmnt)

        catalog_tree = self.state_manager.get_catalog_tree()
        catalog_schema = self.state_manager.get_catalog_schema()
        # scan table and determine which keys to delete, based on where condition
        cursor = Cursor(self.state_manager.get_pager(), catalog_tree)
        # keys to delete based on where condition
        # there should only be a single key, i.e. the table with name
        del_keys = []

        raise NotImplementedError

    def visit_truncate_stmnt(self, stmnt: TruncateStmnt):
        pass

    def visit_update_stmnt(self, stmnt: UpdateStmnt):
        pass

    # section : sub-statement handlers

    @staticmethod
    def evaluate_on_clause(on_clause: OnClause, raccessor: RecordAccessor) -> bool:
        """
        evaluate on condition

        :return:
        """
        if on_clause is None:
            # no-condition; all results pass
            return True

        # result of or over all or-clauses
        or_result = False
        # NOTE: `on_clause.or_clause` is a list of and'ed predicates
        # at least one and clause must be true for condition to be true
        for and_clause in on_clause.or_clause:
            # result of and over and clauses within or-clause
            and_result = True
            for predicate in and_clause.predicates:
                # decompose predicate, and resolve value
                # the predicate contains 2 operands: left and right;
                # the operands could either be a column reference, e.g. foo.colx or a literal value, e.g. 4
                left_token = predicate.first.value
                right_token = predicate.second.value

                if left_token.token_type == TokenType.IDENTIFIER:
                    # left_token is a column reference
                    left_value = raccessor.get(left_token.literal)
                else:
                    # left token is a literal value
                    left_value = left_token.literal

                if right_token.token_type == TokenType.IDENTIFIER:
                    right_value = raccessor.get(right_token.literal)
                else:
                    right_value = right_token.literal

                # evaluate predicate
                if predicate.op.token_type == TokenType.EQUAL:
                    pred_val = left_value == right_value
                elif predicate.op.token_type == TokenType.NOT_EQUAL:
                    pred_val = left_value != right_value
                elif predicate.op.token_type == TokenType.LESS_EQUAL:
                    pred_val = left_value <= right_value
                elif predicate.op.token_type == TokenType.LESS:
                    pred_val = left_value < right_value
                elif predicate.op.token_type == TokenType.GREATER_EQUAL:
                    pred_val = left_value >= right_value
                else:
                    assert predicate.op.token_type == TokenType.GREATER
                    pred_val = left_value > right_value

                and_result = and_result and pred_val

            or_result = or_result or and_result
            if or_result:
                # condition is true, eagerly exit
                return True
        return False

    @staticmethod
    def evaluate_where_clause(where_clause: WhereClause, record: Record) -> bool:
        """
        evaluate condition

        on (join) could be implemented similarly
        :param where_clause:
        :param record:
        :return:
        """
        if where_clause is None:
            # no-condition; all results pass
            return True

        # result of or over all or-clauses
        or_result = False
        # NOTE: `where_clause.or_clause` is a list of and'ed predicates
        # at least one and clause must be true for condition to be true
        for and_clause in where_clause.or_clause:
            # result of and over and clauses within or-clause
            and_result = True
            for predicate in and_clause.predicates:
                # decompose predicate
                # determine which operand contains value and which contains column name
                left_token = predicate.first.value
                right_token = predicate.second.value
                if left_token.token_type == TokenType.IDENTIFIER:
                    column = left_token.literal
                    cond_value = right_token.literal
                else:
                    cond_value = left_token.literal
                    column = right_token.literal

                # determine the record's value for given column
                record_value = record.get(column)
                # evaluate predicate
                if predicate.op.token_type == TokenType.EQUAL:
                    pred_val = record_value == cond_value
                elif predicate.op.token_type == TokenType.NOT_EQUAL:
                    pred_val = record_value != cond_value
                elif predicate.op.token_type == TokenType.LESS_EQUAL:
                    pred_val = record_value <= cond_value
                elif predicate.op.token_type == TokenType.LESS:
                    pred_val = record_value < cond_value
                elif predicate.op.token_type == TokenType.GREATER_EQUAL:
                    pred_val = record_value >= cond_value
                else:
                    assert predicate.op.token_type == TokenType.GREATER
                    pred_val = record_value > cond_value

                and_result = and_result and pred_val

            or_result = or_result or and_result
            if or_result:
                # condition is true, eagerly exit
                return True
        return False

    @staticmethod
    def resolve_name(symbol: Symbol) -> str:
        """
        attempt to resolve table name by inspecting argument symbol
        :param symbol:
        :return:
        """
        if isinstance(symbol, AliasableSource):
            return symbol.source_name.literal.lower()
        elif isinstance(symbol, (DeleteStmnt, InsertStmnt, DropStmnt, TruncateStmnt, UpdateStmnt)):
            return symbol.table_name.literal.lower()
        raise NameResolutionError(f"Unable to resolve [{symbol}]")

    def get_record_iter(self, source: AliasableSource) -> SerializedRecordIter:
        """
        Return iterator over source records.

        :return:
        """
        schema, tree = self.get_schema_and_tree(source)
        cursor = Cursor(self.state_manager.get_pager(), tree)
        return SerializedRecordIter(cursor, schema)

    def get_null_record(self, source: AliasableSource) -> Record:
        """
        Return a null record for given source.
        A null record is one which has the structure consistent with
        the schema, but all the values are set to null.

        :param source:
        :return:
        """
        schema = self.get_schema(source)
        return create_null_record(schema)

    def get_schema(self, source: Symbol) -> Schema:
        """
        helper to resolve name and return schema
        :param source:
        :return:
        """
        table_name = self.resolve_name(source)
        if table_name != CATALOG and not self.state_manager.table_exists(table_name):
            raise ExecutionException(f"table [{table_name}] does not exist")

        return self.state_manager.get_catalog_schema() if table_name == CATALOG else \
            self.state_manager.get_schema(table_name)

    def get_schema_and_tree(self, source: Symbol) -> Tuple[Schema, Tree]:
        """
        helper to resolve name and return schema and tree corresponding
        to name
        :param source:
        :return:
        """
        table_name = self.resolve_name(source)
        if table_name != CATALOG and not self.state_manager.table_exists(table_name):
            raise ExecutionException(f"table [{table_name}] does not exist")

        if table_name == CATALOG:
            # system table/objects
            schema = self.state_manager.get_catalog_schema()
            tree = self.state_manager.get_catalog_tree()
        else:
            # user tables/objects
            schema = self.state_manager.get_schema(table_name)
            tree = self.state_manager.get_tree(table_name)

        return schema, tree

    def materialize_source(self, source: Union[AliasableSource, Joining]) -> RecordSet:
        """
        This should handle the materialization of source.
        The source is whatever is in the from clause.
        For now, this includes:
            - single tables
            - joined tables
        eventually, will also have to handle nested select expr,
        both correlated and uncorrelated.

        :return:
        """
        rset = RecordSet()
        # 1. handle single source
        if isinstance(source, AliasableSource):
            # 1.1. add each record in source to result set
            for record in self.get_record_iter(source):
                rset.append(record)
            return rset

        assert isinstance(source, Joining)
        # 2. handle joined sources
        stack = []
        # 2.1. materialize the most nested element in the joining first
        while isinstance(source, Joining):
            stack.append(source)
            # 2.2. check if joining has a nested joining
            # the parser nests joins s.t. left source is the child join
            source = source.left_source

        # 3. perform join by walking up the recursive stack
        is_innermost_join = True
        while stack:
            joining = stack.pop()
            # 3.1. get left source
            # NOTE: in most nested joining, left source will be a table
            # however, after that, it will be a previously joined recordSet
            if is_innermost_join:
                left_src = joining.left_source
                left_record_iter = self.get_record_iter(left_src)
                assert left_src.alias_name and left_src.alias_name.literal is not None, "alias is required"
                left_alias = left_src.alias_name.literal
            else:
                left_record_iter = RecordSetIter(rset)
                left_alias = None

            # 3.2. get right source
            right_src = joining.right_source
            assert right_src.alias_name and right_src.alias_name.literal is not None, "alias is required"
            right_alias = right_src.alias_name.literal

            # 3.3. rset created for next join
            next_rset = RecordSet()

            # for left, right, and full outer join, if evaluation fails,
            # there should be one and only one record
            # with other side column set to null

            # track first iter of right source
            # so we can build index of right source record indices
            right_src_first_iter = True
            # track row ids of right records that have not been joined
            right_src_non_joined_rowids = set()

            # 3.4. do nested loop to join left and right sources
            for left_record in left_record_iter:
                # flag to track whether `left_record` has joined with any
                # records in right_record; this is needed for left, and full outer join
                has_joined_right_record = False

                # 3.4.1. iterate over record in right source
                for right_rowid, right_record in enumerate(self.get_record_iter(right_src)):
                    # add joined record if cross join or on condition is true
                    # 3.4.1.1. create a record accessor to access column values in `left_` and `right_record`
                    raccessor = RecordAccessor()
                    # 3.4.1.2. add left_record to record accessor
                    if left_alias is None:
                        # left is a multi-record
                        raccessor.add_multi_record(left_record)
                    else:
                        raccessor.add_record(left_alias, left_record)

                    # 3.4.1.3. add right_record to record accessor
                    raccessor.add_record(right_alias, right_record)

                    if right_src_first_iter:
                        # only add during first iteration of right src
                        right_src_non_joined_rowids.add(right_rowid)

                    # 3.4.1.4. check if the on clause evaluates to true
                    evaluation = self.evaluate_on_clause(joining.on_clause, raccessor)
                    if evaluation:
                        # on-condition evaluated true; add joined record
                        # to result set
                        joined_record = join_records(left_record, right_record, left_alias, right_alias)
                        next_rset.append(joined_record)
                        has_joined_right_record = True
                        # remove this row id- since we've
                        right_src_non_joined_rowids.remove(right_rowid)

                # 3.4.2. check if we need to add empty record for left or outer join
                if not has_joined_right_record and \
                        (joining.join_type == JoinType.LeftOuter or joining.join_type == JoinType.FullOuter):
                    # create null right record
                    right_null_record = self.get_null_record(right_src)
                    # join record
                    joined_record = join_records(left_record, right_null_record, left_alias, right_alias)
                    # attach to record set
                    next_rset.append(joined_record)

                right_src_first_iter = False

            # 3.5. for right, and full outer join add any records from right source
            # that was not joined add added to result set
            if joining.join_type == JoinType.RightOuter or joining.join_type == JoinType.FullOuter:
                # TODO: create left_null_record correctly
                left_null_record = self.get_null_record(left_src)
                for right_rowid, right_record in enumerate(self.get_record_iter(right_src)):
                    if right_rowid in right_src_non_joined_rowids:
                        # join record
                        joined_record = join_records(left_null_record, right_record, left_alias, right_alias)
                        # attach to record set
                        next_rset.append(joined_record)

            # prepare for next join op
            # inner_most join is only True one
            is_innermost_join = False
            rset = next_rset

        return rset
