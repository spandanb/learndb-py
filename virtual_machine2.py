"""
Rewrite of virtual machine class.
This focuses on select execution- later merge
the rest of the logic from the old VM in here
TODO: rename virtual_machine.py
"""
import logging
import random
import string

from typing import Any, List, Optional, Union, Set, Dict, Tuple, Type
from enum import Enum, auto
from collections import defaultdict

from btree import Tree, TreeInsertResult, TreeDeleteResult
from cursor import Cursor
from datatypes import DataType
from dataexchange import Response
from functions import resolve_function_name, is_aggregate_function, is_scalar_function
from schema import (generate_schema, generate_unvalidated_schema, schema_to_ddl, AbstractSchema, SimpleSchema,
                    ScopedSchema, make_grouped_schema, GroupedSchema, Column)
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
    ComparisonOp,
    WhereClause,
    TableName,
    HavingClause,
    SelectClause,
    FuncCall,
    ColumnName,
    OrClause
)

from lang_parser.sqlhandler import SqlFrontEnd

from record_utils import (
    SimpleRecord,
    GroupedRecord,
    create_catalog_record,
    join_records,
    ScopedRecord,
    create_record,
    create_null_record,
    create_record_from_raw_values
)

from value_generators import (ValueGeneratorFromRecordOverFunc, ValueExtractorFromRecord,
                              ValueGeneratorFromRecordOverExpr,
                              ValueGeneratorFromRecordGroupOverExpr)
from vm_utilclasses import ExpressionInterpreter, NameRegistry, SemanticAnalyzer

logger = logging.getLogger(__name__)


class UnGroupedColumnException(Exception):
    pass


# constants in scoped dict
SCOPE_COLLECTION_ALIASED_SOURCES_KEY = "aliased_sources"
SCOPE_COLLECTION_UNALIASED_SOURCES_KEY = "unaliased_sources"


class SelectClauseSourceType(Enum):
    """
    Represents the kind of source that the select clause is executed over;
    This passed from validation layer of select clause to evaluator
    """
    NoSource = auto()
    SimpleSource = auto()
    ScopedSource = auto()
    GroupedSource = auto()


class VirtualMachine(Visitor):
    """
    Learndb (New) Virtual machine.
    This essentially, runs a program (compiled AST) over a
    database (a set of tables backed by btrees).

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
        self.schemas = {}
        self.rsets = {}
        self.grouprsets = {}
        self.init_catalog()
        # scopes are tracked as a stack of scopes; one scopes for logical environment where names can be defined
        self.scopes = []
        self.name_registry = NameRegistry()
        self.interpreter = ExpressionInterpreter(self.name_registry)
        self.type_checker = SemanticAnalyzer(self.name_registry)

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

    def run(self, program: Program, stop_on_err=False) -> List:
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
        return_value = stmnt.accept(self)
        return return_value

    # section : top-level statement handlers

    def visit_program(self, program: Program) -> Response:
        """
        Visit a Program (list of statements)
        For each statement,
            - syntactic analysis of statement (implicitly done in constructing the Program)
            - TODO: validate statement, e.g. algebraic expressions don't violate type invariants, etc.
            - bind symbols to objects, e.g. column_name to Column object
            -- one wrinkle here is that some objects may be yet to be created
        """
        statemnt_return = []
        for stmt in program.statements:
            statemnt_return.append(self.execute(stmt))
        return Response(True, body=statemnt_return)

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
        table_name = table_schema.name.table_name
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
        # 1.1. create statement level scope
        self.init_new_scope()
        self.output_pipe.reset()

        # 2. check and handle from clause
        rsname = None  # name of result set
        from_clause = stmnt.from_clause
        if from_clause:
            # materialize source in from clause
            resp = self.materialize(stmnt.from_clause.source.source)
            if not resp.success:
                return Response(False, error_message=f"[from_clause] source materialization failed due to {resp.error_message}")
            rsname = resp.body

            # 3. apply filter on source - where clause
            if from_clause.where_clause:
                resp = self.filter_recordset(from_clause.where_clause, rsname)
                if not resp.success:
                    return Response(False, error_message=f"[where_clause] filtering failed due to {resp.error_message}")
                # filtering produces a new resultset
                rsname = resp.body

            # 4. apply group by clause
            if from_clause.group_by_clause:
                resp = self.group_recordset(from_clause.group_by_clause, rsname)
                if not resp.success:
                    return Response(False, error_message=f"[group_by_clause] failed due to {resp.error_message}")
                rsname = resp.body

            # 5. apply having clause
            if from_clause.having_clause:
                # having without group - by should treat entire
                # resultset as one group
                resp = self.filter_grouped_recordset(from_clause.having_clause, rsname)
                assert resp.success
                rsname = resp.body

        # 6. handle select columns
        # NOTE: this explicity sets the schema to resolve column names from;
        # another approach, would be for the name_registry to be initialized with the
        # StateManager, and then the NameRegistry could get any schema it needed. However, this would
        # additionally require a way to register joined recordsets, and groupedrecordsets to the StateManager
        self.name_registry.set_schema(self.get_recordset_schema(rsname))
        resp = self.evaluate_select_clause(stmnt.select_clause, rsname)
        assert resp.success
        rsname = resp.body

        # 7. order, limit clause
        if from_clause:
            if from_clause.order_by_clause:
                pass
            if from_clause.limit_clause:
                pass

            for record in self.recordset_iter(rsname):
                # todo: create records with only selected columns
                self.output_pipe.write(record)

        # end scope, and recycle any ephemeral objects in scope
        self.end_scope()

        # output pipe for sanity
        #for msg in self.output_pipe.store:
        #    logger.info(msg)

    def is_agg_func(self, func_name: str) -> bool:
        return is_aggregate_function(func_name)

    def is_scalar_func(self, func_name: str) -> bool:
        return is_scalar_function(func_name)

    def evaluate_select_clause(self, select_clause: SelectClause, source_rsname: str):
        """
        Doing a rewrite- old func was too big and messy;
        Dispatcher

        Evaluate the select clause.
        The select clause can be evaluated on 3 kinds of data sources:
            - ungrouped data_source (i.e. no group by)
            - grouped data_source (i.e. with a group by)
            - no data_source

        The grouping can be implicit from the function, e.g.
        select max(id)
        from foo
        having
        """
        # 1. no source
        if source_rsname is None:
            return self.evaluate_select_clause_no_source(select_clause)
        else:
            source_schema = self.get_recordset_schema(source_rsname)
            if isinstance(source_schema, GroupedSchema):
                return self.evaluate_select_clause_grouped_source(select_clause, source_rsname)
            else:
                return self.evaluate_select_clause_ungrouped_source(select_clause, source_rsname)

    def evaluate_select_clause_no_source(self, select_clause: SelectClause) -> Response:
        raise NotImplementedError

    def generate_output_schema_ungrouped_source(self, selectables: List[Any], source_schema) -> Response:
        """
        Generate output schema for an ungrouped source
        """
        out_columns = []
        for selectable in selectables:
            if isinstance(selectable, FuncCall):
                # find target type
                # 1.1.1. only scalar functions can be used
                func = selectable
                # TODO: perhaps checking whether func is scalar should be rolled up under SemanticAnalyzer
                assert self.is_scalar_func(func.name)
                func_def = resolve_function_name(func.name)
                oname = f"{func.name}_{func.args[0].name}"
                out_column = Column(oname, func_def.return_type)
                out_columns.append(out_column)
            else:
                # selectable is some expr, represented as an instance of `OrClause`-
                # the de-facto root of the expr hierarchy
                assert isinstance(selectable, OrClause)
                # a stringified or_clause to use as output column name
                expr_name = self.interpreter.stringify(selectable)
                resp = self.type_checker.analyze_scalar(selectable, source_schema)
                assert resp.success
                expr_type = resp.body
                out_column = Column(expr_name, expr_type)
                out_columns.append(out_column)

        # 1.2. generate schema from columns
        # we use an "unvalidated" schema because all schema of data persisted is expected to have a primary key
        resp = generate_unvalidated_schema("output_set", out_columns)
        if not resp.success:
            return Response(False, error_message=f"Generate output schema failed due to {resp.error_message}")
        return Response(True, body=resp.body)

    def generate_output_schema_grouped_source(self, selectables: List[Any], source_schema) -> Response:
        """
        Generate output schema for a grouped source
        """
        out_columns = []
        for selectable in selectables:
            # TODO: with the refactor of grammar; doesn't look like we'll hit anything but selectable: OrClause
            if isinstance(selectable, FuncCall):
                # find target type
                # 1.1.1. only aggregation functions can be used
                func = selectable
                # TODO: this check is already rolled up under SemanticAnalyzer; perhaps this check as well is_agg_func
                #  should be removed
                assert self.is_agg_func(func.name)
                func_def = resolve_function_name(func.name)
                oname = f"{func.name}_{func.args[0].name}"
                out_column = Column(oname, func_def.return_type)
                out_columns.append(out_column)
            else:
                # selectable is some expr, represented as an instance of `OrClause`-
                # the de-facto root of the expr hierarchy
                assert isinstance(selectable, OrClause)
                # a stringified or_clause to use as output column name
                expr_name = self.interpreter.stringify(selectable)
                resp = self.type_checker.analyze_grouped(selectable, source_schema)
                assert resp.success, resp.error_message
                expr_type = resp.body
                out_column = Column(expr_name, expr_type)
                out_columns.append(out_column)

        # 1.2. generate schema from columns
        # we use an "unvalidated" schema because all schema of data persisted is expected to have a primary key
        resp = generate_unvalidated_schema("output_set", out_columns)
        if not resp.success:
            return Response(False, error_message=f"Generate output schema failed due to {resp.error_message}")
        return Response(True, body=resp.body)

    def generate_value_generators_over_recordset(self, selectables: List) -> Response:
        """
        Return Response[List[Generators]]
        """
        generators = []
        for selectable in selectables:
            if isinstance(selectable, FuncCall):
                # NOTE: this only exists
                func = resolve_function_name(selectable.name)
                # TODO: check this is a scalar function
                generators.append(ValueGeneratorFromRecordOverFunc(selectable.args, {}, func))
            elif isinstance(selectable, ColumnName):
                # TODO: nuke if unused; seems this should be replaced by clause below
                generators.append(ValueExtractorFromRecord(selectable.name))
            else:
                # expression, i.e. default it's default root OrClause
                assert isinstance(selectable, OrClause)
                # NOTE: selectable can be arbitrary algebraic expression, including columns
                generators.append(ValueGeneratorFromRecordOverExpr(selectable, self.interpreter))
        return Response(True, body=generators)

    def generate_value_generators_over_grouped_recordset(self, selectables: List) -> Response:
        """
        Return Response[List[Generators]]
        """
        generators = []
        for selectable in selectables:
            if isinstance(selectable, OrClause):
                generators.append(ValueGeneratorFromRecordGroupOverExpr(selectable, self.interpreter))
            else:
                # this is unexpected
                breakpoint()
                return Response(False)

        return Response(True, body=generators)

    def evaluate_select_clause_ungrouped_source(self, select_clause: SelectClause, source_rsname: str) -> Response:
        """
        This is a select on non-grouped source
        """
        # 0. setup
        source_schema = self.get_recordset_schema(source_rsname)
        assert isinstance(source_schema, ScopedSchema) or isinstance(source_schema, SimpleSchema)

        # 1. generate output schema
        resp = self.generate_output_schema_ungrouped_source(select_clause.selectables, source_schema)
        if not resp.success:
            return Response(False, error_message=f"schema generation failed with [{resp.error_message}]")
        out_schema = resp.body

        # 2. generate output value generators
        resp = self.generate_value_generators_over_recordset(select_clause.selectables)
        if not resp.success:
            return Response(False, error_message=f"Unable to generate value generators due to [{resp.error_message}]")
        value_generators = resp.body

        # 3. generate output resultset
        resp = self.init_recordset(out_schema)
        assert resp.success
        out_rsname = resp.body

        out_column_names = [col.name for col in out_schema.columns]
        # populate output resultset
        for record in self.recordset_iter(source_rsname):
            # get value, one for each output column
            value_list = [val_gen.get_value(record) for val_gen in value_generators]
            # convert column values to a record
            resp = create_record_from_raw_values(out_column_names, value_list, out_schema)
            assert resp.success
            out_record = resp.body
            self.append_recordset(out_rsname, out_record)

        return Response(True, body=out_rsname)

    def evaluate_select_clause_grouped_source(self, select_clause: SelectClause, source_rsname: str) -> Response:
        """
        This is a select on a grouped source
        """
        source_schema = self.get_recordset_schema(source_rsname)
        assert isinstance(source_schema, GroupedSchema)
        resp = self.generate_output_schema_grouped_source(select_clause.selectables, source_schema)
        if not resp.success:
            return Response(False, error_message=f"schema generation failed with [{resp.error_message}]")
        out_schema = resp.body

        # 2. generate output value generators
        resp = self.generate_value_generators_over_grouped_recordset(select_clause.selectables)
        if not resp.success:
            return Response(False, error_message=f"Unable to generate value generators due to [{resp.error_message}]")
        value_generators = resp.body

        # 3. generate output resultset
        # NOTE: a groupedrecordset materializes to a resultset, i.e. groups are squashed
        resp = self.init_recordset(out_schema)
        assert resp.success
        out_rsname = resp.body

        out_column_names = [col.name for col in out_schema.columns]
        # populate output resultset
        for grouped_record in self.grouped_recordset_iter(source_rsname):
            # get value, one for each output column
            group_schema = self.schemas[source_rsname]
            assert isinstance(group_schema, GroupedSchema)
            value_list = [val_gen.get_value(grouped_record) for val_gen in value_generators]
            # convert column values to a record
            resp = create_record_from_raw_values(out_column_names, value_list, out_schema)
            assert resp.success
            out_record = resp.body
            self.append_recordset(out_rsname, out_record)

        return Response(True, body=out_rsname)

    def visit_insert_stmnt(self, stmnt) -> Response:
        """
        handle insert stmnt
        """
        table_name = stmnt.table_name.table_name
        if not self.state_manager.has_schema(table_name):
            # check if table exists
            return Response(False, error_message=f"Table [{table_name}] does not exist")

        # get schema
        schema = self.state_manager.get_schema(table_name)
        resp = create_record(stmnt.column_name_list, stmnt.value_list, schema)
        if not resp.success:
            return Response(False, error_message=f"Insert record failed due to [{resp.error_message}]")

        record = resp.body
        # get table's tree
        tree = self.state_manager.get_tree(table_name)

        resp = serialize_record(record)
        assert resp.success, f"serialize record failed due to {resp.error_message}"

        cell = resp.body
        resp = tree.insert(cell)
        assert resp == TreeInsertResult.Success, f"Insert op failed with status: {resp}"
        return Response(True, body=TreeInsertResult.Success)

    def visit_delete_stmnt(self, stmnt) -> Response:
        """
        handle delete stmnt
        """

        # 1. iterate over source dataset
        # materializing the entire recordset is expensive, but cleaner/easier/faster to implement
        resp = self.materialize(stmnt.table_name)
        assert resp.success
        rsname = resp.body

        if stmnt.where_condition:
            resp = self.filter_recordset(stmnt.where_condition, rsname)
            assert resp.success
            rsname = resp.body

        # 2. create list of keys to delete
        del_keys = []
        for record in self.recordset_iter(rsname):
            del_keys.append(record.get_primary_key())

        # 3. delete the keys
        table_name = stmnt.table_name.table_name
        tree = self.get_tree(table_name)
        for del_key in del_keys:
            resp = tree.delete(del_key)
            if resp != TreeDeleteResult.Success:
                logging.warning(f"delete failed for key {del_key}")
                return Response(False, resp)
        # return list of deleted keys
        return Response(True, body=del_keys)


    # section : statement helpers
    # general principles:
    # 1) helpers should be able to handle null types

    def get_schema(self, table_name) -> SimpleSchema:

        if table_name.table_name.lower() == "catalog":
            return self.state_manager.get_catalog_schema()
        else:
            return self.state_manager.get_schema(table_name.table_name)

    def get_tree(self, table_name) -> Tree:
        if table_name.table_name.lower() == "catalog":
            return self.state_manager.get_catalog_tree()
        else:
            return self.state_manager.get_tree(table_name.table_name)

    def materialize(self, source) -> Response:
        """
        Materialize source.
        """
        if isinstance(source, SingleSource):
            # NOTE: single source means a single physical table
            self.scope_register_single_source(source)
            return self.materialize_single_source(source)

        elif isinstance(source, Joining):
            self.scope_register_single_joining(source)
            return self.materialize_joining(source)

        elif isinstance(source, TableName):
            return self.materialize_source_from_name(source.table_name)

        else:
            raise ValueError(f"Unknown materialization source type {source}")
        # case nestedSelect

    def materialize_single_source(self, source: SingleSource) -> Response:
        """
        Materialize single source and return
        """
        assert isinstance(source, SingleSource), f"Unexpected {source}"

        # does table_names need to be resolved?
        return self.materialize_source_from_name(source.table_name, source.table_alias)

    def materialize_source_from_name(self, table_name: str, table_alias: str = None) -> Response:
        # get schema for table, and cursor on tree corresponding to table
        schema = self.get_schema(table_name)
        tree = self.get_tree(table_name)

        if table_alias is not None:
            # record set schema is a scoped schema, since that contains
            # table alias info; however, the cursor requires a (simple)
            # schema
            rs_schema = ScopedSchema.from_single_schema(schema, table_alias)
        else:
            rs_schema = schema

        resp = self.init_recordset(rs_schema)
        assert resp.success
        rsname = resp.body

        cursor = Cursor(self.state_manager.get_pager(), tree)
        # iterate over entire table
        while cursor.end_of_table is False:
            cell = cursor.get_cell()
            resp = deserialize_cell(cell, schema)
            assert resp.success
            record = resp.body
            # if an alias is defined
            record = ScopedRecord.from_single_simple_record(record, table_alias, rs_schema) if table_alias else record

            self.append_recordset(rsname, record)
            # advance cursor
            cursor.advance()
        return Response(True, body=rsname)

    def materialize_joining(self, source: Joining) -> Response:
        """
        Materialize a joining.
        After a pairwise joining of recordsets
        """
        # parser places first table in a series of joins in the most nested
        # join; recursively traverse the join object(s) and construct a ordered list of
        # tables to materialize
        stack = [source]
        ptr = source
        while True:
            stack.append(ptr.left_source)
            if isinstance(ptr.left_source, Joining):
                # recurse down
                ptr = ptr.left_source
            else:
                assert isinstance(ptr.left_source, SingleSource)
                # end of join nesting
                break

        # now materialize joins
        # starting from stack top, each materialization is the left_source
        # in the nest iteration of joining
        first = stack.pop()
        resp = self.materialize(first)
        assert resp.success
        rsname = resp.body
        left_source_name = first.table_alias or first.table_name.table_name
        while stack:
            # join next source with existing rset
            next_join = stack.pop()
            assert isinstance(next_join, Joining)
            # NOTE: currently, materialize and join are separate steps
            # these could be combined; but for now keep them separate
            # to keep it simple

            right_source = next_join.right_source
            # name within local scope, i.e. tablename or alias
            right_source_name = right_source.table_alias or right_source.table_name.table_name
            resp = self.materialize_single_source(right_source)
            assert resp.success
            next_rsname = resp.body

            # join next_join with rsname
            # NOTE: in the first joining, i.e. when 2 simple sources are joined, I need to pass
            # the table_name for left and right sources. This creates a joined_recordsets, containing
            # JoinedRecords. JoinedRecords, unlike (simple) Records, contain the table_name, where the
            # record came from. Thus, in subsequent joinings, i.e. the previously joined with a simple
            # source (containing Records), I only need to pass the alias for the right source, since
            # the JoinedRecord already contains the table name info

            # NOTE: this API could be made more consistent by always working with JoinedRecord, even
            # for simple joins; also no need to refactor this now; once I test more complex cases
            # the API will become clearer

            resp = self.join_recordset(next_join, rsname, next_rsname, left_source_name, right_source_name)
            left_source_name = None
            assert resp.success
            rsname = resp.body

        return Response(True, body=rsname)

    def check_resolve_name(self, operand, record) -> Response:
        """
        Check if the `operand` is logical name, if so
        attempt to resolve it from record
        TODO: nuke this in favor of NameRegistry.{is_name, resolve_name}
        """
        if isinstance(operand, Token):
            if operand.type == "IDENTIFIER":
                # check if we can resolve this
                assert isinstance(record, SimpleRecord)  # this may not needed, but this is what this logic is assuming
                if operand in record.values:
                    return Response(True, body=record.values[operand])
            elif operand.type == "SCOPED_IDENTIFIER":
                # attempt resolve scoped identifier
                assert isinstance(record, ScopedRecord), f"Expected MultiRecord; received {type(record)}"
                # this operand is <alias>.<column>
                value = record.get(operand)
                return Response(True, body=value)
            return Response(False, f"Unable to resolve {operand.type}")
        else:
            # not name, return as is
            return Response(True, body=operand)

    def filter_recordset(self, where_clause: WhereClause, source_rsname: str) -> Response:
        """
        Apply where_clause on source_rsname and return filtered resultset
        """
        assert isinstance(where_clause, WhereClause)

        schema = self.get_recordset_schema(source_rsname)
        resp = self.init_recordset(schema)
        assert resp.success
        # generate new result set
        rsname = resp.body

        for record in self.recordset_iter(source_rsname):
            value = self.interpreter.evaluate_over_record(where_clause.condition, record)
            assert isinstance(value, bool)
            if value:
                self.append_recordset(rsname, record)

        return Response(True, body=rsname)

    def filter_grouped_recordset(self, having_clause: HavingClause, source_rsname: str):
        """
        A having op, i.e. filtering on grouped recordset can also
        be applied on an ungrouped set where the the whole group is treated as
        set. SEE: https://stackoverflow.com/a/9099170/1008921
        """
        assert isinstance(having_clause, HavingClause)

        schema = self.get_recordset_schema(source_rsname)
        # todo: evaluate any aggregation functions

        if isinstance(schema, GroupedSchema):
            pass
        elif isinstance(schema, ScopedSchema):
            raise NotImplementedError
        else:
            # Schema - not sure if this needs to be split
            raise NotImplementedError

    def join_recordset(self, join_clause, left_rsname: str, right_rsname: str, left_sname: Optional[str],
                       right_sname: str) -> Response:
        """
        join record based on record type and return joined recordset.

        NOTE: this handles 2 cases:
            - joins over simple sources (singular_join)
            - joins over 1 simple source, and joined source (multi_join)

        TOOD: the tricky bit here, handling 2 single sources, and subsequently
        a single source, and a joined source

        :param left_rsname: left record set name (could be single or joined recordset)
        :param right_rsname: right record set name (single source)
        :param left_sname: left source name (optional); when nulled' when left is joinedrecord
        :param right_sname: right source name
        """
        left_schema = self.get_recordset_schema(left_rsname)
        right_schema = self.get_recordset_schema(right_rsname)
        schema = ScopedSchema.from_schemas(left_schema, right_schema, left_sname, right_sname)
        resp = self.init_recordset(schema)
        assert resp.success
        rsname = resp.body

        left_iter = self.recordset_iter(left_rsname)
        # inner join
        if join_clause.join_type == JoinType.Inner:
            for left_rec in left_iter:
                # for each left record we need to iterate over each right_record
                right_iter = self.recordset_iter(right_rsname)
                for right_rec in right_iter:
                    record = ScopedRecord.from_records(left_rec, right_rec, left_sname, right_sname, schema)
                    if self.evaluate_condition(join_clause.condition, record):
                        # join condition matched
                        self.append_recordset(rsname, record)

        elif join_clause.join_type == JoinType.LeftOuter:
            # there should be at least one record each left record
            left_record_added = False
            for left_rec in left_iter:
                right_iter = self.recordset_iter(right_rsname)
                for right_rec in right_iter:
                    record = ScopedRecord.from_records(left_rec, right_rec, left_sname, right_sname, schema)
                    if self.evaluate_condition(join_clause.condition, record):
                        # join condition matched
                        self.append_recordset(rsname, record)
                        left_record_added = True
                if not left_record_added:
                    # add a null right record
                    # create and join records
                    right_rec = create_null_record(right_schema)
                    record = ScopedRecord.from_records(left_rec, right_rec, left_sname, right_sname, schema)
                    self.append_recordset(rsname, record)

        elif join_clause.join_type == JoinType.RightOuter:
            # there should be at least one record for each right record
            # NOTE: since the right is the inner record_set, we maintain the index on the
            # index positions records in the right record set, and whether they have been joined.
            # this is problematic because it assumes the iter order of records in a recordset
            # will be the same, which isn't explicitly part of the recordset API
            right_joined_index = [False for _ in self.recordset_iter(right_rsname)]
            for left_rec in left_iter:
                right_iter = self.recordset_iter(right_rsname)
                for index, right_rec in enumerate(right_iter):
                    record = ScopedRecord.from_records(left_rec, right_rec, left_sname, right_sname, schema)
                    if self.evaluate_condition(join_clause.condition, record):
                        # join condition matched
                        self.append_recordset(rsname, record)
                        right_joined_index[index] = True

            # handle any un-joined right records
            for index, right_rec in self.recordset_iter(right_rsname):
                if right_joined_index[index]:
                    continue
                left_rec = create_null_record(left_schema)
                record = ScopedRecord.from_records(left_rec, right_rec, left_sname, right_sname, schema)
                self.append_recordset(rsname, record)

        elif join_clause.join_type == JoinType.FullOuter:
            # there should be atleast one record for each left and right record
            left_record_added = False
            right_joined_index = [False for _ in self.recordset_iter(right_rsname)]
            for left_rec in left_iter:
                right_iter = self.recordset_iter(right_rsname)
                for index, right_rec in enumerate(right_iter):
                    record = ScopedRecord.from_records(left_rec, right_rec, left_sname, right_sname, schema)
                    if self.evaluate_condition(join_clause.condition, record):
                        # join condition matched
                        self.append_recordset(rsname, record)
                        left_record_added = True
                        right_joined_index[index] = True
                if not left_record_added:
                    # add a null right record
                    # create and join records
                    right_rec = create_null_record(right_schema)
                    record = ScopedRecord.from_records(left_rec, right_rec, left_sname, right_sname, schema)
                    self.append_recordset(rsname, record)
            # handle any un-joined right records
            for index, right_rec in self.recordset_iter(right_rsname):
                if right_joined_index[index]:
                    continue
                left_rec = create_null_record(left_schema)
                record = ScopedRecord.from_records(left_rec, right_rec, left_sname, right_sname, schema)
                self.append_recordset(rsname, record)

        else:
            assert join_clause.join_type == JoinType.Cross
            for left_rec in left_iter:
                right_iter = self.recordset_iter(right_rsname)
                for right_rec in right_iter:
                    record = ScopedRecord.from_records(left_rec, right_rec, left_sname, right_sname, schema)
                    self.append_recordset(rsname, record)

        return Response(True, body=rsname)

    def evaluate_condition(self, condition: OrClause, record) -> bool:
        """
        Evaluate condition on record Union(Record, JoinedRecord) and return bool result
        TODO: this should be moved to ExpressionInterpreter
        """
        or_result = False
        for and_clause in condition.and_clauses:
            and_result = True
            for predicate in and_clause.predicates:
                assert isinstance(predicate, Comparison), f"Expected Comparison received {predicate}"
                # the predicate contains a condition, which contains
                # logical column refs and literal values
                # 1. resolve all names
                resp = self.check_resolve_name(predicate.left_op, record)
                assert resp.success
                left_value = resp.body
                resp = self.check_resolve_name(predicate.right_op, record)
                assert resp.success
                right_value = resp.body

                # 2. evaluate predicate
                # value of evaluated predicate
                pred_value = False
                if predicate.operator == ComparisonOp.Greater:
                    pred_value = left_value > right_value
                elif predicate.operator == ComparisonOp.Less:
                    pred_value = left_value < right_value
                elif predicate.operator == ComparisonOp.GreaterEqual:
                    pred_value = left_value >= right_value
                elif predicate.operator == ComparisonOp.LessEqual:
                    pred_value = left_value <= right_value
                elif predicate.operator == ComparisonOp.Equal:
                    pred_value = left_value == right_value
                else:
                    assert predicate.operator == ComparisonOp.NotEqual
                    pred_value = left_value != right_value

                # optimization note: once an and_result is False, stop inner loop
                and_result = and_result and pred_value

            or_result = or_result or and_result
        return or_result

    def group_recordset(self, group_by_clause, source_rsname):
        """
        Apply by group-by on records in rsname
        """
        # generate grouped schema

        source_schema = self.get_recordset_schema(source_rsname)
        resp = make_grouped_schema(source_schema, group_by_clause.columns)
        assert resp.success
        grouped_schema = resp.body

        # init new grouped-recordset
        resp = self.init_grouped_recordset(grouped_schema)
        assert resp.success
        rsname = resp.body

        # iterate over records, get group-key, add record to group
        for record in self.recordset_iter(source_rsname):
            # get group-key
            group_values = []
            for col in grouped_schema.group_by_columns:
                group_values.append(record.get(col.name))
            group_key = tuple(group_values)
            self.append_grouped_recordset(rsname, group_key, record)

        return Response(True, body=rsname)

    # section: scope management

    def init_new_scope(self):
        """
        Each scope contains many different kinds of objects
                self.scope_aliased_sources = []
        """

        self.scopes.append({
            SCOPE_COLLECTION_ALIASED_SOURCES_KEY: {},
            # this is logically an unordered collection;
            # however since TableName is unhashable, it can't be stored in a set
            SCOPE_COLLECTION_UNALIASED_SOURCES_KEY: [],
        })

    def end_scope(self):
        self.scopes.pop()

    def scoped_register_scoped_object(self, name, obj) -> None:
        pass

    def scope_register_single_source(self, source: SingleSource):
        if source.table_alias is None:
            self.scopes[-1][SCOPE_COLLECTION_UNALIASED_SOURCES_KEY].append(source.table_name)
        else:
            self.scopes[-1][SCOPE_COLLECTION_ALIASED_SOURCES_KEY][source.table_alias] = source.table_name

    def scope_register_single_joining(self, source: Joining):
        self.scope_register_single_source(source.left_source)
        self.scope_register_single_source(source.right_source)

    def scope_resolve_column_name_type(self, name: ColumnName) -> Response:
        """
        Return Response[Type[DataType]], i.e. Response(type of column)
        NOTE: This search is very inefficient; current goal is completeness/correctness - optimization later
        """
        parent_alias = name.get_parent_alias()
        column_base_name = name.get_base_name()
        # iterate over scopes, starting at most recent scope, and attempt to resolve name
        for scope in reversed(self.scopes):
            if parent_alias is not None:
                aliased_sources = scope[SCOPE_COLLECTION_ALIASED_SOURCES_KEY]
                source = aliased_sources.get(parent_alias)
                if source is not None:
                    # get schema for source
                    source_schema = self.get_schema(source)
                    # lookup column in source_schema
                    column = source_schema.get_column_by_name(column_base_name)
                    if column is None:
                        return Response(False, error_message="column not found on source")
                    return Response(True, body=column.datatype)
            else:
                unaliased_sources = scope[SCOPE_COLLECTION_UNALIASED_SOURCES_KEY]
                # check all unaliased sources
                # todo: build reverse index column_name -> source_object
                # presumably building a reverse index should help, and also help catch ambiguous column references
                for candidate_source in unaliased_sources:
                    # get schema
                    source_schema = self.get_schema(candidate_source)
                    # check if object contains name
                    column = source_schema.get_column_by_name(column_base_name)
                    if column is not None:
                        return Response(True, body=column.datatype)
                return Response(False, error_message="column not found on source")

    def scope_resolve_name(self, name: str) -> Tuple[Type[DataType], Any]:
        """
        For v1, do a brute force search for name,
        later, this can be optimized by more efficient searching
        """
        breakpoint()
        pass

    # section: record set utilities

    @staticmethod
    def gen_randkey(size=10, prefix=""):
        return prefix + "".join(random.choice(string.ascii_letters) for i in range(size))

    def init_recordset(self, schema) -> Response:
        """
        initialize recordset; this requires a unique name
        for each recordset
        """
        name = self.gen_randkey(prefix="r")
        while name in self.rsets:
            # generate while non-unique
            name = self.gen_randkey(prefix="r")
        self.rsets[name] = []
        self.schemas[name] = schema
        return Response(True, body=name)

    def init_grouped_recordset(self, schema):
        """
        init a grouped recordset.
        NOTE: A grouped record set is internally stored like
        {group_key_tuple -> list_of_records}
        """
        name = self.gen_randkey(prefix="g")
        while name in self.grouprsets:
            # generate while non-unique
            name = self.gen_randkey(prefix="g")
        self.grouprsets[name] = defaultdict(list)
        self.schemas[name] = schema
        return Response(True, body=name)

    def get_recordset_schema(self, name: str) -> AbstractSchema:
        return self.schemas[name]

    def append_recordset(self, name: str, record):
        assert name in self.rsets
        self.rsets[name].append(record)

    def append_grouped_recordset(self, name: str, group_key: tuple, record):
        self.grouprsets[name][group_key].append(record)

    def drop_recordset(self, name: str):
        del self.rsets[name]

    def recordset_iter(self, name: str):
        """Return an iterator over recordset
        NOTE: The iterator will be consumed after one iteration
        """
        return iter(self.rsets[name])

    def grouped_recordset_iter(self, name) -> List[GroupedRecord]:
        """
        return a pair of (group_key, group_recordset_iterator)
        """
        # NOTE: cloning the group_rset, since it may need to be iterated multiple times
        ret = [GroupedRecord(self.schemas[name], group_key, list(group_rset))
                 for group_key, group_rset in self.grouprsets[name].items()]
        return ret

