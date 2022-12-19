"""
Rewrite of virtual machine class.
This focuses on select execution- later merge
the rest of the logic from the old VM in here

"""
import logging
import random
import string

from typing import Any, List, Optional, Union, Set, Dict, Tuple, Type
from enum import Enum, auto
from collections import defaultdict, OrderedDict

from btree import Tree, TreeInsertResult, TreeDeleteResult
from cursor import Cursor
from datatypes import DataType
from dataexchange import Response
from functions import resolve_function_name, FunctionInvocation
from schema import (generate_schema, generate_unvalidated_schema, schema_to_ddl, BaseSchema, Schema, MultiSchema, ScopedSchema,
                    make_grouped_schema, GroupedSchema, Column)
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
    Record,
    create_catalog_record,
    join_records,
    MultiRecord,
    create_record,
    create_null_record,
    create_record_from_raw_values
)

from value_generators import ValueGeneratorFromRecordOverFunc, ValueExtractorFromRecord, ValueGeneratorFromRecordOverExpr
from vm_utilclasses import ExpressionInterpreter, NameRegistry, InterpreterMode

logger = logging.getLogger(__name__)


class RecordSet:
    # todo: nuke me - unused
    pass


class GroupedRecordSet(RecordSet):
    # todo: nuke me - unused
    pass


class NullRecordSet(RecordSet):
    # todo: nuke me - unused
    pass


class UnGroupedColumnException(Exception):
    pass


# constants in scoped dict
SCOPE_COLLECTION_ALIASED_SOURCES_KEY = "aliased_sources"
SCOPE_COLLECTION_UNALIASED_SOURCES_KEY = "unaliased_sources"

# names of supported builtin functions
AGGREGATE_FUNCTIONS = ["MIN", "MAX", "COUNT", "SUM", "AVG"]
SCALAR_FUNCTIONS = ["CURRENT_DATETIME", "TO_STRING", "SQUARE", "SQUARE_FLOAT"]


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
        self.init_scopes()
        self.name_registry = NameRegistry()
        self.interpreter = ExpressionInterpreter(self.name_registry)

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
        # TODO: handle semantic validation, and name binding - actually not here
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
        # because order by and limit depend on a materialized/flattened,
        # resultset; flattened, means any non-group by columns are aggregated
        # e.g. order by count(*)
        # NOTE: select may not refer to any columns, since from is optional
        resp = self.evaluate_select_clause(stmnt.select_clause, rsname)
        assert resp.success
        rsname = resp.body

        # 7.
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

    def is_agg_func(self, func_name: str):
        return func_name.upper() in AGGREGATE_FUNCTIONS

    def is_scalar_func(self, func_name: str):
        return func_name.upper() in SCALAR_FUNCTIONS

    def has_group_by_col_args(self, func_name, func_args, group_by_schema: GroupedSchema):
        """
        Returns True if func uses group by columns as arguments
        :param func_name:
        :param func_args:
        :param group_by_schema:
        :return:
        """
    def has_non_group_by_col_args(self, func_name, func_args, group_by_schema: GroupedSchema):
        """
        Return true if func does not use any group by columsn 
        :param func_name:
        :param func_args:
        :param group_by_schema:
        :return:
        """

    def is_group_by_col(self, schema: GroupedSchema, column: ColumnName) -> bool:
        """
        Determine if column is a groupby column
        """
        for col in schema.group_by_columns:
            if col == column:
                return True
        return False

    def evaluate_agg_function(self, func_name: str, func_args, schema, source_rsname) -> Response:
        """
        Evaluate function and return computed result
        """
        # this should a map from group_key -> value
        # if source is a simple recordset - the group key will be None
        computed = defaultdict(int)
        func_name = func_name.lower()
        # TODO: validate args; for all agg funcs, only * or a single column name is accepted
        if func_name == "min":
            raise NotImplementedError
        elif func_name == "max":
            raise NotImplementedError
        elif func_name == "avg":
            raise NotImplementedError
        elif func_name == "count":
            # 1. validate args
            assert len(func_args) == 1
            # todo: validate function args: column or *
            # todo; handle * arg; currently parser fails
            if isinstance(func_args[0], ColumnName):
                column = func_args[0]
                # 2. evaluate function
                for group_key, group_rset in self.grouped_recordset_iter(source_rsname):
                    group_value = 0
                    for record in group_rset:
                        group_value += 1 if record.get(column.name) is not None else 0
                    computed[group_key] += group_value

        elif func_name == "sum":
            # 1. validate args
            assert len(func_args) == 1
            column = func_args[0]
            # 2. evaluate function
            for group_key, group_rset in self.grouped_recordset_iter(source_rsname):
                group_value = 0
                for record in group_rset:
                    group_value += record.get(column.name)
                computed[group_key] += group_value
        else:
            raise ValueError(f"Unrecognized aggregation function: {func_name}")

        return Response(True, body=computed)

    def evaluate_scalar_function(self, func_name, func_args, schema, source_rsname) -> Response:
        raise NotImplementedError

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
                assert self.is_scalar_func(func.name)
                func_def = resolve_function_name(func.name)
                oname = f"{func.name}_{func.args[0].name}"
                out_column = Column(oname, func_def.return_type)
                out_columns.append(out_column)
            else:
                # selectable is some expr, represented as an instance of `OrClause`-
                # the de-facto root of the expr hierarchy
                assert isinstance(selectable, OrClause)
                # a stringified version of or_clause
                expr_name = self.stringify_expr(selectable)
                expr_type = self.evaluate_expr_type(selectable)
                out_column = Column(expr_name, expr_type)
                out_columns.append(out_column)

        # 1.2. generate schema from columns
        # we use an "unvalidated" schema because all schema of data persisted is expected to have a primary key
        resp = generate_unvalidated_schema("output_set", out_columns)
        if not resp.success:
            return Response(False, error_message=f"Generate output schema failed due to {resp.error_message}")
        return Response(True, body=resp.body)

    def generate_output_value_generators(self, selectables) -> Response:
        """
        Return Response[List[Generators]]
        """
        generators = []
        for selectable in selectables:
            if isinstance(selectable, FuncCall):
                func = resolve_function_name(selectable.name)
                generators.append(ValueGeneratorFromRecordOverFunc(selectable.args, {}, func))
            elif isinstance(selectable, ColumnName):
                generators.append(ValueExtractorFromRecord(selectable.name))
            else:
                # todo: selectable can be arbitrary algebraic expression, including columns
                # use self.interpreter
                # what is the value_generator here?
                # one thought could be to apply a func-like object here, that returns takes a record and returns the value
                # but then we need a function-like object, i.e. currently func interface has one method: apply(self, record) -> value
                generators.append(ValueGeneratorFromRecordOverExpr(selectable, self.interpreter))
                #raise NotImplementedError

        return Response(True, body=generators)

    def evaluate_select_clause_ungrouped_source(self, select_clause: SelectClause, source_rsname: str) -> Response:
        """
        This is a select on non-grouped source
        """
        # 0. setup
        source_schema = self.get_recordset_schema(source_rsname)
        assert isinstance(source_schema, ScopedSchema) or isinstance(source_schema, Schema)

        # 1. generate output schema
        resp = self.generate_output_schema_ungrouped_source(select_clause.selectables, source_schema)
        if not resp.success:
            return Response(False, error_message=f"schema generation failed with [{resp.error_message}]")
        out_schema = resp.body

        # 2. generate output value generators
        resp = self.generate_output_value_generators(select_clause.selectables)
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
        # 1. iterate over selectables and
        # 1.1. validate output selectables
        # 1.2. generate output mapping; that allows constructing
        # output record from input record
        source_schema = self.get_recordset_schema(source_rsname)
        assert isinstance(source_schema, GroupedSchema)
        # NOTE: the output columns are used to construct output schema
        # but also, are the "mapping" from the input to output schema
        # this implementation of mapping will change when I introduce selectable remapping via "AS"
        out_columns = OrderedDict()
        # one value generator for each output column
        value_generators = []
        for selectable in select_clause.selectables:
            if isinstance(selectable, FuncCall):
                # 1.1. only aggregate functions can be used
                # todo: convert func into Func Object?
                # parse arguments - currently they are parser level things
                func = selectable
                assert self.is_agg_func(func.name)
                # determine return type of function
                funcsig = get_function_signature(func.name)
                # generate Column object corresponding to applied func
                otype = funcsig.output_type
                oname = f"{func.name}_{func.args[0].name}"
                ocolumn = Column(oname, otype)
                out_columns[oname] = ocolumn
                # ValueFromAggregateFuncMapper accepts a function invocation
                # a function invocation can be modelled as :
                # 1) function name, and args, or 2) FunctionObject, with args
                # resolve
                call = FunctionInvocation()
                value_generators.append(ValueFromAggregateFuncMapper())
            else:
                # todo: this is not true
                assert isinstance(selectable, ColumnName)

                # a raw column must be a group-by column
                if not self.is_group_by_col(source_schema, selectable):
                    raise UnGroupedColumnException(f"Column [{selectable.name}] must appears either in "
                                                   f"group-by or in aggregation function")

                # find selectable in source schema
                otype = source_schema.schema.get_column_by_name(selectable.name).datatype
                ocolumn = Column(selectable.name, otype)
                out_columns[ocolumn.name] = ocolumn
                value_generators.append(ValueFromValueMapper())

                # 1.2. assert this column occurs in source schema
                scolumn = source_schema.get_column_by_name(selectable.name)
                if scolumn is None:
                    # source schema doesn't contain columns, i.e. column doesn't exist
                    return Response(False, error_message=f"Unknown column name [{selectable.name}]")
                ocolumn = Column(selectable.name, scolumn.datatype)
                out_columns[ocolumn.name] = ocolumn

        # 2. generate output_schema
        resp = generate_unvalidated_schema("output_set", list(out_columns.values()))
        assert resp.success
        out_schema = resp.body

        # 3. generate output resultset
        resp = self.init_recordset(out_schema)
        assert resp.success
        out_rsname = resp.body

        # 4. evaluate select clause; populate output resultset
        out_column_names = list(out_columns.keys())
        for group_key, group_rsiter in self.grouped_recordset_iter(source_rsname):

            # NOTE: group_key must be special-handled, since mappers don't have any special support for it
            # actually how do the mappers work here?
            for value_gen in value_generators:
                value_gen.get_value()


            # each group_key is a row in output


            # how to use out_column_names mapping to construct value list?
            # how do we generalize the mapping
            for out_column in out_column_names:
                pass

            value_list = []
            for out_column in out_column_names:
                value_list.append(record.get(out_column))

            # create an output record
            resp = create_record_from_raw_values(out_column_names, value_list, out_schema)
            assert resp.success
            out_record = resp.body
            self.append_recordset(out_rsname, out_record)

        return Response(True, body=out_rsname)


    def evaluate_select_clause_old(self, select_clause: SelectClause, source_rsname: str) -> Response:
        """
        Evaluate the select clause, i.e. flatten any operations on any groups

        First generate, the schema for the result rset.
        This schema will be Schema (or ScopedSchema)
        Then evaluate
        The result rset will have one record for each record in the
        source for ungrouped recordsets; and will have one record
        for each group for a grouped recordsets.

        TODO: Nuke me
        """
        # 1. validate select clause and get columns of output schema
        out_columns = []
        source_schema = None
        if source_rsname is None:
            # no source
            # selectables can be literals, or scalar functions
            for selectable in select_clause.selectables:
                pass
        else:
            source_schema = self.get_recordset_schema(source_rsname)
            if isinstance(source_schema, GroupedSchema):
                for selectable in select_clause.selectables:
                    if isinstance(selectable, FuncCall):
                        func = selectable
                        # validate arguments
                        # very crude way to ensuring argument size matches function arity, i.e. 1 for all existing
                        # functions; when I have the function declaration story better scoped out, I should revisit this
                        assert len(func.args) == 1, f"Expected 1 argument; received {len(func.args)}"
                        if self.is_agg_func(func.name):
                            # agg functions must be applied to non-grouped columns
                            for arg in func.args:
                                if isinstance(arg, ColumnName) and self.is_group_by_col(source_schema, arg):
                                    raise ValueError(f"Column [{arg}] cannot appear in both "
                                                     f"group-by and aggregation function")
                        # determine return type of function
                        funcsig = get_function_signature(func.name)
                        # generate Column object corresponding to applied func
                        otype = funcsig.output_type
                        oname = f"{func.name}_{func.args[0].name}"
                        ocolumn = Column(oname, otype)
                        out_columns.append(ocolumn)

                    elif isinstance(selectable, ColumnName):
                        # a raw column must be a group-by column
                        if not self.is_group_by_col(source_schema, selectable):
                            raise UnGroupedColumnException(f"Column [{arg}] must appears either in "
                                                           f"group-by or in aggregation function")

                        # find selectable in source schema
                        otype = source_schema.schema.get_column_by_name(selectable.name).datatype
                        ocolumn = Column(selectable.name, otype)
                        out_columns.append(ocolumn)

            elif isinstance(source_schema, ScopedSchema):
                raise NotImplementedError
            else:
                assert isinstance(source_schema, Schema)
                raise NotImplementedError

        # 2. generate output schema
        output_schema = Schema("result_set", out_columns)

        # 3. generate N single column result sets
        # where N is the number of selectables in select clause
        # next we'll zip each stream to create records corresponding to `output_schema`
        # NOTE, the parallel between steps 1: get output schema columns, 2: generate output schema, and
        # 3 generate single column result sets for output columns, 4: weave single column RSs into one result set

        # NOTE: the selectables must be mapped to columns in the resutlset
        # if the resultset is grouped, then' it'll need to be eval via evaluate_agg_function
        # otherwise the source resultset is used

        column_result_sets = []
        if source_schema is None:
            raise NotImplementedError
        elif isinstance(source_schema, GroupedSchema):
            for selectable in select_clause.selectables:
                # function could be agg or non-agg
                if isinstance(selectable, FuncCall):
                    func = selectable
                    if self.is_agg_func(func.name):
                        resp = self.evaluate_agg_function(func.name, func.args, source_schema, source_rsname)
                        assert resp.success
                        computed = resp.body  # dict of group -> group_val
                        # TODO: convert computed into RecordSet
                        # TODO: map group by columns to select column order

                    else:
                        # non-agg function can only be applied to group-by columns
                        for arg in selectable.args:
                            if isinstance(arg, ColumnName):
                                if not self.is_group_by_col(source_schema, arg):
                                    raise UnGroupedColumnException(f"Column [{arg}] must appears either in both "
                                                                   f"group-by or in aggregation function")

                elif isinstance(selectable, ColumnName):
                    # this must be a group-by column
                    is_groupby_col = self.is_group_by_col(source_schema, selectable)
                    if not is_groupby_col:
                            raise UnGroupedColumnException(f"Column [{selectable}] must appears either in both group-by "
                                                           f"or in aggregation function")
                    else:
                        # this value should be used as is
                        raise NotImplementedError

        elif isinstance(source_schema, ScopedSchema):
            raise NotImplementedError
        else:
            # Schema
            raise NotImplementedError

            # iterate over resultset and flatten it

        # 4. weave into one resultsets
        # TODO: actually I'm not sure if this step is needed
        # not needed for grouped recordset, for sure

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

    def get_schema(self, table_name) -> Schema:
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
            record = MultiRecord.from_single_simple_record(record, table_alias, rs_schema) if table_alias else record

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
                assert isinstance(record, Record)  # this may not needed, but this is what this logic is assuming
                if operand in record.values:
                    return Response(True, body=record.values[operand])
            elif operand.type == "SCOPED_IDENTIFIER":
                # attempt resolve scoped identifier
                assert isinstance(record, MultiRecord), f"Expected MultiRecord; received {type(record)}"
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
            #if self.evaluate_condition(where_clause.condition, record):  # old method

            self.interpreter.set_mode(InterpreterMode.BoolEval)
            self.interpreter.set_record(record)
            value = self.interpreter.evaluate(where_clause.condition)
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
        elif isinstance(schema, MultiSchema):
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
        schema = MultiSchema.from_schemas(left_schema, right_schema, left_sname, right_sname)
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
                    record = MultiRecord.from_records(left_rec, right_rec, left_sname, right_sname, schema)
                    if self.evaluate_condition(join_clause.condition, record):
                        # join condition matched
                        self.append_recordset(rsname, record)

        elif join_clause.join_type == JoinType.LeftOuter:
            # there should be at least one record each left record
            left_record_added = False
            for left_rec in left_iter:
                right_iter = self.recordset_iter(right_rsname)
                for right_rec in right_iter:
                    record = MultiRecord.from_records(left_rec, right_rec, left_sname, right_sname, schema)
                    if self.evaluate_condition(join_clause.condition, record):
                        # join condition matched
                        self.append_recordset(rsname, record)
                        left_record_added = True
                if not left_record_added:
                    # add a null right record
                    # create and join records
                    right_rec = create_null_record(right_schema)
                    record = MultiRecord.from_records(left_rec, right_rec, left_sname, right_sname, schema)
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
                    record = MultiRecord.from_records(left_rec, right_rec, left_sname, right_sname, schema)
                    if self.evaluate_condition(join_clause.condition, record):
                        # join condition matched
                        self.append_recordset(rsname, record)
                        right_joined_index[index] = True

            # handle any un-joined right records
            for index, right_rec in self.recordset_iter(right_rsname):
                if right_joined_index[index]:
                    continue
                left_rec = create_null_record(left_schema)
                record = MultiRecord.from_records(left_rec, right_rec, left_sname, right_sname, schema)
                self.append_recordset(rsname, record)

        elif join_clause.join_type == JoinType.FullOuter:
            # there should be atleast one record for each left and right record
            left_record_added = False
            right_joined_index = [False for _ in self.recordset_iter(right_rsname)]
            for left_rec in left_iter:
                right_iter = self.recordset_iter(right_rsname)
                for index, right_rec in enumerate(right_iter):
                    record = MultiRecord.from_records(left_rec, right_rec, left_sname, right_sname, schema)
                    if self.evaluate_condition(join_clause.condition, record):
                        # join condition matched
                        self.append_recordset(rsname, record)
                        left_record_added = True
                        right_joined_index[index] = True
                if not left_record_added:
                    # add a null right record
                    # create and join records
                    right_rec = create_null_record(right_schema)
                    record = MultiRecord.from_records(left_rec, right_rec, left_sname, right_sname, schema)
                    self.append_recordset(rsname, record)
            # handle any un-joined right records
            for index, right_rec in self.recordset_iter(right_rsname):
                if right_joined_index[index]:
                    continue
                left_rec = create_null_record(left_schema)
                record = MultiRecord.from_records(left_rec, right_rec, left_sname, right_sname, schema)
                self.append_recordset(rsname, record)

        else:
            assert join_clause.join_type == JoinType.Cross
            for left_rec in left_iter:
                right_iter = self.recordset_iter(right_rsname)
                for right_rec in right_iter:
                    record = MultiRecord.from_records(left_rec, right_rec, left_sname, right_sname, schema)
                    self.append_recordset(rsname, record)

        return Response(True, body=rsname)

    def evaluate_condition(self, condition: OrClause, record) -> bool:
        """
        Evaluate condition on record Union(Record, JoinedRecord) and return bool result
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

    def evaluate_expr_type(self, expr: OrClause) -> Any:
        """
        Determine the type of this expr.

        However, the expr may ref column name, (and perhaps other objects e.g. functions).
        So we need a name_resolve: str -> DataType

        Note: an OrClause can represent: 1) a condition (evaluates to a bool),
        or act as the de-facto root of the expr hierarchy (evaluates to a literal or a symbol) (here)
        """
        expr_type = None
        for and_clause in expr.and_clauses:
            assert len(and_clause.predicates) == 1, "algebraic evaluation not implemented"
            for predicate in and_clause.predicates:
                if isinstance(predicate, ColumnName):
                    resp = self.scope_resolve_column_name_type(predicate)
                    assert resp.success
                    column_type = resp.body
                    expr_type = column_type
                    return expr_type
                else:
                    # todo: handle algebraic expression and the like
                    raise NotImplementedError

    def stringify_expr(self, expr: OrClause) -> str:
        """
        TODO: move this to parser, or some other utils
        """
        assert len(expr.and_clauses) == 1, "algebraic evaluation not implemented"
        assert len(expr.and_clauses[0].predicates) == 1, "algebraic evaluation not implemented"
        assert isinstance(expr.and_clauses[0].predicates[0], ColumnName)
        return expr.and_clauses[0].predicates[0].name

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

    def init_scopes(self):
        """

        Should be invoked by __init__
        # TODO: move this under init_catalog
        scopes can be nested;
        Will need to reiterate on this interface
        """
        self.scopes = []

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
        # todo: complete me
        breakpoint()

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

    def get_recordset_schema(self, name: str) -> BaseSchema:
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

    def grouped_recordset_iter(self, name):
        """
        return a pair of (group_key, group_recordset_iterator)
        """
        return [(group_key, iter(group_rset)) for group_key, group_rset in self.grouprsets[name].items()]

