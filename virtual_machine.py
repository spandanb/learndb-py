# from __future__ import annotations
import logging

from cursor import Cursor
from btree import Tree, TreeInsertResult, TreeDeleteResult
from statemanager import StateManager
from schema import create_record, create_catalog_record, generate_schema, schema_to_ddl
from serde import serialize_record, deserialize_cell
from dataexchange import Response

from lang_parser.visitor import Visitor
from lang_parser.tokens import TokenType
from lang_parser.symbols import Symbol, Program, CreateStmnt, SelectExpr, InsertStmnt, DeleteStmnt
from lang_parser.sqlhandler import SqlFrontEnd


class VirtualMachine(Visitor):
    """
    Execute prepared statements corresponding to some sql statements, on some
    state. The state is encoded as a catalog (which maps to the table
    containing information of all objects (tables + indices).
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
            print(f"bootstrapping schema from [{sql_text}]")
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
                print(f"ERROR: virtual machine failed on: [{stmt}]")
                raise

    def execute(self, stmnt: 'Symbol'):
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
        print(f'visit_create_stmnt: generated DDL: {sql_text}')
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

    def visit_select_expr(self, expr: SelectExpr):
        """
        Handle select expr
        For now prints rows/ or return entire result set
        Later I can consider some fancier/performant mechanism like pipes

        :param expr:
        :return:
        """
        print(f"In vm: select expr")
        self.output_pipe.reset()

        table_name = expr.from_location.literal
        if table_name.lower() == 'catalog':
            tree = self.state_manager.get_catalog_tree()
            schema = self.state_manager.get_catalog_schema()
        else:
            tree = self.state_manager.get_tree(table_name)
            schema = self.state_manager.get_schema(table_name)

        cursor = Cursor(self.state_manager.get_pager(), tree)

        # iterate cursor
        while cursor.end_of_table is False:
            cell = cursor.get_cell()
            resp = deserialize_cell(cell, schema)
            assert resp.success
            record = resp.body
            # print(f"printing record: {record}")
            self.output_pipe.write(record)
            cursor.advance()

    def visit_insert_stmnt(self, stmnt: InsertStmnt):
        # identifier are case sensitive, so don't convert
        table_name = stmnt.table_name.literal

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
        NOTE: the delete condition can cover multiple rows
        for now where cond is restricted to equality condition

        :param stmnt:
        :return:
        """
        # identifier are case sensitive, so don't convert
        table_name = stmnt.table_name.literal
        # check table is not catalog
        assert table_name.lower() != 'catalog', "cannot delete table from catalog; use drop table"

        # get tree and schema
        tree = self.state_manager.get_tree(table_name)
        schema = self.state_manager.get_schema(table_name)

        # scan table and determine which keys to delete, based on where condition
        cursor = Cursor(self.state_manager.get_pager(), tree)
        # keys to delete based on where condition
        del_keys = []

        # for now will restrict where cond to be a single equality
        assert stmnt.where_clause is not None
        assert len(stmnt.where_clause.and_clauses) == 1
        and_clause = stmnt.where_clause.and_clauses[0]
        assert len(and_clause.predicates) == 1
        predicate = and_clause.predicates[0]
        assert predicate.op.token_type == TokenType.EQUAL
        pred_column = predicate.first.value.literal
        pred_value = predicate.second.value.literal

        logging.debug(f'in delete pred-col: {pred_column}, pred-val: {pred_value}')

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

            # only support equality condition
            if record.get(pred_column) == pred_value:
                del_key = record.get(primary_key_col)
                del_keys.append(del_key)

            cursor.advance()
        
        # delete matching keys
        for del_key in del_keys:
            resp = tree.delete(del_key)
            assert resp == TreeDeleteResult.Success, f"delete failed for key {del_key}"
