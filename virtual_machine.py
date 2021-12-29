# from __future__ import annotations
from cursor import Cursor
from btree import Tree
from table import Table
from statemanager import StateManager
from schema import Record, create_record, create_catalog_record, generate_schema, schema_to_ddl
from serde import serialize_record, deserialize_cell
from dataexchange import Response, ExecuteResult, Statement, Row

from lang_parser.visitor import Visitor
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
            print(f"printing record: {record}")
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
        tree.insert(cell)

    def visit_delete_stmnt(self, stmnt: DeleteStmnt):
        pass

    # section : handler helpers
    # special helpers for catalog

    def create_table(self):
        """
        helper method to check whether table exists and if not
        create it in the catalog
        :return:
        """
        # ensure this named table cannot be created
        catalog = Table("CATALOG")

    def delete_row(self, table_name, key):
        pass

    def insert_cell(self, cell: bytes):
        pass

    def insert_row(self, table_name, row):
        '''
        row is an in-mem struct containing data

        :param table_name:
        :param row:
        :return:
        '''
        # table specific serializer
        table = Table(table_name)
        # serialize byte string
        serialized = table.serialize(row)

        # get underlying store, currently tree
        store = self.get_store(table_name)
        # create cursor on store
        cursor = Cursor(store)
        cursor.insert_serialized_row(serialized)

    def execute_select(self) -> Response:
        """
        Execute select
        :return:
        """

        print("executing select...")

        rows = []
        cursor = Cursor(self.database.table)

        while cursor.end_of_table is False:
            row = cursor.get_row()
            print(f"printing row: {row}")
            cursor.advance()
            rows.append(row)

        return Response(True, body=rows)

    def execute_insert(self, statement: 'Statement', table: 'Table') -> Response:
        """
        TODO: change `statement` to key or perhaps entire row objects

        :param statement:
        :param table:
        :return:
        """
        print("executing insert...")
        cursor = Cursor(table)

        row_to_insert = statement.row_to_insert
        print(f"inserting row with id: [{row_to_insert.identifier}]")
        resp = cursor.insert_row(row_to_insert)
        if resp.success:
            print(f"insert [{row_to_insert.identifier}] is successful")
            return Response(True, status=ExecuteResult.Success)
        else:
            print(f"insert [{row_to_insert.identifier}] failed, due to [{resp.body}]")
            return Response(False, resp.body)

    def execute_delete(self, statement: 'Statement', table: 'Table') -> Response:
        print("executing delete...")
        key_to_delete = statement.key_to_delete

        print(f"deleting key: [{key_to_delete}]")
        # return ExecuteResult.Success

        cursor = Cursor(table)
        resp = cursor.delete_key(key_to_delete)
        if resp.success:
            print(f"delete [{key_to_delete}] is successful")
            return Response(True, status=ExecuteResult.Success)
        else:
            print(f"delete [{key_to_delete}] failed")
            return Response(False, error_message=f"delete [{key_to_delete}] failed")


