# from __future__ import annotations
from cursor import Cursor
from table import Table
from statemanager import StateManager
from schema import Record, create_record, create_catalog_record, generate_schema
from serde import serialize_record, deserialize_cell
from dataexchange import Response, ExecuteResult, Statement, Row

from lang_parser.visitor import Visitor
from lang_parser.symbols import Symbol, Program, CreateStmnt, SelectExpr, InsertStmnt, DeleteStmnt


class VirtualMachine(Visitor):
    """
    This will interpret/execute the prepared statements on some
    state. The state is encoded as a catalog (which maps to the table
    containing information of all objects (tables + indices).

    """
    def __init__(self):
        self.state_manager = None

    def run(self, program, state_manager):
        """
        run the virtual machine with program on state
        :param program:
        :return:
        """

        self.state_manager = state_manager
        for stmt in program.statements:
            self.execute(stmt)

    def execute(self, stmnt: 'Symbol'):
        """
        execute statement
        :param stmnt:
        :return:
        """
        stmnt.accept(self)

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
        schema = response.body
        table_name = schema.name
        assert isinstance(table_name, str), "table_name is not string"

        # 2. check whether table name is unique
        # create cursor on catalog table
        pager = self.state_manager.get_pager()
        catalog_tree = self.state_manager.get_catalog_tree()
        cursor = Cursor(pager, catalog_tree)

        # todo: iterate over all records to get all table names
        # while cursor.end_of_table is False:
        #    cell = cursor.get_cell()
        #    record = deserialize_cell(cell, schema)
        #    print(f"printing record: {record}")
        #    cursor.advance()

        # 3. allocate tree for new table
        page_num = self.state_manager.allocate_tree()

        # 4. construct record for table
        # NOTE: for now using page_num as unique int key
        pkey = page_num
        catalog_schema = self.state_manager.get_catalog_schema()
        response = create_catalog_record(pkey, table_name, page_num, catalog_schema)
        if not response.success:
            return Response(False, error_message=f'Failure due to {response.error_message}')

        # 5. serialize record
        schema_record = response.body
        cell = serialize_record(schema_record)

        # 6. insert entry into catalog
        catalog_tree.insert(cell)

    def visit_select_expr(self, expr: SelectExpr):
        """
        Handle select expr
        For now prints rows; should
        :param expr:
        :return:
        """
        print(f"In vm: select expr")

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
            record = deserialize_cell(cell, schema)
            print(f"printing record: {record}")
            cursor.advance()

    def visit_insert_stmnt(self, stmnt: InsertStmnt):
        table_name = stmnt.table_name

        # get schema
        schema = self.state_manager.get_schema(table_name)

        # extract values from stmnt and construct record
        # todo: extract literal from tokens
        record = create_record(stmnt.column_name_list, stmnt.value_list, schema)

        # get table's tree
        tree = self.state_manager.get_tree(table_name)

        cell = serialize_record(record)

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


