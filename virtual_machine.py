from cursor import Cursor
from table import Table
from database import Database, StateManager
from datatypes import Response, ExecuteResult, Statement, Row

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

    def run(self, program: Program, state):
        """
        run the virtual machine with program on state
        :param program:
        :return:
        """
        self.state = state
        for stmt in program.statements:
            self.execute(stmt)

    def run_no_state(self, program: Program):
        """
        creating this to handle the special case
        of initializing the database. In all other cases run should be used.
        however, any stateful change will require
        :param program:
        :return:
        """


    def execute(self, stmnt: Symbol):
        """
        execute statement
        :param stmnt:
        :return:
        """
        stmnt.accept(self)

    # section : top-level handlers

    def visit_create_stmnt(self, stmnt: CreateStmnt):
        """
        How does the DDL get handled?



        :param stmnt:
        :return:
        """
        # print(f"In vm: creating table [name={stmnt.table_name}, cols={stmnt.column_def_list}]")
        if self.state is None:
            # handle initialize db
            self.initialize_catalog(stmnt)
        else:
            self.catalog.create_table(stmnt.table_name)


    def visit_select_expr(self, expr: SelectExpr):
        print(f"In vm: select expr")
        self.execute_select()

    def visit_insert_stmnt(self, stmnt: InsertStmnt):
        # statement = Statement(Row(stmnt))
        pass

    def visit_delete_stmnt(self, stmnt: DeleteStmnt):
        pass

    # section : handler helpers
    # special helpers for catalog

    def initialize_catalog(self, stmnt):
        """
        initialize the catalog
        this is needed so state objects can be created
        :return:
        """

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


