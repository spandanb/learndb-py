from cursor import Cursor
from database import Database
from datatypes import Response, ExecuteResult

from lang_parser.visitor import Visitor
from lang_parser.symbols import Symbol, Program, CreateTableStmnt, SelectExpr


class VirtualMachine(Visitor):
    """
    This will interpret/execute the prepared statements
    Calling it virtual machine to sound more badass and aligns
    with sqlite terminology
    """
    def __init__(self):
        self.database = None

    def run(self, program: Program, database: Database):
        """
        run the virtual machine with program
        :param program:
        :return:
        """
        self.database = database
        for stmt in program.statements:
            self.execute(stmt)

    def execute(self, stmnt: Symbol):
        """
        execute statement
        :param stmnt:
        :return:
        """
        stmnt.accept(self)

    def visit_create_table_stmnt(self, stmnt: CreateTableStmnt):
        print(f"In vm: creating table [name={stmnt.table_name}, cols={stmnt.column_def_list}]")

    def visit_select_expr(self, expr: SelectExpr):
        print(f"In vm: select expr")
        self.execute_select()

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


