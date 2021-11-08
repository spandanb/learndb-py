
from lang_parser.visitor import Visitor
from lang_parser.symbols import Symbol, Program, CreateTableStmnt, SelectExpr


class VirtualMachine(Visitor):
    """
    This will interpret/execute the prepared statements
    Calling it virtual machine to sound more badass and align
    with sqlite terminology
    """
    def __init__(self):
        # todo: pass table
        pass

    def run(self, program: Program):
        """
        run the virtual machine with program
        :param program:
        :return:
        """
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