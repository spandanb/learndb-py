from tokens import TokenType, KEYWORDS, Token
from symbols import *
from utils import pascal_to_snake, ParseError


class Parser:
    """
    sql parser

    grammar :
        program -> stmnt* EOF

        stmnt   -> select_expr
                | create_table_stmnt
                | insert_stmnt
                | update_stmnt
                | delete_stmnt

        select_expr  -> "select" selectable "from" from_item "where" where_clause
        selectable    -> (column_name ",")* (column_name)
        column_name   -> IDENTIFIER
        from_location -> IDENTIFIER
        where_clause  -> (and_clause "or")* (and_clause)
        and_clause    -> (predicate "and")* (predicate)
        predicate     -> term ( ( ">" | ">=" | "<" | "<=" | "<>" | "=" ) term ) ;

        create_table_stmnt -> "create" "table" table_name "(" column_def_list ")"
        column_def_list -> (column_def ",")* column_def
        column_def -> column_name datatype ("primary key")? ("not null")?
        table_name -> IDENTIFIER

        -- term should also handle other literals
        term          -> NUMBER | STRING | IDENTIFIER
    """
    def __init__(self, tokens: List):
        self.tokens = tokens
        self.current = 0  # pointer to current token
        self.errors = []

    def advance(self) -> Token:
        """
        consumes current token(i.e. increments
        current pointer) and returns it
        """
        if self.is_at_end() is False:
            self.current += 1
        return self.previous()

    def consume(self, token_type: TokenType, message: str) -> Token:
        """
        check whether next token matches expectation; otherwise
        raises exception
        """
        if self.check(token_type):
            return self.advance()
        self.errors.append((self.peek(), message))
        raise ParseError(self.peek(), message)

    def peek(self) -> Token:
        return self.tokens[self.current]

    def previous(self) -> Token:
        return self.tokens[self.current - 1]

    def is_at_end(self) -> bool:
        return self.peek().token_type == TokenType.EOF

    def match(self, *types: TokenType) -> bool:
        """
        if any of `types` match the current type consume and return True
        """
        for token_type in types:
            if self.check(token_type):
                self.advance()
                return True
        return False

    def match_symbol(self, symbol_type: Type[Symbol]) -> Union[Tuple[bool, Any]]:
        """
        Analog to `match` that `matches` on symbol instead of tokens.


        :return: tuple: (is_match, matched_symbol)
        """
        # store token ptr position, so we can reset on non-match
        # otherwise, on non-match, some tokens would still be consumed.
        old_current = self.current

        # determine method that will attempt to parse the symbol
        symbol_typename = symbol_type.__name__.__str__()
        handler_name = pascal_to_snake(symbol_typename)
        handler = getattr(self, handler_name)
        try:
            symbol = handler()
            return True, symbol
        except ParseError as e:
            self.current = old_current
            return False, None

    def check(self, token_type: TokenType):
        """
        return True if `token_type` matches current token
        """
        if self.is_at_end():
            return False
        return self.peek().token_type == token_type

    def error(self, token, message: str):
        """
        report error and return error sentinel object
        """
        self.errors.append((token, message))

    # section: rule handlers

    def program(self) -> Symbol:
        program = []
        while not self.is_at_end():
            if self.peek().token_type == TokenType.SELECT:
                # select statement
                program.append(self.select_expr())
            elif self.peek().token_type == TokenType.CREATE:
                # create
                program.append(self.create_table_stmnt())
        return Program(program)

    def create_table_stmnt(self) -> CreateTableStmnt:
        self.consume(TokenType.CREATE, "Expected keyword [CREATE]")
        self.consume(TokenType.TABLE, "Expected keyword [TABLE]")
        table_name = self.table_name()
        self.consume(TokenType.LEFT_PAREN, "Expected [(]")  # '('
        column_def_list = self.column_def_list()
        self.consume(TokenType.RIGHT_PAREN, "Expected [)]")
        return CreateTableStmnt(table_name=table_name, column_def_list=column_def_list)

    def column_def_list(self) -> List[ColumnDef]:
        column_defs = []
        while True:
            is_match, matched_symbol = self.match_symbol(ColumnDef)
            if not is_match:
                break
            column_defs.append(matched_symbol)
            if self.check(TokenType.COMMA):
                self.advance()
            else:
                break
        return column_defs

    def table_name(self) -> Token:
        return self.consume(TokenType.IDENTIFIER, "Expected identifier for table name")

    def column_def(self) -> ColumnDef:
        """
        column_def -> column_name datatype ("primary key")? ("not null")?
        :return:
        """
        col_name = self.column_name()
        datatype_token = None
        if self.match(TokenType.REAL, TokenType.INTEGER, TokenType.TEXT):
            datatype_token = self.previous()
        else:
            raise ParseError(f"Expected datatype found {self.peek()}")
        return ColumnDef(col_name, datatype_token)

    def select_expr(self) -> SelectExpr:
        # select and from are required
        self.consume(TokenType.SELECT, "Expected keyword [SELECT]")
        selectable = self.selectable()
        # todo: make from clause optional
        self.consume(TokenType.FROM, "Expected keyword [FROM]")
        from_location = self.from_location()
        # where clause is optional
        where_clause = None
        if self.match(TokenType.WHERE):
            where_clause = self.where_clause()

        return SelectExpr(selectable=selectable, from_location=from_location, where_clause=where_clause)

    def selectable(self):
        # there must be at least one column
        items = [self.column_name()]
        while self.match(TokenType.COMMA):
            # keep looping until we see commas
            items.append(self.column_name())
        return Selectable(items)

    def column_name(self) -> Token:
        return self.consume(TokenType.IDENTIFIER, "expected identifier for column name")

    def from_location(self) -> Token:
        return self.consume(TokenType.IDENTIFIER, "expected identifier for from location")

    def where_clause(self):
        """
        where_clause  -> (and_clause "or")* (and_clause)
        :return:
        """
        clauses = [self.and_clause()]
        while self.match(TokenType.OR):
            clauses.append(self.and_clause())
        return WhereClause(clauses)

    def and_clause(self) -> AndClause:
        """
        and_clause -> (predicate "and")* (predicate)
        """
        predicates = [self.predicate()]
        while self.match(TokenType.AND):
            predicates.append(self.predicate())
        return AndClause(predicates=predicates)

    def predicate(self) -> Predicate:
        """
        predicate     -> term ( ( ">" | ">=" | "<" | "<=" | "<>" | "=" ) term ) ;
        :return:
        """
        first = self.term()
        operator = None
        if self.match(TokenType.LESS, TokenType.LESS_EQUAL, TokenType.GREATER, TokenType.GREATER_EQUAL,
                      TokenType.NOT_EQUAL, TokenType.EQUAL):
            operator = self.previous()
        second = self.term()
        return Predicate(first=first, op=operator, second=second)

    def term(self):
        """
        -- term should also handle other literals
        term          -> NUMBER | STRING | IDENTIFIER

        :return:
        """
        if self.match(TokenType.NUMBER, TokenType.STRING, TokenType.IDENTIFIER):
            return Term(value=self.previous())

        raise ParseError("expected a valid term")

    def parse(self) -> List:
        """
        parse tokens and return a list of statements
        :return:
        """
        statements = []
        while self.is_at_end() is False:
            statements.append(self.program())
        return statements
