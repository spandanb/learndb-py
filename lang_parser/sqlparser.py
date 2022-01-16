from typing import Any, List, Type, Tuple, Optional, Union

from .tokens import TokenType, KEYWORDS, Token
from .symbols import (Symbol,
                      Program,
                      CreateStmnt,
                      InsertStmnt,
                      DeleteStmnt,
                      DropStmnt,
                      TruncateStmnt,
                      UpdateStmnt,
                      ColumnDef,
                      SelectExpr,
                      Selectable,
                      AliasableSource,
                      Joining,
                      JoinType,
                      OnClause,
                      WhereClause,
                      AndClause,
                      Predicate,
                      Term)
from .utils import pascal_to_snake, ParseError


class Parser:
    """
    sql parser

    Args:
        raise_exception(bool): whether to raise on parse error

    grammar :
        program -> (stmnt ";") * EOF

        stmnt   -> select_expr
                | create_stmnt
                | drop_stmnt
                | insert_stmnt
                | update_stmnt
                | delete_stmnt
                | truncate_stmnt

        select_expr      -> "select" selectable "from" from_item "where" where_clause
        selectable       -> (column_name ",")* (column_name)
        column_name      -> IDENTIFIER
        from_location    -> table_name/view_name source_alias? | source_alias? | joined_objects | ( select_expr ) source_alias ?
        joined_objects   -> from_location ( "inner" | "left" | "right" | "outer" )
                            "join" from_location
                            "on" predicate
        where_clause     -> or_clause*
        or_clause        -> (and_clause "or")* (and_clause)
        and_clause       -> (predicate "and")* (predicate)
        predicate        -> term ( ( ">" | ">=" | "<" | "<=" | "<>" | "=" ) term ) ;

        create_stmnt     -> "create" "table" table_name "(" column_def_list ")"
        column_def_list  -> (column_def ",")* column_def
        column_def       -> column_name datatype ("primary key")? ("not null")?
        table_name       -> IDENTIFIER

        drop_stmnt       -> "drop" "table" table_name

        insert_stmnt     -> "insert" "into" table_name "(" column_name_list ")" "values" "(" value_list ")"
        column_name_list -> (column_name ",")* column_name
        value_list -> (value ",")* value

        delete_stmnt     -> "delete" "from" table_name ("where" where_clause)?

        update_stmnt     -> "update" table_name "set" column_name = value ("where" where_clause)?

        truncate_stmnt   -> "truncate" table_name

        term             -> NUMBER | STRING | IDENTIFIER

        INTEGER_NUMBER   -> {0-9}+
        FLOAT_NUMBER     -> {0-9}+(.{0-9}+)?
        STRING           -> '.*'
        IDENTIFIER       -> {_a-zA-z0-9}+
    """
    def __init__(self, tokens: List, raise_exception=True):
        self.tokens = tokens
        self.current = 0  # pointer to current token
        self.errors = []
        self.raise_exception = raise_exception

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
        """
        Peek next token
        :return:
        """
        return self.tokens[self.current]

    def peekpeek(self) -> Optional[Token]:
        """
        Peek past next token
        :return:
        """
        if self.current + 1 > len(self.tokens):
            return None
        return self.tokens[self.current + 1]

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
        # check point; we need parser to raise error so
        # we may backtrack
        old_raise_exc_state = self.raise_exception
        self.raise_exception = True

        # determine method that will attempt to parse the symbol
        symbol_typename = symbol_type.__name__.__str__()
        handler_name = pascal_to_snake(symbol_typename)
        handler = getattr(self, handler_name)
        resp = None, None
        try:
            symbol = handler()
            resp = True, symbol
        except ParseError as e:
            self.current = old_current
            resp = False, None
        # restore raise_error
        self.raise_exception = old_raise_exc_state
        return resp

    def check(self, *types: TokenType) -> bool:
        """
        return True if `token_type` matches current token
        """
        if self.is_at_end():
            return False
        for token_type in types:
            if self.peek().token_type == token_type:
                return True
        return False

    def report_error(self, message: str, token: Token = None):
        """
        report error and return error sentinel object
        """
        self.errors.append((token, message))
        if self.raise_exception:
            raise ParseError(message, token)

    # section: rule handlers - top level statements/expr

    def program(self) -> Program:
        program = []
        while not self.is_at_end():
            peeked = self.peek().token_type
            if peeked == TokenType.SELECT:
                # select statement
                program.append(self.select_expr())
            elif peeked == TokenType.CREATE:
                # create
                program.append(self.create_stmnt())
            elif peeked == TokenType.INSERT:
                # insert
                program.append(self.insert_stmnt())
            elif peeked == TokenType.DELETE:
                # delete
                program.append(self.delete_stmnt())
            elif peeked == TokenType.DROP:
                # drop
                program.append(self.drop_stmnt())
            elif peeked == TokenType.TRUNCATE:
                # truncate
                program.append(self.truncate_stmnt())
            elif peeked == TokenType.UPDATE:
                # update
                program.append(self.update_stmnt())
            elif self.check(TokenType.SEMI_COLON):
                self.consume(TokenType.SEMI_COLON, "Expected [;]")
            else:
                self.report_error("Unexpected token", self.peek())
                break
        return Program(program)

    def create_stmnt(self) -> CreateStmnt:
        """
        create_stmnt -> "create" "table" table_name "(" column_def_list ")"
        :return:
        """
        self.consume(TokenType.CREATE, "Expected keyword [CREATE]")
        self.consume(TokenType.TABLE, "Expected keyword [TABLE]")
        table_name = self.table_name()
        self.consume(TokenType.LEFT_PAREN, "Expected [(]")  # '('
        column_def_list = self.column_def_list()
        self.consume(TokenType.RIGHT_PAREN, "Expected [)]")
        return CreateStmnt(table_name=table_name, column_def_list=column_def_list)

    def select_expr(self) -> SelectExpr:
        """
        select_expr  -> "select" selectable "from" from_item "where" where_clause
        :return:
        """
        # select and from are required
        self.consume(TokenType.SELECT, "Expected keyword [SELECT]")
        selectable = self.selectable()
        from_location = None
        where_clause = None
        # from is optional
        if self.match(TokenType.FROM):
            from_location = self.from_location()
            # where clause is optional
            if self.match(TokenType.WHERE):
                where_clause = self.where_clause()

        return SelectExpr(selectable=selectable, from_location=from_location, where_clause=where_clause)

    def insert_stmnt(self) -> InsertStmnt:
        """
        insert_stmnt -> "insert" "into" table_name "(" column_name_list ")" "values" "(" value_list ")"
        :return:
        """
        self.consume(TokenType.INSERT, "Expected keyword [INSERT]")
        self.consume(TokenType.INTO, "Expected keyword [INTO]")
        table_name = self.table_name()
        self.consume(TokenType.LEFT_PAREN, "Expected [(]")
        column_name_list = self.column_name_list()
        self.consume(TokenType.RIGHT_PAREN, "Expected [)]")
        self.consume(TokenType.VALUES, "Expected keyword [VALUES]")
        self.consume(TokenType.LEFT_PAREN, "Expected [(]")
        value_list = self.value_list()
        self.consume(TokenType.RIGHT_PAREN, "Expected [)]")
        return InsertStmnt(table_name, column_name_list, value_list)

    def delete_stmnt(self) -> DeleteStmnt:
        """
        delete_stmnt -> "delete" "from" table_name ("where" where_clause)?
        :return:
        """
        self.consume(TokenType.DELETE, "Expected keyword [DELETE]")
        self.consume(TokenType.FROM, "Expected keyword [FROM]")
        table_name = self.table_name()
        # where clause is optional
        where_clause = None
        if self.match(TokenType.WHERE):
            where_clause = self.where_clause()
        return DeleteStmnt(table_name, where_clause=where_clause)

    def drop_stmnt(self) -> DropStmnt:
        """
        drop_stmnt -> "drop" "table" table_name
        :return:
        """
        self.consume(TokenType.DROP, "Expected keyword [DROP]")
        self.consume(TokenType.TABLE, "Expected keyword [TABLE]")
        table_name = self.table_name()
        return DropStmnt(table_name)

    def truncate_stmnt(self) -> TruncateStmnt:
        """
        "truncate" table_name
        :return:
        """
        self.consume(TokenType.TRUNCATE, "Expected keyword [TRUNCATE]")
        table_name = self.table_name()
        return TruncateStmnt(table_name)

    def update_stmnt(self) -> UpdateStmnt:
        """
        update_stmnt -> "update" table_name "set" column_name = value ("where" where_clause)?

        :return:
        """
        self.consume(TokenType.UPDATE, "Expected keyword [UPDATE]")
        table_name = self.table_name()
        self.consume(TokenType.SET, "Expected keyword [SET]")
        column_name = self.column_name()
        self.consume(TokenType.EQUAL, "Expected [=]")
        value = self.term()
        where_clause = None
        if self.match(TokenType.WHERE):
            where_clause = self.where_clause()
        return UpdateStmnt(table_name, column_name, value, where_clause=where_clause)

    # section: rule handlers - child statements/expr

    def column_def_list(self) -> List[ColumnDef]:
        column_defs = []
        while True:
            is_match, matched_symbol = self.match_symbol(ColumnDef)
            if not is_match:
                break
            column_defs.append(matched_symbol)
            if not self.check(TokenType.COMMA):
                break
            self.advance()
        return column_defs

    def column_name_list(self) -> List:
        """
        column_name_list -> (column_name ",")* column_name
        :return:
        """
        # there must be at least one column
        names = [self.column_name()]
        while self.match(TokenType.COMMA):
            # keep looping until we see commas
            names.append(self.column_name())
        return names

    def value_list(self) -> List:
        values = [self.term()]
        while self.match(TokenType.COMMA):
            values.append(self.term())
        return values

    def table_name(self) -> Token:
        return self.consume(TokenType.IDENTIFIER, "Expected identifier for table name")

    def column_def(self) -> ColumnDef:
        """
        column_def -> column_name datatype ("primary key")? ("not null")?
        :return:
        """
        col_name = self.column_name()
        datatype_token = None
        # attempt to read data type
        if self.match(TokenType.REAL, TokenType.INTEGER, TokenType.TEXT):
            datatype_token = self.previous()

        is_primary_key = False
        is_nullable = True
        # optional modifiers - break when we reach end of column definition
        while self.peek().token_type not in [TokenType.COMMA, TokenType.RIGHT_PAREN]:
            if self.peek().token_type == TokenType.PRIMARY and self.peekpeek().token_type == TokenType.KEY:
                is_primary_key = True
                is_nullable = False
                self.advance()
                self.advance()
            elif self.peek().token_type == TokenType.NOT and self.peekpeek().token_type == TokenType.NULL:
                is_nullable = False
                self.advance()
                self.advance()
            else:
                self.report_error(f"Expected datatype or column constraint; found {self.peek()}", self.peek())
                break
        # NOTE: there is a another representation of the column that models the columns
        # and it's physical representations; this models a language symbol
        return ColumnDef(col_name, datatype_token, is_primary_key=is_primary_key, is_nullable=is_nullable)

    def selectable(self):
        """
        NOTE: currently I'm only checking column names;
        this should be able to also handle other expressions
        :return:
        """
        # TODO: handle terms
        return Selectable(self.column_name_list())

    def column_name(self) -> Token:
        return self.consume(TokenType.IDENTIFIER, "expected identifier for column name")

    def from_location(self) -> AliasableSource:
        """
        from_location    -> table_name source_alias? | view_name source_alias? | joined_objects | ( select_expr ) source_alias ?
        joined_objects   -> from_location ( "inner" | "left" "outer"? | "right" "outer"? | "cross" )
                            "join" from_location
                            "on" predicate

        This can either be a single logical source (table, view, or nested select expr) or
        a joined source

        # TODO: supported nested select
        :return:
        """
        # old
        # return self.consume(TokenType.IDENTIFIER, "expected identifier for from location")

        source = None
        # 1. read first source
        if self.peek().token_type == TokenType.LEFT_PAREN:
            # 1.1. check if this is a nested sub-query
            raise NotImplementedError
        else:
            #
            source_name = self.consume(TokenType.IDENTIFIER, "expected identifier for from location")
            alias_name = None
            # check if there is an alias
            if self.match(TokenType.IDENTIFIER):
                alias_name = self.previous()
            source = AliasableSource(source_name, alias_name)

        # 2. check for other joined sources
        # 2.1. loop until we see either "where" token or reach end of token stream
        while self.is_at_end() is False and self.peek().token_type != TokenType.WHERE:
            # handle join clause
            if self.match(TokenType.JOIN, TokenType.INNER, TokenType.CROSS):
                self.previous()
                join_type = JoinType.Inner
            elif self.match(TokenType.LEFT):
                join_type = JoinType.LeftOuter
                # check for optional "outer" keyword
                if self.check(TokenType.OUTER):
                    self.advance()
            elif self.match(TokenType.RIGHT):
                join_type = JoinType.RightOuter
                if self.check(TokenType.OUTER):
                    self.advance()
            else:
                self.consume(TokenType.CROSS, "expected join")
                join_type = JoinType.Cross

            # handle right source
            if self.peek().token_type == TokenType.LEFT_PAREN:
                raise NotImplementedError
            else:
                source_name = self.consume(TokenType.IDENTIFIER, "expected identifier for from location")
                alias_name = None
                # check if there is an alias
                if self.match(TokenType.IDENTIFIER):
                    alias_name = self.peek()
                right_source = AliasableSource(source_name, alias_name)

            # if exists, handle on-clause
            on_clause = None
            if self.match(TokenType.ON):
                on_clause = self.on_clause()

            if join_type == JoinType.Cross:
                assert on_clause is None, "cross join cannot have on clause"
            else:
                assert on_clause is not None, "join requires on clause"

            # 2.2. combine and left and right into a single joined source
            source = Joining(source, right_source, join_type, on_clause)

        return source

    def where_clause(self) -> WhereClause:
        """
        where_clause  -> or_clause*

        where_clause  -> (and_clause "or")* (and_clause)
        :return:
        """
        return WhereClause(self.conjunctive_disjunction())

    def on_clause(self) -> OnClause:
        return OnClause(self.conjunctive_disjunction())

    def conjunctive_disjunction(self) -> List[AndClause]:
        """
        Or (conjunction) of ands (disjunction)
        :return:
        """
        or_clauses = [self.and_clause()]
        while self.match(TokenType.OR):
            or_clauses.append(self.and_clause())
        return or_clauses

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
        if self.match(TokenType.INTEGER_NUMBER, TokenType.FLOAT_NUMBER, TokenType.STRING, TokenType.IDENTIFIER):
            return Term(value=self.previous())
        self.report_error("expected a valid term")

    def parse(self) -> Program:
        """
        parse tokens and return a list of statements
        :return:
        """
        return self.program()
