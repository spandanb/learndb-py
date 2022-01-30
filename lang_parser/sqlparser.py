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
                      GroupByClause,
                      HavingClause,
                      LimitClause,
                      AndClause,
                      Predicate,
                      Term,
                      Literal)
from .utils import pascal_to_snake, ParseError


class Parser:
    """
    sql parser

    Args:
        raise_exception(bool): whether to raise on parse error

    grammar :

        program          -> (stmnt ";") * EOF

        stmnt            -> select_expr
                         | create_stmnt
                         | drop_stmnt
                         | insert_stmnt
                         | update_stmnt
                         | delete_stmnt
                         | truncate_stmnt

        select_expr      -> select_clause
                          | from_clause
                          | where_clause
                          | group_by_clause
                          | having_clause
                          | order_by_clause
                          | limit_clause

        select_clause    -> "select" (expr ",")* (expr)

        # inspiration: https://www.postgresql.org/docs/14/sql-expressions.html
        expr             -> column_name
                         | case_stmnt
                         | func_call
                         | sub_query    # NOTE: at runtime, VM must enforce that this be a scalar subquery (returns <= 1 rows)
                         | literal
                         | or_clauses
                         | "(" expr ")"

        case_stmnt       -> "case" ("when" expr "then" expr)+ "else" expr
        func_call        -> IDENTIFIER "(" (func_arg ",")* )
        func_arg         -> expr

        from_clause      -> "from" from_location
        from_location    -> source_name source_alias?
                         | joined_objects
                         | ( select_expr ) source_alias
        joined_objects   -> cross_join | non_cross_join
        inner_join       -> from_location "inner"? "join" from_location "on" expr
        outer_join       -> from_location ("left" | "right" | "full") "outer"? "on" expr
        cross_join       -> from_location "cross" "join" from_location

        source_name      -> IDENTIFIER
        column_name      -> IDENTIFIER

        or_clauses       -> or_clause*
        or_clause        -> (and_clause "or")* (and_clause)
        and_clause       -> (predicate "and")* (predicate)

        and_clause       -> (predicate "and")* (predicate)
        predicate        -> term ( ( ">" | ">=" | "<" | "<=" | "<>" | "=" ) term )
        term             -> factor ( ( "-" | "+" ) factor )*
        factor           -> unary ( ( "/" | "*" ) unary )*
        unary            -> ( "!" | "-" ) unary
                         | primary
        primary          -> INTEGER_NUMBER | FLOAT_NUMBER | STRING | "true" | "false" | "null"
                         | "(" expr ")"
                         | IDENTIFIER

        sub_query        -> "(" select_expr ")"

        where_clause     -> "where" expr
        group_by_clause  -> "group" "by" (column_name ",")? (column_name)+
        having_clause    -> "having" or_clause
        order_by_clause  -> "order" "by" (column_name (asc|desc)?)*
        limit_clause     -> "limit" INTEGER_NUMBER, ("offset" INTEGER_NUMBER)?

        create_stmnt     -> "create" "table" table_name "(" column_def_list ")"
        column_def_list  -> (column_def ",")* column_def
        column_def       -> column_name datatype ("primary key")? ("not null")?
        table_name       -> IDENTIFIER

        drop_stmnt       -> "drop" "table" table_name

        insert_stmnt     -> "insert" "into" table_name "(" column_name_list ")" "values" "(" value_list ")"
        column_name_list -> (column_name ",")* column_name
        value_list       -> (value ",")* value

        delete_stmnt     -> "delete" "from" table_name ("where" where_clause)?

        update_stmnt     -> "update" table_name "set" column_name = value ("where" where_clause)?

        truncate_stmnt   -> "truncate" table_name

        or_clause        -> (and_clause "or")* (and_clause)
        and_clause       -> (predicate "and")* (predicate)

        and_clause       -> (predicate "and")* (predicate)
        predicate        -> term ( ( ">" | ">=" | "<" | "<=" | "<>" | "=" ) term )
        term             -> factor ( ( "-" | "+" ) factor )*
        factor           -> unary ( ( "/" | "*" ) unary )*
        unary            -> ( "!" | "-" ) unary
                         | primary
        primary          -> INTEGER_NUMBER | FLOAT_NUMBER | STRING | "true" | "false" | "null"
                         | "(" expr ")"
                         | IDENTIFIER

        # for ref
        #INTEGER_NUMBER   -> {0-9}+
        #FLOAT_NUMBER     -> {0-9}+(.{0-9}+)?
        #STRING           -> '.*'
        #IDENTIFIER       -> {_a-zA-z0-9}+
    """

    def program(self):
        """
        program          -> (stmnt ";") * EOF
        """

    def stmnt(self):
        """
        stmnt       -> select_expr
                    | create_stmnt
                    | drop_stmnt
                    | insert_stmnt
                    | update_stmnt
                    | delete_stmnt
                    | truncate_stmnt
        """

    def select_expr(self):
        """
        select_expr      -> select_clause
                          | from_clause
                          | where_clause
                          | group_by_clause
                          | having_clause
                          | order_by_clause
                          | limit_clause
        """
        if not self.check(TokenType.SELECT):
           raise ParserException("Expected keyword [SELECT]")

        select_clause = self.select_clause()

        # below is old
        self.consume(TokenType.SELECT, "Expected keyword [SELECT]")
        selectable = self.selectable()
        from_clause = None
        where_clause = None
        group_by_clause = None
        having_clause = None
        order_by_clause = None
        limit_clause = None

        # from is optional
        if self.match(TokenType.FROM):
            from_location = self.from_location()
            # optional where clause
            if self.match(TokenType.WHERE):
                where_clause = self.where_clause()
            # optional group by clause
            if self.match(TokenType.GROUP) and self.consume(TokenType.BY, "expected 'by' keyword"):
                group_by_clause = self.group_by_clause()
            # optional having clause
            if self.match(TokenType.HAVING):
                having_clause = self.having_clause()
            # optional order by clause
            if self.match(TokenType.ORDER) and self.consume(TokenType.BY, "expected 'by' keyword"):
                order_by_clause = self.order_by_clause()
            # optional limit clause
            if self.match(TokenType.LIMIT):
                limit_clause = self.limit_clause()

        return SelectExpr(selectable=selectable, from_location=from_clause, where_clause=where_clause,
                          group_by_clause=group_by_clause, having_clause=having_clause,
                          order_by_clause=order_by_clause, limit_clause=limit_clause)

    def select_clause(self):
        """
        select_clause    -> "select" (expr ",")* (expr)
        """
        self.match(TokenType.SELECT, "Expected keyword [SELECT]")
        selectables = []
        loop = True
        while loop:
            is_match, matched_symbol = self.match_symbol(Expr)
            if not is_match:
                loop = False
            else:
                selectables.append(matched_symbol)
        return Selectables[selectables]

    def expr(self):
        """
        expr  -> column_name
        | case_stmnt
        | func_call
        | sub_query    # NOTE: at runtime, VM must enforce that this be a scalar subquery (returns <= 1 rows)
        | literal  # this is parsed under or_clauses
        | "(" expr ")"
        | or_clauses
        """



    def case_stmnt(self):
        """
        case_stmnt       -> "case" ("when" expr "then" expr)+ "else" expr
        """

    def func_call(self):
        """
        func_call        -> IDENTIFIER "(" (func_arg ",")* )
        """

    def func_arg(self):
        """
        func_arg         -> expr
        """

    def from_clause(self):
        """
        from_clause      -> "from" from_location
        """

    def from_location(self):
        """
        from_location    -> source_name source_alias?
        | joined_objects
        | ( select_expr ) source_alias
        """

    def joined_objects(self):
        """
        joined_objects   -> cross_join | non_cross_join
        """

    def inner_join(self):
        """
        inner_join       -> from_location "inner"? "join" from_location "on" expr
        """

    def outer_join(self):
        """
        outer_join       -> from_location ("left" | "right" | "full") "outer"? "on" expr
        """

    def cross_join(self):
        """
        cross_join       -> from_location "cross" "join" from_location
        """

    def source_name(self):
        """
        source_name      -> IDENTIFIER
        """

    def column_name(self):
        """
        column_name      -> IDENTIFIER
        """

    def or_clauses(self):
        """
        or_clauses       -> or_clause*
        """

    def or_clause(self):
        """
        or_clause        -> (and_clause "or")* (and_clause)
        """

    def and_clause(self):
        """
        and_clause       -> (predicate "and")* (predicate)
        """

    def predicate(self):
        """
        predicate        -> term ( ( ">" | ">=" | "<" | "<=" | "<>" | "=" ) term )
        """

    def term(self):
        """
        term             -> factor ( ( "-" | "+" ) factor )*
        """

    def factor(self):
        """
        factor           -> unary ( ( "/" | "*" ) unary )*
        """

    def unary(self):
        """
        unary  -> ( "!" | "-" ) unary
        | primary
        """

    def primary(self):
        """
        primary          -> INTEGER_NUMBER | FLOAT_NUMBER | STRING | "true" | "false" | "null"
        | "(" expr ")"
        | IDENTIFIER
        """

    def sub_query(self):
        """
        sub_query        -> "(" select_expr ")"
        """

    def where_clause(self):
        """
        where_clause     -> "where" expr
        """

    def group_by_clause(self):
        """
        group_by_clause  -> "group" "by" (column_name ",")? (column_name)+
        """

    def having_clause(self):
        """
        having_clause    -> "having" or_clause
        """

    def order_by_clause(self):
        """
        order_by_clause  -> "order" "by" (column_name (asc|desc)?)*
        """

    def limit_clause(self):
        """
        limit_clause     -> "limit" INTEGER_NUMBER, ("offset" INTEGER_NUMBER)?
        """

    def create_stmnt(self):
        """
        create_stmnt     -> "create" "table" table_name "(" column_def_list ")"
        """

    def column_def_list(self):
        """
        column_def_list  -> (column_def ",")* column_def
        """

    def column_def(self):
        """
        column_def       -> column_name datatype ("primary key")? ("not null")?
        """

    def table_name(self):
        """
        table_name       -> IDENTIFIER
        """

    def drop_stmnt(self):
        """
        drop_stmnt       -> "drop" "table" table_name
        """

    def insert_stmnt(self):
        """
        insert_stmnt     -> "insert" "into" table_name "(" column_name_list ")" "values" "(" value_list ")"
        """

    def column_name_list(self):
        """
        column_name_list -> (column_name ",")* column_name
        """

    def value_list(self):
        """
        value_list       -> (value ",")* value
        """

    def delete_stmnt(self):
        """
        delete_stmnt     -> "delete" "from" table_name ("where" where_clause)?
        """

    def update_stmnt(self):
        """
        update_stmnt     -> "update" table_name "set" column_name = value ("where" where_clause)?
        """

    def truncate_stmnt(self):
        """
        truncate_stmnt   -> "truncate" table_name
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
        from_clause = None
        where_clause = None
        group_by_clause = None
        having_clause = None
        order_by_clause = None
        limit_clause = None

        # from is optional
        if self.match(TokenType.FROM):
            from_location = self.from_location()
            # optional where clause
            if self.match(TokenType.WHERE):
                where_clause = self.where_clause()
            # optional group by clause
            if self.match(TokenType.GROUP) and self.consume(TokenType.BY, "expected 'by' keyword"):
                group_by_clause = self.group_by_clause()
            # optional having clause
            if self.match(TokenType.HAVING):
                having_clause = self.having_clause()
            # optional order by clause
            if self.match(TokenType.ORDER) and self.consume(TokenType.BY, "expected 'by' keyword"):
                order_by_clause = self.order_by_clause()
            # optional limit clause
            if self.match(TokenType.LIMIT):
                limit_clause = self.limit_clause()

        return SelectExpr(selectable=selectable, from_location=from_clause, where_clause=where_clause,
                          group_by_clause=group_by_clause, having_clause=having_clause,
                          order_by_clause=order_by_clause, limit_clause=limit_clause)

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
        """
        value_list       -> (value ",")* value
        :return:
        """
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
            if self.match(TokenType.PRIMARY) and self.match(TokenType.KEY):
                is_primary_key = True
                is_nullable = False
            elif self.match(TokenType.NOT) and self.match(TokenType.NULL):
                is_nullable = False
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
            join_type = None
            # consume join type
            if self.match(TokenType.INNER):
                join_type = JoinType.Inner
            elif self.match(TokenType.LEFT):
                join_type = JoinType.LeftOuter
                # check for and consume optional "outer" keyword
                self.match(TokenType.OUTER)
            elif self.match(TokenType.RIGHT):
                join_type = JoinType.RightOuter
                self.match(TokenType.OUTER)
            elif self.match(TokenType.FULL):
                join_type = JoinType.FullOuter
                self.match(TokenType.OUTER)
            elif self.match(TokenType.CROSS):
                join_type = JoinType.Cross

            self.consume(TokenType.JOIN, "expected 'join' keyword")
            if join_type is None:
                # no qualifier means inner join
                join_type = JoinType.Inner

            # handle right source
            if self.peek().token_type == TokenType.LEFT_PAREN:
                raise NotImplementedError
            else:
                source_name = self.consume(TokenType.IDENTIFIER, "expected identifier for from location")
                alias_name = None
                # check if there is an alias
                if self.match(TokenType.IDENTIFIER):
                    alias_name = self.previous()
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

    def group_by_clause(self) -> GroupByClause:
        """

        :return:
        """

    def on_clause(self) -> OnClause:
        return OnClause(self.conjunctive_disjunction())


    def having_clause(self) -> HavingClause:
        """

        :return:
        """

    def limit_clause(self) -> LimitClause:
        """

        :return:
        """

    def conjunctive_disjunction(self) -> List[AndClause]:
        """
        Or (disjunction) of ands (conjunction)
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
        NOTE: I'm forcing predicate to be binary,
        this also avoids parse ambiguity.

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
        term             -> factor ( ( "-" | "+" ) factor )*
        :return:
        """

    def factor(self):
        """
        factor           -> unary ( ( "/" | "*" ) unary )*
        :return:
        """

    def unary(self):
        """
        unary            -> ( "!" | "-" ) unary
                         | primary
        :return:
        """

    def primary(self):
        """
        primary   -> INTEGER_NUMBER | FLOAT_NUMBER | STRING | "true" | "false" | "null"
                 | "(" expr ")"
                 | IDENTIFIER
        :return:
        """
        if self.match(TokenType.FALSE):
            return Literal(False)
        elif self.match(TokenType.TRUE):
            return Literal(True)
        elif self.match(TokenType.NULL):
            return Literal(None)
        elif self.match(TokenType.NUMBER, TokenType.STRING):
            # NOTE: this changes the behavior of literals
            # which were previously enclosed
            return Literal(self.previous().literal)
        elif self.match(TokenType.LEFT_PAREN):
            expr = self.conjunctive_disjunction()
            self.consume(TokenType.RIGHT_PAREN, "Expect ')' after expression.")
            return Grouping(expr)
        else:
            raise self.error(self.peek(), "Expect expression.")




    def termold(self):
        """
        term          -> NUMBER | STRING | IDENTIFIER | NULL

        :return:
        """
        if self.match(TokenType.INTEGER_NUMBER, TokenType.FLOAT_NUMBER, TokenType.STRING, TokenType.IDENTIFIER,
                      TokenType.NULL, TokenType.TRUE, TokenType.FALSE):
            return Term(value=self.previous())
        self.report_error("expected a valid term")

    def parse(self) -> Program:
        """
        parse tokens and return a list of statements
        :return:
        """
        return self.program()
