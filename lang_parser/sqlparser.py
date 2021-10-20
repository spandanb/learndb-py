from __future__ import annotations
"""
PEGs are limited because they expect correctly formed
input. This means, gracefully recovering and displaying meaningful
error messages will be hard/impossible.

Attempting a recursive descent parser.

NOTES about language design;
I'll go with postgres notion- semicolon is (optional) statement terminator
"""
from typing import Any, List, Union
from enum import Enum, auto

from dataclasses import dataclass


# constants
class TokenType(Enum):
    # single-char tokens
    STAR = auto()  # '*'
    LEFT_PAREN = auto()  # '(
    RIGHT_PAREN = auto()  # ')'
    LEFT_BRACKET = auto()  # '['
    RIGHT_BRACKET = auto()  # ']'
    DOT = auto()  # '.'
    EQUAL = auto()  # =
    LESS = auto()  # <
    GREATER = auto()  # >
    COMMA = auto()  # ,

    # 2-char tokens
    LESS_EQUAL = auto()  # <=
    GREATER_EQUAL = auto()  # >=
    NOT_EQUAL = auto()  # represents both: <>, !=

    # misc
    EOF = auto()
    IDENTIFIER = auto()
    NUMBER = auto()
    STRING = auto()

    # keywords
    FROM = auto()
    OR = auto()
    WHERE = auto()
    ON = auto()
    CREATE = auto()
    AND = auto()
    DELETE = auto()
    INSERT = auto()
    JOIN = auto()
    CASE = auto()
    HAVING = auto()
    GROUP = auto()
    ORDER = auto()
    UPDATE = auto()
    BY = auto()
    SELECT = auto()
    NULL = auto()


KEYWORDS = {
    'select',
    'from',
    'where',
    'join',
    'on',
    'group',
    'order',
    'by',
    'having',
    'case',
    'or',
    'and',
    'null',
    'create',
    'delete',
    'update',
    'insert',
}

# exceptions


class ParseError(Exception):
    pass


# core handler: tokenizer
class Token:
    def __init__(self, token_type: TokenType, lexeme: str, literal: Any, line: int):
        self.token_type = token_type
        self.lexeme = lexeme
        self.literal = literal
        self.line = line

    def __str__(self) -> str:
        # when object is printed
        body = f'{self.token_type}'
        tail = self.literal or self.lexeme
        if tail:
            body = f'{body}[{tail}]'
        return body

    def __repr__(self) -> str:
        # appears in collections
        return f'Token({self.__str__()})'


class Tokenizer:
    def __init__(self, source: str, error_reporter):
        self.source = source
        self.tokens = []
        # start of token in current line
        self.start = 0
        # current position in current line
        self.current =0
        self.line = 1
        self.errors = []

    def is_at_end(self) -> bool:
        """return true if scanner is at end of the source"""
        return self.current >= len(self.source)

    def peek(self) -> str:
        """
        return current char without advancing
        :return:
        """
        if self.is_at_end():
            return '\0'
        return self.source[self.current]

    def peek_next(self) -> str:
        """
        returns next to next lookahead character
        """
        if self.current + 1 >= len(self.source):
            return '\0'
        return self.source[self.current + 1]

    def match(self, expected: str):
        """
        conditionally advance, if current
        char matches expected
        """
        if self.is_at_end():
            return False
        if self.source[self.current] != expected:
            return False

        # conditionally increment on match
        self.current += 1
        return True

    def advance(self) -> str:
        """
        advance tokenizer and return consumed char
        :return:
        """
        char = self.source[self.current]
        self.current += 1
        return char

    def add_token(self, token_type: TokenType, literal: Any = None):
        text = self.source[self.start: self.current]
        new_token = Token(token_type, text, literal, self.line)
        self.tokens.append(new_token)

    def scan_token(self):
        """
        scan next token
        :return:
        """
        char = self.advance()
        # single char tokens
        if char == '*':
            self.add_token(TokenType.STAR)
        elif char == '(':
            self.add_token(TokenType.LEFT_PAREN)
        elif char == ')':
            self.add_token(TokenType.RIGHT_PAREN)
        elif char == '[':
            self.add_token(TokenType.LEFT_BRACKET)
        elif char == ']':
            self.add_token(TokenType.RIGHT_BRACKET)
        elif char == '.':
            self.add_token(TokenType.DOT)
        elif char == '=':
            self.add_token(TokenType.EQUAL)
        elif char == ',':
            self.add_token(TokenType.COMMA)
        # could be single or double char; depends on next char
        elif char == '<':
            token_type = TokenType.LESS
            if self.match('='):
                token_type = TokenType.LESS_EQUAL
            elif self.match('>'):
                token_type = TokenType.NOT_EQUAL
            self.add_token(token_type)
        elif char == '>':
            token_type = TokenType.GREATER_EQUAL if self.match('=') else TokenType.GREATER
            self.add_token(token_type)
        # elif
        elif char == ' ' or char == '\r' or char == '\t':
            # ignore whitespace
            pass
        elif char == '\n':
            self.line += 1
        # handle multi-char tokens
        elif char == "'":  # handle string literal
            self.tokenize_string()
        elif char.isdigit():  # handle numeric literal
            self.tokenize_number()
        elif char.isidentifier():
            self.tokenize_identifier()
        else:
            self.scan_error(f"Scanner can't handle char: {char}")

    def scan_tokens(self):
        """
        scan and tokenize source
        returns a list of tokens
        :return:
        """
        while self.is_at_end() is False:
            self.start = self.current
            self.scan_token()

        self.add_token(TokenType.EOF, 'EOF')
        return self.tokens

    def scan_error(self, message: str):
        self.errors.append((self.line, message))

    def tokenize_string(self):
        """
        tokenize string
        """
        while self.peek() != '"' and self.is_at_end() is False:
            if self.peek() == '\n':  # supports multiline strings
                self.line += 1
            self.advance()

        if self.is_at_end():
            self.scan_error(f'Unterminated string')

        # the closing "
        self.advance()
        # trim enclosing quotation marks
        value = self.source[self.start+1: self.current-1]
        self.add_token(TokenType.STRING, value)

    def tokenize_number(self):
        """
        tokenize a number i.e. an integer or floating point
        """
        # whole-number part
        while self.peek().isdigit():
            self.advance()

        # fractional part
        if self.peek() == '.' and self.peek_next().isdigit():
            # consume the "."
            self.advance()

            while self.peek().isdigit():
                self.advance()

        value = float(self.source[self.start: self.current])
        self.add_token(TokenType.NUMBER, value)

    def tokenize_identifier(self):
        """
        tokenize identifer
        :return:
        """
        # consume until we see an alphanumeric char
        while self.peek().isalnum():
            self.advance()
        identifier = self.source[self.start: self.current]
        if identifier in KEYWORDS:
            # reserved keyword
            token_type = TokenType[identifier.upper()]
            self.add_token(token_type)
        else:
            # identifier
            self.add_token(TokenType.IDENTIFIER, identifier)

# parser data classes

@dataclass
class Symbol:
    """
    Symbol is the root of parser hierarchy.
    Comparable to how tokens compose the tokenizer's output, i.e. a stream of tokens,
    Symbols compose the parser's output, i.e. the AST

    """
    def accept(self, visitor: 'Visitor') -> Any:
        return visitor.visit(self)

@dataclass
class Program(Symbol):
    statements: List[Union[SelectExpr]]

@dataclass
class CreateTableStmnt(Symbol):
    table_name: Token
    column_def_list: List[ColumnDef]

@dataclass
class ColumnDef(Symbol):
    column_name: Token
    datatype: Token


@dataclass
class SelectExpr(Symbol):
    """
    NOTE: Expr produce one or more value(s) but do not change the system;
    Stmnt change the state of the system; and may or may not
    return value
    """
    selectable: Any
    from_location: Any
    where_clause: Any = None  # optional

@dataclass
class NamedEntity(Symbol):
    # represents a name, e.g. a column name
    name: str

@dataclass
class Selectable(Symbol):
    selectables: List

@dataclass
class ColumnName(Symbol):
    column_name: Token

@dataclass
class FromLocation(Symbol):
    from_location: Token

@dataclass
class WhereClause(Symbol):
    and_clauses: List[AndClause]

@dataclass
class AndClause(Symbol):
    predicates: List[Predicate]

@dataclass
class Predicate(Symbol):
    # note: this does not support unary function;
    # will likely need to update when predicates can be functions
    first: Term
    op: Token
    second: Term

@dataclass
class Term(Symbol):
    value: Any


# core handler: parser
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

    def match_symbol(self, *symbols: Symbol) -> bool:
        """
        Analog to match that matches on entire symbol
        instead of tokens
        :param symbols:
        :return:
        """
        raise NotImplemented

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
            if self.peek() == TokenType.SELECT:
                # select statement
                program.append(self.select_expr())
            elif self.peek() == TokenType.CREATE:
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
        CreateTableStmnt(table_name=table_name, column_def_list=column_def_list)

    def column_def_list(self) -> List[ColumnDef]:
        column_defs = []
        # todo: change match_symbol to return (bool: matched, symbol: matched)
        while self.match_symbol(ColumnDef):
            pass

    def table_name(self) -> NamedEntity:
        token = self.consume(TokenType.IDENTIFIER, "Expected identifier for table name")
        return NamedEntity(token.literal)

    def column_def(self) -> ColumnDef:
        pass

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

    def column_name(self):
        return ColumnName(self.consume(TokenType.IDENTIFIER, "expected identifier for column name"))

    def from_location(self):
        return FromLocation(self.consume(TokenType.IDENTIFIER, "expected identifier for from location"))

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


# main logic

def sql_parser(source: str):
    """
    tokenize, parse input and return representation of source

    :param source: input to parse
    :return:
    """
    # tokenize
    tokenizer = Tokenizer(source, None)
    tokens = tokenizer.scan_tokens()
    print(f'scanned tokens: {tokens}')
    if tokenizer.errors:
        print("Error: scanner failed with following errors:")
        print(tokenizer.errors)
        return

    # parse the tokens
    parser = Parser(tokens)
    statements = parser.parse()
    print(f'parsed statements: {statements}')
    if parser.errors:
        print("Error: parser failed with following errors:")
        print(parser.errors)
        return


if __name__ == "__main__":
    cmdtext = "select colA from foo where colA <> 4.2"
    # cmdtext = "select colA, colFOO from foo"
    sql_parser(cmdtext)