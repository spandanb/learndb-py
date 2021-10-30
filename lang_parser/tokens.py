"""
Contains types related to tokens
"""
from enum import Enum, auto
from typing import Any, List, Type, Tuple, Union


class TokenType(Enum):
    """
    types of tokens
    """
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
    TABLE = auto()

    INTEGER = auto()
    REAL = auto()
    TEXT = auto()


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
    'table',

    #  datatypes
    'integer',
    'real',  # floating point number
    'text',  # variable length text

}


class Token:
    """
    Represents a token of the source
    """
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

