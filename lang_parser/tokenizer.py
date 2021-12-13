from .tokens import TokenType, KEYWORDS, Token
from .utils import TokenizeError
from typing import Any


class Tokenizer:
    def __init__(self, source: str, raise_exception=True):
        self.source = source
        self.tokens = []
        # start of token in current line
        self.start = 0
        # current position in current line
        self.current = 0
        self.line = 1
        self.errors = []
        self.raise_exceptions = raise_exception

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
        elif char == ';':
            self.add_token(TokenType.SEMI_COLON)
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
        NB: This only support single quoted strings
        """
        while self.peek() != "'" and self.is_at_end() is False:
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

        # number is a float if has fractional part
        if self.peek() == '.' and self.peek_next().isdigit():
            # consume the "."
            self.advance()

            while self.peek().isdigit():
                self.advance()

            value = float(self.source[self.start: self.current])
            self.add_token(TokenType.FLOAT_NUMBER, value)
        else:
            # number is an integer
            value = int(self.source[self.start: self.current])
            self.add_token(TokenType.INTEGER_NUMBER, value)

    def tokenize_identifier(self):
        """
        tokenize identifer
        :return:
        """
        # consume until we see an alphanumeric char
        while self.peek().isidentifier():
            self.advance()

        # NOTE: keywords are case-insensitive; internally we'll
        # use the lower-cased token
        identifier = self.source[self.start: self.current].lower()
        if identifier in KEYWORDS:
            # reserved keyword
            token_type = TokenType[identifier.upper()]
            self.add_token(token_type)
        else:
            # identifier
            self.add_token(TokenType.IDENTIFIER, identifier)
