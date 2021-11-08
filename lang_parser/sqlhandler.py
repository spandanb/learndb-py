from __future__ import annotations

from .tokenizer import Tokenizer
from .sqlparser import Parser
# from .interpreter import Interpreter


class SqlFrontEnd:
    """
    A high level interface to tokenize + parser functionality
    """
    def __init__(self, raise_exception=False):
        self.tokenizer = None
        self.parser = None
        self.raise_exception = raise_exception
        # result of parse operation
        self.parsed = None

    def is_success(self):
        """
        whether parse operation is success
        :return:
        """
        return len(self.tokenizer.errors) == 0 and len(self.parser.errors) == 0

    def error_summary(self) -> str:
        """
        if fail, summary of why
        :return:
        """
        summary = ""
        if len(self.tokenizer.errors) > 0:
            summary += str(self.tokenizer.errors)
        if len(self.parser.errors) > 0:
            summary += str(self.parser.errors)
        return summary

    def get_parsed(self):
        return self.parsed

    def parse(self, text: str):
        """
        parse provided text.
        NOTE: this must be called before any of the other method can be invoked
        :return:
        """
        self.tokenizer = Tokenizer(text, self.raise_exception)
        tokens = self.tokenizer.scan_tokens()
        if len(self.tokenizer.errors) == 0:
            # parse
            self.parser = Parser(tokens, self.raise_exception)
            self.parsed = self.parser.parse()


def sql_handler(source: str):
    """
    tokenize, parse input and return representation of source

    :param source: input to parse
    :return: tuple: (is_success, parsed)
    """
    # tokenize
    tokenizer = Tokenizer(source, None)
    tokens = tokenizer.scan_tokens()
    print(f'scanned tokens: {tokens}')
    if tokenizer.errors:
        print("Error: scanner failed with following errors:")
        print(tokenizer.errors)
        return False, None

    # parse the tokens
    parser = Parser(tokens)
    program = parser.parse()
    print(f'parsed statements: {program}')
    if parser.errors:
        print("Error: parser failed with following errors:")
        print(parser.errors)
        return False, None

    return True, program

