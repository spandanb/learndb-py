from __future__ import annotations

from tokenizer import Tokenizer
from sqlparser import Parser
from interpreter import Interpreter


def sql_handler(source: str):
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

    # likely, the interpreter, vm will sit elsewhere
    interpreter = Interpreter(statements)
    interpreter.interpret()


if __name__ == "__main__":
    # cmdtext = "select colA from foo where colA <> 4.2"
    # cmdtext = "select colA, colFOO from foo"
    cmdtext = "create table foo ( colA integer, colB text)"
    sql_handler(cmdtext)
