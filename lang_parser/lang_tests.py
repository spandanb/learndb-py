from tokenizer import Tokenizer
from sqlparser import Parser


def test_select_stmnt():
    cmd = "select colA from foo where colA <> 4.2"
    tokenizer = Tokenizer(cmd, None)
    tokens = tokenizer.scan_tokens()
    assert len(tokenizer.errors) == 0, tokenizer.errors

    # parse the tokens
    parser = Parser(tokens)
    statements = parser.parse()
    assert len(parser.errors) == 0, parser.errors


def test_create_stmnt():
    cmd = "create table foo ( colA integer, colB text)"
    tokenizer = Tokenizer(cmd, None)
    tokens = tokenizer.scan_tokens()
    assert len(tokenizer.errors) == 0, tokenizer.errors

    # parse the tokens
    parser = Parser(tokens)
    statements = parser.parse()
    assert len(parser.errors) == 0, parser.errors


def test_multi_stmnt():
    cmd = "create table foo ( colA integer, colB text); select cola from foo"
    tokenizer = Tokenizer(cmd, None)
    tokens = tokenizer.scan_tokens()
    assert len(tokenizer.errors) == 0, tokenizer.errors

    # parse the tokens
    parser = Parser(tokens)
    statements = parser.parse()
    assert len(parser.errors) == 0, parser.errors


def test_create_stmnt_fail_no_cols():
    """
    test invalid command raising parser exception.
    NOTE: Currently tokenizer, parser exceptions are
    just messages, and so hard to precisely validate.

    :return:
    """
    cmd = "create table foo ()"
    tokenizer = Tokenizer(cmd, None)
    tokens = tokenizer.scan_tokens()
    assert len(tokenizer.errors) == 0, tokenizer.errors

    # parse the tokens
    parser = Parser(tokens)
    statements = parser.parse()
    assert len(parser.errors) == 1, parser.errors

