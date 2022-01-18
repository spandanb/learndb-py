import pytest

from lang_parser.sqlhandler import SqlFrontEnd


def test_select_stmnt():
    cmds = ["select colA from foo where colA <> 4.2",
            "select colA from foo",
            "select colA from foo",
            # "select 32"  # fails
            ]
    handler = SqlFrontEnd()
    for cmd in cmds:
        handler.parse(cmd)
        assert handler.is_success()


def test_misc_succ_stmnt():
    """
    Collection of misc statements that should
    succeed successfully
    - should be moved into statement type specific tests

    :return:
    """
    cmds = ["select cola, colb from foo where cola = 1 and colb = 2 or colc = 3",
            "select cola, colb from foo where cola = 1 and colb = 2 and colc = 3"
            "select cola, colb from foo where cola = 1 and colb = 2 or colc = 3 and cold = 4",
            "select cola, colb from foo f join bar b on f.cola = b.colb",
            "select cola, colb from foo f inner join bar b on f.cola = b.colb",
            "select cola, colb from foo f inner join bar r on f.cola = r.coly",
            "select cola, colb from foo f inner join bar r on f.b = r.y inner join car c on c.x = f.b",
            "select cola, colb from foo f inner join bar r on f.b = r.y left join car c on c.x = f.b",
            "select cola, colb from foo f left outer join bar r on f.b = r.y right join car c on c.x = f.b",
            "select cola, colb from foo f cross join bar r",

            ]
    for cmd in cmds:
        handler = SqlFrontEnd()
        handler.parse(cmd)
        assert handler.is_success()


def test_misc_fail_stmnt():
    """
    misc collections of statements that should fail
    :return:
    """
    cmds = [
        "select cola, colb from foo f cross join bar r on f.x = r.y",  # cross join should not have an on-clause
    ]
    with pytest.raises(AssertionError):
        for cmd in cmds:
            handler = SqlFrontEnd()
            handler.parse(cmd)
            assert handler.is_success()



def test_create_stmnt():
    cmds = [
        "create table foo ( colA integer, colB text)",
        "create table foo ( colA integer primary key, colB text)"
    ]
    for cmd in cmds:
        handler = SqlFrontEnd()
        handler.parse(cmd)
        assert handler.is_success()


def test_delete_stmnt():
    cmds = [
        "delete from table_foo",
        "delete from table_foo where car_name <> 'marmar'"
    ]
    for cmd in cmds:
        handler = SqlFrontEnd()
        handler.parse(cmd)
        assert handler.is_success()


def test_truncate_stmnt():
    cmd = "truncate foo"
    handler = SqlFrontEnd()
    handler.parse(cmd)
    assert handler.is_success()


def test_drop_stmnt():
    cmd = "drop table foo"
    handler = SqlFrontEnd()
    handler.parse(cmd)
    assert handler.is_success()


def test_multi_stmnt():
    cmd = "create table foo ( colA integer, colB text); select cola from foo"
    handler = SqlFrontEnd()
    handler.parse(cmd)
    assert handler.is_success()


def test_insert_stmnt():
    cmds = [
        "insert into table_name (col_a, col_b) values ('val_a', 32)",
        "insert into table_name (col_a, col_b) values ('val_a', 'val_b')"
        "insert into table_name (col_a, col_b) values (11, 92)"
    ]

    handler = SqlFrontEnd()
    for cmd in cmds:
        handler.parse(cmd)
        assert handler.is_success()


def test_update_stmnt():
    cmds = [
        "update table_name set column_name = value where foo = 'bar'",
        "update table_name set column_name = value"
        ]
    handler = SqlFrontEnd()
    for cmd in cmds:
        handler.parse(cmd)
        assert handler.is_success()


def test_create_stmnt_fail_no_cols():
    """
    test invalid command raising parser exception.
    NOTE: Currently tokenizer, parser exceptions are
    just messages, and so hard to precisely validate.

    :return:
    """
    cmd = "create table foo ()"
    handler = SqlFrontEnd()
    handler.parse(cmd)
    assert handler.is_success() is False

