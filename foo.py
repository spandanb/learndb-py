from lang_parser.symbols3 import Joining, ConditionedJoin
from lang_parser.sqlhandler import SqlFrontEnd



def driver():
    """
    Psrse some text
    """
    fe = SqlFrontEnd()
    # fe.parse("select cola from bar where x > 1;")
    text = "select cola from bar where x > (select car from froot where boot);"
    text = "select cola, colb from foo f left join bar r on (select max(fig, farce) from fodo where x = 1);"
    text = "select cola, colb from foo f left join bar r on fx = ry;"
    text = "create table foo (cola integer primary key, colb text not null)"
    text = "select cola from catalog"
    text = "select cola, colb from foo f join bar on x = 1 join car on y = 2 join dar on fx = ry;"
    text = "select cola, colb from foo f inner join bar on x = 1 join car on y = 2 join dar on fx = ry;"
    text = """select cola, colb 
    from foo f 
    inner join goo g
    on f.x = g.y
    where cond = 'mango' and cond2 = 'banana'
    group by f.x, g.y
    having f.x > 3
    order by f.x
    limit 100"""
    text = "insert into bar ( colx, coly, colz) values (30, 20, 40)"

    fe.parse(text)
    print(f'is_succ: {fe.is_success()}')
    if not fe.is_success():
        print(f'{fe.error_summary()}')
    else:
        print(f'{fe.parsed}')


def driver2():
    print(isinstance(ConditionedJoin(None, None, None), Joining))

# driver()
