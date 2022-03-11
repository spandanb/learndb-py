from lang_parser.sqlhandler import SqlFrontEnd


fe = SqlFrontEnd()
# fe.parse("select cola from bar where x > 1;")
text = "select cola from bar where x > (select car from froot where boot);"
text = "select cola, colb from foo f left join bar r on (select max(fig, farce) from fodo where x = 1);"
text = "select cola, colb from foo f left join bar r on fx = ry;"
text = "select cola, colb from foo f join bar on x = 1 join car on y = 2 join dar on fx = ry;"

fe.parse(text)
print(f'is_succ: {fe.is_success()}')
if not fe.is_success():
    print(f'{fe.error_summary()}')
else:
    print(f'{fe.parsed}')