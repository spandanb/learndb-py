from parsimonious.grammar import Grammar
grammar = Grammar(r"""
select_expr       = select_clause _ from_clause _

select_clause    = "select" _ (column_name _ ",")* _ column_name _ 
from_clause      = "from" _ joined_source
single_source    = source_name _ source_alias?
joined_source    = source_name _ source_alias _ "join" _ source_name _ source_alias _ "on" _ or_clauses

or_clauses       = or_clause*
or_clause        = (and_clause _ "or")* _ (and_clause) _
and_clause       = (predicate _ "and")* _ (predicate) _

and_clause       = (predicate "and")* _ (predicate) _
predicate        = term _ ( ( ">" / ">=" / "<" / "<=" / "<>" / "=" ) _ term )* _
term             = factor _ ( ( "-" / "+" ) _ factor )* _
factor           = unary _ ( ( "/" / "*" ) _ unary )* _
unary            = (( "!" / "-" ) _ unary)* / ( _ primary)
primary          = identifier / integer_number / float_number / "true" / "false" / "null" 

source_name      = identifier
source_alias     = identifier
column_name      = identifier
float_number     = ~"[0-9]+" "." ~"[0-9]*"
integer_number   = ~"[0-9]+"
# identifier       =  ~"[_a-z0-9]+"
identifier = (!Keyword ~"[a-z]" ~"[a-z0-9_\$]"*) / (Keyword ~"[a-z0-9_\$]"+)

Keyword = "select" / "from" / "where" / "on" / "join"  

# whitespace
_ = ~r" "* 

""")

text = "select cola from foo"
text = "select cola from foo f"
text = "select cola from foo f join bar b on x <> y or y >= w"
text = "select cola from foo f join bar b on x <> y"
tree = grammar.parse(text)
print(tree)