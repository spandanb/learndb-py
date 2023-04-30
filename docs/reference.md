# Reference 

The goal of this document is intended to provide a complete reference to learndb from the perspective of a user of 
the system. 

## Preface

### Overview

_Learndb_ is a RDBMS (relational database management system). 

Let's unpack this, _relational_ means it can be used to express relations between different entities, e.g. between
`transactions` and `users` involved in them. In some databases this is expressed through a foreign key constraint, 
which constrains the behavior/evolution of one table based on another table(s). 
This is not supported/yet-implemented in learndb [^1].

_Database_ is a collection of tables, each of which has a schema, and zero or more rows of records. The 
schema defines:
- the names of columns/fields 
- what types of data are supported in each field
- any constraint, e.g. if field data can be null or if the field is primary (i.e. must be unique and not null).

The _state_ of a single database (i.e. the schema of the tables in it, and the data within the tables) is persisted 
in a single file on the host filesystem.

The _management system_ manages multiple databases, i.e. multiple isolated collections of tables. The system exposes
interface(s) for: 
- creating and deleting databases
- creating, modifying, and deleting tables in a database
- adding, and removing data from tables


### Setup

Learndb can only be setup from the source repo (i.e. no installation from package repository, e.g. PyPI). The 
instructions are outlined in [README](../README. md) section `Hacking -> Install`

## Interacting with the Database 

Learndb is an _embedded database_. This means there is no standalone server process. The user/agent connects to the 
RDMBS via: 
- REPL
- python language library
- passing a file of commands to the engine  

Fundamentally, the system takes as input a set of statements and creates and modifies a database based on the system.

### REPL

The _REPL_ (read-evaluate-print loop) provides an interactive interface to provide statements the system can execute.
The user can provide: 1) SQL statements (spec below) or 2) meta commands. SQL statements operate on

#### Meta Commands
Meta commands are special commands that are processed by core engine. These include, commands like `.quit` which exits
the terminal.

But these commands more broadly expose non-standard commands, i.e. not part of sql spec - parser. Why some commands 
are meta commands, rather than part of the sql, e.g. `.nuke` which deletes the content of a database, is a 
peculiarity of how this codebase evolved.   

#### Output

Output is printed to console.

### Python Language Library

`interface.py` defines the `Learndb` class entity which can be imported.

TODO: generate code docs, and link interface.py::Learndb, Pipe here

Two important entities needed to programmatically interact with the database are `Learndb`, i.e. the class that 
represents a handle to the database, and `Pipe`

```
Learndb
  - 
 
 Pipe
  - 
```

```
# create handler instance
db = LearnDB(db_filepath)

# submit statement
resp = db.handle_input("select col_a from foo")
assert resp.success

# below are only needed to read results of statements that produce output
# get output pipe
pipe = db.get_pipe()

# print rows
while pipe.has_msgs():
    print(pipe.read())
    
# close handle - flushes any in-memory state
db.close()
```

#### Output

`Pipe` contains all records.

### Filesystem Storage 

The state of entire DB is stored on a single file. The database can be thought of as a logical entity, that is 
stored in some physical medium.

There is a 1 to 1 correspondence between a file and its database. Hence, we can consider the implied database, when 
discussing a database file, and vice versa. Within the context of a single file, there is a single, global, unnamed 
database. 

This means the language only has 1 part names for tables, i.e. no schema, no namespacing.

Further, deleting the `db.file` effectively equals dropping the entire database.

### ACID compliance

Atomic - not atomic. No transactions. Also, no guarantee database isn't left in an inconsistent state due to 
partial statement execution.

Consistent - strong consistency; storage layer updated synchronously

Isolated - guaranteed by database file being opened in exclusive read/write mode, and hence only a single connection to 
database exists.

Durable - As durable as files on underlying filesystem.

## The SQL Language (learndb-sql)

The learndb-sql grammar can be found at: `<repo_root>/learndb/lang_parser/grammar.py`. 

### Learndb-sql grammar specification

The grammar for learndb-sql is written using [lark](https://github.com/lark-parser/lark). Lark is a parsing library 
that allows defining a custom grammar, and parsers for text based on the grammar into an 
[AST](https://en.wikipedia.org/wiki/Abstract_syntax_tree). We'll go over Lark basics because statements in learndb-sql 
 are specified in lark [grammar language](https://lark-parser.readthedocs.io/en/latest/grammar.html). 

- Grammar rules are specified in a form similar to [EBNR notation](https://en.wikipedia.org/wiki/Extended_Backus%E2%80%93Naur_form).
- the grammar is made up of terminals and rules.
- terminals are named with an uppercase name, and are defined with a literal or regex expression 
  - e.g. `IDENTIFIER : ("_" | ("a".."z") | ("A".."Z"))* ("_" | ("a".."z") | ("A".."Z") | ("0".."9"))+`
  - these define value literals, and keywords of the language
- grammar rules consist of `left-hand-side : right-hand-side`, where the left side has the name of the terminal or 
  rule, and the right side has one or more matching definition expressions   
- rules are named with a lowercase name, and are patterns of literals and symbols (terminals and rules)
- e.g. ```create_stmnt     : "create"i "table"i table_name "(" column_def_list ")" ```
- Here `"create"i`, `"("`, and `")"` are literals that matche `create`, `(`, an`)`,  respectively.
  - `table_name` and `column_def_list` are other rules with their own definitions

### Data Definition

#### Constraints

Tables can have the following constraints:

- `Not Null` - value cannot be null
- `Primary Key` - value cannot be not and must be unique

#### Data Types

Table columns can have the following types:

- `Integer`
  - 32 bit integer
- `Real`
  - single precision floating point number 
- `Text`
  - unlimited length character string
- `Boolean`
- `Null`

Note, how `Real` typed data is handled is different from how floats are typically
handled (i.e. [IEEE754]( https://en.wikipedia.org/wiki/IEEE_754)).

#### Create Table Statement

```
create_stmnt     : "create"i "table"i table_name "(" column_def_list ")"

?column_def_list  : (column_def ",")* column_def
?column_def       : column_name datatype primary_key? not_null?
datatype         : INTEGER | TEXT | BOOL | NULL | REAL
primary_key      : "primary"i "key"i
not_null         : "not"i "null"i
table_name       : SCOPED_IDENTIFIER
IDENTIFIER       : ("_" | ("a".."z") | ("A".."Z"))* ("_" | ("a".."z") | ("A".."Z") | ("0".."9"))+
SCOPED_IDENTIFIER : (IDENTIFIER ".")* IDENTIFIER
```
An example is 
```
Create table fruits (id integer primary key, name text, avg_weight real)
```

> NOTE: an integer primary key must be declared, i.e. it's declaration and datatype are mandatory 

#### Drop Table Statement

```
  drop_stmnt       : "drop"i "table"i table_name
```
An example is 
```
Drop table fruits
```

### Data Manipulation

#### Data Insertion

```
insert_stmnt     : "insert"i "into"i table_name "(" column_name_list ")" "values"i "(" value_list ")"

column_name_list : (column_name ",")* column_name
value_list       : (literal ",")* literal
column_name      : SCOPED_IDENTIFIER
literal          : INTEGER_NUMBER | REAL_NUMBER | STRING | TRUE | FALSE | NULL
```

An example is:

```
insert into fruits (id, name, avg_weight) values (1, 'apple', 4.2);
```


#### Data Deletion

```
delete_stmnt     : "delete"i "from"i table_name where_clause?

where_clause     : "where"i condition
condition        : or_clause
or_clause        : and_clause
                 | or_clause "or"i and_clause
and_clause       : predicate
                 | and_clause "and"i predicate
predicate        : comparison
                 | predicate ( EQUAL | NOT_EQUAL ) comparison
comparison       : term
                 | comparison ( LESS_EQUAL | GREATER_EQUAL | LESS | GREATER ) term
term             : factor
                 | term ( MINUS | PLUS ) factor
factor           : unary
                 | factor ( SLASH | STAR ) unary
unary            : primary
                 | ( BANG | MINUS ) unary

primary          : literal
                 | nested_select
                 | column_name
                 | func_call
```

An example is:

```
delete from fruits where id = 1;
```

### Queries

Let's consider how we can query tables.

```
select_stmnt     : select_clause from_clause?
select_clause    : "select"i selectable ("," selectable)*
selectable       : expr

from_clause      : "from"i source where_clause? group_by_clause? having_clause? order_by_clause? limit_clause?
where_clause     : "where"i condition
group_by_clause  : "group"i "by"i column_name ("," column_name)*
having_clause    : "having"i condition
order_by_clause  : "order"i "by"i (column_name ("asc"i|"desc"i)?)*
limit_clause     : "limit"i INTEGER_NUMBER ("offset"i INTEGER_NUMBER)?

source            : single_source
                  | joining

single_source      : table_name table_alias?

//split conditioned and unconditioned (cross) join as cross join does not have an on-clause
?joining          : unconditioned_join | conditioned_join
conditioned_join  : source join_modifier? "join"i single_source "on"i condition
unconditioned_join : source "cross"i "join"i single_source

join_modifier    : inner | left_outer | right_outer | full_outer

inner            : "inner"i
left_outer       : "left"i ["outer"i]
right_outer      : "right"i ["outer"i]
full_outer       : "full"i ["outer"i]
cross            : "cross"i

// `expr` is the de-facto root of the expression hierarchy
expr             : condition
```

#### Simple Queries

A select statement can contain `from`, `where`, `group by`, `having`, `limit` and `offset` clauses.

The simplest select statement has no `from` clause. This effectively, evaluates any expression. e.g.
```select 1+1```

The simplest select statement over a datasource is  a `select ... from ... ` without a where clause, e.g.
```select name from fruits```

This  will return all rows from the datasource.

#### Query with Conditions

Consider a query with a simple condition

```select name from fruits where id = 1```

Consider a query with a simple condition

```select name from fruits where avg_weight > 2.0 and avg_weight < 5.0 ```

Note, the condition can be composed of arbitrary logical operations, e.g.

```select name from fruits where avg_weight > 2.0 and avg_weight < 5.0 or name = 'apple' ```

#### Scoping

There is a global, assumed scope. All table names live in this global scope. 

Further, aliases for tables in the context of a query, are defined for the duration of the query.

### Functions

#### User-Defined Functions

Theoretically, a user can define functions in one of two ways: 
  - in learndb-sql (non-native); however, this is not yet implemented
  - in the implementation language, i.e. Python (native). For more details see [./functions.txt](./functions.txt)
  
## Internals 

### Storage Layer 

The storage layer consists of an on-disk btree. The btree is accessed through the below API. Any other backing data structure,
that implements the above API could easily replace the current implementation.

#### Storage API

The Storage API, is the implicit (not formally required by virtual machine) API exposed by the storage layer data structure.
The API consists of:
- insert(key, value)
- get(key)
- delete(key)


#### Btree implementation notes
- Many constants that control the layout of the btree are set in `constants.py`
- `LEAF_NODE_MAX_CELLS`, `INTERNAL_NODE_MAX_CELLS` control how many max children, leaf and internal nodes can have, respectively



## Unsupported Features
- at a single time, only a writer, per db; i.e. no multi writer
- no authentication
- floats implemented very crudely; expression eval uses a fixed epsilon

## Footnotes

[^1]: Arguably, a system can't be called _relational_ without foreign key constraints. But relations can still be 
modelled and foreign keys can still be used- just that the integrity of the constraints can't be enforced. So for 
simplicity, I will call this system an RDBMS. 