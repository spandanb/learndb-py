# LearnDB

> What I Cannot Create, I Do Not Understand -Richard Feynman

In the spirit of Feynman's immortal words, the goal of this project is to better understand the internals of databases by
implementing a database management system (DBMS) (sqlite clone) from scratch.

## Parts

The following are some parts of a DBMS (In no particular order):

- Storage Layer
  - how is the data stored (physically and data-structure) and accessed?
  - is the data partitioned or replicated?

- Logical Modelling
  - what is the syntax, semantics, and expressibility of the language/interface for storing data and expressing computations on it
  - sql syntax, relational semantics, declarative style, is the historically dominant choice
  - increasingly, alternatives are becoming popular e.g. functional (hadoop map-reduce, spark) and imperative
  - many different storage models, e.g. key-value, document, graph
  - these alternatives also tend to be more piece-meal, unlike sql
  - and sometimes the distinction between a database and non-database information management/processing system is not clear

- Query Execution
  - how is query plan generated
  - what is the basic unit of work?
  - how is the work distributed and resources managed?

- Query Optimization
  - for a user given query, are there many plans that satisfy it?
  - how do we search this plan-space for the lowest cost plan?

- Transactions and Concurrency
  - how do multiple users concurrently use the system?
  - how does the system ensure concurrent access doesn't leave the system in an inconsistent state?
  - what is an inconsistent state?


## Completed

- Btree implementation
- DB REPL

## Todo
- Use a parse generator (pyparser) to build out logical frontend.

## References consulted

- I started this project by following cstack's awesome [tutorial](https://cstack.github.io/db_tutorial/)

- Later I was primarily referencing: [SQLite Database System: Design and Implementation (1st ed)](https://books.google.com/books?id=9Z6IQQnX1JEC&source=gbs_similarbooks)

- Also consulted:
    - Sqllite file format: [docs](https://www.sqlite.org/fileformat2.html) 
    - MIT's 6-830 Notes: [6-830 Course Notes](https://ocw.mit.edu/courses/electrical-engineering-and-computer-science/6-830-database-systems-fall-2010)

## Run

- Requires > python 3.10 and pytest

- Run btree tests:
`python -m pytest -s btree_tests.py`
  
- Run serde tests:
`... serde_tests.py`

- Run language parser tests:
`... lang_tests.py`

- Run specific test:
`python -m pytest tests.py -k test_name`

- Run REPL: `python learndb.py`

## Getting Started- Tutorial

Let's consider how we can use the repl

start the REPL (Only supports a global, implicit table)

> python learndb.py repl

Currently, supported commands include

insert 3 into tree

>  insert 3

select and output all rows (no filtering support for now)

> select

delete 3 from tree
> delete 3

Supported meta-commands:
quit REPl
> .quit

print btree
> .btree

performs internal consistency checks on tree
> .validate

Deleting the `db.file` will effectively drop the entire database.

### Supported Grammar

Reproduced from `lang_parser/sqlparser.py`

```
    grammar :
        program -> (stmnt ";") * EOF

        stmnt   -> select_expr
                | create_stmnt
                | drop_stmnt
                | insert_stmnt
                | update_stmnt
                | delete_stmnt
                | truncate_stmnt

        select_expr  -> "select" selectable "from" from_item "where" where_clause
        selectable    -> (column_name ",")* (column_name)
        column_name   -> IDENTIFIER
        from_location -> IDENTIFIER
        where_clause  -> (and_clause "or")* (and_clause)
        and_clause    -> (predicate "and")* (predicate)
        predicate     -> term ( ( ">" | ">=" | "<" | "<=" | "<>" | "=" ) term ) ;

        create_stmnt -> "create" "table" table_name "(" column_def_list ")"
        column_def_list -> (column_def ",")* column_def
        column_def -> column_name datatype ("primary key")? ("not null")?
        table_name -> IDENTIFIER

        drop_stmnt -> "drop" "table" table_name

        insert_stmnt -> "insert" "into" table_name "(" column_name_list ")" "values" "(" value_list ")"
        column_name_list -> (column_name ",")* column_name
        value_list -> (value ",")* value

        delete_stmnt -> "delete" "from" table_name ("where" where_clause)?

        update_stmnt -> "update" table_name "set" column_name = value ("where" where_clause)?

        truncate_stmnt -> "truncate" table_name

        NUMBER        -> {0-9}+(.{0-9}+)?
        STRING        -> '.*'
        IDENTIFIER    -> {_a-zA-z0-9}+

```