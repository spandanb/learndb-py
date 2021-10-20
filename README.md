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

- Also consulted MIT's 6-830 Notes: [6-830 Course Notes](https://ocw.mit.edu/courses/electrical-engineering-and-computer-science/6-830-database-systems-fall-2010)

## Run

- Requires > python 3.10 and pytest
- Run tests:
`python -m pytest -s tests.py`

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

performs internal consistentcy checks on tree
> .validate

Deleting the `db.file` will effectively drop the entire database.
