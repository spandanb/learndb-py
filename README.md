# LearnDB

> What I Cannot Create, I Do Not Understand -Richard Feynman

In the spirit of Feynman's immortal words, the goal of this project is to better understand the internals of databases by
implementing a relational database management system (RDBMS) (sqlite clone) from scratch. 

This project was motivated by a desire to: 1) understand databases more deeply and 2) work on a fun project. These dual
goals led to a:
- relatively simple code base 
- relatively complete RDBMS implementation
- written in pure python
  - No build step
- zero configuration
  - configuration can be overriden

This makes the learndb codebase great for tinkering with. But the product has some key limitations that means it 
shouldn't be used as an actual storage solution.

### Features

Learndb supports the following:

- it has a rich sql (learndb-sql) with support for `select, from, where, group by, having, limit, order by` 
- custom lexer and parser built using [`lark`](https://github.com/lark-parser/lark)
- at a high-level, there is an engine that can accept some SQL statements. These statements expresses operations on a 
  database (a collection of tables which contain data)
- allows users/agents to connect to RDBMS in multiple ways: 
  - REPL
  - importing python module  
  - passing a file of commands to the engine  
- on-disk btree implementation as backing data structure

### Limitations

- An approximately verfied system
- Very simplified (and borderline incorrect)[^1] implementation of floating point number arithmetic, e.g. compared to
  [IEEE754](https://en.wikipedia.org/wiki/IEEE_754)). 
- No support for common utility features, like wildcard column expansion, e.g. `select * ...`


## Getting Started: Tinkering and Beyond

- To get started with `learndb` first start with [`tutorial.md`](docs/tutorial.md). 
- Then to understand the system at a deeper technical level read [`reference.md`](docs/reference.md). 
This is essentially a complete reference manual directed at a user of the system. This outlines the operations and 
capabilities of the system. It also describes what is (un)supported and undefined behavior. 
- Architecture.md - this provides a component level breakdown of the repo and the system

## Hacking

### Install 
- System requirements
  - requires a linux/macos system, since it uses `fcntl` to get exclusive read access on database file
  - python >= 3.9
- To install for development, i.e. src can be edited from without having to reinstall:
    - `cd <repo_root>`
    - create virtualenv: `python3 -m venv venv `
    - activate: `source venv/bin/activate`
    - `python -m pip install -r requirements.txt`
    - install in edit mode: `python3 -m pip install -e .`
    
### Run REPL

```
source venv/bin/activate
python learndb.py repl
TODO: ^ validate
```

### Run Tests


- Run btree tests:
-`python -m pytest -s tests/btree_tests.py`  # stdout
- `python -m pytest tests/btree_tests.py`  # suppressed out

- Run end-to-end tests:
`python -m pytest -s  tests/e2e_tests.py`

- Run serde tests:
`... serde_tests.py`

- Run language parser tests:
`... lang_tests.py`

- Run specific test:
`python -m pytest tests.py -k test_name`
  
- Clear pytest cache
`python -m pytest --cache-clear`

- Run REPL: `python learndb.py repl`

### Generate Docs

e.g. ` python -m pydoc -w .\btree.py`


## References consulted

- I started this project by following cstack's awesome [tutorial](https://cstack.github.io/db_tutorial/)
- Later I was primarily referencing: [SQLite Database System: Design and Implementation (1st ed)](https://books.google.com/books?id=9Z6IQQnX1JEC&source=gbs_similarbooks)
- Sqlite file format: [docs](https://www.sqlite.org/fileformat2.html)
- Postgres for how certain SQL statements are implemented and how their [documentation](https://www.postgresql.org/docs/11/index.html) is organized

## Project Management
- immanent work/issues are tracked in `tasks.md`
- long-term ideas are tracked in `docs/future-work.md`

[^1]: When evaluating the difference between two floats, e.g. `3.2 > 4.2`, I consider the condition True if the 
difference between the two is some fixed delta. The accepted epsilon should scale with the magnitude of the number