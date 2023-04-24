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

```
Learndb
  - 
 
 Pipe
  - 
```

#### Output

`Pipe` contains all records.


### ACID compliance

Atomic - not atomic. No transactions. Also, no guarantee database isn't left in an inconsistent state due to 
partial statement execution.

Consistent - 

Isolated - guaranteed by database file being opened in exclusive read/write mode, and hence only a single connection to 
database exists.

Durable - As durable as files on underlying filesystem.

## The SQL Language (learndb-sql)

The language grammar can be found at: `<repo_root>/learndb/lang_parser/grammar.py`

### Data Definition

#### Constraints

Not Null
Primary Key - unique, not null

#### Data Types

Integer

Real



#### Create Table Statement

```
Create table
```

### Data Manipulation

#### Data Insertion

#### Data Deletion

### Queries

`select * from * where * group by * having * limit * offset * `

#### Scoping

### Functions

#### User-Defined Functions




## Internals 

### storage API

### storage layer (this should be)

- encoding/decoding





The main limitations are lack of transactions, approximate correctness testing/validation, some shortcuts in the 
handling and implementation of floating point numbers, lack of support for common SQL features like wildcard column 
expansion, e.g. 
`select * ...`


Deleting the `db.file` will effectively drop the entire database.


 



## Misc Notes
- LEAF_NODE_MAX_CELLS, INTERNAL_NODE_MAX_CELLS control how many max children each node type can support



## Gotchas





## Parts (TODO: ensure all topics below are addressed here ) <begin>

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
  - how does the system ensure system failures, e.g. power failure don't leave in an unrecoverable/inconsistent state
  - what is an inconsistent state?
  - what is a recoverable state?
  - How are ACID properties guaranteed

<end>



Code Coverage
- Methodoly
- Testing for btree was done using randomized number generation. 
- most other functionality is tested with relatively simple test cases
  - although they are comprehensive (high code-cov)
  - but the logical space 



## Unsupported 
- at a single time, only a writer, per db; i.e. no multi writer
- no authentication
- floats implemented very crudely, expr eval uses a fixed epsilon

[^1]: Arguably, a system can't be called _relational_ without foreign key constraints. But relations can still be 
modelled and foreign keys can still be used- just that the integrity of the constraints can't be enforced. So for 
simplicity, I will call this system an RDBMS. 