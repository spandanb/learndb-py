# Architecture of LearnDB

The goal of this document is to give a breakdown of the different components of `Learndb`.
This is targeted at users who want to understand the code base.

At the highest-level, we have an RDBMS ("system") that encapsulates the creation and management of databases.

![](./leardb_architecture.png)

Let's consider how the user interacts with the system in general. 
1. The user interacts via one of the many interfaces
2. Interfaces receives either: 1) meta command (administrative tasks) or 2) sql program (operate on database)
3. If the input in 2 was SQL, this is parsed into an AST by the SQL Parser module.
4. ^
5. AST is executed by Virtual Machine
6. Which operates on a stack of abstractions of storage, which ground out on a single file on the local file system
    - State Manager
    - B-Tree
    - Pager
    - File

-Let's consider interactions from the user's perspective. The user provides a set of statements, which correspond to a set
of operations on the database. The system, processes the input, and attempts to perform these operations - which may change
state of the database. The state of the database is persisted in a single file. But for the execution 
of some statement, the state is held across memory and disk. Only when the system is closed, is the state of the database
persisted to disk. 

This description highlights some core entities/components that constitute `LearnDB`

At one level of abstractions, we can think of the high-level functional areas:
- parsing user inputs
- state/storage management
- computing user inputs over stored 

## Key Modules/Entities
- LearnDB
- State Manager
- Virtual Machine
- Btree (Persistent Storage) 
- Pager

This divides the system into a frontend - that converts user specified sql
into an AST. The AST is the representation that the backend operates on.

The backend consists chiefly of the Learndb virtual machine (VM). The VM takes an AST (instructions), and a database 
(represented by a file) and runs the instructions over the database, in the process evolving the database.

## Entities
### Pager
- manages IO to database file
- db file is exposed as a set of pages to  


Consider a typical flow, where a user (of this or any other DBMS) would: 
- define a database
- define a table
- insert some records into table
- delete some records
- read contents of a table

## Creating a Database
Currently, a database is associated with a single db file. So a database file implicitly corresponds to one database.
The database file has a header; when the header is set- database is initialized 
- a file has pages
- a page is a fixed size contiguous chunk of the file.
- 

## Defining a Table
There is a hardcoded table, catalog. Hardcoded means has a fixed root page number for the tree.
An entry is allocated into 

