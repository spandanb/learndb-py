# Architecture of LearnDB

The goal of this document is to give a breakdown of the different components of `Learndb`.
This is targeted at users who want to understand the code base.

At the highest-level, we have an RDBMS that encapsulates the creation and management of databases.

The operations are specified through a set of statements. These statements  
manipulate the state of the database. The state of the database is persisted in a single file. But for the execution 
of some statement, the state is held across memory and disk.

This divides the system into a frontend - that converts user specified sql
into an AST. The AST is the representation that the backend operates on.

The backend consists chiefly of the Learndb virtual machine (VM). The VM takes an AST (instructions), and a database 
(represented by a file) and runs the instructions over the database, in the process evolving the database.

<DIAGRAM>


Consider a typical flow, where a user (of this or any other DBMS) would: 
- define a database
- define a table
- insert some records into table
- delete some records
- read contents of a table

## Creating a Database
Currently, a database is associated with a single db file.


## Defining a Table
    

## Key Modules/Entities
- LearnDB
- State Manager
- Virtual Machine
- Btree (Persistent Storage) 
- Pager