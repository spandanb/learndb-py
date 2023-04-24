# Architecture of LearnDB

The goal of this document is to give a breakdown of the different components of `Learndb`.
This targeted at users who want to understand and extend the code base.

At the highest-level, we have a RDBMS that encapsulates the creation and management of databases.


This goal of this is to document the key aspects of `LearnDB`.
The following does this from the perspective of typical operations done
on a db.

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