# Architecture of LearnDB

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