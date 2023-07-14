# Architecture of LearnDB

The goal of this document is to give a breakdown of the different components of `Learndb`.

We can consider Learndb an RDBMS (relational database management system)- a system for managing the storage of structured data.

## Data Flow

![](./leardb_architecture.png)

To better understand the architecture, let's consider how the user interacts with the system in general. 
1. The user interacts via one of the many interfaces
2. Interface receives either: 1) meta command (administrative tasks) or 2) sql program (operate on database)
3. If the input in 2 was SQL, this is parsed into an AST by the SQL Parser module.
4. ^
5. AST is executed by Virtual Machine
6. Which operates on a stack of abstractions of storage, which ground out on a single file on the local file system
    - State Manager
    - B-Tree
    - Pager
    - File

## Component Breakdown

Learndb can be decomposed into the four logical areas for: 
- storing data
- interfacing with user
- parsing user input (SQL)
- computing user queries over stored data

### Storage

#### Filesystem

- File system provides access to create database file 
- lowest layer of storage hierarchy 
- a single db corresponds to a single file
- state of the database is persisted in a single file. But for the execution 
of some statement, the state is held across memory and disk. Only when the system is closed, is the state of the database
persisted to disk. 

#### Pager
- manages IO to database file
- db file is exposed as a set of pages

#### B-tree
- manages an ordered set over keys
- understands key and values
- one table corresponds to one btree
- on-disk/file data structure, optimized for efficient insertion and retrieval of ordered data
- b-tree interfaces with pager to get pages. Pages are used to store table data in b-tree.
- each b-tree node corresponds 1:1 with one page
- each row (in a table) is encoded such that the key is the primary key of the table, and the value is an encoding of the rest of the columns/fields in the row 

#### State Manager
- provides a higher level abstraction over database
- understand that a database has many table each with a different schema and b-tree
- provides lookup to schema and b-tree, by name, such that virtual machine can operate on them
- Understands catalog
- - Two levels of access: metadata (resolving table tree from name) and data (operate on a sequence of rows, that allow fast (lgn) search along certain dimensions 
- - Catalog

### User Interface

- REPL
- file 

### SQL Parser

### Parser
- converts user specified sql into an AST. 
- The AST is the representation that the VM operates on.

### Compute

#### Virtual Machine (VM)
- VM executes user sql on database state
- The VM takes an AST (instructions), and a database 
(represented by a file) and runs the instructions over the database, in the process evolving the database.


## Flows
Next, we will consider some typical flow, to highlight how different components interact 
- define a database
- define a table
- insert some records into table
- delete some records
- read contents of a table


### Creating a Database
Currently, a database is associated with a single db file. So a database file implicitly corresponds to one database.
The database file has a header; when the header is set- database is initialized 
- a file has pages
- a page is a fixed size contiguous chunk of the file.

### Defining a Table
There is a hardcoded table, catalog. Hardcoded means has a fixed root page number for the tree.
An entry is allocated into 

