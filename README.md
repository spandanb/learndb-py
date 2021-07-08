# LearnDB

> What I Cannot Create, I Do Not Understand -Richard Feynman

In the spirit of Feynman's immortal words, the goal of this project is to better understand the internals of databases by
implementing a database management system (DBMS) (sqlite clone) from scratch.

## Parts 

The following are some parts of a DBMS (In no particular order):

- Storage Layer
  - what is the physical medium of storage 
  - how is the data stored and accessed; what is it's layout (data-structure)
    
- Query Execution
  - how is query plan generated

- Query Optimization
  - across plans that yield the same output, how do we pick the lowest cost plan

- Transactions and Concurrency


## Completed

- Btree implementation
- Bootstrap DB REPL 

## References consulted

- I started this project by following cstack's awesome [tutorial](https://cstack.github.io/db_tutorial/)
    
- Later I was primarily referencing: [SQLite Database System: Design and Implementation (1st ed)](https://books.google.com/books?id=9Z6IQQnX1JEC&source=gbs_similarbooks)

- Also consulted: https://ocw.mit.edu/courses/electrical-engineering-and-computer-science/6-830-database-systems-fall-2010/

