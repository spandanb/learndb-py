# Tasks

## Storage (btree)
- support deletion/free-list  
  - support defragmentation of unallocated space

## VM
- select statement
  - in addition to cursor iteration; select will have conditions
    and an optimizer
  - how to read results? 
    - simple: return materialized result set       
    - complex: use generator object to avoid materializing entire result set 
  - what is interface for select
    - user executes select and is returned a pipe object

## Pager
- pager
  - persist returned page nums
    - when pager is returned a page- it keeps this in an in-mem structure, that
      is lost when the program is terminated. To avoid space leak, persist the returned
      page nums to an on-disk linked list.

## User API
- there are a few candidates for the user api, e.g. 
  - cursor,
  - input_handler + vm.run + pipe
  - the above wrapped in LearnDB class    
  - python db api, i.e. natively supported 
      

## document/refactoring/cleanliness
- update commits ref email
- add tutorial
- regroup btree methods
- run black
- run mypy

## Bugs
  - duplicate key not erroring