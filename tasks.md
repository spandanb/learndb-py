# Tasks

## Tests
- add E2E tests  

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
      
## I/O 
- Handle read/write to files

## document/refactoring/cleanliness
- update commits ref email
- remove match statements
- add tutorial

- document sql grammar that's supported
- run black
- run mypy

## Bugs
  - it seems that column names are getting converted to their lowercase version. 
    - and subsequent look up the cased name fails. At the very least if the name is being lowercased, a lookup on a cased variant should succeed
  - more broadly what is the "policy" on case sensitivity?