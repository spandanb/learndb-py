# Tasks

## VM
- iterating over rows
  - for internal use e.g. check table_name unique, use cursor 
  - iterating with a cursor == unconditional select
- select statement
  - in addition to cursor iteration; select will have conditions
    and an optimizer
  - how to read results? 
      - use generator to avoid materializing entire result set 

## Storage (btree)
- btree
  - refactor btree to support arbitrary schema
    - btree must understand how to read a cell(bytes) and extract the key
    - this can be 
- pager
  - persist returned page nums
    - when pager is returned a page- it keeps this in an in-mem structure, that
      is lost when the program is terminated. To avoid space leak, persist the returned
      page nums to an on-disk linked list.
  



## document/refactoring/cleanliness
- update commits ref email
- remove match statements
- add tutorial

- document sql grammar that's supported
- run black
- run mypy

