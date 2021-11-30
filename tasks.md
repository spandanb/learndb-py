# Tasks

## Storage (btree)
- btree
  - refactor btree to support arbitrary schema
    - update btree to support new leaf node format
    - allocate from allocation block
    - support free-list  
      - support fragmentation of unallocated space
    - complete serde.py::get_cell_key, deserialize_cell 

## VM
- iterating over rows
  - for internal use e.g. check table_name unique, use cursor 
  - iterating with a cursor == unconditional select
- select statement
  - in addition to cursor iteration; select will have conditions
    and an optimizer
  - how to read results? 
      - use generator to avoid materializing entire result set 
  
## Pager
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

