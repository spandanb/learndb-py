# Tasks


## Storage (btree)
- update btree to support variable len cells
    - update btree to support new leaf node format
       - update insert leaf_node and leaf_node_and_split
    - allocate from allocation block
    - support deletion/free-list  
      - support defragmentation of unallocated space

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

