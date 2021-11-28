# Tasks

## VM


## Storage (btree)
- btree
  - support persisting catalog info
  - refactor btree to support arbitrary schema
    - btree must understand how to read a cell(bytes) and extract the key
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

