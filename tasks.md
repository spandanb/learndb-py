# Tasks

## Testing
- test all permutations of small test cases
- add stress tests (stress-tests.txt)


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
 - in addition to LearnDB do I want to support:
  - cursor?
  - python db api, i.e. natively supported ?

## documentation/refactoring
- add architecture.md
- add future-work.md  
    - should contain high-level roadmap and interesting areas of future work
- add tutorial
- complete btree-structural-ops.txt
- Add MIT licence

## Cleanliness
- move all code into /learndb ?
- update commits ref email
- run black
- run mypy
- replace btree::check_update_parent_key -> update_parent_on_new_greater_right_child
- remove utils.py- replace with debugger.logging
  -   should print_tree and friends check log level 
- remove btree- unused methods 

## Bugs
  - duplicate key not erroring
  - create table bar (col1 integer primary key, col2 text), i.e. num in colname