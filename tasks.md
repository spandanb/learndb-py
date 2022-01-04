# Tasks

## Testing
- test all permutations of small test cases
- add stress tests (stress-tests.txt)


## Storage (btree)
- support deletion/free-list  
  - support defragmentation of unallocated space
- allocating on fragmented node (with enough space) should trigger 
  single-node-in-place compaction

## VM
- select statement
  - in addition to cursor iteration; select will have conditions
    and an optimizer
  - how to read results? 
    - simple: return materialized result set       
    - complex: use generator object to avoid materializing entire result set 
  - what is interface for select
    - user executes select and is returned a pipe object
- name resolution
  - many vm methods operate directly on parsed tokens
  - a separate name resolution phase should check whether identifier map to any objects, and if create a mapping, which is useable vm operates
  - this checks whether names are valid and defined
  - this could vm methods

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
  - repl should have help message at beginning
    - have an additional/secondary command to output sql example/primer

## documentation/refactoring
- complete architecture.md
- complete future-work.md  
    - should contain high-level roadmap and interesting areas of future work
- add tutorial
- complete btree-structural-ops.txt
- Add MIT licence

## Cleanliness
- move all code into /learndb ?
- update commits ref email
- run black
- run mypy


## Bugs
  - duplicate key not erroring
  - create table bar (col1 integer primary key, col2 text), i.e. num in colname
  - create table def , requires space after final column and final ')'