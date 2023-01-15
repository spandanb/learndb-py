# Tasks

## Top Priority
- write e2e tests for passing, and failing use cases
- - ungrouped source impl
- for lang_tests, I should assert on contents, right now only checking if parse is successful
- cleanup learndb.py; ensure all devloops are encoded in some test cases
- how to best structure E2E tests? 
  - how should they be named?

- write tutorial.md
- update README.md

## Lark
  - document how parse tree -> AST is working
  - pretty print transformed tree
  - to_ast 
      - sql_handler should return cleaned up ast
  - _Ast root may need to implement visitor interface
  - write tests for lark
  - validations: when parse tree is being turned into ast, assert things like, e.g. no-on clause on cross-join

## Parser
- ensure rules make sense with `expression` symbol
  - this (expression) should wrap or_clause
  - func_call from grammar/parser side only supports pos args; extend this to allow named args since function objects support named args



## Storage (btree)
- support deletion/free-list  
  - support defragmentation of unallocated space
  - when allocating from free list, allocate whole block, 
    the diff between block size and data size can be accounted for in diff between
    i.e. don't bother chunking blocks- since we'll have to account for padding anyways since free-list blocks have a min size
    
- allocating on fragmented node (with enough space) should trigger 
  single-node-in-place compaction


## Testing
- test all permutations of small test cases
- add stress tests (stress-tests.txt)

## VM
- flow
  - if a statement fails, should exec stop? how is this behavior controlled?
- select statement
  - in addition to cursor iteration; select will have conditions
    and an optimizer
  - what is interface for select
    - user executes select and is returned a pipe object
    
## User API
 - in addition to LearnDB do I want to support:
 - cursor?
 - records should be immutable-since they're shallow copied; or final records returned to user should be separate
 - python db api, i.e. natively supported ?
 - repl should have help message at beginning
   - have an additional/secondary command to output sql example/primer

## documentation/refactoring
- complete architecture.md
- document datatypes (type and valid ranges)
  - int is 4 byte int
  - real is a floating point number, but I'm handling it with much simpler rules than IEEE754
- complete future-work.md  
    - should contain high-level roadmap and interesting areas of future work
- add tutorial
- complete btree-structural-ops.txt
- add btree types (pages/nodes) are bytearray (mutable) not bytes (immutable)

## Cleanliness
- move all code into /learndb ?
- update commits ref email
- run black
- run mypy

## Bugs
  - if I do create table colA, i.e. mixed case columnName, internally I seem to convert and store this as a lowercase identifier
    - and this causes errors, e.g. when I query on colA
  - e2e_test.py::join_test should fail
  - duplicate key not erroring (this might be working now)
  - create table bar (col1 integer primary key, col2 text), i.e. num in colname
  - create table def , requires space after final column and final ')'
  - select count(*) from countries group by country_name

## Release Requirements 
  - complete docs
  - complete tutorial
  - document supported features
  