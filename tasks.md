# Tasks

## Top Priority
- docs 
  - tutorial.md
  - architecture.md
    - move to docs/

- tests
  - add tests for drop table
  - group by + having
  
- implement limit, offset
- generate code docs (where should these be placed, perhaps src_docs)
- remove devloops from `interface.py`
- main user facing file is run.py? can it be called run_learndb.py?
- put admin tasks somewhere (Make, python doit, shell)



- bug
  - invalid column name in having clause (possible also in where), crashes the VM

- release 
    - add config file (controls output filepath, etc)


    - update README.md
    - track bugs/gotchas
- btree tests are failing
- write e2e tests for passing, and failing use cases
- - ungrouped source impl
- for lang_tests, I should assert on contents, right now only checking if parse is successful
- cleanup learndb.py; ensure all devloops are encoded in some test cases
- how to best structure E2E tests? 
  - how should they be named?

## Testing
- btree: test all permutations of small test cases
- add stress tests (stress-tests.txt)
- use pytest fixtures
  - seems it would be cleaner to define fixtures, i.e. pre-inited dbs with different schemas
  - right now, I have a lot of boiler plate
- improve coverage and robustness of test suite
  - robustness: try randomized inputs
  - coverage: auto generate new inputs


## User API
 - metaops to list tables, show table schema
 - add config to control
    - how to pass config- update entry point method
    - stop_execution_on_error
    - output data file
 - in addition to LearnDB do I want to support:
 - cursor?
 - records should be immutable-since they're shallow copied; or final records returned to user should be separate
 - repl should have help message at beginning
   - have an additional/secondary command to output sql example/primer
 - run learndb with input file

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


## Lark
  - document how parse tree -> AST is working
  - pretty print transformed tree
  - to_ast 
      - sql_handler should return cleaned up ast
  - write tests for lark
  - validations: when parse tree is being turned into ast, assert things like, e.g. no-on clause on cross-join


## Optimization
- If a function invocation is used, e.g. a count(col_a_) in both select, and having the expr value should be cached 


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




## VM
- flow
  - if a statement fails, should exec stop? how is this behavior controlled?
- select statement
  - in addition to cursor iteration; select will have conditions
    and an optimizer
  - what is interface for select
    - user executes select and is returned a pipe object



## Cleanliness
- move all code into /learndb ?
- run black
- run mypy

## Bugs
  - e2e_test.py::join_test should fail
  - duplicate key not erroring (this might be working now)
  - create table bar (col1 integer primary key, col2 text), i.e. num in colname
  - create table def , requires space after final column and final ')'
  - select count(*) from countries group by country_name

## Release Requirements 
  - complete docs
  - complete tutorial
  - document supported features
  
