# How to use learndb

This tutorial walks through the basic capabilities of learndb. 
It assumes reader has familiarity with (some dialect of) SQL.

Note: Commands below are shown in pairs of boxes- where the first box is the command to run,
and the second box is the expected output. The output is omitted where unnecessary.


### Preamble

> Ensure learndb is [installed](../README.md)


### Start the REPL

```
python run.py repl
```
```
db >
```

### Create Table and Load Data

Create a table:

```
db > create table fruits (id integer primary key, name text, avg_weight real)
```
```
Execution of command 'create table fruits (id integer primary key, name text, avg_weight real)' succeeded
```
Insert records:
```
db > insert into fruits (id, name, avg_weight) values (1, 'apple', 4.2);
```
```
Execution of command 'insert into fruits (id, name, avg_weight) values (1, 'apple', 4.2);' succeeded
```

> Note: There is no auto incrementing key, and each table requires a primary integer key. Hence, we must specify the id.

Insert more records:
```
db > insert into fruits (id, name, avg_weight) values (2, 'mangoes', 3.5);
...
db > insert into fruits  (id, name, avg_weight) values (3, 'carrots', 3.3);
...
```

### Query records
Note, there is no support wildcard column expansion, i.e. `select * ...`
```
db > select id, name, avg_weight from fruits
```
```
Execution of command 'select id, name, avg_weight from fruits' succeeded
Record(id: 1, name: apple, avg_weight: 4.199999809265137)
Record(id: 2, name: mangoes, avg_weight: 3.5)
Record(id: 3, name: carrots, avg_weight: 3.299999952316284)
```
### Query Catalog

Learndb maintains a table `catalog` which keeps track of all user defined tables and objects.
We can check what tables exist by querying `catalog` directly.

```
db > select sql_text from catalog
```
```
Execution of command 'select sql_text from catalog' succeeded
Record(sql_text: CREATE TABLE fruits ( id Integer PRIMARY KEY, name Text , avg_weight Real  ))
```

### Filtering results

We can specify conditions of equality or inequality (less-or-equal, less, greater, greater-or-equal)

```
db > select name, avg_weight from fruits where avg_weight >= 3.5
```
```
Execution of command 'select name, avg_weight from fruits where avg_weight >= 3.5' succeeded
Record(name: apple, avg_weight: 4.199999809265137)
Record(name: mangoes, avg_weight: 3.5)
```
These conditions consist of a simple predicate where one side has a column reference, and the other side a value.
Learndb expects the two sides to be expressions, and this means they can consist of arbitrary algebraic operations.
For example, the previous condition could have been equivalently written as  `avg_weight + 1 >= 4.5`

Further simple predicates can be combined into complex conditions using boolean operators, example:
```
db > select name, avg_weight from fruits where (avg_weight >= 3.6 and avg_weight <= 10.0) or name = 'mango' 
```

### Joining Tables

For this we'll introduce the employees schema

TODO: fill in
```

```





## Supported meta-commands:
quit REPl
> .quit

print btree
> .btree

performs internal consistency checks on tree
> .validate



## Hacking/Development.md
    - Instructions here to how to start developing, i.e. how to setup an ide, and step through code and tests

## Current Limitations
- repl can only accept a single line, i.e. command can not be split, over multiple lines.
  - No support for select star, i.e. `select * from foo`
  - Input sql can contain column names in mixed case. However, internally names are stored and accessed with the lower case version of the name.
- join type must be explicit, i.e. for inner join, "inner" is required