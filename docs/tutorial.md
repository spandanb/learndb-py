# How to use learndb

The tutorial walks through the basic capabilities of learndb. Commands below
are shown in pairs of boxes- where the first box is the command to run,
and the second box is the expected output.

Start the REPL:

```
python run.py repl
```
```
db >
```
Create a table:

```
create table fruits (id integer primary key, name text, avg_weight real)
```
```
Execution of command 'create table fruits (id integer primary key, name text, avg_weight real)' succeeded
```
Insert records:
```
insert into fruits (id, name, avg_weight) values (1, 'apple', 4.2);
```
```
Execution of command 'insert into fruits (id, name, avg_weight) values (1, 'apple', 4.2);' succeeded
```
Insert more records:
```
insert into fruits (id, name, avg_weight) values (2, 'mangoes', 3.5);
insert into fruits  (id, name, avg_weight) values (3, 'carrots', 3.3);
```

Query inserted records
```
select id, name, avg_weight from fruits
```
```
db > select id, name, avg_weight from fruits
Execution of command 'select id, name, avg_weight from fruits' succeeded
Record(id: 1, name: apple, avg_weight: 4.199999809265137)
Record(id: 2, name: mangoes, avg_weight: 3.5)
Record(id: 3, name: carrots, avg_weight: 3.299999952316284)
```

Check what tables exist by querying `catalog`:
- `select sql_text from catalog`


## Hacking/Development.md
    - Instructions here to how to start developing, i.e. how to setup an ide, and step through code and tests

## Current Limitations
- repl can only accept a single line, i.e. command can not be split, over multiple lines.
  - No support for select star, i.e. `select * from foo`
  - Input sql can contain column names in mixed case. However, internally names are stored and accessed with the lower case version of the name.
