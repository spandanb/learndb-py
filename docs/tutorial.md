# How to use learndb

Start the REPL:
- `run repl`

Create a table:
- `create table fruits (id integer primary key, name text, avg_weight real)`

Insert records:
- `insert into fruits (id, name, avg_weight) values (1, 'apple', 4.2);`
- `insert into fruits (id, name, avg_weight) values (2, 'mangoes', 3.5);`
- `insert into fruits  (id, name, avg_weight) values (3, 'carrots', 3.3);`

Query inserted records
- `sselect id, name, avg_weight from fruits`
- `select kexy, name, avg_weight from fruits`

Check what tables exists by querying `catalog`:
- `select sql_text from catalog`


## Hacking/Development.md
    - Instructions here to how to start developing, i.e. how to setup an ide, and step through code and tests

## Current Limitations
- repl can only accept a single line, i.e. command can not be split, over multiple lines.
- No support for select star, i.e. `select * from foo`
- Input sql can contain column names in mixed case. However, internally names are stored and accessed with the lower case version of the name.
