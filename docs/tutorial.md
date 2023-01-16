# How to use learndb

- start the REPL: `run repl`


## Hacking/Development.md
    - Instructions here to how to start developing, i.e. how to setup an ide, and step through code and tests 

## Current Limitations
- No support for select star, i.e. `select * from foo`
- Input sql can contain column names in mixed case. However, internally names are stored and accessed with the lower case version of the name. 
