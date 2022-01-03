# Future Work
The below is some ideas for future work. 

- Indexing
— default primary key
-— rowid can be alias for primary key; if no pkey- rowid has autoincr behavior. Catalog can store max-row-id
— multiple column key
— Secondary indices- store rowid /pkey + indexed column into primary tree
—- secondary index can use btree class. Key will be column indexed; data will be rowid of row in primary idx. Will require support for var len keys eg if creating an index on a text field

- More complete lang support
— create db cmd
—- db identified via filepath
— select + where, joins, group by
— joins will be nested for loop
— nested subquery
— update columns; 

- Query execution- name resolution, query optimization 
— QO: use primary index, and-clause can use secondary index if needed; else full table scan

- Transactions
— needed for: 
—- multiple table/index updates, eg if one table fails (atomicity)
—- Multi-user reads/writes (consistency + isolation)
—- protect against inconsistent state due to failure (durability)
—- Sqlite implement trans via pager controlling access to page
—- simple trans could be impl with WAL for durability, read and write locks. Locks can be implemented as native FS locks. There can be many read locks. One write lock. 


- Temp tables for large result sets- otherwise OOM. 
- Pager should autoflush pages - ie page cache should be bounded