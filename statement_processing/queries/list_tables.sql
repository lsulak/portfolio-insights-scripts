-- name: list_all_tables
SELECT name
  FROM sqlite_schema
 WHERE type = 'table'
   AND name NOT LIKE 'sqlite_%'


-- name: list_all_tmp_tables
SELECT name
  FROM sqlite_master
 WHERE type = 'table'
   AND name LIKE 'tmp_table%'
