SELECT OWNER
FROM ALL_TABLES
WHERE TABLE_NAME = '{0}'
AND (OWNER LIKE 'GMDMI'
OR OWNER LIKE 'GMDMO'
OR OWNER LIKE 'TOBE%')