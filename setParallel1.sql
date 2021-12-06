with objects as ( select owner,'TABLE' obj_type,table_name obj_name,rtrim(ltrim(degree) ) degree from dba_tables
union select owner,'INDEX' ,index_name ,rtrim(ltrim(degree) ) from dba_indexes)
select
'alter ' || obj_type || ' ' || owner || '.' || obj_name || ' parallel 1 /* OldDegree=' || degree || ' */;'
FROM
objects
WHERE
owner IN ('BNA','TEC','ACE1','SYN1','TGP','WIB'
,'LIQ1','LIQ2','LIQF1','LIQF2')
AND degree != '1' and degree != '0' /* LOB Indexes */
/
