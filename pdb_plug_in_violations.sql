set lines 250 trimout on pages 2000 tab off head on recsep off
col name format a10
col message format a80 word_wrapped
col status format a10
col action format a70
 
break on name
select NAME,MESSAGE,STATUS,ACTION 
from pdb_plug_in_violations 
where status != 'RESOLVED';
