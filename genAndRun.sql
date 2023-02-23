set serveroutput on format wrapped
declare
  procedure execSQL (cmd in varchar2, fail in boolean := false, test boolean := false) is
  begin
    dbms_output.put_line(cmd) ;
    if ( not test )
    then
      execute immediate cmd ;
      dbms_output.put_line('') ;
      dbms_output.put_line('Execution successful') ;
      dbms_output.put_line('') ;
    end if ;
  exception when others then 
    if ( not fail )
    then
      dbms_output.put_line('') ;
      dbms_output.put_line('ERROR') ;
      dbms_output.put_line('') ;
    else
      raise ;
    end if ;
  end ;
begin
  dbms_output.put_line('') ;
  dbms_output.put_line('SQL Execution') ;
  dbms_output.put_line('') ;
  for rec in (
  --
  --   Generate the command here
  --
                SELECT
                  'begin null ; end ;' command
                FROM
                  dba_scheduler_jobs
                WHERE
                  job_name LIKE 'TSTDL_LIQ%_202302%'             
             )
  loop
    execSQL(rec.command, test=>false) ;
  end loop ;
end ;
/    
