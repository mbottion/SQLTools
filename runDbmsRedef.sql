set serveroutput on ;
declare

--
--    This block permit the use of the procedures without creating them in the database 
--  If you can store the procedure (redefine_table) just create the type stmtArray in the database before
--
type stmtArray is table of varchar2(32767) ;

--
--    Examples and explanations in the begin/end block
--
procedure redefine_table (pTableName              in varchar2                      -- Name of the source table
                         ,pBeforeStatements       in stmtArray default stmtArray() -- Statements to run BEFORE the redefinition process
                         ,pCreateStatements       in stmtArray default stmtArray() -- Statements to create the INTERIM Table
                         ,pRedefineStatementsPre  in stmtArray default stmtArray() -- Redefinition statements to be issued BEFORE snapshot creation
                         ,pRedefineStatementsPost in stmtArray default stmtArray() -- Redefinition statements to be issued AFTER snapshot creation
                         ,pAfterStatements        in stmtArray default stmtArray() -- Statements to run AFTER the redefinition process
                         ,pAbort                  in boolean   default false       -- If true : Aborts an ongoing redefinition
                         ,pDropInterim            in boolean   default true        -- If true : Drops the interim TABLE at the end
                         ,pColumnMapping          in varchar2  default null        -- Non default column mappings
                         ,pOwner                  in varchar2  default 'TEC'       -- Owner of the source and target tables
                         ) is
    --
    --    The redefine_table procedure contains all required function, it is used as a package, the main advantage is to have 
    -- only one object in the database. Parameters passed to the procedure can by used in the inner functions, they are
    -- considered as GLOBAL VARIABLES.
    --
    --  Types and Forward declaration of functions
    --
    type colMappingExpTable is table of varchar2(500) index by varchar2(30)     ;
    procedure abortRedef ;
    function toColMappingTable (cmString in varchar2) return colMappingExpTable ;

    --
    --  Global variables
    --
    redefPrefix varchar2(30) := 'REDEF$$'                                       ; -- Name prefix for INTERIM TABLE
    redefTable  varchar2(100) := redefPrefix || pTableName                      ; -- INTERIM table name
    l_num_errors number                                                         ; -- Temporarily hols number of errors in redefinition
    colMappingStringFull varchar2(32767)                                        ; -- String containin full columns remapping
    useCopyDependants boolean := true                                           ; -- If false, you must manage table dependants yourself

    colMappings colMappingExpTable := toColMappingTable(pColumnMapping)         ; -- Table containing column mappings passed on the call

    --
    --  Internal functions
    --

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    --
    --  Function : Transforms a comma separated list of EXPR/COLUMN
    --             couples in an expression table indexed by the
    --             column name.
    --
    --             Result will be used to determine which columns need
    --             to be mapped by default.
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    function toColMappingTable (cmString in varchar2) return colMappingExpTable is
      cmt colMappingExpTable := colMappingExpTable() ;
      l_cmString varchar2(200) ;
      l_sepSTR varchar2(10) := '$@SEP@$' ;
      parenthesisLevel number := 0 ;
      car varchar2 (1) ;
      pos number ;
    begin
      --
      --  Replace separator Commas by a string to differentiate commas in expressions
      --  and separators
      --
      if (cmSTring is null) then return colMappingExpTable() ; end if ;
      
      for i in 1 .. length (cmString)
      loop
        car:=substr(cmString,i,1) ;
        if ( car = '(') then parenthesisLevel := parenthesisLevel + 1 ; end if ;
        if ( car = ')') then parenthesisLevel := parenthesisLevel - 1 ; end if ;
        if (car = ',')
        then
          if (parenthesisLevel = 0)
          then
            l_cmString := l_cmString || l_sepSTR ;
          else
            l_cmString := l_cmString || car ;
          end if;
        else
          l_cmString := l_cmString || car ;
        end if  ;
      end loop ;
      for rec$ in ( with tbl as (select l_cmString str from dual)
                    select           regexp_substr(str, '(.*?)('||l_sepSTR||'|$)', 1, level, null, 1) element
                    from             tbl
                    connect by level <= regexp_count(str, l_sepSTR)+1
                  )
      loop
        pos := instr(rec$.element,' ',-1);
        cmt(upper(substr(rec$.element,pos+1))) := substr(rec$.element,1,pos-1) ;
      end loop ;
      return cmt ;
    end ;
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    --
    --  Function : Prints indented log messages, optionally with
    --             timestamps.
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    procedure log (pText   in varchar2               -- Message to print
                  ,pIndent in varchar2 default null  -- Indentation string
                  ,pNL     in boolean  default true  -- Issue NEW LINE at the end
                  ,pDate   in boolean  default true  -- Prints the timestamp on the line
                  ) is
    begin
      if (pIndent is not null) then dbms_output.put (pIndent) ; end if ;
      if (pDate) then dbms_output.put(to_char(systimestamp,'dd/mm/yyyy hh24:mi:ss.FF3 : ')) ; end if;
      dbms_output.put(pText) ;
      if (pNL) then dbms_output.put_line ('') ; end if ;
    end ;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    --
    --  Function : Logs a section header (Level 1 - Boxed)
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    procedure header1 (pText in varchar2  -- Text of the header
                       ) as
    begin
      dbms_application_info.set_module('Table : ' || upper(pOwner || '.' || pTableName),pText) ;
      dbms_output.put_line('+-' || rpad('-',100,'-') || '-+') ;
      dbms_output.put_line('| ' || rpad(to_char(systimestamp,'dd/mm/yyyy hh24:mi:ss.FF3') || ' : ' || pText,100,' ') || ' |') ;
      dbms_output.put_line('+-' || rpad('-',100,'-') || '-+') ;
    end ;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    --
    --  Function : Logs a section header (Level 2 - Underlined)
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    procedure header2 (pText in varchar2  -- Text of the header
                      ) as
    begin
      dbms_application_info.set_module('Table : ' || upper(pOwner || '.' || pTableName),pText) ;
      dbms_output.put_line('');
      dbms_output.put_line('    - ' || to_char(systimestamp,'dd/mm/yyyy hh24:mi:ss.FF3') || ' : ' || pText) ;
      dbms_output.put_line('      ' || rpad('-',length(to_char(systimestamp,'dd/mm/yyyy hh24:mi:ss.FF3') || ' : ' || pText),'-')) ;
    end ;

    procedure header3 (pText in varchar2  -- Text of the header
                      ) as
    begin
      dbms_application_info.set_module('Table : ' || upper(pOwner || '.' || pTableName),pText) ;
      dbms_output.put_line('');
      dbms_output.put_line('      - ' || to_char(systimestamp,'dd/mm/yyyy hh24:mi:ss.FF3') || ' : ' || pText) ;
      dbms_output.put_line('        ' || replace(rpad('-',length(to_char(systimestamp,'dd/mm/yyyy hh24:mi:ss.FF3') || ' : ' || pText),'-'),'--','- ')) ;
    end ;
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    --
    --  Function : Logs a section footer (Level 1 - Boxed)
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    procedure footer (pText in varchar2) as begin header1(pText) ; end ;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    --
    --  Function : Prints an error messages with optionally aborting
    --             the redefinition process. At the end, a
    --             raise_application_error() is issues with the error code
    --             given
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    procedure error(pNum in number                        -- Error Code to be used with raise_application_error()
                   ,pText in varchar2                     -- Error message
                   ,pAbortRedef in boolean default false  -- If true, aborts the redefinition before exiting
                   ) is
    begin
      if ( pAbortRedef )
      then
        abortRedef ;
      end if ;
      footer (pText) ;
      raise_application_error(pNum,pText) ;
    end ;
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    --
    --  Function : Returns true if a given table exists
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    function tableExists(pTab in varchar2  -- Table name
                        ) return boolean is
      dummy varchar2(100) ;
    begin
      log (rpad('Check table ' || rpad(pOwner||'.'||pTab,30) || ' existence',75)|| ' : ','       - ',false,false) ;
      select table_name
      into   dummy
      from   dba_tables
      where owner = upper(pOwner)
      and   table_name = upper(pTab) ;
      log('Existent',null,true,false) ;
      return true ;
    exception
      when no_data_found then
        log('NON Existent',null,true,false) ;
        return false ;
    end ;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    --
    --  Function : Executes a SQL statement, after replacing
    --             - $$TABLE$$ By the name of the source table
    --             - $$REDEF$$ By the name of the INTERIM TABLE
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   procedure exec_sql(pStmt in varchar2,pAbortRedef in boolean default false) as
     lStmt varchar2(32767) :=  regexp_replace(regexp_replace(pStmt,'\$\$TABLE\$\$'
                                                          ,pOwner || '.' || pTableName,1,0,'i')
                                           ,'\$\$REDEF\$\$',pOwner || '.' || redefTable,1,0,'i') ;
   begin
     log('','',true,false);
     log('Start Statement','      - ') ;
     log(lStmt,'      > ',true,false) ;
     execute immediate lStmt ;
     log('Sucessful execution','      - ') ;
  exception
    when others then
     log(sqlerrm,'      > ',true,false) ;
      log('Execution ERROR','      - ') ;
      error (-20004,sqlerrm,pAbortRedef) ;
  end ;


    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    --
    --  Function : Aborts the redefinition process and optionally drops
    --             the interim table.
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  procedure abortRedef is
  begin
    header1('    Abort a table redefinition ' || upper(pTableName)) ;
    log ('Start ABORT','    - ');
    DBMS_REDEFINITION.abort_redef_table(user, pTableName, redefTable);
    log ('End','    - ') ;
    if ( pDropInterim and tableExists(redefTable) )
    then
      log('Drop interim table','    - ');
      exec_sql ('drop table ' || pOwner || '.' || redefTable) ;
    end if ;
  end ;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    --
    --  Function : Get the metadata of the base table dependent objects 
    --             and creates them on the interim table, this avoids 
    --             using get_dependent_objects at the end since it creates
    --             constraint with VALIDATE, which can be very long
    --
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  procedure createInterimTableFromMetadata is
  begin
    header3 ('Create all objects from metadata') ;
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'PRETTY',true) ;
    for rec$def in ( select 
                       regexp_replace(replace(dbms_metadata.get_ddl('TABLE',table_name,owner),pTableName,redefTable) 
                                             ,' *(GENERATED [^,]*),','  /* \1 */ ,') stmt
                    from dba_tables 
                    where table_name = pTableName and owner=pOwner)
    loop
      exec_sql(rec$def.stmt) ;
    end loop ;
    for rec$def in ( select 
                       replace(replace(dbms_metadata.get_ddl('INDEX',index_name,owner)
                                      ,pTableName
                                      ,redefTable)
                              ,'"SYS_'
                              ,'"SYS'||redefTable||'_') stmt
                    from dba_indexes
                    where table_name = pTableName and table_owner=pOwner and index_type != 'LOB')
    loop
      exec_sql('begin execute immediate ''' || rec$def.stmt || ''' ; exception when others then if (sqlcode in (-1408)) then null ; else raise ; end if ; end ;') ;
    end loop ;
    for rec$def in ( select 
                       replace(replace(dbms_metadata.get_ddl('CONSTRAINT',constraint_name,owner)
                                      ,pTableName
                                      ,redefTable)
                              ,'"SYS_'
                              ,'"'||table_name) stmt
                    from dba_constraints
                    where table_name = pTableName and owner=pOwner and constraint_type != 'R')
    loop
      exec_sql('begin execute immediate ''' || rec$def.stmt || ''' ; exception when others then if (sqlcode in (-40664,-1442,-2260)) then null ; else raise ; end if ; end ;') ;
    end loop ;
    for rec$def in ( select 
                       replace(replace(dbms_metadata.get_ddl('REF_CONSTRAINT',constraint_name,owner)
                                      ,pTableName
                                      ,redefTable)
                              ,'"SYS_'
                              ,'"'||table_name) stmt
                    from dba_constraints
                    where table_name = pTableName and owner=pOwner and constraint_type = 'R')
    loop
      exec_sql('begin execute immediate ''' || rec$def.stmt || ''' ; exception when others then if (sqlcode in (-40664,-1442,-2260)) then null ; else raise ; end if ; end ;') ;
    end loop ;
    for rec$def in ( select 
                       replace(replace(dbms_metadata.get_ddl('trigger_name',trigger_name,owner)
                                      ,pTableName
                                      ,redefTable)
                              ,'"SYS_'
                              ,'"'||table_name) stmt
                    from dba_triggers
                    where table_name = pTableName and owner=pOwner )
    loop
      exec_sql('begin execute immediate ''' || rec$def.stmt || ''' ; exception when others then if (sqlcode in (1)) then null ; else raise ; end if ; end ;') ;
    end loop ;
    --
    --  TODO: Missin table and columns comments
    --
  end ;
-- -----------------------------------------------------------------------
--
--                        M A I N   P R O G R A M
--
-- -----------------------------------------------------------------------
begin
  -- -----------------------------------------------------------------------
  --
  --   Abort from command-line, aborts and exists
  --
  -- -----------------------------------------------------------------------
  if ( pAbort )
  then
    abortRedef ;
    footer('Succesfully aborted') ;
    return;
  end if ;
  -- -----------------------------------------------------------------------
  --
  --   STarts a new redefinition process
  --
  -- -----------------------------------------------------------------------

  dbms_application_info.set_client_info('redefine_table') ;
  header1('    ONLINE table redefinition for table ' || upper(pOwner || '.' || pTableName)) ;


  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- Step 1  : Run pre-processing of the source table, for example to add a pseudo 
  --           pk. The statements of the pBeforeStatements() are run in sequence
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  header2('1) Run PRE-PROCESS statements') ;
  for i in 1 .. pBeforeStatements.count
  loop
    begin
      exec_sql (pBeforeStatements(i),false) ;
      exception
      when others then
        log('Error, no redefinition in progress','',true,false) ;
        log('('||sqlerrm||') .../...','       > ',true,false) ;
        log('','',true,false) ;
        error(-20005,sqlerrm,false) ;
      end ;
  end loop ;

  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- Step 2  : Check pre-requisites
  --           - Source table must exist                   ==> ERR -20001 if non existent
  --           - INTERIM table must NOT exist              ==> ERR -20002 if it exists
  --           - SOurce table must be able to be redefined ==> ERR -20003 if not
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  header2('2) Pre-requisite verifications') ;
  if not tableExists (pTableName) then error(-20001,'Table ' || pOwner || '.' || pTableName || ' does not exist') ; end if ;
  if     tableExists (redefTable) then error(-20002,'Table ' || pOwner || '.' || redefTable || ' already exist') ; end if ;
  log (rpad('Check if table ' || pOwner || '.' || pTableName || ' can be redefined',75)||' : ','       - ',false,false) ;
  begin
    DBMS_REDEFINITION.CAN_REDEF_TABLE (pOwner, pTableName, DBMS_REDEFINITION.CONS_USE_PK) ;
    log('YES, process continues','',true,false) ;
  exception
    when others then
      log('NO, process aborted, no changes made','',true,false) ;
      error(-20003,sqlerrm) ;
  end ;

  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- Step 3  : Create the INTERIM TABLE using passed statements (pCreateStatements),
  --           if no statements are passed, simply create an empty table with
  --           select * from source where 1=2
  --
  --           Special CODES in pCreateStatements: 
  --               -- CREATE_FROM_METADATA : Gets the METADATA creation text of the
  --                                         source table and modify it to create the interim table
  --                                         in that situation, interim table and dependant objects are
  --                                         created before the redefinition
  --
  --           In the statements, use $$TABLE$$ to refer the source TABLE and
  --           $$REDEF$$ to refer to the INTERMIN TABLE.
  --
  --           ==> ERR -20004 in case of error, redefinition is not started and INTERIM
  --               table is dropped unless specified no to drop it in the main call.
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  header2('3) create interim table : ' || redefTable) ;
  if (pCreateStatements.count = 0 )
  then
    exec_sql ('create table ' || pOwner || '.' || redefTable || ' as select * from ' ||  pOwner || '.' || pTableName || ' where 1=2') ;
  else
    for i in 1 .. pCreateStatements.count
    loop
      begin
        if (upper(pCreateStatements(i)) = 'CREATE_FROM_METADATA') 
        then
          useCopyDependants := false ;
          createInterimTableFromMetadata ;
        else
          exec_sql (pCreateStatements(i)) ;
        end if ;
      exception
      when others then
        log('Error, Aborting redefinition','',true,false) ;
        log('('||sqlerrm||') .../...','       > ',true,false) ;
        log('','',true,false) ;
        error(-20004,sqlerrm,true) ;
      end ;
    end loop ;
  end if ;

  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- Step 4:   Apply modifications to the INTERIM TABLE using the array of
  --           statements passed in the pRedefineStatementsPre parameter.
  --
  --           In the statements, use $$TABLE$$ to refer the source TABLE and
  --           $$REDEF$$ to refer to the INTERMIN TABLE.
  --
  --           ==> ERR -20005 in case of error, redefinition is not started and INTERIM
  --               table is dropped unless specified no to drop it in the main call.
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  header2('4) Modify interim table (PRE-SYNC)') ;

  for i in 1 .. pRedefineStatementsPre.count
  loop
    begin
      exec_sql (pRedefineStatementsPre(i),true) ;
      exception
      when others then
        log('Error, Aborting redefinition','',true,false) ;
        log('('||sqlerrm||') .../...','       > ',true,false) ;
        log('','',true,false) ;
        error(-20005,sqlerrm,true) ;
      end ;
  end loop ;

  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- Step 5): Disable check contraints from the interim table
  --           table, they will be re-activaded at the end of the process (in NOVALIDATE mode).
  --
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  header2('5) DISABLE Check Constraints on interim table') ;
  for rec$ in (SELECT
                 'begin execute immediate ''alter table ' || owner || '.' || table_name || ' disable constraint ' || constraint_name || ''' ; exception when others then if (sqlcode = 0) then null ; else raise ; end if ; end; ' disableCtr
               from
                 dba_constraints where owner = pOwner and table_name = redefTable and constraint_type = 'C' and status = 'ENABLED' )
  loop
    exec_sql (rec$.disableCtr) ;
  end loop;
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- Step 6  : Construct the complete list of columns MAPPING, when used
  --           column_mapping must contain all the columns, even those that
  --           are mapped by default. This step builds the full list of
  --           mapping instructions when the pColumnMapping parameter is not null.
  --
  --           This allows the user to only specify the columns that need a
  --           transformation, not the others.
  --
  --           ==> No ERROR Reported at this stage.
  --
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  header2('6) Define full column MAPPING') ;
  colMappingStringFull := '' ;
  if ( pColumnMapping is not null)
  then
    for rec$cols in (select distinct column_name
                     from   dba_tab_columns
                     where  owner = upper(pOwner) 
                     and    table_name in (upper(pTableName),upper(redefTable))
                     and    identity_column != 'YES'
                     )
    loop
      if ( colMappingStringFull is not null) 
      then 
        colMappingStringFull := colMappingStringFull|| ',' ; 
      end if ;
      if colMappings.exists(rec$cols.column_name)
      then
        log(rpad(rec$cols.column_name,30) || ' <----              ' || colMappings(rec$cols.column_name) ,'       - ',true,false) ;
        colMappingStringFull := colMappingStringFull || colMappings(rec$cols.column_name) || ' ' || rec$cols.column_name ;
      else
        log(rpad(rec$cols.column_name,30) || ' <---- ( * Default) ' || rec$cols.column_name  ,'       - ',true,false) ;
        colMappingStringFull := colMappingStringFull || rec$cols.column_name || ' ' || rec$cols.column_name ;
      end if ;
    end loop ;
  end if ;

  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- Step 7  : Starts the redefinition process by building the required shampshot
  --           between the source table and the INTERIM Table.
  --
  --           ==> ERR -20007 in case of error, redefinition is not started and INTERIM
  --               table is dropped unless specified no to drop it in the main call.
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  header2('7) Start redefinition process') ;
  log (rpad('Start ' || pOwner || '.' || pTableName || ' redefinition',75)||' : ','       - ',false,false) ;
  begin
    DBMS_REDEFINITION.start_redef_table(pOwner, pTableName, redefTable,col_mapping=>colMappingStringFull) ;
    log('Success, process continues','',true,false) ;
  exception
    when others then
      log('Error, Aborting redefinition','',true,false) ;
      log('('||sqlerrm||') .../...','       > ',true,false) ;
      log('','',true,false) ;
      error(-20007,sqlerrm,true) ;
  end ;

  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- Step 8  : Apply modifications to the INTERIM TABLE using the array of
  --           statements passed in the pRedefineStatementsPost parameter.
  --
  --           MOdifications must preferably be done before synchronization.
  --           Remember that you cannont alter DATA, all DATA modifications
  --           must be done by the column Mapping
  --
  --           In the statements, use $$TABLE$$ to refer the source TABLE and
  --           $$REDEF$$ to refer to the INTERMIN TABLE.
  --
  --           ==> ERR -20008 in case of error, redefinition is not started and INTERIM
  --               table is dropped unless specified no to drop it in the main call.
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  header2('8) Modify interim table (POST-SYNC)') ;
  for i in 1 .. pRedefineStatementsPost.count
  loop
    begin
      exec_sql (pRedefineStatementsPost(i),true) ;
      exception
      when others then
        log('Error, Aborting redefinition','',true,false) ;
        log('('||sqlerrm||') .../...','       > ',true,false) ;
        log('','',true,false) ;
        error(-20008,sqlerrm,true) ;
      end ;
  end loop ;

  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- Step 9  : Copy dependent objects.
  --
  --           ALL dependent objects are copied, this phase copies the NOT NULL .
  --           Constraints from source to interim. If you need to avoid some
  --           dependancies to by copied, you can use pRedefineStatementsPre or
  --           pRedefineStatementsPost to issue the relevant statements
  --
  --           In the statements, use $$TABLE$$ to refer the source TABLE and
  --           $$REDEF$$ to refer to the INTERMIN TABLE.
  --
  --           This step is NOT executed if the INTERIM TABLE has been created
  --           from the source table METADATA.
  --
  --           ==> ERR -20010 in case of error, redefinition is not started and INTERIM
  --               table is dropped unless specified no to drop it in the main call.
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  header2('9) Copy dependencies') ;
  begin
    if ( useCopyDependants )
    then
      log (rpad('Start ' || pOwner || '.' || pTableName || ' dependencies copy',75)||' : ','       - ',false,false) ;
      DBMS_REDEFINITION.copy_table_dependents(
        uname             => pOwner,
        orig_table        => pTableName,
        int_table         => redefTable,
        copy_indexes      => DBMS_REDEFINITION.cons_orig_params, 
        copy_triggers     => TRUE,  
        copy_constraints  => false, 
        copy_privileges   => TRUE,  
        ignore_errors     => true, 
        num_errors        => l_num_errors);
        error(-20999,'Stop Redef Here',true) ;
      if ( l_num_errors != 0 )
      then
        log('Error, Aborting redefinition','',true,false) ;
        error(-20009,'Copy dependencies had ' || l_num_errors || ' errors, Aborting redefinition',true) ;
      end if ;
      log('Success, process continues','',true,false) ;
    end if ;
  exception
    when others then
      log('Error, Aborting redefinition','',true,false) ;
      log('('||sqlerrm||') .../...','       > ',true,false) ;
      log('','',true,false) ;
      error(-20010,sqlerrm,true) ;
  end ;
  
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- Step 10 : Re-ENABLE check constraints, with the NOVALIDATE option.
  --
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  
  header2('10) ENABLE Check Constraints on interim table (NOVALIDATE)') ;
  for rec$ in (SELECT
                 'begin execute immediate ''alter table ' || owner || '.' || table_name || ' modify constraint ' || constraint_name || ' ENABLE NOVALIDATE'' ; exception when others then if (sqlcode = 0) then null ; else raise ; end if ; end; ' disableCtr
               from
                 dba_constraints where owner = pOwner and table_name = redefTable and constraint_type = 'C' and status = 'DISABLED' )
  loop
    exec_sql (rec$.disableCtr) ;
  end loop;
  
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- Step 11  : Ends the redefinition and exchange tables.
  --
  --           Fully synchronize the two tables and swap then .
  --
  --          After the operation, the redefined table has taken the place of
  --          the source table without application interruption.
  --
  --           ==> ERR -20011 in case of error, redefinition is not started and INTERIM
  --               table is dropped unless specified no to drop it in the main call.
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  header2('11) Terminate redefinition process') ;
  log (rpad('Finish ' || pOwner || '.' || pTableName || ' redefinition',75)||' : ','       - ',false,false) ;
  begin
    DBMS_REDEFINITION.finish_redef_table(pOwner, pTableName, redefTable) ;
    log('Success','',true,false) ;
  exception
    when others then
      log('Error, process aborted, please abort the process if needed (use pAbort=>true)','',true,false) ;
      error(-20011,sqlerrm,true) ;
  end ;

  
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- Step 12  : Drop the INTERIM TABLE (which in fact is the initial source table).
  --
  --           Fully synchronize the two tables and swap then .
  --
  --          After the operation, the redefined table has taken the place of
  --          the source table without application interruption.
  --
  --           ==> ERR -20012 in case of error, redefinition finished, but INTERIM
  --               table remains.
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  header2('12) Drop interim table (corresponding to previous source table)') ;
  begin
    exec_sql ('Drop table ' || pOwner || '.' || redefTable ) ;
  exception
    when others then
      log('Error, redefinition terminated but INTERIM Table remains','',true,false) ;
      error(-20012,sqlerrm,false) ;
  end ;

  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- Step 13  : Rename objects that have been renamed in the metadata creation
  --            step
  --
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  header2('13) Rename dependent objects (for re-entrance)') ;
  for rec$ in (select 'alter index ' || owner || '.' || index_name || ' rename to ' || replace (index_name,redefTable,pTableName) stmt 
               from dba_indexes where table_name=pTableName and index_name like '%' || redefTable || '%')
  loop
    exec_sql(rec$.stmt) ;
  end loop ;
  
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  -- Step 14  : Run post-Processing statements
  --
  -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  header2('Run POST-PROCESS statements') ;
  for i in 1 .. pAfterStatements.count
  loop
    begin
      exec_sql (pAfterStatements(i),false) ;
      exception
      when others then
        log('Error, no redefinition in progress','',true,false) ;
        log('('||sqlerrm||') .../...','       > ',true,false) ;
        log('','',true,false) ;
        error(-20005,sqlerrm,false) ;
      end ;
  end loop ;

  --
  --  That's all
  --
  footer ('Redefinition sucessfully terminated INTERIM TABLE Dropped') ;

end ;

-------------------------------------------------------------------------------------------------------------------------------

begin 

/*
 *     Procedure call parameters ....
 *
 *  redefine_table (pTableName              in varchar2                      -- Name of the source table
 *                 ,pBeforeStatements       in stmtArray default stmtArray() -- Statements to run BEFORE the redefinition process
 *                 ,pCreateStatements       in stmtArray default stmtArray() -- Statements to create the INTERIM Table
 *                 ,pRedefineStatementsPre  in stmtArray default stmtArray() -- Redefinition statements to be issued BEFORE snapshot creation
 *                 ,pRedefineStatementsPost in stmtArray default stmtArray() -- Redefinition statements to be issued AFTER snapshot creation
 *                 ,pAfterStatements        in stmtArray default stmtArray() -- Statements to run AFTER the redefinition process
 *                 ,pAbort                  in boolean   default false       -- If true : Aborts an ongoing redefinition
 *                 ,pDropInterim            in boolean   default true        -- If true : Drops the interim TABLE at the end
 *                 ,pColumnMapping          in varchar2  default null        -- Non default column mappings
 *                 ,pOwner                  in varchar2  default 'TEC'       -- Owner of the source and target tables
 *                 ) 
 */
 
  --
  --    Call redefinition statements below, you can perform redefinitions in sequence but should not
  -- run in parallel in the same database
  --
  --    Use pCreateStatements to change the session parameter if required
  --
  --    Syntax HINTS :
  --      $$TABLE$$ for the source  table (replaced by owner.table)
  --      $$REDEF$$ for the interim table (replaced by owner.table)
  --
  --    If the source table does not have a PRIMARY KEY, you can add a fake one 
  --  for example : alter table owner.table add (tmp_id number generated always as identity) ;
  --                alter table owner.table add primary key (tmp_id) ;
  --  and remove it after (or change the above procedures to use rowids instead of PK)
  --

  /* 
   *   Call Example 
   */  
  redefine_table(
     pTableName                => 'STG_IN_JMS'
    ,pOwner                    => 'TEC'
    ,pBeforeStatements         => stmtArray( -- Pre-processing staements, to modify source table, for 
                                             -- Example to add a primary key if needed
                                            'begin execute immediate ''drop materialized view $$REDEF$$'' ; exception when others then null; end ;'
                                           ,'begin execute immediate ''drop table $$REDEF$$'' ; exception when others then null; end ;'
                                           ,'begin execute immediate ''alter table $$TABLE$$ drop primary key'' ; exception when others then null; end ;'
                                           ,'begin execute immediate ''alter table $$TABLE$$ drop column tmp_id'' ; exception when others then null; end ;'
                                           ,'alter table $$TABLE$$ add (tmp_id number generated always as identity)'
                                           ,'alter table $$TABLE$$ add primary key (tmp_id)'
                                           )
    ,pCreateStatements         => stmtArray( -- Interim table creation if needed
                                             -- Use CREATE_FROM_METADATA to use the real DDL
                                             -- In those statements, you can modify the INTERIM TABLE (change tablespace)
                                             -- and the dependant indexes if needed
                                             -- you can also change session parameters if needed
                                             --'create table $$REDEF$$ as select * from $$TABLE$$ where 1=2'
                                            'CREATE_FROM_METADATA'
                                           ,'alter table $$REDEF$$ move tablespace TBS_BNA0PRD_TEC_ACTIVE_NEW'
                                           ,'alter table $$REDEF$$ move  lob(MESSAGE_JSON) store as (tablespace TBS_BNA0PRD_TEC_ACTIVE_NEW)'
                                           ,'alter index $$REDEF$$_IX1 rebuild tablespace TBS_BNA0PRD_TEC_ACTIVE_NEW'
                                           ,'alter session force parallel dml'
                                           ,'alter session force parallel query'
                                           ,'alter session set INMEMORY_QUERY=disable'
                                           --,'alter session set parallel_force_local=false'
                                           )
    ,pAfterStatements          => stmtArray( -- List of statements to be run after the redefinition, for example to
                                             -- remove a pseudo PK you added
                                            'begin execute immediate ''alter table $$TABLE$$ drop primary key'' ; exception when others then null; end ;'
                                           ,'begin execute immediate ''alter table $$TABLE$$ drop column tmp_id'' ; exception when others then null; end ;'
                                           )
                ) ;
    /* */
end ;
/
