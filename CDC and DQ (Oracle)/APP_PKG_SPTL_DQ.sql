CREATE OR REPLACE PACKAGE PKG_SPTL_DQ
AUTHID CURRENT_USER AS
------------------------------------------------------------------------------------------------------
--   PACKAGE:      PKG_SPTL_DQ
--
--   DESCRIPTION:  Executes data validations defined in MTD_DQ_SCREEN and stores results of validations 
--                 in MTD_DQ_SCREEN_EVENT.
--                             
--   The framework is meta-data driven. Meta-data tables define validations and severity level of each validation.
--   Each validation is a SQL query that is supposed to return ROWID of each row that fails the validation.
--   Each such ROWID is records in the logging table - MTD_DQ_SCREEN_EVENT.
--       MTD_DQ_SCREEN - 
--               SCREEN_ID         - unique screen ID
--               SCREEN_NM         - screen name
--               SCREEEN_DS        - screen description
--               SRC_FILE_ID       - file identifier (FK to MTD_SRC_FILE)
--               SCREEN_ORDER      - execution order within each file
--               EXCEPTION_ACTION  - 'PASS'   - log the exception in MTD_DQ_SCREEN_EVENT
--                                   'REJECT' - log the exception in MTD_DQ_SCREEN_EVENT and exclude it 
--                                              from further processing
--                                   'STOP'   - log the exception in MTD_DQ_SCREEN_EVENT and stop the processing
--               SCREEN_SQL        - screen SQL, it needs to return single column - ROWID for all the rows 
--                                   that fail validation
--       MTD_DQ_SCREEN_EVENT - 
--               SCREEN_ID         - unique screen ID
--               BATCH_ID          - batch ID (FK to MTD_ETL_FILE_BATCH)
--               SRC_FILE_ID       - file identifier (FK to MTD_SRC_FILE)
--               LOG_DT            - log entry date 
--               EXCEPTION_ACTION  - exception action as defined in MTD_DQ_SCREEN
--               ROW_ID            - ROWID of the rejected row
--               TABLE_NM          - source table name from SPLSRC_OWNER schema 
--
--   PL/SQL Packages/Procedures:
--      P_VALIDATE_GRP(grp_id)
--         for each file in a group execute P_VALIDATE_FILE(file_id)
--      P_VALIDATE_FILE(file_id)
--         for each screen for a file (ordered by screen order) execute P_EXEC_SCREEN(screen_id)  
--      P_EXEC_SCREEN(screen_id)    
--         get validation SQL
--         execute validation SQL
--         if EXCEPTION_ACTION = 'STOP' and number of inserted rows > 0 return error to stop the processing
-------------------------------------------------------------------------------------------------------
--   HISTORY:
--                 Version   Date        Author       Notes
--                 --------- ----------- ------------ -------------------------------------------------
--                 1.0       22/04/2014  rako2002 Initial version
-------------------------------------------------------------------------------------------------------

   -------------------------------------------------------------------------------------------------------
   -- FUNCTION DECLARATION
   -------------------------------------------------------------------------------------------------------

   -------------------------------------------------------------------------------------------------------
   -- PROCEDURE DECLARATION
   -------------------------------------------------------------------------------------------------------
   PROCEDURE P_EXEC_SCREEN(
     -- Executes single DQ validation
     p_screen_id IN SPLETL_APP.MTD_DQ_SCREEN.SCREEN_ID%TYPE
   );
   
   PROCEDURE P_VALIDATE_FILE(
     -- Executes DQ validations for a single file
     p_file_id IN SPLETL_APP.MTD_SRC_FILE.SRC_FILE_ID%TYPE
   );

   PROCEDURE P_VALIDATE_GRP(
     -- Executes DQ validations for the whole file group
     p_file_grp_id IN SPLETL_APP.MTD_SRC_FILE_GROUP.SRC_FILE_GROUP_ID%TYPE
   );
      
END PKG_SPTL_DQ;
/


CREATE OR REPLACE PACKAGE BODY PKG_SPTL_DQ AS

   FUNCTION F_EXEC_SQL(
     -- Private procedure to execute dynamic SQL
     l_sql IN VARCHAR2
   ) RETURN NUMBER
   IS
   BEGIN
      DBMS_OUTPUT.PUT_LINE(l_sql);
      EXECUTE IMMEDIATE l_sql;      
      RETURN SQL%ROWCOUNT;
   END;   
   
   FUNCTION F_GET_VLDN_SQL(
      p_screen_id IN SPLETL_APP.MTD_DQ_SCREEN.SCREEN_ID%TYPE
   ) RETURN VARCHAR2
   IS
     l_sqlv      VARCHAR2(32767);
     l_sql       VARCHAR2(32767);     
   BEGIN
     -- STEP 01 - get valdiation SQL
     SELECT SCREEN_SQL
       INTO l_sqlv 
       FROM MTD_DQ_SCREEN
      WHERE SCREEN_ID = p_screen_id;

     -- STEP 02 - prepare remaining parts of validation SQL
     l_sql := 'INSERT INTO MTD_DQ_SCREEN_EVENT(SCREEN_ID, BATCH_ID, SRC_FILE_ID, LOG_DT, EXCEPTION_ACTION, ROW_ID, TABLE_NM) '   || CHR(10) ||
              'SELECT A.SCREEN_ID, A.BATCH_ID, A.SRC_FILE_ID, A.LOG_DT, A.EXCEPTION_ACTION, B.ROWID AS ROW_ID, A.TABLE_NM '      || CHR(10) ||
              '  FROM (SELECT S.SCREEN_ID '                                                                                      || CHR(10) ||
              '             , (SELECT MAX(BATCH_ID) FROM MTD_ETL_FILE_BATCH B WHERE B.SRC_FILE_ID = S.SRC_FILE_ID) AS BATCH_ID ' || CHR(10) ||
              '             , S.SRC_FILE_ID '                                                                                    || CHR(10) ||
              '             , SYSDATE AS LOG_DT '                                                                                || CHR(10) ||
              '             , S.EXCEPTION_ACTION '                                                                               || CHR(10) ||
              '             , F.SRC_TABLE_NM AS TABLE_NM '                                                                       || CHR(10) ||
              '          FROM MTD_DQ_SCREEN S  '                                                                                 || CHR(10) ||
              '             , MTD_SRC_FILE F '                                                                                   || CHR(10) ||
              '         WHERE S.SCREEN_ID = ' || p_screen_id                                                                     || CHR(10) ||
              '           AND F.SRC_FILE_ID = S.SRC_FILE_ID '                                                                    || CHR(10) ||
              '       ) A '                                                                                                      || CHR(10) ||
              '     , ( ' || l_sqlv || ') B';
     
     RETURN l_sql;
   END;

   PROCEDURE P_EXEC_SCREEN(
     -- Executes single DQ validation
     p_screen_id IN SPLETL_APP.MTD_DQ_SCREEN.SCREEN_ID%TYPE
   )
   IS
     l_sql          VARCHAR2(32767);
     l_rowcnt       NUMBER;
     l_excpt_action SPLETL_APP.MTD_DQ_SCREEN.EXCEPTION_ACTION%TYPE;
     l_screen_nm    SPLETL_APP.MTD_DQ_SCREEN.SCREEN_NM%TYPE;
   BEGIN
      -- STEP 01 get validation SQL
      l_sql :=  F_GET_VLDN_SQL(p_screen_id);
      
      -- STEP 02 execute validation SQL
      l_rowcnt := F_EXEC_SQL(l_sql);
      COMMIT;
      
      -- STEP 03 if EXCEPTION_ACTION = 'STOP' and number of inserted rows > 0 return error to stop the processing
      SELECT EXCEPTION_ACTION, SCREEN_NM
        INTO l_excpt_action, l_screen_nm 
        FROM MTD_DQ_SCREEN
       WHERE SCREEN_ID = p_screen_id;

      IF l_excpt_action = 'STOP' AND l_rowcnt > 0 THEN
         RAISE_APPLICATION_ERROR(-90001, 'Received STOP signal in validation SCREEN_ID=' || p_screen_id || ' SCREEN_NM=' || l_screen_nm);           
      END IF;      
       
   EXCEPTION
      WHEN OTHERS THEN
         RAISE_APPLICATION_ERROR(-20101, 'Error in P_EXEC_SCREEN code: ' || SQLCODE || ' errm:' || SQLERRM || CHR(10)|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);           
   END;
    
   PROCEDURE P_VALIDATE_FILE(
     -- Executes DQ validations for a single file
     p_file_id IN SPLETL_APP.MTD_SRC_FILE.SRC_FILE_ID%TYPE
   )
   IS
      -- get only the files that have structure defined in MTD_SRC_FILE_COLUMNS
      CURSOR cur_screens IS
         SELECT SCREEN_ID
           FROM MTD_DQ_SCREEN 
          WHERE SRC_FILE_ID = p_file_id
          ORDER BY SCREEN_ORDER;
   
   BEGIN      
      FOR rec_screens IN cur_screens
      LOOP
         P_EXEC_SCREEN(rec_screens.screen_id);
      END LOOP;            
   EXCEPTION
      WHEN OTHERS THEN
         RAISE_APPLICATION_ERROR(-20101, 'Error in P_VALIDATE_FILE code: ' || SQLCODE || ' errm:' || SQLERRM || CHR(10)|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);                 
   END;   

   PROCEDURE P_VALIDATE_GRP(
     -- Executes DQ validations for the whole file group
     p_file_grp_id IN SPLETL_APP.MTD_SRC_FILE_GROUP.SRC_FILE_GROUP_ID%TYPE
   )
   IS
      -- get only the files that have structure defined in MTD_SRC_FILE_COLUMNS
      CURSOR cur_files IS
      SELECT DISTINCT S.SCREEN_ID, S.SCREEN_ORDER, F.SRC_FILE_ID
        FROM MTD_SRC_FILE F
           , MTD_SRC_FILE_COLUMNS C
           , MTD_DQ_SCREEN S
       WHERE F.SRC_FILE_GROUP_ID = p_file_grp_id
         AND C.SRC_FILE_ID = F.SRC_FILE_ID
         AND C.SRC_FILE_ID = S.SRC_FILE_ID         
       ORDER BY F.SRC_FILE_ID, S.SCREEN_ORDER ;
   
   BEGIN      
      FOR rec_files IN cur_files
      LOOP
         P_EXEC_SCREEN(rec_files.screen_id);
      END LOOP;            
   EXCEPTION
      WHEN OTHERS THEN
         RAISE_APPLICATION_ERROR(-20101, 'Error in P_VALIDATE_GRP code: ' || SQLCODE || ' errm:' || SQLERRM || CHR(10)|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);                 
   END;
     
END PKG_SPTL_DQ;
/

GRANT EXECUTE ON PKG_SPTL_DQ to SPLSTG_OWNER;
GRANT EXECUTE ON PKG_SPTL_DQ to SPLSRC_OWNER;  
  