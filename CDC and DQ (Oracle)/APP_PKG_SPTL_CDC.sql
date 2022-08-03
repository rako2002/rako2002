CREATE OR REPLACE PACKAGE PKG_SPTL_CDC 
AUTHID CURRENT_USER AS
------------------------------------------------------------------------------------------------------
--   PACKAGE:      PKG_SPTL_CDC
--
--   DESCRIPTION:  Handling of delta generation
--   CDC Framework provides change data capture (delta recognition) capability. It relies on SQL full 
--   outer joins to calculate delta between staging layer and source data store layer.
--   
--   Data Layer:
--     Staging - contains latest snapshot of data loaded from the data source, intermediate 
--               delta tables are also created within the staging layer, these tables contain
--               most recent data increment (changes between Staging and Source Data Store)
--     Source Data Store - contains full history of soruce data changes, structure of 
--                         Source Data Store is the same as the structure of Staging Layer  
--                         but it contains versioning columns - Start Date, End Date and 
--                         Current Indicator
--                             
--   The framework is meta-data driven. Meta-data tables define structure of the staging layer.
--      MTD_SRC_PROVIDER  - List of source data providers (e.g. internal or external organisations)
--      MTD_SRC_FILE_GROUP – List of file groups (all files in a group should be loaded together)
--      MTD_SRC_FILE – List of files within each group 
--      MTD_SRC_FILE_COLUMNS – defines file columns (IS_NATURAL_KEY_IND (Y/N) defines whether a column is part of primary key in the file
--                                                   PRESERVE_HISTORY_IND(Y/N) defines whether history (SCD Type 2) should be tracked for given column))
--      MTD_ETL_FILE_BATCH – with each file load a new entry should be created in this table with information like: load date, processing status (e.g. completed, failed, processing)
--
--   PL/SQL Packages/Procedures:
--      PKG_CREATE_TBL - Package creates Staging, Delta and Source Data Store tables based on metadata from MTD_SRC_FILE_COLUMNS
--          - P_CREATE_STG_TBL(p_file_id)   - creates staging 
--          - P_CREATE_STG_D_TBL(p_file_id) - creates delta table
--          - P_CREATE_SRC_TBL(p_file_id)   - creates source data store table
--      
--      PKG_SPTL_CDC - Main package responsible for calculating delta and maintaining the Source Data Store                 
--          - P_POPULATE_DLT_TBL(p_file_id)    - Inserts data into delta table based on staging and previous version of data in SDS layer
--          - P_INSERT_ODS_FROM_DLT(p_file_id) - Populates SDS table with the increment from delta table
--          
--      P_TRUNCATE_DELTA - truncates data in the delta table    
--          
--   Setup steps:
--    1. Populate metada in MTD% (MTD_SRC_PROVIDER, MTD_SRC_FILE_GROUP, MTD_SRC_FILE, MTD_SRC_FILE_COLUMNS) tables        
--    2. Create staging layer tables by executing P_CREATE_STG_TBL procedure
--    3. Apply appropriate grants of staging tables if needed.
--    4. Create delta layer tables by executing P_CREATE_STG_D_TBL procedure
--    5. Apply appropriate grants on delta tables if needed. 
--    6. Create source data store layer tables by executing P_CREATE_SRC_TBL procedure
--    7. Apply appropriate grants on source data store tables if needed.         
--
--   Typical execution steps:
--    1. Populate staging table, create spatial index on the table if it has spatial data
--    2. Truncate delta table: P_TRUNCATE_DELTA(src_file_id)
--    3. Populate delta table: PKG_SPTL_CDC.P_POPULATE_DLT_TBL(src_file_id)
--    4. Populate source data store table with increment from delta: PKG_SPTL_CDC.P_INSERT_ODS_FROM_DLT(src_file_id)       
-------------------------------------------------------------------------------------------------------
--   HISTORY:
--                 Version   Date        Author       Notes
--                 --------- ----------- ------------ -------------------------------------------------
--                 1.0       13/02/2014  rako2002 Initial version
-------------------------------------------------------------------------------------------------------

   -------------------------------------------------------------------------------------------------------
   -- FUNCTION DECLARATION
   -------------------------------------------------------------------------------------------------------

   -------------------------------------------------------------------------------------------------------
   -- PROCEDURE DECLARATION
   -------------------------------------------------------------------------------------------------------
   PROCEDURE P_POPULATE_DLT_TBL(
     -- Inserts delta table based on staging and previous version of data in source data store layer
     p_file_id IN SPLETL_APP.MTD_SRC_FILE.SRC_FILE_ID%TYPE
   );
   
   PROCEDURE P_INSERT_ODS_FROM_DLT(
     -- Populates source data store table with the increment from delta table
     p_file_id   IN SPLETL_APP.MTD_SRC_FILE.SRC_FILE_ID%TYPE
   , p_file_date IN DATE DEFAULT NULL
   );

   PROCEDURE P_POPULATE_DLT_TBL_GRP(
     -- Inserts delta tables for a whole group of files based on staging and previous version of data in source data store layer
     p_file_grp_id IN SPLETL_APP.MTD_SRC_FILE_GROUP.SRC_FILE_GROUP_ID%TYPE
   );
   
   PROCEDURE P_INSERT_ODS_FROM_DLT_GRP(
     -- Populates source data store table for a whole group of files with the increment from delta table
     p_file_grp_id IN SPLETL_APP.MTD_SRC_FILE_GROUP.SRC_FILE_GROUP_ID%TYPE
   , p_file_date IN DATE DEFAULT NULL     
   );
   
   FUNCTION F_GET_DLT_SQL(
     -- Return SQL statement that creates delta table based staging and previous
     -- version of data in ODS layer
      p_file_id IN SPLETL_APP.MTD_SRC_FILE.SRC_FILE_ID%TYPE
   ) RETURN VARCHAR2;
   
END PKG_SPTL_CDC;
/


CREATE OR REPLACE PACKAGE BODY PKG_SPTL_CDC AS

   c_start_date  VARCHAR2(20) := 'START_DT';
   c_end_date    VARCHAR2(20) := 'END_DT';
   c_curr_ind    VARCHAR2(20) := 'CURR_IND';
   c_src_owner   VARCHAR2(20) := 'SPLSRC_OWNER';
   c_stg_owner   VARCHAR2(20) := 'SPLSTG_OWNER';

   PROCEDURE P_EXEC_SQL(
     -- Private procedure to execute dynamic SQL
     l_sql1 IN VARCHAR2
   , l_sql2 IN VARCHAR2 DEFAULT NULL
   , l_sql3 IN VARCHAR2 DEFAULT NULL 
   )
   IS
   BEGIN
      DBMS_OUTPUT.PUT_LINE(l_sql1 || NVL(l_sql2, '') || NVL(l_sql3,''));
      EXECUTE IMMEDIATE l_sql1 || NVL(l_sql2, '') || NVL(l_sql3,'');
   END;   

   
   PROCEDURE P_GET_DLT_SQL(
     -- Return SQL statement that creates delta table based staging and previous
     -- version of data in ODS layer
      p_file_id IN SPLETL_APP.MTD_SRC_FILE.SRC_FILE_ID%TYPE
    , r_sql1    OUT VARCHAR2
    , r_sql2    OUT VARCHAR2
    , r_sql3    OUT VARCHAR2
   ) 
   IS
     l_sql1          VARCHAR2(32767);
     l_sql2          VARCHAR2(32767);
     l_sql3          VARCHAR2(32767);
     l_table_nm      MTD_SRC_FILE.SRC_TABLE_NM%TYPE;
     l_src_table_nm  VARCHAR2(100);
     l_dlt_table_nm  VARCHAR2(100);
     l_stg_table_nm  VARCHAR2(100);
     l_first_key     MTD_SRC_FILE_COLUMNS.SRC_COLUMN_NM%TYPE;
     l_counter       SMALLINT;   
     l_nat_key_counter SMALLINT;   
     
     CURSOR cur_cols IS
       SELECT C.IS_NATURAL_KEY_IND
            , C.SRC_COLUMN_NM
            , C.SRC_COLUMN_ID
            , C.SRC_FILE_ID
            , C.DATA_TYPE
            , C.PRESERVE_HISTORY_IND
         FROM SPLETL_APP.MTD_SRC_FILE F
            , SPLETL_APP.MTD_SRC_FILE_COLUMNS C
        WHERE F.SRC_FILE_ID = C.SRC_FILE_ID
          AND F.SRC_FILE_ID = p_file_id
        ORDER BY C.SRC_COLUMN_ID;
     
   BEGIN
      -- STEP 01 - get name of table and name of the first primary key column
      SELECT DISTINCT 
             F.SRC_TABLE_NM
           , FIRST_VALUE(C.SRC_COLUMN_NM) OVER (PARTITION BY C.SRC_FILE_ID ORDER BY SRC_COLUMN_ID) AS FIRST_KEY
        INTO l_table_nm, l_first_key   
        FROM SPLETL_APP.MTD_SRC_FILE F
           , SPLETL_APP.MTD_SRC_FILE_COLUMNS C
       WHERE F.SRC_FILE_ID = C.SRC_FILE_ID
         AND F.SRC_FILE_ID = p_file_id
         AND C.IS_NATURAL_KEY_IND = 'Y';
   
      -- STEP 02 - calculate table names
      l_stg_table_nm := c_stg_owner || '.' || l_table_nm;
      l_dlt_table_nm := c_stg_owner || '.' || 'D_' || SUBSTR(l_table_nm,1, 28);
      l_src_table_nm := c_src_owner || '.' || l_table_nm;
   
      -- STEP 03 - first part of the INSERT SQL
      l_sql1 := 'INSERT INTO ' || l_dlt_table_nm || CHR(10) ||
                -- DLT_OPERATION_NM indicates type of changes - new row (ADD), modified row (MODIFY), delted row (REMOVE)  
                'SELECT NVL2(O.' || l_first_key || ', NVL2(S.'|| l_first_key || ', ''MODIFY'', ''REMOVE'') , ''ADD'') AS DLT_OPERATION_NM' || CHR(10);
      
      
      l_sql2 := '  FROM ' || l_stg_table_nm || ' S' || CHR(10) ||          
                '  FULL OUTER JOIN (SELECT * FROM ' || l_src_table_nm || ' WHERE ' || c_curr_ind ||' = ''Y'') O' || CHR(10) ||
                '    ON ';
      
      l_sql3 := '';
      
      -- STEP 04 - go through the columns in the table
      l_counter := 0;
      l_nat_key_counter := 0;
      
      FOR rec_cols IN cur_cols
      LOOP
         l_counter := l_counter + 1;
         
         -- if natural key in one of the tables (stage or source) is null then it means the whole row is not present in one these tables
         -- if a row is present in both stage and source than stage always takes priority (MODIFY case)
         -- for each column an update indicator column is created, which indicates whether this column was updated
         l_sql1 := l_sql1 || '     , NVL2(S.'|| l_first_key || ', S.' || rec_cols.SRC_COLUMN_NM || ', O.' || rec_cols.SRC_COLUMN_NM || ') AS ' || rec_cols.SRC_COLUMN_NM ||
                    CASE WHEN rec_cols.IS_NATURAL_KEY_IND = 'N' AND rec_cols.PRESERVE_HISTORY_IND = 'Y' AND rec_cols.DATA_TYPE <> 'SDO_GEOMETRY'
                         THEN CHR(10) || '     , CASE WHEN S.' || rec_cols.SRC_COLUMN_NM || ' <> O.' || rec_cols.SRC_COLUMN_NM || ' THEN ''Y'' ELSE ''N'' END AS '|| SUBSTR(rec_cols.SRC_COLUMN_NM, 1, 22) || '_UPD_IND'
                         WHEN rec_cols.IS_NATURAL_KEY_IND = 'N' AND rec_cols.PRESERVE_HISTORY_IND = 'Y' AND rec_cols.DATA_TYPE = 'SDO_GEOMETRY'
                         THEN CHR(10) || '     , CASE WHEN S.' || rec_cols.SRC_COLUMN_NM ||' IS NOT NULL AND O.' || rec_cols.SRC_COLUMN_NM ||' IS NOT NULL AND SDO_EQUAL(S.' || rec_cols.SRC_COLUMN_NM ||', O.' || rec_cols.SRC_COLUMN_NM ||') <> ''TRUE'' THEN ''Y'' ELSE ''N'' END AS '|| SUBSTR(rec_cols.SRC_COLUMN_NM, 1, 22) || '_UPD_IND'
                    END || CHR(10);

         -- join on all natural keys
         IF rec_cols.IS_NATURAL_KEY_IND = 'Y' THEN
            l_nat_key_counter := l_nat_key_counter + 1;
            l_sql2 := l_sql2 || CASE WHEN l_nat_key_counter <> 1 THEN CHR(10) || '   AND ' END ||
                                'O.' || rec_cols.SRC_COLUMN_NM || ' = S.' || rec_cols.SRC_COLUMN_NM; 
         END IF;
         
         -- where any of rows do not exist in one of the tables or the fields are different
         l_sql3 := l_sql3 || 
                    CASE WHEN rec_cols.IS_NATURAL_KEY_IND = 'N' AND rec_cols.DATA_TYPE <> 'SDO_GEOMETRY' AND rec_cols.PRESERVE_HISTORY_IND = 'Y'
                         THEN CHR(10) || '    OR S.' || rec_cols.SRC_COLUMN_NM || ' <> O.' || rec_cols.SRC_COLUMN_NM
                         WHEN rec_cols.IS_NATURAL_KEY_IND = 'N' AND rec_cols.DATA_TYPE = 'SDO_GEOMETRY' AND rec_cols.PRESERVE_HISTORY_IND = 'Y'
                         THEN CHR(10) || '    OR SDO_EQUAL(S.' || rec_cols.SRC_COLUMN_NM ||', O.' || rec_cols.SRC_COLUMN_NM ||') <> ''TRUE'''
                    END;        
      END LOOP;            
      
      l_sql2 := l_sql2 || ' WHERE O.' || l_first_key || ' IS NULL'  || CHR(10) ||
                          '    OR S.' || l_first_key || ' IS NULL';
   
     -- STEP 05 - concatenate parts of SQL
     r_sql1 := l_sql1;
     r_sql2 := l_sql2;
     r_sql3 := l_sql3;     

   END;

   FUNCTION F_GET_DLT_SQL(
     -- Return SQL statement that creates delta table based staging and previous
     -- version of data in ODS layer
     -- this function is used only for testing purposes
      p_file_id IN SPLETL_APP.MTD_SRC_FILE.SRC_FILE_ID%TYPE
   ) RETURN VARCHAR2
   IS   
     l_sql1      VARCHAR2(32767);
     l_sql2      VARCHAR2(32767);
     l_sql3      VARCHAR2(32767);
   
   BEGIN
     P_GET_DLT_SQL(p_file_id, l_sql1, l_sql2, l_sql3); 
      
     RETURN l_sql1 || l_sql2 || l_sql3;  
   END;

   PROCEDURE P_POPULATE_DLT_TBL(
     -- Populates delta table based staging and previous version of data in ODS layer
     p_file_id IN SPLETL_APP.MTD_SRC_FILE.SRC_FILE_ID%TYPE
   )
   IS
     l_sql1   VARCHAR2(32767);
     l_sql2   VARCHAR2(32767);
     l_sql3   VARCHAR2(32767);
   BEGIN
      P_GET_DLT_SQL(p_file_id, l_sql1, l_sql2, l_sql3);

      P_EXEC_SQL(l_sql1, l_sql2, l_sql3);
      
      COMMIT;
   EXCEPTION
      WHEN OTHERS THEN
         RAISE_APPLICATION_ERROR(-20101, 'Error in P_POPULATE_DLT_TBL code: ' || SQLCODE || ' errm:' || SQLERRM || CHR(10)|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);           
   END;
    
   FUNCTION F_GET_UPDATE_FRM_DLT_SQL(
     -- Return SQL statement that updates source table for all the removed and updated rows from delta table
      p_file_id   IN SPLETL_APP.MTD_SRC_FILE.SRC_FILE_ID%TYPE
    , p_curr_date IN DATE 
   ) RETURN VARCHAR2
   IS
     l_sql       VARCHAR2(32767);
   BEGIN
     -- STEP 01 - update end date (:=SYSDATE-1) and curr_ind for updated and deleted records from the delta (where curr_ind = 'Y')
     -- This function produces SQL like this:
     -- UPDATE SPLSRC_OWNER.NSWP_REGIONS
     --    SET END_DT = TRUNC(SYSDATE)-1
     --      , CURR_IND = 'N'
     --  WHERE CURR_IND = 'Y'
     --    AND (REGION_CODE) IN (SELECT REGION_CODE FROM SPLSTG_OWNER.D_NSWP_REGIONS WHERE DLT_OPERATION_NM IN ('MODIFY', 'REMOVE')); 
     SELECT 'UPDATE ' || c_src_owner || '.' || SRC_TABLE_NM || CHR(10) ||
            '   SET ' || c_end_date || ' = TO_DATE(''' || TO_CHAR(p_curr_date,'YYYYMMDD HH24:MI:SS') || ''',''YYYYMMDD HH24:MI:SS'') - 1/24/3600' || CHR(10) ||
            '     , ' || c_curr_ind || ' = ''N''' || CHR(10) ||
            ' WHERE ' || c_curr_ind || ' = ''Y''' || CHR(10) ||
            '   AND (' || PRIM_KEYS || ') IN (SELECT ' || PRIM_KEYS || ' FROM ' || c_stg_owner || '.D_' || SUBSTR(SRC_TABLE_NM,1, 28) ||
            ' WHERE DLT_OPERATION_NM IN (''MODIFY'', ''REMOVE''))'
       INTO l_sql     
       FROM ( 
            SELECT LISTAGG(C.SRC_COLUMN_NM, ',') WITHIN GROUP (ORDER BY SRC_COLUMN_ID) AS PRIM_KEYS
                 , F.SRC_TABLE_NM
              FROM SPLETL_APP.MTD_SRC_FILE F
                 , SPLETL_APP.MTD_SRC_FILE_COLUMNS C
             WHERE F.SRC_FILE_ID = C.SRC_FILE_ID
               AND F.SRC_FILE_ID = p_file_id
               AND C.IS_NATURAL_KEY_IND = 'Y'
             GROUP BY F.SRC_FILE_ID, F.SRC_TABLE_NM
            ) A;

     RETURN l_sql;
   END;   
   
   FUNCTION F_GET_INSERT_FRM_DLT_SQL(
     -- Return SQL statement that inserts into source table  all the added and updated rows from delta table     
      p_file_id   IN SPLETL_APP.MTD_SRC_FILE.SRC_FILE_ID%TYPE
    , p_curr_date IN DATE 
   ) RETURN VARCHAR2
   IS
     l_sql       VARCHAR2(32767);
   BEGIN
     -- STEP 01 - insert new and updated records from delta (start_date:=sysdate)
     -- This function produces SQL similar to this:
     -- INSERT INTO SPLSRC_OWNER.NSWP_REGIONS
     --   SELECT REGION                 
     --        , REGION_CODE            
     --        ...
     --        , STATE                  
     --        , TRUNC(SYSDATE)                 AS START_DT               
     --        , TO_DATE('99991231','YYYYMMDD') AS END_DT                 
     --        , 'Y'                            AS CURR_IND               
     --     FROM SPLSTG_OWNER.D_NSWP_REGIONS WHERE DLT_OPERATION_NM IN ('MODIFY', 'ADD');          
     SELECT 'INSERT INTO ' || c_src_owner || '.' || SRC_TABLE_NM || 
            '(' || SRC_COLS || ', ' || c_start_date || ', ' || c_end_date || ', ' || c_curr_ind || ')' || CHR(10) ||
            'SELECT ' || SRC_COLS || CHR(10) ||
            '     , TO_DATE(''' || TO_CHAR(p_curr_date,'YYYYMMDD HH24:MI:SS') || ''',''YYYYMMDD HH24:MI:SS'') AS ' || c_start_date || CHR(10) ||
            '     , TO_DATE(''' || '99991231' || ''',''YYYYMMDD'') AS ' || c_end_date || CHR(10) ||       
            '     , ''Y'' AS ' || c_curr_ind || CHR(10) ||
            '  FROM ' || c_stg_owner || '.D_' || SUBSTR(SRC_TABLE_NM,1, 28) || CHR(10) ||
            ' WHERE DLT_OPERATION_NM IN (''MODIFY'', ''ADD'')'
       INTO l_sql     
       FROM ( 
             SELECT LISTAGG(C.SRC_COLUMN_NM, ', ') WITHIN GROUP (ORDER BY SRC_COLUMN_ID) AS SRC_COLS
                  , F.SRC_TABLE_NM
               FROM SPLETL_APP.MTD_SRC_FILE F
                  , SPLETL_APP.MTD_SRC_FILE_COLUMNS C
              WHERE F.SRC_FILE_ID = C.SRC_FILE_ID
                AND F.SRC_FILE_ID = p_file_id
              GROUP BY  F.SRC_FILE_ID, F.SRC_TABLE_NM
            ) A;

     RETURN l_sql;
   END;   
  
   
   PROCEDURE P_INSERT_ODS_FROM_DLT(
     -- Populates ODS table with the increment from delta table
     p_file_id IN SPLETL_APP.MTD_SRC_FILE.SRC_FILE_ID%TYPE
   , p_file_date IN DATE DEFAULT NULL          
   )
   IS
     l_sql       VARCHAR2(32767);
     l_file_date DATE;
   BEGIN
     l_file_date := NVL(p_file_date, SYSDATE);
   
     l_sql := F_GET_UPDATE_FRM_DLT_SQL(p_file_id, l_file_date);
     P_EXEC_SQL(l_sql);
     
     l_sql := F_GET_INSERT_FRM_DLT_SQL(p_file_id, l_file_date);
     P_EXEC_SQL(l_sql);
     
     COMMIT;
   EXCEPTION
      WHEN OTHERS THEN
         RAISE_APPLICATION_ERROR(-20101, 'Error in P_INSERT_ODS_FROM_DLT code: ' || SQLCODE || ' errm:' || SQLERRM || CHR(10)|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);                 
   END;

   
   PROCEDURE P_POPULATE_DLT_TBL_GRP(
     -- Inserts delta tables for a whole group of files based on staging and previous version of data in source data store layer
     p_file_grp_id IN SPLETL_APP.MTD_SRC_FILE_GROUP.SRC_FILE_GROUP_ID%TYPE
   )
   IS
      -- get only the files that have structure defined in MTD_SRC_FILE_COLUMNS
      CURSOR cur_files IS
      SELECT DISTINCT F.SRC_FILE_ID AS SRC_FILE_ID
        FROM MTD_SRC_FILE F
           , MTD_SRC_FILE_COLUMNS C
       WHERE F.SRC_FILE_GROUP_ID = p_file_grp_id
         AND C.SRC_FILE_ID = F.SRC_FILE_ID
       ORDER BY F.SRC_FILE_ID ;
   
   BEGIN      
      FOR rec_files IN cur_files
      LOOP
         P_POPULATE_DLT_TBL(rec_files.src_file_id);
      END LOOP;            
   EXCEPTION
      WHEN OTHERS THEN
         RAISE_APPLICATION_ERROR(-20101, 'Error in P_POPULATE_DLT_TBL_GRP code: ' || SQLCODE || ' errm:' || SQLERRM || CHR(10)|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);                 
   END;
   
   PROCEDURE P_INSERT_ODS_FROM_DLT_GRP(
     -- Populates source data store table for a whole group of files with the increment from delta table
     p_file_grp_id IN SPLETL_APP.MTD_SRC_FILE_GROUP.SRC_FILE_GROUP_ID%TYPE
   , p_file_date IN DATE DEFAULT NULL     
   )
   IS
      l_file_date DATE;
      -- get only the files that have structure defined in MTD_SRC_FILE_COLUMNS
      CURSOR cur_files IS
      SELECT DISTINCT F.SRC_FILE_ID AS SRC_FILE_ID
        FROM MTD_SRC_FILE F
           , MTD_SRC_FILE_COLUMNS C
       WHERE F.SRC_FILE_GROUP_ID = p_file_grp_id
         AND C.SRC_FILE_ID = F.SRC_FILE_ID
       ORDER BY F.SRC_FILE_ID ;
   
   BEGIN     
      l_file_date := NVL(p_file_date, SYSDATE);
      
      FOR rec_files IN cur_files
      LOOP
         P_INSERT_ODS_FROM_DLT(rec_files.src_file_id, l_file_date);
      END LOOP;            
   EXCEPTION
      WHEN OTHERS THEN
         RAISE_APPLICATION_ERROR(-20101, 'Error in P_INSERT_ODS_FROM_DLT_GRP code: ' || SQLCODE || ' errm:' || SQLERRM || CHR(10)|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);                 
   END;
   
END PKG_SPTL_CDC;
/

GRANT EXECUTE ON PKG_SPTL_CDC to SPLSTG_OWNER;
GRANT EXECUTE ON PKG_SPTL_CDC to SPLSRC_OWNER;