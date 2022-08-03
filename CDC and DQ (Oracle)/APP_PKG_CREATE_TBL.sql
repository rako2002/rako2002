CREATE OR REPLACE PACKAGE PKG_CREATE_TBL
AUTHID CURRENT_USER AS
------------------------------------------------------------------------------------------------------
--   PACKAGE:      PKG_CREATE_TBL
--
--   DESCRIPTION:  Package creates Staging and SRC tables based on metadata from MTD_SRC_FILE_COLUMNS
--
--   HISTORY:
--                 Version   Date        Author       Notes
--                 --------- ----------- ------------ -------------------------------------------------
--                 1.0       17/02/2014  rako2002 Initial version
-------------------------------------------------------------------------------------------------------

   -------------------------------------------------------------------------------------------------------
   -- FUNCTION DECLARATION
   -------------------------------------------------------------------------------------------------------

   -------------------------------------------------------------------------------------------------------
   -- PROCEDURE DECLARATION
   -------------------------------------------------------------------------------------------------------
   PROCEDURE P_CREATE_STG_TBL(
     -- Creates staging table based on metadata from MTD_SRC_FILE_COLUMNS
     p_file_id IN MTD_SRC_FILE.SRC_FILE_ID%TYPE
   );

   PROCEDURE P_CREATE_STG_D_TBL(
     -- Creates delta table based on metadata from MTD_SRC_FILE_COLUMNS
     p_file_id IN MTD_SRC_FILE.SRC_FILE_ID%TYPE
   );  
   
   PROCEDURE P_CREATE_SRC_TBL(
     -- Creates SRC table based on metadata from MTD_SRC_FILE_COLUMNS
     p_file_id IN MTD_SRC_FILE.SRC_FILE_ID%TYPE
   );

   FUNCTION F_GET_SQL(
     -- returns table creation SQL based on columns with data types from  MTD_SRC_FILE_COLUMNS
      p_file_id IN MTD_SRC_FILE.SRC_FILE_ID%TYPE
    , p_is_src  IN CHAR := 'N'
	, p_is_dlt  IN CHAR := 'N'
   ) RETURN VARCHAR2;   
   
END PKG_CREATE_TBL;
/


CREATE OR REPLACE PACKAGE BODY PKG_CREATE_TBL AS

   c_start_date  VARCHAR2(20) := 'START_DT';
   c_end_date    VARCHAR2(20) := 'END_DT';
   c_curr_ind    VARCHAR2(20) := 'CURR_IND';
   c_src_owner   VARCHAR2(20) := 'SPLSRC_OWNER';
   c_stg_owner   VARCHAR2(20) := 'SPLSTG_OWNER';
 
   
   PROCEDURE P_EXEC_SQL(
     -- Private procedure to execute dynamic SQL
     l_sql IN VARCHAR2
   )
   IS
   BEGIN
      DBMS_OUTPUT.PUT_LINE(l_sql);
      EXECUTE IMMEDIATE l_sql;
   END;   

   FUNCTION F_GET_SQL(
     -- returns table creation SQL based on columns with data types from  MTD_SRC_FILE_COLUMNS
      p_file_id IN MTD_SRC_FILE.SRC_FILE_ID%TYPE
    , p_is_src  IN CHAR := 'N'
	, p_is_dlt  IN CHAR := 'N'
   ) RETURN VARCHAR2
   IS
     l_sql       VARCHAR2(32767);
   BEGIN
     -- STEP 01 - Generate table creation SQL for STG and SRC layers
     SELECT b.sql
       INTO l_sql
       FROM (
     SELECT 'CREATE TABLE ' || DECODE(p_is_src, 'Y', c_src_owner, c_stg_owner) || '.' 
	                        || DECODE(p_is_dlt, 'Y', 'D_' || SUBSTR(SRC_TABLE_NM, 1, 28), SRC_TABLE_NM)  || '( ' || CHR(10) ||
	        CASE WHEN p_is_dlt = 'Y' THEN 
			   'DLT_OPERATION_NM VARCHAR2(6) NOT NULL,' || CHR(10)
			END ||
            LISTAGG(SRC_COLUMN_NM || ' ' || DATA_TYPE ||
                       CASE WHEN DATA_TYPE NOT IN ('SDO_GEOMETRY', 'DATE') THEN
                           NVL2(LENGTH_NUM, '(' || LENGTH_NUM || NVL2(SCALE_NUM, ', ' || SCALE_NUM, '') || ')', '')
                       END ||
					   CASE WHEN p_is_dlt = 'Y' AND IS_NATURAL_KEY_IND = 'N' AND PRESERVE_HISTORY_IND = 'Y' THEN 
					       ',' || CHR(10) || SUBSTR(SRC_COLUMN_NM, 1, 22) || '_UPD_IND CHAR(1)' 
					   END
                   , ',' || CHR(10)
                   ) WITHIN GROUP (ORDER BY SRC_COLUMN_ID) ||
            CASE WHEN p_is_src = 'Y' THEN
              ',' || CHR(10) ||
              c_start_date || ' DATE NOT NULL,' || CHR(10) ||
              c_end_date   || ' DATE NOT NULL,' || CHR(10) ||
              c_curr_ind   || ' CHAR(1) NOT NULL'
            END ||
            ')' AS sql
       FROM (SELECT C.IS_NATURAL_KEY_IND
                  , C.SRC_COLUMN_NM
                  , C.SRC_COLUMN_ID
                  , C.SRC_FILE_ID
                  , C.DATA_TYPE
                  , F.SRC_TABLE_NM
                  , C.PRESERVE_HISTORY_IND
                  , C.SCALE_NUM
                  , C.LENGTH_NUM
               FROM SPLETL_APP.MTD_SRC_FILE F
                  , SPLETL_APP.MTD_SRC_FILE_COLUMNS C
              WHERE F.SRC_FILE_ID = C.SRC_FILE_ID
                AND F.SRC_FILE_ID = p_file_id
            ) A
      GROUP BY SRC_FILE_ID, SRC_TABLE_NM
      ) B;

     RETURN l_sql;
   END;
      
   PROCEDURE P_CREATE_STG_TBL(
     -- Creates staging table based on metadata from MTD_SRC_FILE_COLUMN
     p_file_id IN MTD_SRC_FILE.SRC_FILE_ID%TYPE
   )
   IS
     l_sql   VARCHAR2(32767);
   BEGIN
      l_sql :=  F_GET_SQL(p_file_id, 'N');
      P_EXEC_SQL(l_sql);
   EXCEPTION
      WHEN OTHERS THEN
         RAISE_APPLICATION_ERROR(-20101, 'Error in P_CREATE_STG_TBL code: ' || SQLCODE || ' errm:' || SQLERRM || CHR(10)|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
   END;

   PROCEDURE P_CREATE_STG_D_TBL(
     -- Creates delta table based on metadata from MTD_SRC_FILE_COLUMN
     p_file_id IN MTD_SRC_FILE.SRC_FILE_ID%TYPE
   )
   IS
     l_sql   VARCHAR2(32767);
   BEGIN
      l_sql :=  F_GET_SQL(p_file_id, 'N', 'Y');
      P_EXEC_SQL(l_sql);
   EXCEPTION
      WHEN OTHERS THEN
         RAISE_APPLICATION_ERROR(-20101, 'Error in P_CREATE_STG_TBL code: ' || SQLCODE || ' errm:' || SQLERRM || CHR(10)|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
   END;

	  
   PROCEDURE P_CREATE_SRC_TBL(
     -- Creates SRC table based on metadata from MTD_SRC_FILE_COLUMNS
     p_file_id IN MTD_SRC_FILE.SRC_FILE_ID%TYPE
   )
   IS
     l_sql   VARCHAR2(32767);
   BEGIN
      -- STEP 01 - create SRC table
      l_sql :=  F_GET_SQL(p_file_id, 'Y');
      P_EXEC_SQL(l_sql);

      -- STEP 02 - create index on the table
      SELECT 'CREATE UNIQUE INDEX '|| c_src_owner || '.' || SUBSTR(SRC_TABLE_NM,1 , 23) || '_PK_IDX ON ' || c_src_owner || '.' || SRC_TABLE_NM || ' (' ||
             LISTAGG(SRC_COLUMN_NM, ', ') WITHIN GROUP (ORDER BY SRC_COLUMN_ID) ||
             ', ' || c_start_date || ') NOLOGGING'
        INTO l_sql
        FROM SPLETL_APP.MTD_SRC_FILE F
           , SPLETL_APP.MTD_SRC_FILE_COLUMNS C
       WHERE F.SRC_FILE_ID = C.SRC_FILE_ID
         AND F.SRC_FILE_ID = p_file_id
         AND C.IS_NATURAL_KEY_IND = 'Y'
       GROUP BY C.SRC_FILE_ID, F.SRC_TABLE_NM;

      P_EXEC_SQL(l_sql);

      -- STEP 03 - create PK on the table
      SELECT 'ALTER TABLE ' || c_src_owner || '.' || SRC_TABLE_NM || ' ADD CONSTRAINT ' || SUBSTR(SRC_TABLE_NM,1 , 27) || '_PK PRIMARY KEY (' ||
             LISTAGG(SRC_COLUMN_NM, ', ') WITHIN GROUP (ORDER BY SRC_COLUMN_ID) ||
             ', ' || c_start_date || ') USING INDEX ' || SUBSTR(SRC_TABLE_NM,1 , 23) || '_PK_IDX'
        INTO l_sql             
        FROM SPLETL_APP.MTD_SRC_FILE F
           , SPLETL_APP.MTD_SRC_FILE_COLUMNS C
       WHERE F.SRC_FILE_ID = C.SRC_FILE_ID
         AND F.SRC_FILE_ID = p_file_id
         AND C.IS_NATURAL_KEY_IND = 'Y'
      GROUP BY C.SRC_FILE_ID, F.SRC_TABLE_NM;
      
      P_EXEC_SQL(l_sql);

   EXCEPTION
      WHEN OTHERS THEN
         RAISE_APPLICATION_ERROR(-20101, 'Error in P_CREATE_SRC_TBL code: ' || SQLCODE || ' errm:' || SQLERRM || CHR(10)|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
   END;

END PKG_CREATE_TBL;
/

GRANT EXECUTE ON PKG_CREATE_TBL to SPLSTG_OWNER;
GRANT EXECUTE ON PKG_CREATE_TBL to SPLSRC_OWNER;

GRANT SELECT ON MTD_SRC_FILE TO SPLSRC_OWNER;
GRANT SELECT ON MTD_SRC_FILE_COLUMNS TO SPLSRC_OWNER;

GRANT SELECT ON MTD_SRC_FILE TO SPLSTG_OWNER;
GRANT SELECT ON MTD_SRC_FILE_COLUMNS TO SPLSTG_OWNER;