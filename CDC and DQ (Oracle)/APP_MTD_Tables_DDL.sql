DROP TABLE MTD_DQ_SCREEN_LOG;
DROP TABLE MTD_DQ_SCREEN;
DROP TABLE MTD_ETL_FILE_BATCH;
DROP TABLE MTD_SRC_FILE_COLUMNS;
DROP TABLE MTD_SRC_FILE;
DROP TABLE MTD_SRC_FILE_GROUP;
DROP TABLE MTD_SRC_PROVIDER;

CREATE TABLE MTD_SRC_PROVIDER
(  SRC_PROVIDER_ID  NUMBER(12,0)
 , SRC_PROVIDER_NM  VARCHAR2(200)
 , SRC_PROVIDER_DS  VARCHAR2(4000)
)
;

CREATE UNIQUE INDEX XPK_MTD_SRC_PROVIDER ON MTD_SRC_PROVIDER(SRC_PROVIDER_ID ASC)
  NOLOGGING
;

ALTER TABLE MTD_SRC_PROVIDER ADD CONSTRAINT MTD_SRC_PROVIDER_PK PRIMARY KEY (SRC_PROVIDER_ID) USING INDEX XPK_MTD_SRC_PROVIDER
;


CREATE TABLE MTD_SRC_FILE_GROUP
(  SRC_FILE_GROUP_ID            NUMBER(12,0)
 , SRC_FILE_GROUP_NM            VARCHAR2(200)
 , SRC_FILE_GROUP_DS            VARCHAR2(4000)
)
;

CREATE UNIQUE INDEX XPK_MTD_SRC_FILE_GROUP ON MTD_SRC_FILE_GROUP(SRC_FILE_GROUP_ID ASC)
  NOLOGGING
;

ALTER TABLE MTD_SRC_FILE_GROUP ADD CONSTRAINT MTD_SRC_FILE_GROUP_PK PRIMARY KEY (SRC_FILE_GROUP_ID) USING INDEX XPK_MTD_SRC_FILE_GROUP
;



CREATE TABLE MTD_SRC_FILE
(  SRC_FILE_ID            NUMBER(12,0)
 , SRC_FILE_NM            VARCHAR2(200)
 , SRC_FILE_DS            VARCHAR2(4000)
 , SRC_TABLE_NM           VARCHAR2(30)
 , SNAPSHOT_TYPE          VARCHAR2(100)
 , FILE_FORMAT_TYPE       VARCHAR2(100)
 , SRC_PROVIDER_ID        NUMBER(12,0)
 , SRC_FILE_GROUP_ID      NUMBER(12,0)
)
;

CREATE UNIQUE INDEX XPK_MTD_SRC_FILE ON MTD_SRC_FILE(SRC_FILE_ID ASC) NOLOGGING
;
ALTER TABLE MTD_SRC_FILE ADD CONSTRAINT MTD_SRC_FILE_PK PRIMARY KEY (SRC_FILE_ID) USING INDEX XPK_MTD_SRC_FILE
;
ALTER TABLE MTD_SRC_FILE ADD CONSTRAINT MTD_SRC_FILE_PROVIDER_FK FOREIGN KEY (SRC_PROVIDER_ID) REFERENCES MTD_SRC_PROVIDER (SRC_PROVIDER_ID)
;
ALTER TABLE MTD_SRC_FILE ADD CONSTRAINT MTD_SRC_FILE_GROUP_FK FOREIGN KEY (SRC_FILE_GROUP_ID) REFERENCES MTD_SRC_FILE_GROUP (SRC_FILE_GROUP_ID)
;
ALTER TABLE MTD_SRC_FILE ADD CONSTRAINT MTD_SRC_FILE_SNAPSHOT_CHK CHECK (SNAPSHOT_TYPE IN ('FULL', 'DELTA'))
;


CREATE TABLE MTD_SRC_FILE_COLUMNS
(  SRC_FILE_ID            NUMBER(12,0)
 , SRC_COLUMN_ID          NUMBER(12,0)
 , SRC_COLUMN_NM          VARCHAR2(30)
 , SRC_COLUMN_DS          VARCHAR2(4000)
 , DATA_TYPE              VARCHAR2(106)
 , LENGTH_NUM             NUMBER(12,0)
 , SCALE_NUM              NUMBER(12,0)
 , IS_NATURAL_KEY_IND     VARCHAR2(1)
 , PRESERVE_HISTORY_IND   VARCHAR2(1)
)
;

CREATE UNIQUE INDEX XPK_MTD_SRC_FILE_COLUMNS ON MTD_SRC_FILE_COLUMNS(SRC_FILE_ID ASC, SRC_COLUMN_ID ASC) NOLOGGING
;
ALTER TABLE MTD_SRC_FILE_COLUMNS ADD CONSTRAINT MTD_SRC_FILE_COLUMNS_PK PRIMARY KEY (SRC_FILE_ID, SRC_COLUMN_ID) USING INDEX XPK_MTD_SRC_FILE_COLUMNS
;
ALTER TABLE MTD_SRC_FILE_COLUMNS ADD CONSTRAINT MTD_SRC_FILE_COLUMNS_CHK1 CHECK (IS_NATURAL_KEY_IND IN ('Y', 'N'))
;
ALTER TABLE MTD_SRC_FILE_COLUMNS ADD CONSTRAINT MTD_SRC_FILE_COLUMNS_CHK2 CHECK (PRESERVE_HISTORY_IND IN ('Y', 'N'))
;
ALTER TABLE MTD_SRC_FILE_COLUMNS ADD CONSTRAINT MTD_SRC_FILE_COLUMNS_FILE_FK FOREIGN KEY (SRC_FILE_ID) REFERENCES MTD_SRC_FILE (SRC_FILE_ID)
;

CREATE TABLE MTD_ETL_FILE_BATCH
(  BATCH_ID        NUMBER(12,0)
 , SRC_FILE_ID     NUMBER(12,0)
 , LOAD_DATE       DATE
 , STATUS_NM       VARCHAR2(100)
);

CREATE UNIQUE INDEX XPK_MTD_ETL_FILE_BATCH ON MTD_ETL_FILE_BATCH(BATCH_ID ASC, SRC_FILE_ID ASC) NOLOGGING
;
ALTER TABLE MTD_ETL_FILE_BATCH ADD CONSTRAINT MTD_ETL_FILE_BATCH_PK PRIMARY KEY (BATCH_ID, SRC_FILE_ID) USING INDEX XPK_MTD_ETL_FILE_BATCH
;
ALTER TABLE MTD_ETL_FILE_BATCH ADD CONSTRAINT MTD_ETL_FILE_BATCH_FILE_FK FOREIGN KEY (SRC_FILE_ID) REFERENCES MTD_SRC_FILE (SRC_FILE_ID)
;

CREATE SEQUENCE SQ_MTD_BATCH_ID MINVALUE 1 INCREMENT BY 1 NOCYCLE NOCACHE;


CREATE TABLE MTD_DQ_SCREEN
(  SCREEN_ID        NUMBER(12,0)
 , SCREEN_NM        VARCHAR2(100)   NOT NULL
 , SCREEEN_DS       VARCHAR2(4000)
 , SRC_FILE_ID      NUMBER(12,0)    NOT NULL 
 , SCREEN_ORDER     NUMBER(12,0)    NOT NULL
 , EXCEPTION_ACTION VARCHAR2(10)    
 , SCREEN_SQL       CLOB
);

CREATE UNIQUE INDEX XPK_MTD_DQ_SCREEN ON MTD_DQ_SCREEN(SCREEN_ID) NOLOGGING
;
ALTER TABLE MTD_DQ_SCREEN ADD CONSTRAINT MTD_DQ_SCREEN_PK PRIMARY KEY (SCREEN_ID) USING INDEX XPK_MTD_DQ_SCREEN
;
CREATE UNIQUE INDEX XUK_MTD_DQ_SCREEN_ORDER ON MTD_DQ_SCREEN(SRC_FILE_ID, SCREEN_ORDER) NOLOGGING
;
ALTER TABLE MTD_DQ_SCREEN ADD CONSTRAINT MTD_DQ_SCREEN_CHK1 CHECK (EXCEPTION_ACTION IN ('PASS', 'REJECT', 'STOP'))
;
ALTER TABLE MTD_DQ_SCREEN ADD CONSTRAINT MTD_DQ_SCREEN_FILE_FK FOREIGN KEY (SRC_FILE_ID) REFERENCES MTD_SRC_FILE (SRC_FILE_ID)
;

CREATE TABLE MTD_DQ_SCREEN_EVENT
(  
   SCREEN_ID        NUMBER(12,0)
 , BATCH_ID         NUMBER(12,0)
 , SRC_FILE_ID      NUMBER(12,0)
 , LOG_DT           DATE
 , EXCEPTION_ACTION VARCHAR2(10) 
 , ROW_ID           ROWID     
 , TABLE_NM         VARCHAR2(30) 
);


ALTER TABLE MTD_DQ_SCREEN_EVENT ADD CONSTRAINT MTD_DQ_SCREEN_EVENT_BATCH_FK FOREIGN KEY (BATCH_ID, SRC_FILE_ID) REFERENCES MTD_ETL_FILE_BATCH (BATCH_ID, SRC_FILE_ID)
;
ALTER TABLE MTD_DQ_SCREEN_EVENT ADD CONSTRAINT MTD_DQ_SCREEN_EVENT_SCREEN_FK FOREIGN KEY (SRC_FILE_ID) REFERENCES MTD_SRC_FILE (SRC_FILE_ID)
;
-- disable constraints for better performance (keep them in the model for reference only)
ALTER TABLE MTD_DQ_SCREEN_EVENT DISABLE CONSTRAINT MTD_DQ_SCREEN_EVENT_BATCH_FK;
ALTER TABLE MTD_DQ_SCREEN_EVENT DISABLE CONSTRAINT MTD_DQ_SCREEN_EVENT_SCREEN_FK;




