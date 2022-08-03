/****** Object:  StoredProcedure [mtdta].[spLoadDVRawSat]    Script Date: 12/03/2020 12:31:26 PM ******/
DROP PROCEDURE [mtdta].[spLoadDVRawSat]
GO
/****** Object:  StoredProcedure [mtdta].[spLoadDVRawLink]    Script Date: 12/03/2020 12:31:26 PM ******/
DROP PROCEDURE [mtdta].[spLoadDVRawLink]
GO
/****** Object:  StoredProcedure [mtdta].[spLoadDVRawHub]    Script Date: 12/03/2020 12:31:26 PM ******/
DROP PROCEDURE [mtdta].[spLoadDVRawHub]
GO
/****** Object:  StoredProcedure [mtdta].[spLoadDVBusinessSat]    Script Date: 12/03/2020 12:31:26 PM ******/
DROP PROCEDURE [mtdta].[spLoadDVBusinessSat]
GO
/****** Object:  StoredProcedure [mtdta].[spLoadDVBusinessLink]    Script Date: 12/03/2020 12:31:26 PM ******/
DROP PROCEDURE [mtdta].[spLoadDVBusinessLink]
GO
/****** Object:  StoredProcedure [mtdta].[spLoadDVBusinessHub]    Script Date: 12/03/2020 12:31:26 PM ******/
DROP PROCEDURE [mtdta].[spLoadDVBusinessHub]
GO
/****** Object:  StoredProcedure [mtdta].[spDropDVRawFK]    Script Date: 12/03/2020 12:31:26 PM ******/
DROP PROCEDURE [mtdta].[spDropDVRawFK]
GO
/****** Object:  StoredProcedure [mtdta].[spCreateDVRawSat]    Script Date: 12/03/2020 12:31:26 PM ******/
DROP PROCEDURE [mtdta].[spCreateDVRawSat]
GO
/****** Object:  StoredProcedure [mtdta].[spCreateDVRawLink]    Script Date: 12/03/2020 12:31:26 PM ******/
DROP PROCEDURE [mtdta].[spCreateDVRawLink]
GO
/****** Object:  StoredProcedure [mtdta].[spCreateDVRawHub]    Script Date: 12/03/2020 12:31:26 PM ******/
DROP PROCEDURE [mtdta].[spCreateDVRawHub]
GO
/****** Object:  StoredProcedure [mtdta].[spCreateDVRawHub]    Script Date: 12/03/2020 12:31:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE proc [mtdta].[spCreateDVRawHub]  @pSrcSchema varchar(25) = NULL, @pSrcName varchar(100) = NULL as
/* **********************************************************************
Author:  Deloitte
Creation Date: 27-02-2020
Desc: Procedure generates DDL scripts to create raw data vault Hubs. 
      The procedure is driven from the following metadata 
	  table [mtdta].[DataVaultHubTransform]
*************************************************************************
Sample exection:
----------------
exec [mtdta].[spCreateDVRawHub] @pSrcSchema = 'STG_CMS', @pSrcName = 'CMS_CROKER'

Sample code created by the above execution
----------------
IF EXISTS (SELECT * FROM sysobjects WHERE name='HubContract' and xtype='U')
DROP TABLE [RV_CMS].[HubContract]
CREATE TABLE [RV_CMS].[HubContract] (
  [ContractHashKey] [varbinary](16) NOT NULL
, [LoadDateTime] [datetime] NOT NULL
, [RecordSource] [varchar](255) NOT NULL
, [LoadID] [bigint] NOT NULL
, [CC_ID] [varchar](16) NOT NULL
)
CONSTRAINT [HubContract_PK] PRIMARY KEY CLUSTERED 
(
	[ContractHashKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
*/

DECLARE @sqlstatement     nvarchar(MAX)
      , @Hubname          varchar(100)
      , @Hubschema        varchar(100)
      , @cHubname         varchar(100)
      , @cHubschema       varchar(100)
	  ;


set @Hubschema = @pSrcSchema;
set @Hubname = @pSrcName;


if @pSrcSchema is null or @pSrcSchema = 'NULL' 
begin
	select @Hubschema = 'ALL'
end;

if @pSrcName is null or @pSrcName = 'NULL' 
begin
	select @Hubname = 'ALL'
end;


DECLARE  cur_raw_ddl CURSOR FOR 
with with_meta as
(
	select SrcSchema, SrcObject, SrcDatabase, SrcBusinessKey, HubSchema, HubName, RecordSource
	     , substring(HubName,4,255) + 'HashKey' as HubHashKey         -- name of Hub hash key, e.g. ContractBrokerHashKey
		 , HubBusinessKey, ColumnPosition 
      from [mtdta].[DataVaultHubTransform] 
     WHERE 1=1
	  --AND HubName = 'HubContract'
	  AND SrcSchema = case @Hubschema when 'ALL' then SrcSchema else @Hubschema end
      AND SrcObject = case @Hubname when 'ALL' then SrcObject else @Hubname end
)
select 
 aa.HubSchema, aa.HubName
,'IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES where table_name = ''' + HubName + ''' and table_schema =  ''' + HubSchema + ''' )
DROP TABLE [' + HubSchema + '].[' + HubName + ']
CREATE TABLE [' + HubSchema + '].[' + HubName + '] (
  ['+ HubHashKey + '] [varbinary](16) NOT NULL
, [LoadDateTime] [datetime] NOT NULL
, [RecordSource] [varchar](255) NOT NULL
, [LoadID] [bigint] NOT NULL
'+ HubColumns +
'CONSTRAINT [' + HubName + '_PK] PRIMARY KEY CLUSTERED 
(
	['+ HubHashKey + '] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]' + char(10) + char(10) as DDL
from 
(
   select distinct t1.SrcSchema, t1.SrcObject, t1.SrcDatabase, t1.HubSchema, t1.HubName, t1.RecordSource,  t1.HubHashKey, 
          STUFF((SELECT distinct  ', [' + HubBusinessKey + '] ' +
						CASE WHEN isc.DATA_TYPE = 'decimal' OR isc.DATA_TYPE = 'numeric'
						 THEN '['+ isc.DATA_TYPE+']('
								+ cast(isc.NUMERIC_PRECISION as nvarchar)
								+','
								+ cast(isc.NUMERIC_SCALE  as nvarchar)
								+')'
						WHEN isc.DATA_TYPE = 'varchar' or isc.DATA_TYPE = 'char' 
						 THEN '['+ isc.DATA_TYPE+']('
								+ cast(isc.CHARACTER_MAXIMUM_LENGTH as nvarchar)
								+')'
						WHEN isc.DATA_TYPE = 'date' or isc.DATA_TYPE = 'datetime'
						 THEN '['+ isc.DATA_TYPE+']'
						ELSE '[nvarchar](MAX)'
						END +
						CASE WHEN isc.IS_NULLABLE = 'YES' THEN '' ELSE ' NOT NULL' END + char(10)
					 from with_meta t2
		             left outer join INFORMATION_SCHEMA.COLUMNS isc
					   on t2.srcSchema = isc.TABLE_SCHEMA
					  and t2.srcObject = isc.TABLE_NAME
					  and t2.SrcBusinessKey = isc.COLUMN_NAME
					 where t1.HubName = t2.HubName AND t1.HubSchema = t2.HubSchema
						FOR XML PATH(''), TYPE
						).value('.', 'NVARCHAR(MAX)') 
					,1,0,''
					)  HubColumns
			from with_meta t1
) aa

OPEN cur_raw_ddl
FETCH NEXT FROM cur_raw_ddl INTO @cHubname, @cHubschema, @sqlstatement
WHILE @@FETCH_STATUS = 0
BEGIN
    print @sqlstatement
	
    exec sp_executesql  @Query  = @sqlstatement
    
	FETCH NEXT FROM cur_raw_ddl INTO @cHubname, @cHubschema, @sqlstatement
END
CLOSE cur_raw_ddl
DEALLOCATE cur_raw_ddl

GO
/****** Object:  StoredProcedure [mtdta].[spCreateDVRawLink]    Script Date: 12/03/2020 12:31:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE proc [mtdta].[spCreateDVRawLink]  @pSrcSchema varchar(25) = NULL, @pSrcName varchar(100) = NULL as
/* **********************************************************************
Author:  Deloitte
Creation Date: 27-02-2020
Desc: Procedure generates DDL scripts to create raw data vault links. 
      The procedure is driven from the following metadata 
	  table [mtdta].[DataVaultLinkTransform]
*************************************************************************
Sample exection:
----------------
exec [mtdta].[spCreateDVRawLink] @pSrcSchema = 'STG_CMS', @pSrcName = 'CMS_BROKER'

Sample code created by the above execution
----------------

--------- DDL for [RV_CMS].[LinkContractBroker]
IF EXISTS (SELECT * FROM sysobjects WHERE name='LinkContractBroker' and xtype='U')
DROP TABLE [RV_CMS].[LinkContractBroker]
CREATE TABLE [RV_CMS].[LinkContractBroker] (
  [ContractBrokerHashKey] [varbinary](16) NOT NULL
, [LoadDateTime] [datetime] NOT NULL
, [RecordSource] [varchar](255) NOT NULL
, [LoadID] [bigint] NOT NULL
, [BrokerHashKey] [varbinary](16) NOT NULL
, [ContractHashKey] [varbinary](16) NOT NULL
CONSTRAINT [LinkContractBroker_PK] PRIMARY KEY CLUSTERED 
([ContractBrokerHashKey] ASC) 
    WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

ALTER TABLE [RV_CMS].[LinkContractBroker] ADD CONSTRAINT LinkContractBroker_HubBroker_FK FOREIGN KEY(BrokerHashKey) REFERENCES [RV_CMS].[HubBroker](BrokerHashKey)
ALTER TABLE [RV_CMS].[LinkContractBroker] NOCHECK CONSTRAINT LinkContractBroker_HubBroker_FK
ALTER TABLE [RV_CMS].[LinkContractBroker] ADD CONSTRAINT LinkContractBroker_HubContract_FK FOREIGN KEY(ContractHashKey) REFERENCES [RV_CMS].[HubContract](ContractHashKey)
ALTER TABLE [RV_CMS].[LinkContractBroker] NOCHECK CONSTRAINT LinkContractBroker_HubContract_FK



*/

DECLARE @sqlstatement     nvarchar(MAX)
      , @linkname          varchar(100)
      , @linkschema        varchar(100)
      , @clinkname         varchar(100)
      , @clinkschema       varchar(100)
	  ;


set @linkschema = @pSrcSchema;
set @linkname = @pSrcName;


if @pSrcSchema is null or @pSrcSchema = 'NULL' 
begin
	select @linkschema = 'ALL'
end;

if @pSrcName is null or @pSrcName = 'NULL' 
begin
	select @linkname = 'ALL'
end;


DECLARE  cur_raw_ddl CURSOR FOR 
with with_meta as
(
	select SrcSchema, SrcObject, SrcDatabase, LinkSchema, LinkName, RecordSource
	     , substring(LinkName,5,255) + 'HashKey' as LinkHashKey         -- name of link hash key, e.g. ContractBrokerHashKey
		 , LinkHubHashKeyName  -- name of hash key for hubs realted to the link, e.g. BrokerHashKey
		 , HubName
	  from [mtdta].[DataVaultLinkTransform] 
     WHERE 1=1
	   --AND LinkName = 'LinkContractBroker
	  AND SrcSchema = case @linkschema when 'ALL' then SrcSchema else @linkschema end
      AND SrcObject = case @linkname when 'ALL' then SrcObject else @linkname end
)
select 
 aa.LinkSchema, aa.LinkName
,'--------- DDL for [' + LinkSchema + '].[' + LinkName + ']' + char(10) +
'IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES where table_name = ''' + LinkName + ''' and table_schema =  ''' + LinkSchema + ''' )
DROP TABLE [' + LinkSchema + '].[' + LinkName + ']
CREATE TABLE [' + LinkSchema + '].[' + LinkName + '] (
  ['+ LinkHashKey + '] [varbinary](16) NOT NULL
, [LoadDateTime] [datetime] NOT NULL
, [RecordSource] [varchar](255) NOT NULL
, [LoadID] [bigint] NOT NULL
'+ LinkHubHashKeyNames +
'CONSTRAINT [' + LinkName + '_PK] PRIMARY KEY CLUSTERED 
(['+ LinkHashKey + '] ASC) 
    WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]' + char(10) + char(10) + 
FKdef
+ char(10) + char(10)  as DDL
from 
(
   select distinct t1.SrcSchema, t1.SrcObject, t1.SrcDatabase, t1.LinkSchema, t1.LinkName, t1.RecordSource,  t1.LinkHashKey, 
          -- list of hash key names related to hubs
		  STUFF((SELECT distinct  ', [' + t2.LinkHubHashKeyName + '] [varbinary](16) NOT NULL' + char(10)
					 from with_meta t2
					 where t1.LinkName = t2.LinkName AND t1.LinkSchema = t2.LinkSchema
						FOR XML PATH(''), TYPE
						).value('.', 'NVARCHAR(MAX)') 
					,1,0,''
					)  LinkHubHashKeyNames,
		  -- FK definitions between link and hubs
          STUFF((SELECT distinct  
						  'ALTER TABLE [' + t2.LinkSchema + '].[' + t2.LinkName + ']' + 
						+ ' ADD CONSTRAINT ' + t2.LinkName + '_' + t2.LinkHubHashKeyName + '_FK FOREIGN KEY(' + t2.LinkHubHashKeyName + ') REFERENCES [' 
						+ t2.LinkSchema + '].[' + t2.HubName + '](' + SUBSTRING(t2.HubName,4,255) + 'HashKey)' + char(10) 
						+ 'ALTER TABLE [' + t2.LinkSchema + '].[' + t2.LinkName + '] NOCHECK CONSTRAINT ' + t2.LinkName + '_' + t2.LinkHubHashKeyName + '_FK' + char(10)
					 from with_meta t2
					 where t1.LinkName = t2.LinkName AND t1.LinkSchema = t2.LinkSchema
						FOR XML PATH(''), TYPE
						).value('.', 'NVARCHAR(MAX)') 
					,1,0,''
					)  FKdef

			from with_meta t1
) aa



OPEN cur_raw_ddl
FETCH NEXT FROM cur_raw_ddl INTO @clinkname, @clinkschema, @sqlstatement
WHILE @@FETCH_STATUS = 0
BEGIN
    print @sqlstatement
	
    exec sp_executesql  @Query  = @sqlstatement
    
	FETCH NEXT FROM cur_raw_ddl INTO @clinkname, @clinkschema, @sqlstatement
END
CLOSE cur_raw_ddl
DEALLOCATE cur_raw_ddl
GO
/****** Object:  StoredProcedure [mtdta].[spCreateDVRawSat]    Script Date: 12/03/2020 12:31:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE proc [mtdta].[spCreateDVRawSat]  @pSrcSchema varchar(25) = NULL, @pSrcName varchar(100) = NULL as
/* **********************************************************************
Author:  Deloitte
Creation Date: 27-02-2020
Desc: Procedure generates DDL scripts to create raw data vault sats. 
      The procedure is driven from the following metadata 
	  table [mtdta].[DataVaultSatTransform]
*************************************************************************
Sample exection:
----------------
exec [mtdta].[spCreateDVRawSat] @pSrcSchema = 'STG_CMS', @pSrcName = 'CMS_BROKER'

Sample code created by the above execution
----------------
IF EXISTS (SELECT * FROM sysobjects WHERE name='SatContractBroker' and xtype='U')
DROP TABLE [RV_CMS].[SatContractBroker]
CREATE TABLE [RV_CMS].[SatContractBroker] (
  [ContractBrokerHashKey] [varbinary](16) NOT NULL
, [LoadDateTime] [datetime] NOT NULL
, [RecordSource] [varchar](255) NOT NULL
, [LoadID] [bigint] NOT NULL
, [BrokerHashKey] [varbinary](16) NOT NULL
, [ContractHashKey] [varbinary](16) NOT NULL
)
CONSTRAINT [SatContractBroker_PK] PRIMARY KEY CLUSTERED 
(
	[ContractBrokerHashKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
*/

DECLARE @sqlstatement     nvarchar(MAX)
      , @satname          varchar(100)
      , @satschema        varchar(100)
      , @csatname         varchar(100)
      , @csatschema       varchar(100)
	  ;


set @satschema = @pSrcSchema;
set @satname = @pSrcName;


if @pSrcSchema is null or @pSrcSchema = 'NULL' 
begin
	select @satschema = 'ALL'
end;

if @pSrcName is null or @pSrcName = 'NULL' 
begin
	select @satname = 'ALL'
end;


DECLARE  cur_raw_ddl CURSOR FOR 
with with_meta as
(
	select SrcSchema, SrcObject, SrcDatabase, SrcColumn, SatSchema, SatName, RecordSource
	     , (select max(x.SatColumn) from [mtdta].[DataVaultSatTransform] x where x.ColumnPosition = 1 and x.SatName = a.SatName) as SatHashKey         -- name of sat hash key, e.g. ContractBrokerHashKey
	     , (select max('Hub'+substring(x.SatColumn,0,len(x.SatColumn)-6)) from [mtdta].[DataVaultSatTransform] x where x.ColumnPosition = 1 and x.SatName = a.SatName) as HubName         
		 , SatColumn, ColumnPosition 
      from [mtdta].[DataVaultSatTransform] a
     WHERE 1=1
	  AND IsColumnBusinessKey = 0
	  --AND SatName = 'SatStaffFast'
	  AND SrcSchema = case @satschema when 'ALL' then SrcSchema else @satschema end
      AND SrcObject = case @satname when 'ALL' then SrcObject else @satname end
)
select 
 aa.SatSchema, aa.SatName
,'IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES where table_name = ''' + SatName + ''' and table_schema =  ''' + SatSchema + ''' )
DROP TABLE [' + SatSchema + '].[' + SatName + ']
CREATE TABLE [' + SatSchema + '].[' + SatName + '] (
  ['+ SatHashKey + '] [varbinary](16) NOT NULL
, [LoadDateTime] [datetime] NOT NULL
, [RecordSource] [varchar](255) NOT NULL
, [HashDiff] [varbinary](16) NOT NULL
, [AppliedDateTime] [datetime] NOT NULL
, [LoadID] [bigint] NOT NULL
'+ SatColumns +
'CONSTRAINT [' + SatName + '_PK] PRIMARY KEY CLUSTERED 
(
	['+ SatHashKey + '] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]' + char(10) + char(10) +
 'ALTER TABLE [' + SatSchema + '].[' + SatName + ']' + 
+ ' ADD CONSTRAINT ' + SatName + '_' + HubName + '_FK FOREIGN KEY(' + SatHashKey + ') REFERENCES [' 
+ SatSchema + '].[' + HubName + '](' + SatHashKey + ')' + char(10) 
+ 'ALTER TABLE [' + SatSchema + '].[' + SatName + '] NOCHECK CONSTRAINT ' + SatName + '_' + HubName + '_FK' + char(10)

+ char(10) + char(10)  as DDL
from 
(
   select distinct t1.SrcSchema, t1.SrcObject, t1.SrcDatabase, t1.SatSchema, t1.SatName, t1.RecordSource,  t1.SatHashKey, t1.HubName,
          STUFF((SELECT distinct  ', [' + SatColumn + '] ' +
						CASE WHEN isc.DATA_TYPE = 'decimal'
						 THEN '['+ isc.DATA_TYPE+']('
								+ cast(isc.NUMERIC_PRECISION as nvarchar)
								+','
								+ cast(isc.NUMERIC_SCALE  as nvarchar)
								+')'
						WHEN isc.DATA_TYPE = 'varchar' or isc.DATA_TYPE = 'char' 
						 THEN '['+ isc.DATA_TYPE+']('
								+ cast(isc.CHARACTER_MAXIMUM_LENGTH as nvarchar)
								+')'
						WHEN isc.DATA_TYPE = 'date' or isc.DATA_TYPE = 'datetime'
						 THEN '['+ isc.DATA_TYPE+']'
						ELSE '[nvarchar](MAX)'
						END +
						CASE WHEN isc.IS_NULLABLE = 'YES' THEN '' ELSE ' NOT NULL' END + char(10)
					 from with_meta t2
		             left outer join INFORMATION_SCHEMA.COLUMNS isc
					   on t2.srcSchema = isc.TABLE_SCHEMA
					  and t2.srcObject = isc.TABLE_NAME
					  and t2.SrcColumn = isc.COLUMN_NAME
					 where t1.SatName = t2.SatName AND t1.SatSchema = t2.SatSchema
						FOR XML PATH(''), TYPE
						).value('.', 'NVARCHAR(MAX)') 
					,1,0,''
					)  SatColumns
			from with_meta t1
) aa

OPEN cur_raw_ddl
FETCH NEXT FROM cur_raw_ddl INTO @csatname, @csatschema, @sqlstatement
WHILE @@FETCH_STATUS = 0
BEGIN
    print @sqlstatement
	
    exec sp_executesql  @Query  = @sqlstatement
    
	FETCH NEXT FROM cur_raw_ddl INTO @csatname, @csatschema, @sqlstatement
END
CLOSE cur_raw_ddl
DEALLOCATE cur_raw_ddl
GO
/****** Object:  StoredProcedure [mtdta].[spDropDVRawFK]    Script Date: 12/03/2020 12:31:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE proc [mtdta].[spDropDVRawFK]  @pSchema varchar(255) as
/* **********************************************************************
Author:  Deloitte
Creation Date: 27-02-2020
Desc: Procedure drops FK constraints in provided RV schema
*************************************************************************
Sample exection:
----------------
	exec [mtdta].[spDropDVRawFK] @pSchema = 'RV_CMS'

*/


DECLARE @database nvarchar(255),
        @table_schema nvarchar(255);

set @database = db_name();
set @table_schema = @pSchema;


DECLARE @sql nvarchar(255)
WHILE EXISTS(select * from INFORMATION_SCHEMA.TABLE_CONSTRAINTS where constraint_catalog = @database and TABLE_SCHEMA = @table_schema)
BEGIN
    select    @sql = 'ALTER TABLE ' + TABLE_SCHEMA  + '.' + TABLE_NAME +  ' DROP CONSTRAINT ' + CONSTRAINT_NAME 
    from    INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
    where   constraint_catalog = @database and 
            TABLE_SCHEMA = @table_schema
    print @sql
    exec    sp_executesql @sql
END


GO
/****** Object:  StoredProcedure [mtdta].[spLoadDVBusinessHub]    Script Date: 12/03/2020 12:31:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [mtdta].[spLoadDVBusinessHub]  @pHubSchema varchar(25) = 'BV', @pHubName varchar(25) = NULL AS
/* **********************************************************************
Author:  Deloitte
Creation Date: 27-02-2020
Desc: Procedure generates DML scripts to load business data vault hubs from
      staging views. 
*************************************************************************
Sample exection:
----------------
	exec [mtdta].[spLoadDVBusinessHub] @pHubSchema = 'BV', @pHubName = 'HubAgreement'

Sample code created by the above execution 
----------------
INSERT INTO bv.HubAgreement(AgreementHashKey, LoadDateTime, RecordSource, LoadID, Agreement_ID)
SELECT src.AgreementHashKey
     , getdate() as LoadDateTime
     , src.RecordSource
     , -1 as LoadID
     , src.Agreement_ID
  FROM STG_BV.HubAgreement src
  LEFT OUTER JOIN BV.HubAgreement tgt
    ON src.AgreementHashKey = tgt.AgreementHashKey
 WHERE src.AgreementHashKey IS NOT NULL 
   AND tgt.AgreementHashKey IS NULL 
*/

DECLARE @sqlstatement     nvarchar(MAX)
      , @hubname          varchar(100)
      , @hubschema        varchar(100)
      , @chubname         varchar(100)
      , @chubschema       varchar(100)
	  ;

set @hubschema = @pHubSchema;
set @hubname = @pHubName;


if @pHubSchema is null or @pHubSchema = 'NULL' 
begin
	select @hubschema = 'ALL'
end;

if @pHubName is null or @pHubName = 'NULL' 
begin
	select @hubname = 'ALL'
end;


DECLARE  cur_raw_load CURSOR FOR 
SELECT
   m.HubName
 , m.HubSchema,
'INSERT INTO ' + HubSchema + '.' + HubName + '(' +  HubHashKeyName + ', LoadDateTime, RecordSource, LoadID ' + HubBusinessKey + ')
SELECT src.' + HubHashKeyName + '
    , getdate() AS LoadDateTime
    , src.RecordSource
    , -1 AS LoadID' + HubBusinessKeySrcList + '
 FROM STG_' + HubSchema + '.'+ HubName + ' src
 LEFT OUTER JOIN ' + HubSchema + '.'+ HubName + ' tgt
   ON src.' + HubHashKeyName + ' = tgt.' + HubHashKeyName + '
WHERE src.' + HubHashKeyName + ' IS NOT NULL
  AND tgt.' + HubHashKeyName + ' IS NULL'as DMLstmt
FROM 
(
	select t1.TABLE_SCHEMA as HubSchema
		 , t1.TABLE_NAME as HubName
		 , max(case when t1.ordinal_position = 1 then t1.COLUMN_NAME else null end) as HubHashKeyName
	  	 ,STUFF((SELECT distinct char(10) + ' , ' + t2.COLUMN_NAME 
						  from INFORMATION_SCHEMA.COLUMNS t2
						 where t1.TABLE_SCHEMA = t2.TABLE_SCHEMA 
						   and t1.TABLE_NAME = t2.TABLE_NAME 
						   and t2.COLUMN_NAME not in ('LoadDateTime', 'LoadId', 'RecordSource')
						   and t2.ordinal_position > 1
						FOR XML PATH(''), TYPE
						).value('.', 'NVARCHAR(MAX)') 
					,1,0,''
					) as HubBusinessKey
	   	 ,STUFF((SELECT distinct char(10) + ' , src.' + t2.COLUMN_NAME 
						  from INFORMATION_SCHEMA.COLUMNS t2
						 where t1.TABLE_SCHEMA = t2.TABLE_SCHEMA 
						   and t1.TABLE_NAME = t2.TABLE_NAME 
						   and t2.COLUMN_NAME not in ('LoadDateTime', 'LoadId', 'RecordSource')
						   and t2.ordinal_position > 1
						FOR XML PATH(''), TYPE
						).value('.', 'NVARCHAR(MAX)') 
					,1,0,''
					) as HubBusinessKeySrcList
	  from INFORMATION_SCHEMA.COLUMNS t1
	 where TABLE_SCHEMA = case @hubschema when 'ALL' then t1.TABLE_SCHEMA else @hubschema end
       AND t1.TABLE_NAME = case @hubname when 'ALL' then t1.TABLE_NAME else @hubname end
	   AND t1.TABLE_NAME like 'Hub%'
 	   and t1.COLUMN_NAME not in ('LoadDateTime', 'LoadId', 'RecordSource')
	 group by t1.TABLE_SCHEMA, t1.TABLE_NAME
) m




OPEN cur_raw_load
FETCH NEXT FROM cur_raw_load INTO @chubname, @chubschema, @sqlstatement
WHILE @@FETCH_STATUS = 0
BEGIN
    print @sqlstatement
	
    exec sp_executesql  @Query  = @sqlstatement
     
	FETCH NEXT FROM cur_raw_load INTO @chubname, @chubschema, @sqlstatement
END
CLOSE cur_raw_load
DEALLOCATE cur_raw_load

GO
/****** Object:  StoredProcedure [mtdta].[spLoadDVBusinessLink]    Script Date: 12/03/2020 12:31:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE PROC [mtdta].[spLoadDVBusinessLink]  @pLinkSchema varchar(25) = 'BV', @pLinkName varchar(25) = NULL AS
/* **********************************************************************
Author:  Deloitte
Creation Date: 27-02-2020
Desc: Procedure generates DML scripts to load business data vault links from
      staging views. 
*************************************************************************
Sample exection:
----------------
	exec [mtdta].[spLoadDVBusinessLink] @pLinkSchema = 'BV', @pLinkName = 'LinkAgreementBroker'

Sample code created by the above execution 
----------------
INSERT INTO BV.LinkAgreementBroker(AgreementBrokerHashKey, LoadDateTime, RecordSource, LoadID, AgreementHashKey, BrokerHashKey)
SELECT src.AgreementBrokerHashKey
    , getdate() AS LoadDateTime
    , src.RecordSource
    , -1 AS LoadID
    , src.AgreementHashKey
    , src.BrokerHashKey
 FROM STG_BV.LinkAgreementBroker src
 LEFT OUTER JOIN BV.LinkAgreementBroker tgt
   ON src.AgreementBrokerHashKey = tgt.AgreementBrokerHashKey
WHERE src.AgreementBrokerHashKey IS NOT NULL
  AND tgt.AgreementBrokerHashKey IS NULL
*/

DECLARE @sqlstatement     nvarchar(MAX)
      , @linkname          varchar(100)
      , @linkschema        varchar(100)
      , @clinkname         varchar(100)
      , @clinkschema       varchar(100)
	  ;

set @linkschema = @pLinkSchema;
set @linkname = @pLinkName;


if @pLinkSchema is null or @pLinkSchema = 'NULL' 
begin
	select @linkschema = 'ALL'
end;

if @pLinkName is null or @pLinkName = 'NULL' 
begin
	select @linkname = 'ALL'
end;


DECLARE  cur_raw_load CURSOR FOR 
SELECT
   m.LinkName
 , m.LinkSchema,
'INSERT INTO ' + LinkSchema + '.' + LinkName + '(' +  LinkHashKeyName + ', LoadDateTime, RecordSource, LoadID ' + LinkBusinessKey + ')
SELECT src.' + LinkHashKeyName + '
    , getdate() AS LoadDateTime
    , src.RecordSource
    , -1 AS LoadID' + LinkBusinessKeySrcList + '
 FROM STG_' + LinkSchema + '.'+ LinkName + ' src
 LEFT OUTER JOIN ' + LinkSchema + '.'+ LinkName + ' tgt
   ON src.' + LinkHashKeyName + ' = tgt.' + LinkHashKeyName + '
WHERE src.' + LinkHashKeyName + ' IS NOT NULL
  AND tgt.' + LinkHashKeyName + ' IS NULL'as DMLstmt
FROM 
(
	select t1.TABLE_SCHEMA as LinkSchema
		 , t1.TABLE_NAME as LinkName
		 , max(case when t1.ordinal_position = 1 then t1.COLUMN_NAME else null end) as LinkHashKeyName
	  	 ,STUFF((SELECT distinct ', ' + t2.COLUMN_NAME 
						  from INFORMATION_SCHEMA.COLUMNS t2
						 where t1.TABLE_SCHEMA = t2.TABLE_SCHEMA 
						   and t1.TABLE_NAME = t2.TABLE_NAME 
						   and t2.COLUMN_NAME not in ('LoadDateTime', 'LoadId', 'RecordSource')
						   and t2.ordinal_position > 1
						FOR XML PATH(''), TYPE
						).value('.', 'NVARCHAR(MAX)') 
					,1,0,''
					) as LinkBusinessKey
	   	 ,STUFF((SELECT distinct char(10) + '    , src.' + t2.COLUMN_NAME 
						  from INFORMATION_SCHEMA.COLUMNS t2
						 where t1.TABLE_SCHEMA = t2.TABLE_SCHEMA 
						   and t1.TABLE_NAME = t2.TABLE_NAME 
						   and t2.COLUMN_NAME not in ('LoadDateTime', 'LoadId', 'RecordSource')
						   and t2.ordinal_position > 1
						FOR XML PATH(''), TYPE
						).value('.', 'NVARCHAR(MAX)') 
					,1,0,''
					) as LinkBusinessKeySrcList
	  from INFORMATION_SCHEMA.COLUMNS t1
	 where 1=1
	   and TABLE_SCHEMA = case @linkschema when 'ALL' then t1.TABLE_SCHEMA else @linkschema end
       and t1.TABLE_NAME = case @linkname when 'ALL' then t1.TABLE_NAME else @linkname end
	   and t1.TABLE_NAME like 'Link%'
	   --and t1.TABLE_SCHEMA = 'BV'
 	   and t1.COLUMN_NAME not in ('LoadDateTime', 'LoadId', 'RecordSource')
	 group by t1.TABLE_SCHEMA, t1.TABLE_NAME
) m




OPEN cur_raw_load
FETCH NEXT FROM cur_raw_load INTO @clinkname, @clinkschema, @sqlstatement
WHILE @@FETCH_STATUS = 0
BEGIN
    print @sqlstatement
	
    exec sp_executesql  @Query  = @sqlstatement
     
	FETCH NEXT FROM cur_raw_load INTO @clinkname, @clinkschema, @sqlstatement
END
CLOSE cur_raw_load
DEALLOCATE cur_raw_load

GO
/****** Object:  StoredProcedure [mtdta].[spLoadDVBusinessSat]    Script Date: 12/03/2020 12:31:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [mtdta].[spLoadDVBusinessSat]  @pSatSchema varchar(25) = 'BV', @pSatName varchar(25) = NULL AS
/* **********************************************************************
Author:  Deloitte
Creation Date: 27-02-2020
Desc: Procedure generates DML scripts to load business data vault sats from
      staging views. 
*************************************************************************
Sample exection:
----------------
	exec [mtdta].[spLoadDVBusinessSat] @pSatSchema = 'BV', @pSatName = 'SatBroker'

Sample code created by the above execution 
----------------
INSERT INTO BV.SatBroker(BrokerHashKey,  LoadDateTime, RecordSource, HashDiff, AppliedDateTime, LoadID  , Name)
select stg.* From (
	select BrokerHashKey
		 , getdate() as LoadDateTime
		 , RecordSource
		 , HASHBYTES('MD5', ISNULL(RTRIM(CONVERT(NVARCHAR(100), Name)),'NA')+'|' ) AS HashDiff
		 , getdate() as AppliedDateTime
		 , -1 as LoadID
	     , Name 
	from STG_BV.SatBroker
	) stg
 LEFT OUTER JOIN
		(SELECT *
		   FROM (SELECT s.BrokerHashKey, s.HashDiff
					  , row_number() over (partition by BrokerHashKey order by LoadDateTime desc) as rownum
				   FROM BV.SatBroker s
				) a
		  WHERE rownum = 1
		) sat
	   ON stg.BrokerHashKey = sat.BrokerHashKey
	WHERE stg.HashDiff <> sat.HashDiff
	   OR (sat.BrokerHashKey is null AND stg.BrokerHashKey is not null)
*/

DECLARE @sqlstatement     nvarchar(MAX)
      , @satname          varchar(100)
      , @satschema        varchar(100)
      , @csatname         varchar(100)
      , @csatschema       varchar(100)
	  ;

set @satschema = @pSatSchema;
set @satname = @pSatName;


if @pSatSchema is null or @pSatSchema = 'NULL' 
begin
	select @satschema = 'ALL'
end;

if @pSatName is null or @pSatName = 'NULL' 
begin
	select @satname = 'ALL'
end;



DECLARE  cur_raw_load CURSOR FOR 
SELECT
   m.SatName
 , m.SatSchema,
'INSERT INTO ' + SatSchema + '.' + SatName + '(' +  SatHashKeyName + ',  LoadDateTime, RecordSource, HashDiff, AppliedDateTime, LoadID ' + SatBusinessKey + ')
select stg.* From (
	select ' +  SatHashKeyName + '
		 , getdate() as LoadDateTime
		 , RecordSource
		 , HASHBYTES(''MD5'', ' + substring(HashKeyCalc, 0, len(HashKeyCalc)) + ' ) AS HashDiff
		 , getdate() as AppliedDateTime
		 , -1 as LoadID
	    ' + SatBusinessKey + ' 
	from STG_' + SatSchema + '.' + SatName + '
	) stg
 LEFT OUTER JOIN
		(SELECT *
		   FROM (SELECT s.' +  SatHashKeyName + ', s.HashDiff
					  , row_number() over (partition by ' +  SatHashKeyName + ' order by LoadDateTime desc) as rownum
				   FROM ' + SatSchema + '.' + SatName + ' s
				) a
		  WHERE rownum = 1
		) sat
	   ON stg.' +  SatHashKeyName + ' = sat.' +  SatHashKeyName + '
	WHERE stg.HashDiff <> sat.HashDiff
	   OR (sat.' +  SatHashKeyName + ' is null AND stg.' +  SatHashKeyName + ' is not null)' as DMLstmt
FROM 
(
	select t1.TABLE_SCHEMA as SatSchema
		 , t1.TABLE_NAME as SatName
		 , max(case when t1.ordinal_position = 1 then t1.COLUMN_NAME else null end) as SatHashKeyName
	  	 ,STUFF((SELECT distinct ' , ' + t2.COLUMN_NAME 
						  from INFORMATION_SCHEMA.COLUMNS t2
						 where t1.TABLE_SCHEMA = t2.TABLE_SCHEMA 
						   and t1.TABLE_NAME = t2.TABLE_NAME 
						   and t2.COLUMN_NAME not in ('LoadDateTime', 'LoadId', 'RecordSource', 'AppliedDateTime', 'HashDiff')
						   and t2.ordinal_position > 1
						FOR XML PATH(''), TYPE
						).value('.', 'NVARCHAR(MAX)') 
					,1,0,''
					) as SatBusinessKey
         ,STUFF((SELECT distinct 'ISNULL(RTRIM(CONVERT(NVARCHAR(100), '+t2.COLUMN_NAME+')),''NA'')+''|''+' 
					 from INFORMATION_SCHEMA.COLUMNS t2
						 where t1.TABLE_SCHEMA = t2.TABLE_SCHEMA 
						   and t1.TABLE_NAME = t2.TABLE_NAME 
						   and t2.COLUMN_NAME not in ('LoadDateTime', 'LoadId', 'RecordSource', 'AppliedDateTime', 'HashDiff')
						   and t2.ordinal_position > 1
						FOR XML PATH(''), TYPE
						).value('.', 'NVARCHAR(MAX)') 
					,1,0,''
					)  HashKeyCalc

	  from INFORMATION_SCHEMA.COLUMNS t1
	 where 1=1
	   AND TABLE_SCHEMA = case @satschema when 'ALL' then t1.TABLE_SCHEMA else @satschema end
       AND t1.TABLE_NAME = case @satname when 'ALL' then t1.TABLE_NAME else @satname end
	   AND t1.TABLE_NAME like 'Sat%'
 	   and t1.COLUMN_NAME not in ('LoadDateTime', 'LoadId', 'RecordSource', 'AppliedDateTime', 'HashDiff')
	 group by t1.TABLE_SCHEMA, t1.TABLE_NAME
) m


OPEN cur_raw_load
FETCH NEXT FROM cur_raw_load INTO @csatname, @csatschema, @sqlstatement
WHILE @@FETCH_STATUS = 0
BEGIN
    print @sqlstatement
	
    exec sp_executesql  @Query  = @sqlstatement
     
	FETCH NEXT FROM cur_raw_load INTO @csatname, @csatschema, @sqlstatement
END
CLOSE cur_raw_load
DEALLOCATE cur_raw_load

GO
/****** Object:  StoredProcedure [mtdta].[spLoadDVRawHub]    Script Date: 12/03/2020 12:31:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [mtdta].[spLoadDVRawHub]  @pHubSchema varchar(25) = NULL, @pHubName varchar(25) = NULL AS
/* **********************************************************************
Author:  Deloitte
Creation Date: 27-02-2020
Desc: Procedure generates DML scripts to load raw data vault hubs from
      staging tables. The procedure is driven from the following metadata 
	  table [mtdta].[DataVaultHubTransform]
*************************************************************************
Sample exection:
----------------
	exec [mtdta].[spLoadDVRawHub] @pHubSchema = 'RV_CMS', @pHubName = 'HubBroker'

Sample code created by the above execution 
(please note this code is used in comments throughout the procedeure to explain the proceedure code)
----------------
INSERT INTO RV_CMS.HubBroker(BrokerHashKey, LoadDateTime, RecordSource, LoadID, CB_ID)
SELECT DISTINCT
		HASHBYTES('MD5', ISNULL(RTRIM(CONVERT(NVARCHAR(100), stg.CB_ID)),'NA')+'|') AS BrokerHashKey
	, getdate() AS LoadDateTime
	, 'CMS' AS RecordSource
	, -1 AS LoadID
	, stg.CB_ID AS CB_ID
	FROM DT_STANWELL2020.STG_CMS.CMS_BROKER stg
	LEFT OUTER JOIN RV_CMS.HubBroker tgt
	ON stg.CB_ID = tgt.CB_ID
WHERE stg.CB_ID IS NOT NULL 
	AND tgt.CB_ID IS NULL 
GROUP BY
	HASHBYTES('MD5', ISNULL(RTRIM(CONVERT(NVARCHAR(100), stg.CB_ID)),'NA')+'|'),
	stg.CB_ID

*/

DECLARE @sqlstatement     nvarchar(MAX)
      , @hubname          varchar(100)
      , @hubschema        varchar(100)
      , @chubname         varchar(100)
      , @chubschema       varchar(100)
	  ;

set @hubschema = @pHubSchema;
set @hubname = @pHubName;


if @pHubSchema is null or @pHubSchema = 'NULL' 
begin
	select @hubschema = 'ALL'
end;

if @pHubName is null or @pHubName = 'NULL' 
begin
	select @hubname = 'ALL'
end;


DECLARE  cur_raw_load CURSOR FOR 
SELECT
   m.HubName
 , m.HubSchema,
'INSERT INTO ' + HubSchema + '.' + HubName + '(' +  HubHashKeyName + ', LoadDateTime, RecordSource, LoadID, ' + HubBusinessKey + ')
SELECT DISTINCT
      HASHBYTES(''MD5'', ' + HubHashKey +') AS ' + HubHashKeyName + '
    , getdate() AS LoadDateTime
    , ''' + RecordSource + ''' AS RecordSource
    , -1 AS LoadID' + KeySelectList + '
 FROM ' + SrcDatabase + '.' + SrcSchema + '.'+ SrcObject + ' stg
 LEFT OUTER JOIN ' + HubSchema + '.'+ HubName + ' tgt
   ON ' + JoinON + '
WHERE' + SourceKeyNotNullandTargetNull + '
GROUP BY
  HASHBYTES(''MD5'', '+HubHashKey+'),
  ' + SrcBusinessKey as DMLstmt
FROM 
(
  select HubSchema, HubName, SrcSchema, SrcObject, SrcDatabase, RecordSource
       , substring(hubname,4,255) + 'HashKey' as HubHashKeyName               -- hub hash key name, eg. BrokerHashKey
       , substring(SrcBusinessKey,3,len(SrcBusinessKey)) as SrcBusinessKey    -- comma separated list of business keys inclduing stg prefix, e.g. stg.CCE_CONTRACTID, stg.CCE_DATETIME
       , substring(HubBusinessKey,3,len(HubBusinessKey)) as HubBusinessKey    -- comma separated list of business keys, e.g. CCE_CONTRACTID, CCE_DATETIME
	   , substring(HubHashKey,0,len(HubHashKey)) as HubHashKey                -- part of hub hash key calculation statement, e.g. ISNULL(RTRIM(CONVERT(NVARCHAR(100), stg.CCE_CONTRACTID)),'NA')+'|'+ISNULL(RTRIM(CONVERT(NVARCHAR(100), stg.CCE_DATETIME)),'NA')+'|'
	   , KeySelectList                                                        -- projection part of select for business keys, e.g. stg.CCE_CONTRACTID AS CCE_CONTRACTID, stg.CCE_DATETIME AS CCE_DATETIME
	   , substring(JoinON,8,4000)   as JoinON                                 -- join condition between stage and hub, e.g. stg.CCE_CONTRACTID = tgt.CCE_CONTRACTID AND  stg.CCE_DATETIME = tgt.CCE_DATETIME 
	   , substring(SourceKeyNotNullandTargetNull,0,len(SourceKeyNotNullandTargetNull)-5) as SourceKeyNotNullandTargetNull -- filtering predicates to get only new rows into hub, e.g. stg.CB_ID IS NOT NULL AND tgt.CB_ID IS NULL 
  from
  (
  select distinct t1.HubName, t1.HubSchema, t1.SrcSchema, t1.SrcObject, t1.SrcDatabase, t1.RecordSource
     	-- comma separated list of business keys inclduing stg prefix, e.g. stg.CCE_CONTRACTID, stg.CCE_DATETIME
		,STUFF((SELECT distinct ', stg.' + t2.SrcBusinessKey
				 from [mtdta].[DataVaultHubTransform] t2
				 where t1.HubName = t2.HubName AND t1.HubSchema = t2.HubSchema
					FOR XML PATH(''), TYPE
					).value('.', 'NVARCHAR(MAX)') 
				,1,0,''
				) SrcBusinessKey
     	-- comma separated list of business keys, e.g. CCE_CONTRACTID, CCE_DATETIME
		,STUFF((SELECT distinct ', ' + t2.HubBusinessKey
				 from [mtdta].[DataVaultHubTransform] t2
				 where t1.HubName = t2.HubName AND t1.HubSchema = t2.HubSchema
					FOR XML PATH(''), TYPE
					).value('.', 'NVARCHAR(MAX)') 
				,1,0,''
				) HubBusinessKey
     	-- part of hub hash key calculation statement, e.g. ISNULL(RTRIM(CONVERT(NVARCHAR(100), stg.CCE_CONTRACTID)),'NA')+'|'+ISNULL(RTRIM(CONVERT(NVARCHAR(100), stg.CCE_DATETIME)),'NA')+'|'
		,STUFF((SELECT distinct 'ISNULL(RTRIM(CONVERT(NVARCHAR(100), stg.'+t2.SrcBusinessKey+')),''NA'')+''|''+' 
				 from [mtdta].[DataVaultHubTransform] t2
				 where t1.HubName = t2.HubName AND t1.HubSchema = t2.HubSchema
					FOR XML PATH(''), TYPE
					).value('.', 'NVARCHAR(MAX)') 
				,1,0,''
				) HubHashKey
     	-- projection part of select for business keys, e.g. stg.CCE_CONTRACTID AS CCE_CONTRACTID, stg.CCE_DATETIME AS CCE_DATETIME
		,STUFF((SELECT distinct char(10) + '    , stg.' + t2.SrcBusinessKey + ' AS ' + t2.HubBusinessKey 
				 from [mtdta].[DataVaultHubTransform] t2
				 where t1.HubName = t2.HubName AND t1.HubSchema = t2.HubSchema
					FOR XML PATH(''), TYPE
					).value('.', 'NVARCHAR(MAX)') 
				,1,0,''
				) KeySelectList
     	-- join condition between stage and hub, e.g. stg.CCE_CONTRACTID = tgt.CCE_CONTRACTID AND  stg.CCE_DATETIME = tgt.CCE_DATETIME 
		,STUFF((SELECT distinct char(10) + '  AND stg.' + t2.SrcBusinessKey + ' = tgt.' + t2.HubBusinessKey
				 from [mtdta].[DataVaultHubTransform] t2
				 where t1.HubName = t2.HubName AND t1.HubSchema = t2.HubSchema
					FOR XML PATH(''), TYPE
					).value('.', 'NVARCHAR(MAX)') 
				,1,0,''
				) JoinON
        -- filtering predicates to get only new rows into hub, e.g. stg.CB_ID IS NOT NULL AND tgt.CB_ID IS NULL 
     	,STUFF((SELECT distinct ' stg.' + t2.SrcBusinessKey + ' IS NOT NULL ' + CHAR(10) + '  AND tgt.' + t2.HubBusinessKey + ' IS NULL ' + CHAR(10) + '  AND'  
				 from [mtdta].[DataVaultHubTransform] t2
				 where t1.HubName = t2.HubName AND t1.HubSchema = t2.HubSchema
					FOR XML PATH(''), TYPE
					).value('.', 'NVARCHAR(MAX)') 
				,1,0,''
				) SourceKeyNotNullandTargetNull
		from [mtdta].[DataVaultHubTransform] t1
       WHERE t1.HubSchema = case @hubschema when 'ALL' then t1.HubSchema else @hubschema end
         AND t1.HubName = case @hubname when 'ALL' then t1.HubName else @hubname end
  ) a
) m


OPEN cur_raw_load
FETCH NEXT FROM cur_raw_load INTO @chubname, @chubschema, @sqlstatement
WHILE @@FETCH_STATUS = 0
BEGIN
    print @sqlstatement
	
    exec sp_executesql  @Query  = @sqlstatement
     
	FETCH NEXT FROM cur_raw_load INTO @chubname, @chubschema, @sqlstatement
END
CLOSE cur_raw_load
DEALLOCATE cur_raw_load

GO
/****** Object:  StoredProcedure [mtdta].[spLoadDVRawLink]    Script Date: 12/03/2020 12:31:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE proc [mtdta].[spLoadDVRawLink]  @pLinkSchema varchar(200) = NULL, @pLinkName varchar(200) = NULL as
/* **********************************************************************
Author:  Deloitte
Creation Date: 27-02-2020
Desc: Procedure generates DML scripts to load raw data vault links from
      staging tables. The procedure is driven from the following metadata 
	  table [mtdta].[DataVaultLinkTransform]
*************************************************************************
Sample exection:
----------------
exec [mtdta].[spLoadDVRawLink] @pLinkSchema = 'RV_CMS', @pLinkName = 'LinkContractBroker'

Sample code created by the above execution
----------------
INSERT INTO RV_CMS.LinkContractBroker (ContractBrokerHashKey, LoadDateTime, RecordSource, LoadID , BrokerHashKey, ContractHashKey)
SELECT 
      stg.ContractBrokerHashKey
	, stg.LoadDateTime
    , 'CMS' AS RecordSource
    , -1 AS LoadID
    , stg.BrokerHashKey
    , stg.ContractHashKey
 FROM (SELECT DISTINCT
              HASHBYTES('MD5', ISNULL(RTRIM(CONVERT(NVARCHAR(100), src.CC_BROKER)),'NA')+'|'+ISNULL(RTRIM(CONVERT(NVARCHAR(100), src.CC_ID)),'NA')+'|') AS ContractBrokerHashKey
	        , getdate() AS LoadDateTime 
	        , HASHBYTES('MD5', ISNULL(RTRIM(CONVERT(NVARCHAR(100), src.CC_BROKER)),'NA')+'|') AS BrokerHashKey, HASHBYTES('MD5', ISNULL(RTRIM(CONVERT(NVARCHAR(100), src.CC_ID)),'NA')+'|') AS ContractHashKey
            , NULL as dummy
         FROM DT_STANWELL2020.STG_CMS.CMS_CONTRACT src
        WHERE src.CC_BROKER IS NOT NULL AND src.CC_ID IS NOT NULL
       ) stg
 LEFT OUTER JOIN RV_CMS.LinkContractBroker tgt
   ON stg.ContractBrokerHashKey = tgt.ContractBrokerHashKey 
WHERE tgt.ContractBrokerHashKey IS NULL
*/

DECLARE @sqlstatement     nvarchar(MAX)
      , @linkname          varchar(100)
      , @linkschema        varchar(100)
      , @clinkname         varchar(100)
      , @clinkschema       varchar(100)
	  ;


set @linkschema = @pLinkSchema;
set @linkname = @pLinkName;


if @pLinkSchema is null or @pLinkSchema = 'NULL' 
begin
	select @linkschema = 'ALL'
end;

if @pLinkName is null or @pLinkName = 'NULL' 
begin
	select @linkname = 'ALL'
end;


DECLARE  cur_raw_load CURSOR FOR 
with with_meta as
  (
	select SrcSchema, SrcObject, SrcDatabase, LinkSchema, LinkName, RecordSource
	     , LinkHashKey         -- name of link hash key, e.g. ContractBrokerHashKey
		 , LinkHubHashKeyName  -- name of hash key for hubs realted to the link, e.g. BrokerHashKey
	     , substring(SrcBusKeyNotNull, 2, 8000) as SrcBusKeyNotNull
		 , 'HASHBYTES(''MD5'', ' + substring(LinkHashKeyCalc,0,len(LinkHashKeyCalc)) +  ') AS ' + LinkHashKey as ConcatLinkHashKeyCalc                      -- statement to calculate the hash key for hubs realted to the link, e.g. HASHBYTES('MD5', ISNULL(RTRIM(CONVERT(NVARCHAR(100), src.CC_BROKER)),'NA')+'|'+ISNULL(RTRIM(CONVERT(NVARCHAR(100), src.CC_ID)),'NA')+'|') AS ContractBrokerHashKey
		 , ', HASHBYTES(''MD5'', ' + substring(LinkHubHashKeyCalc,0,len(LinkHubHashKeyCalc)) +  ') AS ' + LinkHubHashKeyName as ConcatLinkHubHashKeyCalc    -- statement to calculate link hash key, e.g.  , HASHBYTES('MD5', ISNULL(RTRIM(CONVERT(NVARCHAR(100), src.CC_ID)),'NA')+'|') AS ContractHashKey
	  from
	  (
	  select distinct t1.SrcSchema, t1.SrcObject, t1.SrcDatabase, t1.LinkSchema, t1.LinkName, t1.LinkHubHashKeyName, t1.RecordSource
	        , substring(t1.LinkName,5,255) + 'HashKey' as LinkHashKey
			--part of statement to calculate the hash key for hubs realted to the link, e.g. ISNULL(RTRIM(CONVERT(NVARCHAR(100), src.CC_BROKER)),'NA')+'|'+
			, STUFF((SELECT distinct 'ISNULL(RTRIM(CONVERT(NVARCHAR(100), src.'+t2.SrcBusinessKey+')),''NA'')+''|''+' 
					 from [mtdta].[DataVaultLinkTransform] t2
					 where t1.LinkName = t2.LinkName AND t1.LinkSchema = t2.LinkSchema
					   and t1.LinkHubHashKeyName = t2.LinkHubHashKeyName
						FOR XML PATH(''), TYPE
						).value('.', 'NVARCHAR(MAX)') 
					,1,0,''
					)  LinkHubHashKeyCalc
              -- part of statement to calculate link hash key, e.g. ISNULL(RTRIM(CONVERT(NVARCHAR(100), src.CC_BROKER)),'NA')+'|'+ISNULL(RTRIM(CONVERT(NVARCHAR(100), src.CC_ID)),'NA')+'|'+
			, STUFF((SELECT distinct 'ISNULL(RTRIM(CONVERT(NVARCHAR(100), src.'+t2.SrcBusinessKey+')),''NA'')+''|''+' 
					 from [mtdta].[DataVaultLinkTransform] t2
					 where t1.LinkName = t2.LinkName AND t1.LinkSchema = t2.LinkSchema
						FOR XML PATH(''), TYPE
						).value('.', 'NVARCHAR(MAX)') 
					,1,0,''
					)  LinkHashKeyCalc
			 -- filtering on only non null business keys - at least 2 business keys have to be not null (important for links linking more than 2 hubs)
			, STUFF((SELECT distinct '+ case when src.'+t2.SrcBusinessKey+' IS NULL then 0 else 1 end ' 
					 from [mtdta].[DataVaultLinkTransform] t2
					 where t1.LinkName = t2.LinkName AND t1.LinkSchema = t2.LinkSchema
						FOR XML PATH(''), TYPE
						).value('.', 'NVARCHAR(MAX)') 
					,1,0,''
					)  SrcBusKeyNotNull
			from [mtdta].[DataVaultLinkTransform] t1
		   WHERE t1.LinkSchema = case @linkschema when 'ALL' then t1.LinkSchema else @linkschema end
		     AND t1.LinkName = case @linkname when 'ALL' then t1.LinkName else @linkname end
	) a
)
select 
 aa.LinkSchema, aa.LinkName
,'INSERT INTO ' + LinkSchema + '.' + LinkName + ' ('+ LinkHashKey + ', LoadDateTime, RecordSource, LoadID '+ LinkHubHashKeyNames + ')
SELECT 
      stg.' + LinkHashKey + '
	, stg.LoadDateTime
    , ''' + aa.RecordSource + ''' AS RecordSource
    , -1 AS LoadID' + char(10)
	+ StgLinkHubHashKeyNames + 
' FROM (SELECT DISTINCT
              ' + LinkHashKeysCalc + '
	        , getdate() AS LoadDateTime 
	        ' + LinkHubHashKeysCalc + '
         FROM ' + SrcDatabase + '.' + SrcSchema + '.'+ SrcObject + ' src
        WHERE (' + SrcBusKeyNotNull + ') >= 2
       ) stg
 LEFT OUTER JOIN ' + LinkSchema + '.' + LinkName + ' tgt
   ON stg.'+ LinkHashKey + ' = tgt.'+ LinkHashKey + ' 
WHERE tgt.'+ LinkHashKey + ' IS NULL'  as DML
from 
(
   select distinct t1.SrcSchema, t1.SrcObject, t1.SrcDatabase, t1.LinkSchema, t1.LinkName, t1.RecordSource,  t1.LinkHashKey, t1.SrcBusKeyNotNull, t1.ConcatLinkHashKeyCalc as LinkHashKeysCalc
			-- concatenated list of hub hahs key calculations, e.g. HASHBYTES('MD5', ISNULL(RTRIM(CONVERT(NVARCHAR(100), src.CC_BROKER)),'NA')+'|') AS BrokerHashKey, HASHBYTES('MD5', ISNULL(RTRIM(CONVERT(NVARCHAR(100), src.CC_ID)),'NA')+'|') AS ContractHashKey
			, STUFF((SELECT distinct  t2.ConcatLinkHubHashKeyCalc 
					 from with_meta t2
					 where t1.LinkName = t2.LinkName AND t1.LinkSchema = t2.LinkSchema
						FOR XML PATH(''), TYPE
						).value('.', 'NVARCHAR(MAX)') 
					,1,0,''
					)  LinkHubHashKeysCalc
   			-- Concatenated list of hub hash key names e.g. , BrokerHashKey, ContractHashKey
			, STUFF((SELECT distinct  ', ' + t2.LinkHubHashKeyName 
					 from with_meta t2
					 where t1.LinkName = t2.LinkName AND t1.LinkSchema = t2.LinkSchema
						FOR XML PATH(''), TYPE
						).value('.', 'NVARCHAR(MAX)') 
					,1,0,''
					)  LinkHubHashKeyNames
   			-- Concatenated list of hub hash key names including stg alias e.g. , stg.BrokerHashKey     , stg.ContractHashKey 
			, STUFF((SELECT distinct  '    , stg.' + t2.LinkHubHashKeyName + char(10)
					 from with_meta t2
					 where t1.LinkName = t2.LinkName AND t1.LinkSchema = t2.LinkSchema
						FOR XML PATH(''), TYPE
						).value('.', 'NVARCHAR(MAX)') 
					,1,0,''
					)  StgLinkHubHashKeyNames
			from with_meta t1
) aa






OPEN cur_raw_load
FETCH NEXT FROM cur_raw_load INTO @clinkname, @clinkschema, @sqlstatement
WHILE @@FETCH_STATUS = 0
BEGIN
    print @sqlstatement
	
    exec sp_executesql  @Query  = @sqlstatement

     
	FETCH NEXT FROM cur_raw_load INTO @clinkname, @clinkschema, @sqlstatement
END
CLOSE cur_raw_load
DEALLOCATE cur_raw_load


GO




CREATE proc [mtdta].[spCreateDVRawSat]  @pSrcSchema varchar(25) = NULL, @pSrcName varchar(255) = NULL as
/* **********************************************************************
Author:  Deloitte
Creation Date: 27-02-2020
Desc: Procedure generates DDL scripts to create raw data vault sats. 
      The procedure is driven from the following metadata 
	  table [mtdta].[DataVaultSatTransform]
*************************************************************************
Sample exection:
----------------
exec [mtdta].[spCreateDVRawSat] @pSrcSchema = 'STG_CMS', @pSrcName = 'CMS_BROKER'

Sample code created by the above execution
----------------
IF EXISTS (SELECT * FROM sysobjects WHERE name='SatContractBroker' and xtype='U')
DROP TABLE [RV_CMS].[SatContractBroker]
CREATE TABLE [RV_CMS].[SatContractBroker] (
  [ContractBrokerHashKey] [varbinary](16) NOT NULL
, [LoadDateTime] [datetime] NOT NULL
, [RecordSource] [varchar](255) NOT NULL
, [LoadID] [bigint] NOT NULL
, [BrokerHashKey] [varbinary](16) NOT NULL
, [ContractHashKey] [varbinary](16) NOT NULL
)
CONSTRAINT [SatContractBroker_PK] PRIMARY KEY CLUSTERED 
(
	[ContractBrokerHashKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
*/

DECLARE @sqlstatement     nvarchar(MAX)
      , @satname          varchar(100)
      , @satschema        varchar(100)
      , @csatname         varchar(100)
      , @csatschema       varchar(100)
	  ;


set @satschema = @pSrcSchema;
set @satname = @pSrcName;


if @pSrcSchema is null or @pSrcSchema = 'NULL' 
begin
	select @satschema = 'ALL'
end;

if @pSrcName is null or @pSrcName = 'NULL' 
begin
	select @satname = 'ALL'
end;


DECLARE  cur_raw_ddl CURSOR FOR 
with with_meta as
(
	select SrcSchema, SrcObject, SrcDatabase, SrcColumn, SatSchema, SatName, RecordSource
	     , (select max(x.SatColumn) from [mtdta].[DataVaultSatTransform] x where x.ColumnPosition = 1 and x.SatName = a.SatName) as SatHashKey         -- name of sat hash key, e.g. ContractBrokerHashKey
	     , (select max('Hub'+substring(x.SatColumn,0,len(x.SatColumn)-6)) from [mtdta].[DataVaultSatTransform] x where x.ColumnPosition = 1 and x.SatName = a.SatName) as HubName         
		 , SatColumn, ColumnPosition 
      from [mtdta].[DataVaultSatTransform] a
     WHERE 1=1
	  AND IsColumnBusinessKey = 0
	  --AND SatName = 'SatStaffFast'
	  AND SrcSchema = case @satschema when 'ALL' then SrcSchema else @satschema end
      AND SrcObject = case @satname when 'ALL' then SrcObject else @satname end
)
select 
 aa.SatSchema, aa.SatName
,'IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES where table_name = ''' + SatName + ''' and table_schema =  ''' + SatSchema + ''' )
DROP TABLE [' + SatSchema + '].[' + SatName + ']
CREATE TABLE [' + SatSchema + '].[' + SatName + '] (
  ['+ SatHashKey + '] [varbinary](16) NOT NULL
, [LoadDateTime] [datetime] NOT NULL
, [RecordSource] [varchar](255) NOT NULL
, [HashDiff] [varbinary](16) NOT NULL
, [AppliedDateTime] [datetime] NOT NULL
, [LoadID] [bigint] NOT NULL
'+ SatColumns +
'CONSTRAINT [' + SatName + '_PK] PRIMARY KEY CLUSTERED 
(
	['+ SatHashKey + '] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]' + char(10) + char(10) +
 'ALTER TABLE [' + SatSchema + '].[' + SatName + ']' + 
+ ' ADD CONSTRAINT ' + SatName + '_' + HubName + '_FK FOREIGN KEY(' + SatHashKey + ') REFERENCES [' 
+ SatSchema + '].[' + HubName + '](' + SatHashKey + ')' + char(10) 
+ 'ALTER TABLE [' + SatSchema + '].[' + SatName + '] NOCHECK CONSTRAINT ' + SatName + '_' + HubName + '_FK' + char(10)

+ char(10) + char(10)  as DDL
from 
(
   select distinct t1.SrcSchema, t1.SrcObject, t1.SrcDatabase, t1.SatSchema, t1.SatName, t1.RecordSource,  t1.SatHashKey, t1.HubName,
          STUFF((SELECT distinct  ', [' + SatColumn + '] ' +
						CASE WHEN isc.DATA_TYPE = 'decimal'
						 THEN '['+ isc.DATA_TYPE+']('
								+ cast(isc.NUMERIC_PRECISION as nvarchar)
								+','
								+ cast(isc.NUMERIC_SCALE  as nvarchar)
								+')'
						WHEN isc.DATA_TYPE = 'varchar' or isc.DATA_TYPE = 'char' 
						 THEN '['+ isc.DATA_TYPE+']('
								+ cast(isc.CHARACTER_MAXIMUM_LENGTH as nvarchar)
								+')'
						WHEN isc.DATA_TYPE = 'date' or isc.DATA_TYPE = 'datetime'
						 THEN '['+ isc.DATA_TYPE+']'
						ELSE '[nvarchar](MAX)'
						END +
						CASE WHEN isc.IS_NULLABLE = 'YES' THEN '' ELSE ' NOT NULL' END + char(10)
					 from with_meta t2
		             left outer join INFORMATION_SCHEMA.COLUMNS isc
					   on t2.srcSchema = isc.TABLE_SCHEMA
					  and t2.srcObject = isc.TABLE_NAME
					  and t2.SrcColumn = isc.COLUMN_NAME
					 where t1.SatName = t2.SatName AND t1.SatSchema = t2.SatSchema
						FOR XML PATH(''), TYPE
						).value('.', 'NVARCHAR(MAX)') 
					,1,0,''
					)  SatColumns
			from with_meta t1
) aa

OPEN cur_raw_ddl
FETCH NEXT FROM cur_raw_ddl INTO @csatname, @csatschema, @sqlstatement
WHILE @@FETCH_STATUS = 0
BEGIN
    print @sqlstatement
	
    exec sp_executesql  @Query  = @sqlstatement
    
	FETCH NEXT FROM cur_raw_ddl INTO @csatname, @csatschema, @sqlstatement
END
CLOSE cur_raw_ddl
DEALLOCATE cur_raw_ddl
GO



