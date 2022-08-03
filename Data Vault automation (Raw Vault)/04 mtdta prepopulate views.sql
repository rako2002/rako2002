create view mtdta.PrepopulateDataVaultHubTransform as
select i.TABLE_CATALOG as SrcDatabase, i.table_schema as SrcSchema, i.TABLE_NAME as SrcObject
     , isnull(p.SrcColumn, 'UNDEFINED') as SrcBusinessKey
     , 'RV_' + (substring(i.table_schema, 5, 100)) as HubSchema
	 , 'Hub' + replace(dbo.initcap(replace(replace(i.TABLE_NAME, 'CMS_', ''),'CMMS_', '')),'_','') as HubName
	 , isnull(p.SrcColumn, 'UNDEFINED') as HubBusinessKey
     , isnull(p.ColumnPosition, -1) as ColumnPosition
	 , (substring(i.table_schema, 5, 100)) as RecordSource
  from INFORMATION_SCHEMA.TABLES i
 		-- do not include staging tables that will become reference data tables
  left outer join [mtdta].[DataVaultRefTransform] r
	on r.SrcObject = i.table_name
   and r.srcSchema = i.TABLE_SCHEMA
  left outer join [mtdta].[SrcPKConstraint] p
    on i.TABLE_NAME  = p.srctable
 where 1=1
   and TABLE_SCHEMA like 'STG%'
   and r.srcObject is null
GO


create view mtdta.PrepopulateDataVaultSatTransform as
select i.TABLE_CATALOG as SrcDatabase, i.table_schema as SrcSchema, i.TABLE_NAME as SrcObject
     , i.COLUMN_NAME as SrcColumn
	 , 'RV_' + (substring(i.table_schema, 5, 100)) as SatSchema
	 , 'Sat' + replace(dbo.initcap(replace(replace(i.TABLE_NAME, 'CMS_', ''),'CMMS_', '')),'_','') as SatName
	 ,case when p.srctable is null then i.COLUMN_NAME 
	       else  replace(dbo.initcap(replace(replace(i.TABLE_NAME, 'CMS_', ''),'CMMS_', '')),'_','') + 'HashKey' 
	  end  as SatColumn --- Add Hash Key for business keys e.g. ContractHashKey
	 , ROW_NUMBER() over (partition by i.TABLE_CATALOG , i.table_schema , i.TABLE_NAME order by i.ORDINAL_POSITION)  as ColumnPosition
	 , case when p.srctable is null then 0 else 1 end as IsColumnBusinessKey
	 , case when p.srctable is null then 1 else 0 end as IsColumnPartOfHashDiffKey
	 , (substring(i.table_schema, 5, 100)) as RecordSource
	 --, f.ParentTable, f.ParentColumn
  from INFORMATION_SCHEMA.COLUMNS i
  -- do not include staging tables that will become reference data tables
  left outer join [mtdta].[DataVaultRefTransform] r  		
	on r.SrcObject = i.table_name
   and r.srcSchema = i.TABLE_SCHEMA
  -- get list of primary keys to identify business keys
  left outer join [mtdta].[SrcPKConstraint] p 
    on i.TABLE_NAME  = p.srctable
   and i.COLUMN_NAME = p.SrcColumn
  -- get list of foreign keys as these would represent business key in other hubs, and these shoudl not be included in satellites as relationships 
  -- will be represented via links
  left outer join [mtdta].[SrcFKConstraint] f
    on i.TABLE_NAME  = f.ChildTable
   and i.COLUMN_NAME =  f.ChildColumn
   -- if a foreign key is related to a reference table, then still include it in the satellite (unlike hub business key)
  left outer join [mtdta].[DataVaultRefTransform] f_r
    on f.ParentTable = f_r.SrcObject
 where 1=1
   and TABLE_SCHEMA like 'STG%'
   --and i.TABLE_NAME = 'CMS_CONTRACT'
   and r.srcObject is null  
   and (f.ParentTable is null or f_r.SrcObject is not null)
GO



create view mtdta.PrepopulateDataVaultLinkTransform as
with parent_keys as (
select i.TABLE_CATALOG as srcDatabase, i.table_schema as srcSchema
     , s.ChildTable as SrcObject, s.ChildColumn as SrcBusinessKey
	 , 'RV_' + (substring(i.table_schema, 5, 100)) as LinkSchema
	 , 'Link' 
	    + replace(dbo.initcap(replace(replace(s.ChildTable, 'CMS_', ''),'CMMS_', '')),'_','') 
	    + replace(dbo.initcap(replace(replace(s.ParentTable, 'CMS_', ''),'CMMS_', '')),'_','') 
	    as LinkName
	 , case when dense_rank() over (partition by i.TABLE_CATALOG, i.table_schema, s.ChildTable, s.ParentTable order by s.FKConstraintName)  = 1 
	     -- when there is only one FK to the child table to a given parent table then use Parent Table name to generate hash key name in link
		 then replace(dbo.initcap(replace(replace(s.ParentTable, 'CMS_', ''),'CMMS_', '')),'_','') + 'HashKey' 
		 else replace(dbo.initcap(replace(replace(replace(s.ChildColumn, 'CMS_', ''),'CMMS_', ''),'ID','')),'_','') + 'HashKey' 
       end as LinkHubHashKeyName
	 , 100 + s.ColumnPosition as LinkHubHashKeyColumnPosition
     , 'Hub' + replace(dbo.initcap(replace(replace(s.ParentTable, 'CMS_', ''),'CMMS_', '')),'_','') as HubName
	 , (substring(i.table_schema, 5, 100)) as RecordSource
	 , s.ChildTable
     --,  'OK' end as cntFK
  from [mtdta].[SrcFKConstraint] s
  left outer join [mtdta].[DataVaultRefTransform] r  		
	on r.SrcObject = s.ParentTable
  left outer join INFORMATION_SCHEMA.TABLES i
    on s.ParentTable = i.TABLE_NAME
   and i.TABLE_SCHEMA like 'STG%'  
where r.SrcObject is null  -- exclude foreign key to reference tables from link generation
   --and s.ChildTable = 'CMMS_TRADE_COMPONENT'
),
child_keys as (
		select distinct c.srcDatabase, c.srcSchema, c.SrcObject, h.SrcBusinessKey
			 , c.LinkSchema, c.LinkName
			 , h.LinkHubHashKeyName
			 , h.ColumnPosition as LinkHubHashKeyColumnPosition
			 , h.HubName
			 , c.RecordSource
		 from (select distinct srcDatabase, srcSchema, SrcObject, ChildTable, LinkSchema, LinkName, RecordSource, LinkHubHashKeyName -- distinct is required in case more than one column business key in the parent table
		              from parent_keys
			  ) c
		 left outer join 
		      (select SrcObject, HubName, SrcBusinessKey, ColumnPosition, replace(dbo.initcap(replace(replace(SrcObject, 'CMS_', ''),'CMMS_', '')),'_','') + 'HashKey' as LinkHubHashKeyName 
			     from mtdta.DataVaultHubTransform
			  ) h
		   on c.ChildTable = h.SrcObject
)
select srcDatabase, srcSchema, SrcObject, SrcBusinessKey
     , LinkSchema, LinkName, LinkHubHashKeyName
	 , row_number() over (partition by LinkName order by  LinkHubHashKeyColumnPosition) as LinkHubHashKeyColumnPosition
	 , HubName, RecordSource
from 
		(
		-- business keys defintions for parent table
		select p.srcDatabase, p.srcSchema, p.SrcObject, p.SrcBusinessKey, p.LinkSchema, p.LinkName
			 -- for self-links (e.g. contract to parent contract) - prefix name of the parent hash key with parent to avoid conflicts on column names
			 , case when c.LinkHubHashKeyName is null then p.LinkHubHashKeyName else 'Parent' + p.LinkHubHashKeyName end as LinkHubHashKeyName
			 , p.LinkHubHashKeyColumnPosition, p.HubName, p.RecordSource
		  from parent_keys p
		  left outer join child_keys c
		   on p.LinkSchema = c.LinkSchema
          and p.LinkName = c.LinkName
		  and p.LinkHubHashKeyName = c.LinkHubHashKeyName
		  
		union all

		select srcDatabase, srcSchema, SrcObject, SrcBusinessKey, LinkSchema, LinkName
		     , LinkHubHashKeyName, LinkHubHashKeyColumnPosition, HubName, RecordSource
		  from child_keys
		-- business keys defintions for child table (join back to hub metadata table - DataVaultHubTransform - to identify these keys)
		) a
where 
1=1
--and LinkName = 'LinkTradeComponentBusinessContact'
--order by 1, 2, 6, 8

GO

