--- exported data is to be imported into [mtdta].[SrcPKConstraint] metadata table

select 
   --all_cons_columns.owner as schema_name,
   all_cons_columns.table_name as "srctable", 
   all_cons_columns.column_name as "srccolumn", 
   all_cons_columns.position as "ColumnPosition",
   all_cons_columns.constraint_name as "PKConstraintName"
from all_constraints , all_cons_columns 
where 
   all_constraints.constraint_type = 'P'
   and all_constraints.constraint_name = all_cons_columns.constraint_name
   and all_constraints.owner = all_cons_columns.owner
   
        and ((all_cons_columns.owner      = 'MATAPPx'
          and all_cons_columns.table_name in ('CMS_CONTRACT','CMS_CONTRACTBOOK','CMS_CONTRACTSUBBOOK','CMS_OPTION_TYPE','CMS_CONTRACT_SHAPE','CMS_STATUS',
                                'CMS_PRICE_TYPE','CMS_COUNTERPARTY','CMS_COUNTERPARTY_STAFF','CMS_HOLIDAY_CALENDAR','CMS_HOLIDAY_SCHEDULE',
                                'CMS_STAFF','CMS_BROKER','REALLOCATION','BUYSELL','CMS_REFERENCE_NODE','CMS_CLEARINGHOUSE','CMS_CLEARING_ACCOUNT',
                                'CMS_CARBONCLAUSE','CMS_ISDAPRODUCT')
         )
         or
         (all_cons_columns.owner = 'EOT' and all_cons_columns.table_name in
                            ('CMMS_TRADE','CMMS_TRADE_COMPONENT','CMMS_HOLIDAY_GROUP','CMMS_HOLIDAY_GROUP_LINK_TYPE','CMMS_HOLIDAY_TYPE','CMMS_HOLIDAYS','CMMS_TRADE_STATUS',
                            'CMMS_TRADE_TYPE','CMMS_TRADE_SETTLEMENT_TYPE','CMMS_TRADE_DEMAND_SOURCE','CMMS_BUSINESS_CONTACT','CMMS_CONTACT','CMMS_BUSINESS_CONTACT_TYPE',
                            'CMMS_BUSINESS_CONTACT_CATEGORY','CMMS_CREDIT_RATING','CMMS_TRADE_COMPONENT_LINK_COND','CMMS_TRADE_SPECIAL_CONDITION','CMMS_TRADE_OPTION_TYPE',
                            'CMMS_TRADE_PORTFOLIO','CMMS_TIME_SPLIT','CMMS_TRADE_COMPONENT_RATES','CMMS_TRADE_TRANSACTION_TYPE','CMMS_TRADE_SETTLEMENT_CYCLE',
                            'CMMS_TRADE_HEDGE_STRATEGY','CMMS_STRATEGY','CMMS_EMPLOYEE','AUDT_TRADE_COMPONENT_RATES','AUDT_TRADE_COMPONENT'                            )
        )
        )
   
order by 
   all_cons_columns.owner,
   all_cons_columns.table_name, 
   all_cons_columns.position