--- exported data is to be imported into [mtdta].[SrcFKConstraint] metadata table


    select
         c.table_name as parenttable,
         c.column_name PARENTCOL,
         b.table_name as childtable,
         b.column_name as CHILDCOL,
         b.position,         
         a.constraint_name
    from all_cons_columns b,
         all_cons_columns c,
         all_constraints a
   where b.constraint_name = a.constraint_name
     and a.owner           = b.owner
     and b.position        = c.position
     and c.constraint_name = a.r_constraint_name
     and c.owner           = a.r_owner
     and a.constraint_type = 'R'
     and ((c.owner      = 'MATAPP'
          and c.table_name in ('CMS_CONTRACT','CMS_CONTRACTBOOK','CMS_CONTRACTSUBBOOK','CMS_OPTION_TYPE','CMS_CONTRACT_SHAPE','CMS_STATUS',
                                'CMS_PRICE_TYPE','CMS_COUNTERPARTY','CMS_COUNTERPARTY_STAFF','CMS_HOLIDAY_CALENDAR','CMS_HOLIDAY_SCHEDULE',
                                'CMS_STAFF','CMS_BROKER','REALLOCATION','BUYSELL','CMS_REFERENCE_NODE','CMS_CLEARINGHOUSE','CMS_CLEARING_ACCOUNT',
                                'CMS_CARBONCLAUSE','CMS_ISDAPRODUCT')
         )
         or
         (c.owner = 'EOT' and c.table_name in
                            ('CMMS_TRADE','CMMS_TRADE_COMPONENT','CMMS_HOLIDAY_GROUP','CMMS_HOLIDAY_GROUP_LINK_TYPE','CMMS_HOLIDAY_TYPE','CMMS_HOLIDAYS','CMMS_TRADE_STATUS',
                            'CMMS_TRADE_TYPE','CMMS_TRADE_SETTLEMENT_TYPE','CMMS_TRADE_DEMAND_SOURCE','CMMS_BUSINESS_CONTACT','CMMS_CONTACT','CMMS_BUSINESS_CONTACT_TYPE',
                            'CMMS_BUSINESS_CONTACT_CATEGORY','CMMS_CREDIT_RATING','CMMS_TRADE_COMPONENT_LINK_COND','CMMS_TRADE_SPECIAL_CONDITION','CMMS_TRADE_OPTION_TYPE',
                            'CMMS_TRADE_PORTFOLIO','CMMS_TIME_SPLIT','CMMS_TRADE_COMPONENT_RATES','CMMS_TRADE_TRANSACTION_TYPE','CMMS_TRADE_SETTLEMENT_CYCLE',
                            'CMMS_TRADE_HEDGE_STRATEGY','CMMS_STRATEGY','CMMS_EMPLOYEE','AUDT_TRADE_COMPONENT_RATES','AUDT_TRADE_COMPONENT'                            )
        )
        )
order by 1,2,3,4
