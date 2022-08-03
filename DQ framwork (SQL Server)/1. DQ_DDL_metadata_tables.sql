
/****** Object:  Table [mtdta].[DQ_check_exec_log]    Script Date: 27/11/2017 3:52:23 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP TABLE [mtdta].[DQ_check_exec_log]

CREATE TABLE [mtdta].[DQ_check_exec_log](
	[dq_check_id]  [varchar](255)   NOT NULL,
    [log_date]     [datetime] NOT NULL,
	[actual_value] [numeric](38, 2) NULL,
    [dq_check_passed_flag]  bit NULL,    
) ON [PRIMARY]

GO


/****** Object:  Table [mtdta].[DQ_check]    Script Date: 27/11/2017 3:52:13 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP TABLE [mtdta].[DQ_check];


CREATE TABLE [mtdta].[DQ_check](
	[dq_check_id]     [varchar](255)   NOT NULL,
	[dq_check_name]   [varchar](500)   NULL,
	[dq_check_desc]   [varchar](4000)  NULL,
    [dq_check_group]  [varchar](255)   NULL,
    [target_min_value][numeric](38, 2) NULL,
    [target_max_value][numeric](38, 2) NULL, 
	[sql_query]       [varchar](4000)  NOT NULL
) ON [PRIMARY]

GO


CREATE VIEW mtdta.VW_DQ_EXEC_LOG AS
SELECT d.dq_check_id, d.dq_check_name, l.log_date, case when l.dq_check_passed_flag = 1 then 'SUCCESSFUL' else 'FAILED' end as status, l.actual_value, d.target_min_value, d.target_max_value, d.dq_check_group, d.sql_query
  FROM [mtdta].[DQ_check_exec_log] l
  LEFT OUTER JOIN [mtdta].[DQ_check] d
    ON l.dq_check_id = d.dq_check_id
    
CREATE VIEW mtdta.VW_DQ_EXEC_LOG_FAILED_TODAY AS    
 select * from mtdta.VW_DQ_EXEC_LOG where cast(log_date as date) = cast(getdate() as date) and status <> 'SUCCESSFUL'
