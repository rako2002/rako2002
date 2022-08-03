

/****** Object:  StoredProcedure [mtdta].[exec_DQ_checks]    Script Date: 27/11/2017 3:53:50 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



alter proc [mtdta].[exec_DQ_checks] as


DECLARE @sqlstatement     nvarchar(4000)
      , @dq_check_id      varchar(255)
      , @target_min_value numeric(38, 2)
      , @target_max_value numeric(38, 2)
      , @sqlstatement_ret nvarchar(4000) 
      , @vi               numeric(38, 2);
--

-- move objects that were created in dbo schema more than 2 weeks ago to x_bin schema (or in case of RAW_UDS_Order% and RAW_HFC_Orders% table - move them after 3 days)
DECLARE  cur_dq_checks CURSOR FOR 
SELECT dq_check_id, target_min_value, target_max_value, sql_query
  from mtdta.DQ_check;


-- droo objects that were moved to x_bin schema more than 2 weeks ago
OPEN cur_dq_checks
FETCH NEXT FROM cur_dq_checks
INTO @dq_check_id, @target_min_value, @target_max_value, @sqlstatement

WHILE @@FETCH_STATUS = 0
BEGIN
    --print @sqlstatement
	select @sqlstatement_ret =   STUFF(@sqlstatement, CHARINDEX('select',@sqlstatement), LEN('select'), 'select @vi=')
    
    exec sp_executesql  @Query  = @sqlstatement_ret, @Params = N'@vi decimal(38,2) OUTPUT', @vi = @vi OUTPUT
    --print @sqlstatement_ret
    --print @vi

    INSERT INTO DQ_check_exec_log (dq_check_id,  log_date, actual_value, dq_check_passed_flag)
    values (@dq_check_id, getdate(), @vi, case when @vi between isnull(@target_min_value, @vi) and isnull(@target_max_value, @vi) then 1 else 0 end);
     
	FETCH NEXT FROM cur_dq_checks INTO @dq_check_id, @target_min_value, @target_max_value, @sqlstatement
END
CLOSE cur_dq_checks
DEALLOCATE cur_dq_checks


GO

