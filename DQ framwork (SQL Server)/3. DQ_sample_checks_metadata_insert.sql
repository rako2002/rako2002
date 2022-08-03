

insert into mtdta.DQ_check values (
'DQ01_ODS_TEAM_VARIANCE', 'Check if any teams with larger than 10% variance from yesterday',
null, 'ODS', 0, 10,
'select max(variance) from
(
	SELECT today.tech_team
	,yesterday.daily_total as yesterday_total
	, today.daily_total as today_total
	, ((yesterday.daily_total - today.daily_total)*100/yesterday.daily_total) as variance

	FROM
	(
	/* get daily counts of yesterday */
	SELECT count(order_number) as daily_total, report_date, report_technology+team_name as tech_team
	FROM stable.ODS_Orders
	WHERE CONVERT(char(10), report_date,126) = CONVERT(char(10), GetDate()-1,126)
	GROUP BY report_date, report_technology, team_name
	) as yesterday

	INNER JOIN

	(
	/*get daily count for today*/
	SELECT count(order_number) as daily_total, report_date, report_technology+team_name as tech_team
	FROM stable.ODS_Orders
	WHERE CONVERT(char(10), report_date,126) = CONVERT(char(10), GetDate(),126)
	GROUP BY report_date, report_technology, team_name
	) as today 

	ON yesterday.tech_team = today.tech_team
) a');



insert into mtdta.DQ_check values (
'DQ02_ODS_NULL_STATE', 'Missing State in ODS',
null, 'ODS', 0, 0,
'SELECT count([state])
FROM stable.ODS_Orders
WHERE report_date = cast(GetDate() as date)
AND [state] IS NULL');


insert into mtdta.DQ_check values (
'DQ03_ODS_NULL_DP', 'Missing deliver_partner in ODS',
null, 'ODS', 0, 0,
'SELECT count(delivery_partner)
FROM stable.ODS_Orders
WHERE report_date = cast(GetDate() as date)
AND delivery_partner IS NULL');


insert into mtdta.DQ_check values (
'DQ04_ODS_NULL_DISTRESSED', 'Missing distressed in ODS',
null, 'ODS', 0, 0,
'SELECT count(distressed)
FROM stable.ODS_Orders
WHERE report_date = cast(GetDate() as date)
AND distressed IS NULL');

insert into mtdta.DQ_check values (
'DQ05_ODS_NULL_SC', 'Missing Service Class in ODS',
null, 'ODS', 0, 0,
'SELECT count(service_class)
FROM stable.ODS_Orders
WHERE report_date = cast(GetDate() as date)
AND service_class IS NULL');


insert into mtdta.DQ_check values (
'DQ07_RAW_UDS_ROW_CNT_VARIANCE', 'Number of rows in RAW_UDS_Orders is witihin 5% compared to average of last week available in WRK table',
null, 'RAW', 0, 5,
'select 100.0 * abs(min(cnt) - avg(b.prev_cnt)) / avg(b.prev_cnt) from 
(select count(*) cnt from rawdata.RAW_UDS_Orders group by [Time Stamp]) a
inner join
(select count(*) as prev_cnt from stable.WRK_UDS_Orders where report_date between dateadd(day, -7, getdate()) and getdate() group by report_Date) b
on 1 = 1');

insert into mtdta.DQ_check values (
'DQ08_RAW_HFC_ROW_CNT_VARIANCE', 'Number of rows in RAW_HFC_Orders is witihin 5% compared to average of last week available in WRK table',
null, 'RAW', 0, 5,
'select 100.0 * abs(min(cnt) - avg(b.prev_cnt)) / avg(b.prev_cnt) from 
(select count(*) cnt from rawdata.RAW_HFC_Orders group by report_Date) a
inner join
(select count(*) as prev_cnt from stable.WRK_HFC_Orders where report_date between dateadd(day, -7, getdate()) and getdate() group by report_Date) b
on 1 = 1');


insert into mtdta.DQ_check values (
'DQ09_RAW_UDS_DUPLICATES', 'Number of duplicate rows in RAW_UDS_Orders is 0',
null, 'RAW', 0, 0,
'select count(*) from
(select count(*) cnt_raw from rawdata.raw_uds_orders group by [Time Stamp], [SP Order ID] having count(*) > 1) a
');

insert into mtdta.DQ_check values (
'DQ10_RAW_HFC_DUPLICATES', 'Number of duplicate rows in RAW_HFC_Orders is 0',
null, 'RAW', 0, 0,
'select count(*) from
(select count(*) cnt_raw from rawdata.raw_hfc_orders group by [REPORT_DATE], [ORDER_NUMBER] having count(*) > 1) a');

insert into mtdta.DQ_check values (
'DQ11_WRK_UDS_ROW_CNT', 'Number of rows in WRK_UDS_Orders is matching RAW table for a given day',
null, 'WRK', 0, 0,
'select abs(cnt_raw - cnt_wrk)
from
(select count(*) cnt_raw from rawdata.raw_uds_orders) a
inner join
(select count(*) cnt_wrk from stable.wrk_uds_orders where reporT_date = cast(getdate() as date)) b
on 1 = 1');

insert into mtdta.DQ_check values (
'DQ12_WRK_HFC_ROW_CNT', 'Number of connect orders in WRK_HFC_Orders is matching RAW table for a given day',
null, 'WRK', 0, 0,
'select abs(cnt_raw - cnt_wrk) from
(select count(*) cnt_raw from rawdata.raw_hfc_orders where order_type = ''Connect'') a
inner join
(select count(*) cnt_wrk from stable.wrk_hfc_orders where reporT_date = cast(getdate() as date)) b
on 1 = 1');







select abs(cnt_raw - cnt_wrk)
from
(select count(*) cnt_raw from rawdata.raw_uds_orders) a
inner join
(select count(*) cnt_wrk from stable.wrk_uds_orders where reporT_date = cast(getdate() as date)) b
on 1 = 1


select abs(cnt_raw - cnt_wrk)
from
(select count(*) cnt_raw from rawdata.raw_hfc_orders where order_type = 'Connect') a
inner join
(select count(*) cnt_wrk from stable.wrk_hfc_orders where reporT_date = cast(getdate() as date)) b
on 1 = 1




select 100.0 * abs(min(cnt) - avg(b.prev_cnt)) / avg(b.prev_cnt) from 
(select count(*) cnt from rawdata.RAW_UDS_Orders group by [Time Stamp]) a
inner join
(select count(*) as prev_cnt from stable.WRK_UDS_Orders where report_date between dateadd(day, -7, getdate()) and getdate() group by report_Date) b
on 1 = 1


select * from rawdata.raw_HFC_Orders

select min(cnt) from 
(select count(*) cnt from rawdata.RAW_UDS_Orders group by [Time Stamp]) a

select avg(cnt)
from
(
select report_date, count(*) cnt from stable.WRK_UDS_Orders
group by report_date
)

