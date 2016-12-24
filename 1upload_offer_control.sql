/*
####################################################################################
# Name: upload_offer_control
# Description: Create an Impala table of the Offer_Control_File
# Input: csv table in the folder /user/rata.suwantong/impala_rata2#/user/rata.suwantong/impala_rata2/Offer_Control_File
# Version:
#   2016/12/09 RS: Initial version
#   
####################################################################################
*/

 
 drop table if exists sgmt_rules.offer_control_pre;
 CREATE EXTERNAL TABLE sgmt_rules.offer_control_pre
 (
	offer STRING,
	UC1_priority INT, 
	active_flg_kd INT,
	active_flg_fb INT,
	active_flg_pt INT,
	screen_size DOUBLE,
	RRP INT,
	apollo_price INT,
	min_pack INT,
	start_year STRING,
	start_month STRING,
	start_day STRING,
	end_year STRING,
	end_month STRING,
	end_day STRING,
	target_RLP_min INT,
	target_RLP_max INT,
	min_CTR STRING,
	min_landings STRING,
	min_submits STRING,
	min_S2L STRING,
	max_CPS STRING
 )   
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ESCAPED BY ','
 LOCATION '/user/rata.suwantong/impala_rata2/Offer_Control_File';
 
drop table if exists sgmt_rules.offer_control;
create table sgmt_rules.offer_control row format delimited fields terminated by '\t' as ( select  
	offer,
	UC1_priority, 
	active_flg_kd,
	active_flg_fb,
	active_flg_pt,
	screen_size,
	RRP,
	apollo_price,
	min_pack, 
	start_year,
	concat_ws('-',start_year,start_month,start_day) as start_date, 
	concat_ws('-',end_year,end_month,end_day) as end_date, 
	target_RLP_min,
	target_RLP_max,
	min_CTR,
	min_landings,
	min_submits,
	min_S2L,
	max_CPS
from (select 
	offer,
	UC1_priority,
	active_flg_kd,
	active_flg_fb,
	active_flg_pt,
	screen_size,
	RRP,
	apollo_price,
	min_pack, 
	start_year,
	case when char_length(start_month)=1 then concat('0',start_month) else start_month end as start_month,
	case when char_length(start_day)=1 then concat('0',start_day) else start_day end as start_day,
	end_year,
	case when char_length(end_month)=1 then concat('0',end_month) else end_month end as end_month,
	case when char_length(end_day)=1 then concat('0',end_day) else end_day end as end_day,
	case when target_RLP_min is null then -99999 else target_RLP_min end as target_RLP_min,
	case when target_RLP_max is null then 99999 else target_RLP_max end as target_RLP_max,
	min_CTR,
	min_landings,
	min_submits,
	min_S2L,
	max_CPS
from sgmt_rules.offer_control_pre where offer!='Offer') A);

/*
select * from sgmt_rules.offer_control;
*/