/*
####################################################################################
# Name: meas_table_dvc_prop
# Description: Create measurement table with device properties
# Input: meas_ana.meas_table, meas_ana.techname_bytpid_meas, apollo_util.techname_prop_map
# Version:
#   2016/12/01 RS: Initial version 
####################################################################################
*/


/*
drop table if exists meas_ana.meas_agreg_byday;
create table meas_ana.meas_agreg_byday row format delimited fields terminated by '\t' as (
select sight_date, source, offer, ful_channel, sum(imps) as imps, count(distinct tapad_id) as reach, sum(clicks) as clicks, sum(landings) as landings, sum(selects) as selects, sum(submits) as submits from meas_ana.meas_table group by sight_date, source, offer, ful_channel);
*/


drop table if exists meas_ana.meas_table_dvc;
create table meas_ana.meas_table_dvc row format delimited fields terminated by '\t' as (
select a.sight_date, a.tapad_id, a.carrier, a.offer, a.source, a.room_id, a.room_name, a.ful_channel, a.imps, a.clicks, a.landings, a.selects, a.submits, b.hl_platform, b.dvc_techname from meas_ana.meas_table a left join meas_ana.techname_bytpid_meas b on a.tapad_id=b.tapad_id);

drop table if exists meas_ana.meas_table_dvc_prop;
create table meas_ana.meas_table_dvc_prop row format delimited fields terminated by '\t' as (
select a.sight_date, a.tapad_id, a.carrier, a.offer, a.source, a.room_id, a.room_name, a.ful_channel, a.imps, a.clicks, a.landings, a.selects, a.submits, a.hl_platform, a.dvc_techname,
		case when b.device_commercname is null then a.dvc_techname else b.device_commercname end as dvc_commercname, b.brand, 
		case 
			when a.hl_platform ='IPHONE' then '700' 
			when b.release_price is null and (a.dvc_techname like '%iris%' or a.dvc_techname like '%lava%' or a.dvc_techname like '%smart%' or a.dvc_techname like '%true%' or a.dvc_techname like '%dtac%' or a.dvc_techname like '%joey%' or a.dvc_techname like '%blade%' or a.dvc_techname like '%eagle%') then '80' 
			when b.release_price is null and (a.dvc_techname like '%i-mobile%' or a.dvc_techname like '%i-style%') then '130' 
			when b.release_price is null and a.dvc_techname like '%vivo%'  then '340' 
			when b.release_price is null and a.dvc_techname like '%asus%'  then '125' 
			when b.release_price is null and a.dvc_techname like '%htc%'  then '470' 
			when b.release_price is null and a.dvc_techname like '%lg%'  then '270' 
			when b.release_price is null and a.dvc_techname like '%huawei%'  then '300' 
			when b.release_price is null and (a.dvc_techname like '%sm-%' or a.dvc_techname like '%gt-%') then '370' 
			when b.release_price is null and (a.dvc_techname like '%x9009%' ) then '400' 
			when b.release_price is null and a.dvc_techname like '%wiko%'  then '130' 
			else b.release_price end as release_price, 
		b.release_year, b.release_month, b.screensize, b.cluster 
 from meas_ana.meas_table_dvc a left join apollo_util.techname_prop_map b on a.dvc_techname=b.device_techname_raw);
