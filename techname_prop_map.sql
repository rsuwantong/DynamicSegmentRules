/*
####################################################################################
# Name: map_techname_prop
# Description: Map device techname from Pt, Kd, Sn to device properties
# Input: default.id_syncs, mobile_atlas_wcluster table
# Version:
#   2016/11/10 RS: Initial version
#   
####################################################################################
*/

drop table if exists rata_sght.techname_list;
create table rata_sght.techname_list row format delimited fields terminated by '\t' as (
select lcase(trim(regexp_replace(regexp_replace(dvc_techname,'(by|cr|u2 |_?dual).*',''),'.*(-|_)?([a-z][a-z])-?[0-9]? ',''))) as dvc_techname, platform from 
(select case
	    when hl_platform = 'PC_OTHERS' then platform 
		when lcase(user_agent) like '%cpu iphone os%' and lcase(user_agent) like '%ipod%' and lcase(platform)='iphone' then 'ipod' 
		when lcase(user_agent) like '%cpu iphone os%' or lcase(user_agent) like '%iphone; u; cpu iphone%' or lcase(user_agent) like '%iphone; cpu os%' and lcase(platform)='iphone' then regexp_replace(regexp_replace(regexp_replace(lcase(user_agent),'.*iphone;( u;)? cpu ',''),'like mac os.*',''),'_.*','') 
		when lcase(user_agent) like '%(null) [fban%' and lcase(user_agent) like '%fbdv/iphone%' and lcase(platform)='iphone' then regexp_extract(regexp_replace(lcase(user_agent),'.*fbdv/',''),'iphone[0-9]',0) 
		when lcase(user_agent) like '%android; mobile; rv%' or lcase(user_agent) like '%mobile rv[0-9][0-9].[0-9] gecko%' then 'unidentified android' 
		when lcase(user_agent) like '%android; tablet; rv%' or lcase(user_agent) like '%tablet rv[0-9][0-9].[0-9] gecko%' then 'unidentified tablet' 
		when lcase(user_agent) like 'iris405' then 'iris405'  when lcase(user_agent) like 'iris700' then 'iris700'   
		else  regexp_replace(regexp_replace(regexp_replace(trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(lcase(user_agent),'.*android [0-9](.[0-9](.[0-9])?)?; ',''),' build.*|; android/.*|\\) applewebkit.*|/v[0-9] linux.*|v_td.*|_td/v[0-9].*|i_style.*',''),'.*([a-z][a-z])(-|_)([a-z][a-z]);? |.*nokia; ',''),'/.*|linux.*','')),'[^0-9a-z\- \_\.]',''),'.*samsung(-| )|.*lenovo |.*microsoft |.*th- ',''),'like.*|lollipop.*','') end as dvc_techname, platform 
	from (
select case when lower(a.header.platform)='iphone' and (lower(a.header.user_agent) like ('%windows phone%') or lower(a.header.user_agent) like ('%lumia%')) then 'WINDOWS_PHONE' else a.header.platform end as platform, case when a.header.platform in ('ANDROID', 'ANDROID_TABLET', 'WINDOWS_PHONE', 'WINDOWS_TABLET', 'BLACKBERRY', 'FEATURE_PHONE') then 'ANDROID' when a.header.platform='IPHONE' then 'IPHONE' else 'PC_OTHERS' end as hl_platform, a.header.user_agent as user_agent from default.id_syncs a, a.header.incoming_ids b, b.sightings_by_id_type c where  partner_id in (2177,2243,2248) and YEAR=2016 and MONTH>=10 and c.key='TAPAD_COOKIE' group by platform, hl_platform, user_agent) A group by dvc_techname, platform) C);



/*Join the dvc_techname from the rata_sght.techname_list table to the mobile atlas table and choose the minimum popularity rank (maximum popularity) when there are multiple matches*/
  
drop table if exists rata_sght.techname_matched2atlas;
create table rata_sght.techname_matched2atlas row format delimited fields terminated by '\t' as (
  select * from (SELECT a.dvc_techname as dvc_techname_raw, min(b.popularity) as popularity
  FROM rata_sght.techname_list a 
left JOIN (select dvc_techname, popularity from apollo_util.mobile_atlas_wcluster order by popularity asc)  b 
  ON a.dvc_techname LIKE CONCAT(b.dvc_techname, '%') group by dvc_techname_raw order by dvc_techname_raw desc) C ) ;

/*Add device properties from mobile atlas table to techname_list table*/

drop table if exists apollo_util.techname_prop_map;
create table apollo_util.techname_prop_map row format delimited fields terminated by '\t' as ( select dvc_techname_raw, popularity, case when dvc_techname is null then dvc_techname_raw else dvc_techname end as dvc_techname, brand, case when dvc_commercname is null then dvc_techname_raw else dvc_commercname end as dvc_commercname, release_year, release_month, 
		case when release_price is null and (dvc_techname_raw like '%iris%' or dvc_techname_raw like '%lava%' or dvc_techname_raw like '%smart%' or dvc_techname_raw like '%true%' or dvc_techname_raw like '%dtac%' or dvc_techname_raw like '%joey%' or dvc_techname_raw like '%blade%' or dvc_techname_raw like '%eagle%') then '80' 
			when release_price is null and (dvc_techname_raw like '%i-mobile%' or dvc_techname_raw like '%i-style%') then '130' 
			when release_price is null and dvc_techname_raw like '%vivo%'  then '340' 
			when release_price is null and dvc_techname_raw like '%asus%'  then '125' 
			when release_price is null and dvc_techname_raw like '%htc%'  then '470' 
			when release_price is null and dvc_techname_raw like '%lg%'  then '270' 
			when release_price is null and dvc_techname_raw like '%huawei%'  then '300' 
			when release_price is null and (dvc_techname_raw like '%sm-%' or dvc_techname_raw like '%gt-%') then '370' 
			when release_price is null and (dvc_techname_raw like '%x9009%' ) then '400' 
			when release_price is null and dvc_techname_raw like '%wiko%'  then '130' 
			else release_price end as release_price, screensize, cluster from 

  (select a.dvc_techname_raw, cast (a.popularity as double) as popularity, case when a.dvc_techname_raw='lenny' then  'lenny' when a.dvc_techname_raw='lenny2' then  'lenny2' else b.dvc_techname end as dvc_techname, b.brand, case when a.dvc_techname_raw='lenny' then  'lenny' when a.dvc_techname_raw='lenny2' then  'lenny2'  else b.dvc_commercname end as dvc_commercname, case when a.dvc_techname_raw='lenny' then  '2014' when a.dvc_techname_raw='lenny2' then  '2015' else b.release_year end as release_year, case when a.dvc_techname_raw='lenny' then  'August' when a.dvc_techname_raw='lenny2' then  'September' else b.release_month end as release_month, b.release_price, b.screensize, b.cluster from  rata_sght.techname_matched2atlas a left join apollo_util.mobile_atlas_wcluster b on a.popularity=b.popularity order by a.popularity asc ) C );
  
  /*Insert unavailable properties*/
  
  insert into table apollo_util.techname_prop_map values ('iphone os 5',0, 'iphone os 5', 'apple', 'iphone os 5', '2011', 'September', '750', '3.5', 11);  
  insert into table apollo_util.techname_prop_map values ('iphone os 6',0, 'iphone os 6', 'apple', 'iphone os 6', '2012', 'September', '750', '4', 11);  
  insert into table apollo_util.techname_prop_map values ('iphone os 7',0, 'iphone os 7', 'apple', 'iphone os 7', '2013', 'September', '750', '4', 11);  
  insert into table apollo_util.techname_prop_map values ('iphone os 8',0, 'iphone os 8', 'apple', 'iphone os 8', '2014', 'September', '750', '4.7', 11);  
  insert into table apollo_util.techname_prop_map values ('iphone os 9',0, 'iphone os 9', 'apple', 'iphone os 9', '2015', 'September', '750', '5', 11);  
  insert into table apollo_util.techname_prop_map values ('iphone os 10',0, 'iphone os 10', 'apple', 'iphone os 10', '2016', 'September', '750', '5', 11);  
  insert into table apollo_util.techname_prop_map values ('asust00j',0, 'asust00j', 'asus', 'zenfone 5', '2014', 'April', '130', '5', 13);  
  insert into table apollo_util.techname_prop_map values ('asusz00ad',0, 'asusz00ad', 'asus', 'zenfone 2', '2015', 'March', '370', '5.5', 13);  
  insert into table apollo_util.techname_prop_map values ('asusz010d',0, 'asusz010d', 'asus', 'zenfone max', '2016', 'January', '170', '5.5', 13);  
  insert into table apollo_util.techname_prop_map values ('asusz00ld',0, 'asusz00ld', 'asus', 'zenfone 2 laser', '2015', 'September', '220', '5.5', 13);  
  insert into table apollo_util.techname_prop_map values ('asusx013d',0, 'asusx013d', 'asus', 'zenfone go', '2016', 'May', 'null', '5.5', 13);  