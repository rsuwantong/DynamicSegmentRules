/*
####################################################################################
# Name: techname_bytpid_meas
# Description: Create tpid - device_techname map for measurement data
# Input: taps, tracked events
# Version:
#   2016/11/29 RS: Initial version (with correction for WINDOWS_PHONE)
####################################################################################
*/


drop table if exists meas_ana.techname_bytpid_meas;
create table meas_ana.techname_bytpid_meas row format delimited fields terminated by '\t' as (
select tapad_id, 
		case when platform in ('ANDROID', 'ANDROID_TABLET', 'WINDOWS_PHONE', 'WINDOWS_TABLET', 'BLACKBERRY', 'FEATURE_PHONE') then 'ANDROID' when platform='IPHONE' then 'IPHONE' else 'PC_OTHERS' end as hl_platform, 
	case 
	    when platform not in ('ANDROID', 'ANDROID_TABLET', 'WINDOWS_PHONE', 'WINDOWS_TABLET', 'BLACKBERRY', 'FEATURE_PHONE','IPHONE') then platform  
		when platform = 'WINDOWS_PHONE' then   trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(lcase(user_agent),'.*(microsoft|nokia); ',''),'\\) like iphone.*',''),';.*','') ,'\\).*',''),' applewebkit.*',''),' dual sim.*',''))
		when lcase(user_agent) like '%cpu iphone os%' and lcase(user_agent) like '%ipod%' and lcase(platform)='iphone' then 'ipod' 
		when lcase(user_agent) like '%cpu iphone os%' or lcase(user_agent) like '%iphone; u; cpu iphone%' or lcase(user_agent) like '%iphone; cpu os%' and lcase(platform)='iphone' then regexp_replace(regexp_replace(regexp_replace(lcase(user_agent),'.*iphone;( u;)? cpu ',''),'like mac os.*',''),'_.*','') 
		when lcase(user_agent) like '%(null) [fban%' and lcase(user_agent) like '%fbdv/iphone%' and lcase(platform)='iphone' then regexp_extract(regexp_replace(lcase(user_agent),'.*fbdv/',''),'iphone[0-9]',0) 
		when lcase(user_agent) like '%android; mobile; rv%' or lcase(user_agent) like '%mobile rv[0-9][0-9].[0-9] gecko%' then 'unidentified android' 
		when lcase(user_agent) like '%android; tablet; rv%' or lcase(user_agent) like '%tablet rv[0-9][0-9].[0-9] gecko%' then 'unidentified tablet' 
		else  trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(lcase(user_agent),'.*android [0-9](.[0-9](.[0-9])?)?; ',''),' build.*|; android/.*|\\) 
		applewebkit.*|/v[0-9] linux.*|v_td.*|_td/v[0-9].*|i_style.*',''),'.*(th|en|zh|zz)(-|_)(gb|au|ph|th|us|cn|nz|gb|tw|fi|jp|za|sg|ie|zz);? |.*nokia; ',''),'/.*|linux.*','')),'[^0-9a-z\- \.]',''),'.*samsung(-| )|.*lenovo |.*microsoft |.*th- ',''),'like.*|lollipop.*',''),' applewebkit.*',''),' dual sim.*','')) end as dvc_techname 
		from (	select tapad_id, platform, user_agent from 
				((select b.value as tapad_id, case when lower(a.header.platform)='iphone' and (lower(a.header.user_agent) like ('%windows phone%') or lower(a.header.user_agent) like ('%lumia%')) then 'WINDOWS_PHONE' else a.header.platform end as platform,  a.header.user_agent as user_agent
				from default.taps a, a.header.incoming_ids b, a.header.query_params q, a.header.http_headers h where q.key='ext_cat' and h.key = 'Referer' and a.campaign_id=5138 and a.action_id IN ('impression','click')  group by tapad_id, platform, user_agent ) union all (select e.value as tapad_id, case when lower(d.header.platform)='iphone' and (lower(d.header.user_agent) like ('%windows phone%') or lower(d.header.user_agent) like ('%lumia%')) then 'WINDOWS_PHONE' else d.header.platform end as platform,  d.header.user_agent as user_agent from default.tracked_events d, d.header.incoming_ids e where d.property_id = '2868' and (d.action_id like '%submit%' or d.action_id ='select' or d.action_id ='undefined') and d.header.referrer_url like '%specialoffer%' group by tapad_id, platform, user_agent)) N group by tapad_id, platform, user_agent ) M 
	);
	
	select * from meas_ana.techname_bytpid_meas limit 5;
	