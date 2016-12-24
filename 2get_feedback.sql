/*
####################################################################################
# Name: init_feedback_tbl_sql
# Description: Create initial feedback table
# Input: taps, tracked_events
# Version:
#   2016/12/08 RS: Initial version 
####################################################################################
*/

insert into table sgmt_rules.feedback_tbl partition(year=${var:year}, month=${var:month}, day=${var:day}) 
select sight_date, offer, source, hl_platform, dvc_techname, imps, clicks, landings, submits from 
(select case when c.sight_date is not null then c.sight_date else f.sight_date end as sight_date, 
		case when c.offer is not null then c.offer else f.offer end as offer, 
		case when c.source is not null then c.source else f.source end as source, 
		case when c.hl_platform is not null then c.hl_platform else f.hl_platform end as hl_platform,
		case when c.dvc_techname is not null then c.dvc_techname else f.dvc_techname end as dvc_techname,
		c.imps, c.clicks, f.landings, f.submits, 
		case when c.year is not null then c.year else f.year end as year,
		case when c.month is not null then c.month else f.month end as month,
		case when c.day is not null then c.day else f.day end as day 
		from 
 (select sight_date, offer, hl_platform, dvc_techname, 'kd' as source,  
sum(imp_flg) as imps, sum(click_flg) as clicks, year, month, day   from 
(select sight_date, 
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
		applewebkit.*|/v[0-9] linux.*|v_td.*|_td/v[0-9].*|i_style.*',''),'.*(th|en|zh|zz)(-|_)(gb|au|ph|th|us|cn|nz|gb|tw|fi|jp|za|sg|ie|zz);? |.*nokia; ',''),'/.*|linux.*','')),'[^0-9a-z\- \.]',''),'.*samsung(-| )|.*lenovo |.*microsoft |.*th- ',''),'like.*|lollipop.*',''),' applewebkit.*',''),' dual sim.*','')) end as dvc_techname, 
	case when action_id ='impression' then 1 else 0 end as imp_flg, 
    case when action_id ='click' then 1 else 0 end as click_flg, 
	offer,
		year, month, day 
	from 
(select regexp_replace(cast(cast(a.header.created_at/1000 as timestamp) as string),' .*','') as sight_date, 
		case when lower(a.header.platform)='iphone' and (lower(a.header.user_agent) like ('%windows phone%') or lower(a.header.user_agent) like ('%lumia%')) then 'WINDOWS_PHONE' else a.header.platform end as platform,  
		a.header.user_agent as user_agent,
		a.action_id as action_id, 
	case
		when a.tactic_id = 186858 then 'mnp-device-discount-samsung'
		when a.tactic_id = 191242 then 'mnp-device-discount-samsung'
		when a.tactic_id = 191243 then 'mnp-free-device'
		when a.tactic_id = 191244  then 'tariff'
		when a.tactic_id = 199183 then 'booster'
		when a.tactic_id in (197236, 213768) then 'mnp-samsung-galaxy-j2'
		when a.tactic_id = 200320 then 'mnp-samsung-galaxy-j5'
		when a.tactic_id = 201014 then 'mnp-samsung-galaxy-a5'
		when a.tactic_id in (203164,217118) then 'mnp-asus-zenfone-45' 
		when a.tactic_id = 214301 then 'mnp-oppo-mirror5' 
		when a.tactic_id = 217958 then 'mnp-free-dtac-pocket-wifi' 
		when a.tactic_id = 221701 then 'mnp-free-dtac-phone-s2'	
		when a.tactic_id = 222237 then 'Nice Number'	
		when a.tactic_id = 223067 then 'mnp-vivo-v5' 
		when a.tactic_id = 222299 then 'mnp-samsung-galaxy-j5-prime'
		end as offer, 
	h.value as header_url , a.year, a.month, a.day
 from default.taps a, a.header.incoming_ids b, a.header.query_params q, a.header.http_headers h where q.key='ext_cat' and h.key = 'Referer' and a.campaign_id=5138 and a.action_id IN ('impression','click') and a.year=${var:year} and a.month=${var:month} and a.day =${var:day} ) A) B group by sight_date, offer, hl_platform, dvc_techname, source, year, month, day) C 
 
 full outer join 
 
 (select  sight_date, hl_platform, dvc_techname, offer,  source, sum(landing_flg) as landings, sum(submit_flg) as submits, year, month, day  from 
 
(select sight_date, 
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
		applewebkit.*|/v[0-9] linux.*|v_td.*|_td/v[0-9].*|i_style.*',''),'.*(th|en|zh|zz)(-|_)(gb|au|ph|th|us|cn|nz|gb|tw|fi|jp|za|sg|ie|zz);? |.*nokia; ',''),'/.*|linux.*','')),'[^0-9a-z\- \.]',''),'.*samsung(-| )|.*lenovo |.*microsoft |.*th- ',''),'like.*|lollipop.*',''),' applewebkit.*',''),' dual sim.*','')) end as dvc_techname,
	   case when action_id ='undefined' then 1 else 0 end as landing_flg,
       case when action_id like '%submit%' then 1 else 0 end as submit_flg,
	case 
		when referrer_url like '%special-package%' then 'tariff' 
		when referrer_url like '%asus-zenfone-45%' then 'mnp-asus-zenfone-45' 
		else regexp_replace(regexp_replace(regexp_replace(referrer_url,'.*specialoffer/',''),'\\.html.*',''),'-lite.*','') end as offer, 
	case
		when referrer_url like '%kaidee%' then 'kd'
		when referrer_url like '%facebook%' then 'fb' else 'oth' end as source, year, month, day  
from 
(
select regexp_replace(cast(cast(a.header.created_at/1000 as timestamp) as string),' .*','') as sight_date, b.value as tapad_id, a.action_id as action_id, case when lower(a.header.platform)='iphone' and (lower(a.header.user_agent) like ('%windows phone%') or lower(a.header.user_agent) like ('%lumia%')) then 'WINDOWS_PHONE' else a.header.platform end as platform,  a.header.user_agent as user_agent, a.header.referrer_url as referrer_url, a.year, a.month, a.day 

 from default.tracked_events a, a.header.incoming_ids b where a.property_id = '2868' and (a.action_id like '%submit%' or a.action_id ='undefined') and a.header.referrer_url like '%specialoffer%' and a.year=${var:year} and a.month=${var:month} and a.day =${var:day}) D ) E group by sight_date, hl_platform, dvc_techname, offer,  source, year, month, day) F 
 on c.sight_date=f.sight_date and c.hl_platform=f.hl_platform and c.dvc_techname=f.dvc_techname and c.offer=f.offer and c.source = f.source) P  ;