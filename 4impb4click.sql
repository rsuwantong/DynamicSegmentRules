/*
####################################################################################
# Name: techname_bytpid_meas
# Description: Create impclick_daydif and impb4click by offer, tpid 
# Input: taps
# Version:
#   2016/12/01 RS: Initial version 
####################################################################################
*/

drop table if exists meas_ana.action_byoffer_tpid;
use meas_ana;create table meas_ana.action_byoffer_tpid row format delimited fields terminated by '\t' as ( 

select case
		
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
		when a.tactic_id = 217958 then 'mnp-free-dtac-pocket-wifi' end as offer,
	b.value as tapad_id, 
	cast(a.header.created_at/1000 as timestamp) as time, 
	a.action_id as action_id 
 from default.taps a, a.header.incoming_ids b, a.header.query_params q where q.key='ext_cat' and a.year=2016 and a.month>6 and a.campaign_id=5138 and a.action_id IN ('impression','click') order by offer asc, tapad_id asc, time asc);
 
 
drop table if exists meas_ana.impb4click_byoffer_tpid;
use meas_ana;create table meas_ana.impb4click_byoffer_tpid row format delimited fields terminated by '\t' as ( 
 select a.offer, a.tapad_id, count(distinct time) as impb4click, datediff(b.click_moment, min(time)) as impclick_daydif 
from meas_ana.action_byoffer_tpid a
join (select 
		 offer, 
         tapad_id,
         min(time) as click_moment, 
		 regexp_replace(cast(min(time) as string),' .*','') as click_date 
        from meas_ana.action_byoffer_tpid where action_id = 'click'
        group by offer, tapad_id 
        ) b on a.tapad_id = b.tapad_id and a.offer=b.offer and a.time < b.click_moment where action_id='impression' 
		
		group by a.offer, a.tapad_id, b.click_moment order by offer asc, tapad_id asc);
		
