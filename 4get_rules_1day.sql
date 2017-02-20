/*
####################################################################################
# Name: get_rules.sql
# Description: Generate rules from offer control file and feedback_tbl
# Input: sgmt_rules.offer_control, sgmt_rules.feedback_tbl
# Version:
#   2016/12/23 RS: Initial version
#   
####################################################################################
*/


/*Select the offers with valid dates from the offer control file*/

drop table if exists sgmt_rules.dvc_selected_control;
create table sgmt_rules.dvc_selected_control row format delimited fields terminated by '\t' as (
select a.offer,a.uc1_priority, a.active_flg_kd, a.active_flg_fb, a.active_flg_pt, b.dvc_techname, a.min_CTR, a.min_landings, a.min_submits, a.min_S2L, a.max_CPS,  concat_ws(' and ', concat('CTR>=', a.min_ctr), concat('submits>=', a.min_submits), concat('S2L>=', a.min_S2L),concat('CPS<=', a.max_CPS) ) as feedback_cond from (select  * from sgmt_rules.offer_control where cast(start_date as string) <= '2017-02-01' and '2017-02-01'  < cast(end_date as string) and (active_flg_fb is not null or active_flg_kd is not null or active_flg_pt is not null) ) a join apollo_util.techname_prop_map b on a.target_rlp_min <=cast(b.release_price as double)*40 and cast(b.release_price as double)*40<=a.target_rlp_max group by offer, uc1_priority, active_flg_kd, active_flg_fb, active_flg_pt, dvc_techname, min_CTR, min_landings, min_submits, min_S2L, max_CPS);


drop table if exists sgmt_rules.offer_list;
create table sgmt_rules.offer_list row format delimited fields terminated by '\t' as 
(select offer, uc1_priority, active_flg_kd, active_flg_fb, active_flg_pt, min_CTR, min_landings, min_submits, min_S2L, max_CPS, feedback_cond from sgmt_rules.dvc_selected_control group by offer,uc1_priority, active_flg_kd, active_flg_fb, active_flg_pt, min_CTR, min_landings, min_submits, min_S2L, max_CPS, feedback_cond);

/*Select the successful devices by offer*/

drop table if exists sgmt_rules.dvc_selected_feedback_submits;
create table sgmt_rules.dvc_selected_feedback_submits row format delimited fields terminated by '\t' as (
select offer, dvc_techname from
(select a.source, a.offer, a.dvc_techname from 
( select source, offer, regexp_replace(regexp_replace(dvc_techname,'COMPUTER','windows nt'),'IPAD','ipad') as dvc_techname, CTR, submits, CPS from
(select source, offer, dvc_techname, sum(clicks)/sum(imps) as CTR, sum(submits) as submits, sum(submits)/sum(landings) as S2L, sum(imps)*3/(1000*sum(submits)) as CPS from  
		(select m.* 
		from sgmt_rules.feedback_tbl M join ( select p.* from (select offer, submits from (select offer, count(distinct sighted_date) as launch_days, sum(submits) as submits from sgmt_rules.feedback_tbl group by offer ) R where launch_days >=3) P inner join  ( select offer from (select offer, count(distinct dvc_techname) as num_submit_dvc from sgmt_rules.feedback_tbl where submits>0 group by offer) G where num_submit_dvc >= 20) Q on P.offer = Q.offer ) N on m.offer = n.offer) O 
		
		group by offer, source, dvc_techname) C ) A inner join sgmt_rules.offer_list B on a.offer=b.offer and (a.CTR>= cast(b.min_CTR as double) and a.submits >= cast(b.min_submits as double) and a.CPS <= cast(b.max_CPS as double) or (source='fb' and a.submits>=cast(b.min_submits as double)))) D group by offer, dvc_techname )  ; /*modify or source = fb to and case when source = fb ?*/
		
/*Select the landing devices by offer in case of small number of successful devices*/

drop table if exists sgmt_rules.dvc_selected_feedback_landings;
create table sgmt_rules.dvc_selected_feedback_landings row format delimited fields terminated by '\t' as (
select e.* from 
(select offer, dvc_techname from
(select a.offer, a.dvc_techname from 
( select offer, regexp_replace(regexp_replace(dvc_techname,'COMPUTER','windows nt'),'IPAD','ipad') as dvc_techname, landings, S2L from
(select offer, dvc_techname, sum(landings) as landings, sum(landings)/sum(submits) as S2L from (select m.* 
		from sgmt_rules.feedback_tbl M join (select offer from (select offer, count(distinct sighted_date) as launch_days from sgmt_rules.feedback_tbl group by offer ) A where launch_days >=3 ) N on m.offer = n.offer) O group by offer, dvc_techname) C ) A inner join sgmt_rules.offer_list b on a.offer=b.offer and a.landings >= cast(b.min_landings as double)) D group by offer, dvc_techname) E inner join ( select offer from (select offer, count(distinct dvc_techname) as num_succeed_dvc from sgmt_rules.dvc_selected_feedback_submits group by offer) G where num_succeed_dvc <  20) F  on e.offer=f.offer)  ;

/*Select the device target list by offer from feedback for the active offers*/

drop table if exists sgmt_rules.dvc_selected_feedback;
create table sgmt_rules.dvc_selected_feedback row format delimited fields terminated by '\t' as (
select m.offer, n.dvc_techname from (select offer from sgmt_rules.offer_list) M left join 
(select offer, dvc_techname from 
(select a.offer, case when b.offer is null then a.dvc_techname else b.dvc_techname end as dvc_techname from sgmt_rules.dvc_selected_feedback_submits A left join sgmt_rules.dvc_selected_feedback_landings B on a.offer = b.offer) C group by offer, dvc_techname order by offer asc, dvc_techname asc) N on m.offer=n.offer 
);

/*Generate rules for each source. When a dvc_techname is in multiple offer, it will be only in the offer with highest priority*/

drop table if exists sgmt_rules.offer_dvc_map_kd;
create table sgmt_rules.offer_dvc_map_kd row format delimited fields terminated by '\t' as ( select * from ( 
select d.offer, d.uc1_priority, regexp_replace(regexp_replace(c.dvc_techname,'.*.com|.*sprd-|.*[a-z][a-z]-([0-9])? |.*th--1 ',''),'(dual.*| opera.*|-orange.*)','') as dvc_techname from (select min(uc1_priority) as uc1_priority, dvc_techname from ( select uc1_priority,  
	case when b.dvc_techname is null then a.dvc_techname 
		 when b.dvc_techname like '%a.dvc_techname%' then a.dvc_techname 
		 else b.dvc_techname end as dvc_techname 
	from (select * from sgmt_rules.dvc_selected_control where active_flg_kd=1) a left join sgmt_rules.dvc_selected_feedback b on a.offer=b.offer) E group by dvc_techname) C inner join (select * from sgmt_rules.dvc_selected_control where active_flg_kd=1) D on c.uc1_priority=d.uc1_priority group by offer, uc1_priority, dvc_techname order by uc1_priority asc, offer asc, dvc_techname asc) E where char_length(dvc_techname)>2 order by uc1_priority asc, offer asc, dvc_techname asc);
	
drop table if exists sgmt_rules.offer_dvc_map_kd_app;
create table sgmt_rules.offer_dvc_map_kd_app row format delimited fields terminated by '\t' as ( select * from ( 
select d.offer, d.uc1_priority, regexp_replace(regexp_replace(c.dvc_techname,'.*.com|.*sprd-|.*[a-z][a-z]-([0-9])? |.*th--1 ',''),'(dual.*| opera.*|-orange.*)','') as dvc_techname from (select min(uc1_priority) as uc1_priority, dvc_techname from ( select uc1_priority, regexp_replace(dvc_techname,'iphone os (1)?[0-9]','iphone; ios') as dvc_techname from (select uc1_priority,  
	case when b.dvc_techname is null then a.dvc_techname 
		 when b.dvc_techname like '%a.dvc_techname%' then a.dvc_techname 
		 else b.dvc_techname end as dvc_techname 
	from (select * from sgmt_rules.dvc_selected_control where active_flg_kd=1) a left join sgmt_rules.dvc_selected_feedback b on a.offer=b.offer) G ) E group by dvc_techname) C inner join (select * from sgmt_rules.dvc_selected_control where active_flg_kd=1) D on c.uc1_priority=d.uc1_priority group by offer, uc1_priority, dvc_techname order by uc1_priority asc, offer asc, dvc_techname asc) F where char_length(dvc_techname)>2 order by uc1_priority asc, offer asc, dvc_techname asc);	

drop table if exists sgmt_rules.offer_dvc_map_fb;
create table sgmt_rules.offer_dvc_map_fb row format delimited fields terminated by '\t' as ( select * from ( 
select d.offer, d.uc1_priority, regexp_replace(regexp_replace(c.dvc_techname,'.*.com|.*sprd-|.*[a-z][a-z]-([0-9])? |.*th--1 ',''),'(dual.*| opera.*|-orange.*)','') as dvc_techname from (select min(uc1_priority) as uc1_priority, dvc_techname from ( select uc1_priority, regexp_replace(dvc_techname,'iphone os (1)?[0-9]','apple iphone') as dvc_techname from (select uc1_priority,  
	case when b.dvc_techname is null then a.dvc_techname 
		 when b.dvc_techname like '%a.dvc_techname%' then a.dvc_techname 
		 else b.dvc_techname end as dvc_techname 
	from (select * from sgmt_rules.dvc_selected_control where active_flg_fb=1) a left join sgmt_rules.dvc_selected_feedback b on a.offer=b.offer) G ) E group by dvc_techname) C inner join (select * from sgmt_rules.dvc_selected_control where active_flg_fb=1) D on c.uc1_priority=d.uc1_priority group by offer, uc1_priority, dvc_techname order by uc1_priority asc, offer asc, dvc_techname asc) F where char_length(dvc_techname)>2 order by uc1_priority asc, offer asc, dvc_techname asc);

drop table if exists sgmt_rules.offer_dvc_map_pt;
create table sgmt_rules.offer_dvc_map_pt row format delimited fields terminated by '\t' as ( select * from ( 
select d.offer, d.uc1_priority, regexp_replace(regexp_replace(c.dvc_techname,'.*.com|.*sprd-|.*[a-z][a-z]-([0-9])? |.*th--1 ',''),'(dual.*| opera.*|-orange.*)','') as dvc_techname from (select min(uc1_priority) as uc1_priority, dvc_techname from ( select uc1_priority,  
	case when b.dvc_techname is null then a.dvc_techname 
		 when b.dvc_techname like '%a.dvc_techname%' then a.dvc_techname 
		 else b.dvc_techname end as dvc_techname 
	from (select * from sgmt_rules.dvc_selected_control where active_flg_pt=1) a left join sgmt_rules.dvc_selected_feedback b on a.offer=b.offer) E group by dvc_techname) C inner join (select * from sgmt_rules.dvc_selected_control where active_flg_pt=1) D on c.uc1_priority=d.uc1_priority group by offer, uc1_priority, dvc_techname order by uc1_priority asc, offer asc, dvc_techname asc) E where char_length(dvc_techname)>2 order by uc1_priority asc, offer asc, dvc_techname asc);

/*Create the rule table*/

drop table if exists sgmt_rules.rules_1day;
create table sgmt_rules.rules_1day row format delimited fields terminated by '\t' as (
select * from 
(select 'kd' as source, offer, uc1_priority, concat('userAgent(".*(?i)(',group_concat(trim(dvc_techname),'|'),').*")') as rule from sgmt_rules.offer_dvc_map_kd where dvc_techname not in ('asus','huawei','zte-blade','vivo','smart','iris','lava','true','asus','lenovo','htc','dtac','i-mobile','i-style','smartphone','i_mobile','i_style') group by source, offer, uc1_priority union all
select 'kd_app' as source, offer, uc1_priority, concat('userAgent(".*(?i)(',group_concat(trim(dvc_techname),'|'),').*")') as rule from sgmt_rules.offer_dvc_map_kd_app where dvc_techname not in ('asus','huawei','zte-blade','vivo','smart','iris','lava','true','asus','lenovo','htc','dtac','i-mobile','i-style','smartphone','i_mobile','i_style') group by source, offer, uc1_priority union all
select 'fb' as source, offer, uc1_priority, concat('userAgent(".*(?i)(',group_concat(trim(dvc_techname),'|'),').*")') as rule from sgmt_rules.offer_dvc_map_fb where dvc_techname not in ('asus','huawei','zte-blade','vivo','smart','iris','lava','true','asus','lenovo','htc','dtac','i-mobile','i-style','smartphone','i_mobile','i_style')  group by source, offer, uc1_priority union all
select 'pt' as source, offer, uc1_priority, concat('userAgent(".*(?i)(',group_concat(trim(dvc_techname),'|'),').*")') as rule from sgmt_rules.offer_dvc_map_pt where dvc_techname not in ('asus','huawei','zte-blade','vivo','smart','iris','lava','true','asus','lenovo','htc','dtac','i-mobile','i-style','smartphone','i_mobile','i_style')  group by source, offer, uc1_priority) A order by source asc, uc1_priority asc) ;

select * from sgmt_rules.rules_1day order by source asc, uc1_priority asc;

/*
impala-shell -i impala.prd.sg1.tapad.com:21000 -B -o /local/home/rata.suwantong/rules_170124.csv --output_delimiter=',' -q "select * from sgmt_rules.rules_1day order by source asc,uc1_priority asc"

select * from sgmt_rules.rules_1day;
*/