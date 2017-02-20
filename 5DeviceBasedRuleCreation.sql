/*
####################################################################################
# Name: DeviceBasedRuleCreation
# Description: Create set of segmentation rules using device characteristics from dvccluster table
# Version:
#   2017/02/17 RS: Initial version
#   
####################################################################################
*/

/*Upload device clustering csv on an Impala's Database Folder (here apollo_util) and write a corresponding Impala Table*/

drop table if exists apollo_util.dvccluster_pre;
CREATE EXTERNAL TABLE if not exists apollo_util.dvccluster_pre
 (
	MODEL STRING,
    REACH DOUBLE,
    MARKETING_NAME STRING,
	VENDOR STRING,
	PRICE_RELEASED DOUBLE,
    YEAR_RELEASED DOUBLE,
    MONTH_RELEASED DOUBLE,
	TIME_RELEASED DOUBLE,
	DIAGONAL_SCREEN_SIZE DOUBLE,
	CAMERA_PIXELS DOUBLE,
	DISPLAY_HEIGHT DOUBLE,
	DISPLAY_WIDTH DOUBLE,
	AGE DOUBLE,
	CLUSTERSCRCAM_KM7 DOUBLE
 )   
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ESCAPED BY ','
 LOCATION '/user/rata.suwantong/impala_rata2/DvcClusteringResults_km7/';


/*When uploading the first row (column names) will be regarded as the data, hence we will have to leave it out ourselves.*/

drop table if exists apollo_util.dvccluster;
create table apollo_util.dvccluster row format delimited fields terminated by '\t' as ( select 
	MODEL,
    MARKETING_NAME,
	VENDOR,
	PRICE_RELEASED,
    YEAR_RELEASED,
    MONTH_RELEASED,
	TIME_RELEASED,
	DIAGONAL_SCREEN_SIZE,
	CAMERA_PIXELS,
	DISPLAY_HEIGHT,
	DISPLAY_WIDTH,
	AGE,
	CLUSTERSCRCAM_KM7 as CLUSTER
from apollo_util.dvccluster_pre where VENDOR !='VENDOR' );

drop table if exists apollo_util.dvccluster_brand;
create table apollo_util.dvccluster_brand row format delimited fields terminated by '\t' as ( select 
	a.*, case when cluster = 1 and vendor ='samsung' then '1_SS' 
			  when cluster = 1 and vendor !='samsung' then '1_OTH'
			  when cluster = 2 and vendor ='samsung' then '2_SS'
			  when cluster = 2 and vendor in ('ais','true','i-mobile','zte','lava') then '2_OEM'
			  when cluster = 2 then '2_OTH'
			  else cast(cluster as string) end as cluster_brand 
from apollo_util.dvccluster a );


select * from apollo_util.dvccluster_brand limit 10;

/*
+---------------------+---------------------+---------+----------------+---------------+----------------+-------------------+----------------------+---------------+----------------+---------------+--------------------+---------+---------------+
| model               | marketing_name      | vendor  | price_released | year_released | month_released | time_released     | diagonal_screen_size | camera_pixels | display_height | display_width | age                | cluster | cluster_brand |
+---------------------+---------------------+---------+----------------+---------------+----------------+-------------------+----------------------+---------------+----------------+---------------+--------------------+---------+---------------+
| SM-J700F            | galaxy j7 duo       | samsung | 250            | 2015          | 6              | 2015.5            | 5.5                  | 13            | 1280           | 720           | 1.666666666666742  | 1       | 1_SS          |
| SM-J200GU           | galaxy j2           | samsung | 150            | 2015          | 9              | 2015.75           | 4.5                  | 5             | 960            | 540           | 1.416666666666742  | 2       | 2_SS          |
| SM-J710F            | galaxy j7 (2016)    | samsung | 230            | 2016          | 4              | 2016.333333333333 | 5.5                  | 13            | 1920           | 1080          | 0.8333333333337123 | 1       | 1_SS          |
| SM-G7102            | galaxy grand 2      | samsung | 180            | 2013          | 1              | 2013.083333333333 | 5.25                 | 8             | 1280           | 720           | 4.083333333333712  | 4       | 4             |
| A1601               | f1s                 | oppo    | 300            | 2016          | 8              | 2016.666666666667 | 5.5                  | 16            | 1280           | 720           | 0.4999999999997726 | 1       | 1_OTH         |
| GT-I8552B           | galaxy win duos     | samsung | 170            | 2013          | 5              | 2013.416666666667 | 4.66                 | 5             | 800            | 480           | 3.749999999999773  | 3       | 3             |
| GT-N7100            | galaxy note 2       | samsung | 300            | 2012          | 9              | 2012.75           | 5.5                  | 8             | 1280           | 720           | 4.416666666666742  | 4       | 4             |
| SM-N920C            | galaxy note 5       | samsung | 530            | 2015          | 8              | 2015.666666666667 | 5.7                  | 16            | 2560           | 1440          | 1.499999999999773  | 5       | 5             |
| GT-I8552            | galaxy win          | samsung | 170            | 2013          | 5              | 2013.416666666667 | 4.7                  | 5             | 800            | 480           | 3.749999999999773  | 3       | 3             |
| Smart 4G Speedy 4.0 | smart 4g speedy 4.0 | true    | 80             | 2015          | NULL           | 2015.5            | 4                    | 5             | 480            | 800           | 1.666666666666742  | 2       | 2_OEM         |
+---------------------+---------------------+---------+----------------+---------------+----------------+-------------------+----------------------+---------------+----------------+---------------+--------------------+---------+---------------+

*/

/*Example of rule creations */

/*Kaidee, Pantip, FB (The dvccluster table have only models of Android device, for iPhones please refer to .txt for Facebook and previous rule sheet on jira */

drop table if exists sgmt_rules.rule_cluster_brand;
create table sgmt_rules.rule_cluster_brand row format delimited fields terminated by '\t' as ( 
select cluster_brand, concat('userAgent(".*(?i)(',group_concat(trim(lcase(model)),'|'),').*")') as rule from apollo_util.dvccluster_brand group by cluster_brand order by cluster_brand asc);

/*For DSP, the rules still have to be case-sensitive*/
select cluster_brand, group_concat(model,',') as rule from apollo_util.dvccluster_brand group by cluster_brand order by cluster_brand asc;

/*Exporting to csv*/
impala-shell -i impala.prd.sg1.tapad.com:21000 -B -o /local/home/rata.suwantong/rule_cluster_brand_pre.csv --output_delimiter=',' -q "select * from sgmt_rules.rule_cluster_brand order by cluster_brand asc"

/*Name the columns in the csv files (the column names of an Impala table will not be presented when exporting to csv) */  
echo $'CLUSTER, RULE' | cat - rule_cluster_brand_pre.csv > rule_cluster_brand.csv


/*Facebook audience creation, example*/

/*Creating a list of tpid of Facebook visitors who have been sighted on OMO (all period) and not on Dtac during the last 30 days */

drop table if exists sgmt_rules.tpidlist_omo;
create table sgmt_rules.tpidlist_omo row format delimited fields terminated by '\t' as ( 
select distinct tapad_id, model, id_type, event_source from apollo.dtac_vertical_dataset where carrier in ('True Move', 'AIS', 'TOT') 
);


drop table if exists sgmt_rules.tpidlist_dtac_last30d;
create table sgmt_rules.tpidlist_dtac_last30d row format delimited fields terminated by '\t' as ( 
select distinct tapad_id from apollo.dtac_vertical_dataset where carrier in ('DTAC') and year = 2017 and ((month = 1 and day > 15) or month = 2) and id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA') 
);

drop table if exists sgmt_rules.tpidlist_omo_nondtac_FB;
create table sgmt_rules.tpidlist_omo_nondtac_FB row format delimited fields terminated by '\t' as ( 
select omo.*  FROM from sgmt_rules.tpidlist_omo  omo 
LEFT JOIN sgmt_rules.tpidlist_dtac_last30d dtac ON omo.tapad_id = dtac.tapad_id 
AND dtac.tapad_id IS NULL
);

select * from sgmt_rules.tpidlist_omo  omo  limit 5;

/*Reach on Facebook (for the list of Tpid to be updated as audience of FB, just use select distinct omo.tapad_id ...*/



/*cluster 0*/

SELECT COUNT(DISTINCT omo.tapad_id) as 'Facebook Mobile Advertiser Ids' FROM apollo.dtac_vertical_dataset omo
LEFT JOIN (SELECT tapad_id FROM apollo.dtac_vertical_dataset WHERE
(user_agent REGEXP '(?i)(sm-t116nu|sm-t116bu|sm-p355|sm-t285|sm-t705|t1-701u|sm-t715y|sm-p555|fe171cg (k01n)|z170cg (p01y)|sm-t815y|t1-821l|sm-t719y|a5000|model 2|z370cg (p01v)|a5500-hv|sm-t819|pro 4 tab 9.0|a3500-hv|a3300|sm-t550|sm-t331|me175cg (k00z)|z380kl (p024)|v490|sgp621|sm-t555|fe380cg (k016)|sm-t810|mi pad|me581cl (k015)|fire (2015)|m2-801l|t1-a21l|sm-t560|3830|sm-t255s|sm-t365|sm-t330|sm-t815|sm-t700|sm-t2397|me181c (k011)|sm-t813|a3300-hv|sm-t715|tb3-710i|tb3-730x|nexus 9|sm-t710|sm-t280|s8-50lc|sm-p550|sm-t561|sm-t355y|sgp611|fire hd 8 (2016)|lk430|sm-t320|a85|v500|sm-t560nu|sm-n916k|830lc|a8-50lc|sm-t330nu|sm-t713|lifetab p891x|m1 8.0|sm-t325|sm-t335l|a3500-fl|sm-t335|sm-t285yd|sm-t116|sm-t239|sm-t375l|sm-t705y|sm-p355y|sm-t3777|sm-t719|z170c (p01z)|a5500-f|a5500-h|me572c (k007)|sm-t715c|sm-t815n0|a8-50f|fire hd 8 (2015)|p1050x|me372cl (k00y)|sm-p350|fire hdx 8.9|a1-840fhd|830l|tab 8|m733a|sm-t815c|sm-t715n0|a3500-h|a1-840|v480|sm-t335k|z370c (p01w)|puls|venue 8 7840|v607l|agora hd mini|sm-t360|m2-803l|sm-p555y|sm-t357t|s8-50l|sm-t377v|lifetab e7316|a902|vk810|sgp641|sm-p555l|s8-306l|lifetab s785x|sm-t377w|sm-t357w|k90|sm-t561y|fire hdx 8.9 4g|7 voice tab|k107|8055|403hw|sm-t719c|p1000|lifetab e733x|m733|venue 7 3740|sm-t705c|slate8 pro|sm-p355m|sm-t817w|e8qp|sm-t116ny|p480|sm-t707a|v90|pmt5287 4g|iq1010|sm-t561m|9020a|sm-t705m|sm-t562|a3300-t|t1-821w|tab 8.9|sm-t321|sm-p555m|i221|sm-t705w|p1040x|life view tab|p100|738 3g|wv8-l|b1-820|a110|kinder 7|pmt7787 3d|slate s5|elite 7q|sm-t239m|sm-t116nq|w032i-c3|sm-t817|t1-a22l|iq1010w|pro q8 plus|me175kg (k00s)|venue 7 3741|sgt-7000s|p290|mira|s7q|sm-t232|tab fast 2|sm-t332|colortab 7|p702|hero x|v tab 7 lite iii|elite 9q|a8i quad|p70221|mid-1013d|p1001|mid7308w|a10iq|wave i72|elite 8qs|edison 3 mini|a10ix|lifetab e7313|elite 9ql|a746|p900|zenpad 7.0|p666|elite8q)'
)
AND carrier IN ('DTAC')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA') and year = 2017 and ((month = 1 and day > 15) or month = 2)
) as dtac ON omo.tapad_id = dtac.tapad_id
WHERE
(user_agent REGEXP '(?i)(sm-t116nu|sm-t116bu|sm-p355|sm-t285|sm-t705|t1-701u|sm-t715y|sm-p555|fe171cg (k01n)|z170cg (p01y)|sm-t815y|t1-821l|sm-t719y|a5000|model 2|z370cg (p01v)|a5500-hv|sm-t819|pro 4 tab 9.0|a3500-hv|a3300|sm-t550|sm-t331|me175cg (k00z)|z380kl (p024)|v490|sgp621|sm-t555|fe380cg (k016)|sm-t810|mi pad|me581cl (k015)|fire (2015)|m2-801l|t1-a21l|sm-t560|3830|sm-t255s|sm-t365|sm-t330|sm-t815|sm-t700|sm-t2397|me181c (k011)|sm-t813|a3300-hv|sm-t715|tb3-710i|tb3-730x|nexus 9|sm-t710|sm-t280|s8-50lc|sm-p550|sm-t561|sm-t355y|sgp611|fire hd 8 (2016)|lk430|sm-t320|a85|v500|sm-t560nu|sm-n916k|830lc|a8-50lc|sm-t330nu|sm-t713|lifetab p891x|m1 8.0|sm-t325|sm-t335l|a3500-fl|sm-t335|sm-t285yd|sm-t116|sm-t239|sm-t375l|sm-t705y|sm-p355y|sm-t3777|sm-t719|z170c (p01z)|a5500-f|a5500-h|me572c (k007)|sm-t715c|sm-t815n0|a8-50f|fire hd 8 (2015)|p1050x|me372cl (k00y)|sm-p350|fire hdx 8.9|a1-840fhd|830l|tab 8|m733a|sm-t815c|sm-t715n0|a3500-h|a1-840|v480|sm-t335k|z370c (p01w)|puls|venue 8 7840|v607l|agora hd mini|sm-t360|m2-803l|sm-p555y|sm-t357t|s8-50l|sm-t377v|lifetab e7316|a902|vk810|sgp641|sm-p555l|s8-306l|lifetab s785x|sm-t377w|sm-t357w|k90|sm-t561y|fire hdx 8.9 4g|7 voice tab|k107|8055|403hw|sm-t719c|p1000|lifetab e733x|m733|venue 7 3740|sm-t705c|slate8 pro|sm-p355m|sm-t817w|e8qp|sm-t116ny|p480|sm-t707a|v90|pmt5287 4g|iq1010|sm-t561m|9020a|sm-t705m|sm-t562|a3300-t|t1-821w|tab 8.9|sm-t321|sm-p555m|i221|sm-t705w|p1040x|life view tab|p100|738 3g|wv8-l|b1-820|a110|kinder 7|pmt7787 3d|slate s5|elite 7q|sm-t239m|sm-t116nq|w032i-c3|sm-t817|t1-a22l|iq1010w|pro q8 plus|me175kg (k00s)|venue 7 3741|sgt-7000s|p290|mira|s7q|sm-t232|tab fast 2|sm-t332|colortab 7|p702|hero x|v tab 7 lite iii|elite 9q|a8i quad|p70221|mid-1013d|p1001|mid7308w|a10iq|wave i72|elite 8qs|edison 3 mini|a10ix|lifetab e7313|elite 9ql|a746|p900|zenpad 7.0|p666|elite8q)'
)
AND carrier IN ('True Move', 'AIS', 'TOT')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA')
AND dtac.tapad_id IS NULL;

/*
+--------------------------------+
| facebook mobile advertiser ids |
+--------------------------------+
| 19438                          |
+--------------------------------+
*/



/*cluster 1_OTH*/

SELECT COUNT(DISTINCT omo.tapad_id) as 'Facebook Mobile Advertiser Ids' FROM apollo.dtac_vertical_dataset omo
LEFT JOIN (SELECT tapad_id FROM apollo.dtac_vertical_dataset WHERE
(user_agent REGEXP '(?i)(a1601|n5111|f1f|x9009|zb551kl|zc550kl (z010d)|cam-l21|v3 max|z00ld|z00ed|r7kf|a601cg (z002)|zd551kl (z00ud)|r7sf|ze550ml (z008d)|vns-l22|r8106|zc520tl (x008d)|desire 816|desire eye|a7010a48|d6503|one|a7020a48|blade s6|rio-l02|d855|ze500cl (z00d)|e5653|d6653|ze601kl|c6902|e5553|h818|m2 note|f3116|blade v6|d690|blade v0720|ze500kg (z00rd)|s90-a|mi max|mi 5|chc-u23|gra-ul00|desire 820s|z90a40|p70-a|t00g|a6020a46|s60-a|pf500kl (t00n)|p7-l10|t06|rm-1096|a6020a40|7d-501u|h790|d5303|ale-l21|zc551kl (z01bdc)|n5116|e2353|gem-702l|d5503|redmi note 2|g7-l03|fire phone|s1a40|z017d|s850|mi 4 lte|one e8 dual sim|p1a42|d5803|d5833|c6903|pb1-750m|816g|a3000|rm-937|h540|moto g (3rd gen)|one e9+|gra-ul10|x3a40|one e8|vns-l31|mi 4c|m681h|d6603|desire 826|mi note lte|blade s6 plus|plk-l01|redmi note 4|d5322|mx5|s59|2pst610|maya max|rm-1116|xt1562|redmi 3s|hm note 1lte|redmi pro|nx511j|m10h|redmi 3|mi note pro|hm note 1w|mi 4i|a300|gra-l09|frd-l09|k50-t5|h60-l04|rm-1118|maya|mi 4s|k50a40|x2-ap|le x820|e6553|2ps6500|d826w|z10|chc-u01|one m8s|d826d|che2-ul00|tb3-710f|marathon m5|one a9|m910x|che2-l11|b2016|nx531j|a311|e5603|kiw-l21|e1003|x600-lte|rio-l01|k50-t3s|f5121|vns-l21|nem-l21|f3111|g7-l01|d816x|hm note 1s|le x620|d852|k520|e380|chm-u01|h1611|d820u|d6502|desire 820|crr-l09|one a9u|x500|kii-l21|desire 816 dual sim|d958|d728x|le x821|m1 note|lumia 640 xl lte|m3s|f3311|note 1 lte w|tit-al00|tit-l01|one x9 dual sim|d820f|desire 728 dual sim|plus|6039y|d850|desire 628 dual sim|6032|smart ultra 6|desire 626g plus|blade a910|a916|h955|pro 5|redmi 3x|m1 metal|h812|blade a452|power|vs985|k420|s60-t|820g+|d851|x900+|h60-l02|a806|sm-j700t|one me dual sim|desire 626|k220|r7sm|p7000|h60-l01|h811|x501|k535|vs986|ale-l04|e5353|a936|noir x900|z90-7|x2-eu|s90-u|ao5510|blade a510|d955|h950|life one x|lgl24|r7s|desire 728g dual sim|ms631|desire 828|h631|h810|desire 626 dual sim|x5004|s551|x600|d820us|k580|f180|d820pi|us991|lgv32|life one x2|desire 820q|che-tl00h|vivo 5|life s5004|vivo xl|d950|p55|d6563|lgv31|ls990|xm50h|h60-l12|k500|ze600kl (z00md)|k500n|e5303|life xl|d802tr|s6s|e5306|h635a|one e9 dual sim|kiw-tl00h|s308|chm-tl00h|f650k|d628u|d816d|c6916|a6020l36|p5000|ls996|d820t|v7a|h60-l03|ls775|che-tl00|ls991|a350|d6643|d802t|a1001|s950|f1fw|ath-tl00|v10|chm-tl00|e455|blade l6|elife e7 mini|s550|diamond s|vns-dl00|blade x9|one e9s dual sim|rm-1074|kiw-tl00|d722ar|d816w|k550|note 3|f3213|p55 novo|t84|d5316|c6906|e481|rm-1067|r7s plus|x2-to|chc-u03|6039a|c6943|0pja2|e313|rio-l03|e484|e5506|ms550|highway 4g|ls675|h520|r106|vns-l23|r7c|a310|p70-t|e311|tit-tl00|c55|t50|blade v580|grand 2|yu5530|a2015|g900w|vivo 5r|zero|h630d|h815t|u10|yu5510|e5606|irulu v3|c2016|spark +|r8201|h542|f650l|d816t|d693n|nem-ul10|crr-ul20|noir z8|x509|discovery 2|a7020a40|blade a475|life one xl|c20|le max|a808t|hercules|emax|primo s2|kii-l23|d693tr|eluga icon|padfone x|blade s|k540|ls660|cynus f7|s60-w|d6543|ice 2|6039j|blade v220|smart 505|d690n|a1p|vns-l53|y29l|smart e4|l39h|vs880pp|r7t|diamond 2 plus|prime 558|studio 6.0 lte|2pq93|6039k|g735-l03|diamond plus|5.0q|fire2 lte|storm|m2 mini|6607|s800c|p81|vs835|iris x5 4g|eluga u|life mark|iris x5|f3 pro|max2 plus|e485|l60|d852g|primo rm2|pure xl|a6020a41|z100|max2|gsmart mika m2|primo z|sc-ul10|us610|e352|n9518|k530|l82vl|up ultra|x2 soul|a316|6032a|x5001|blade l3 plus|eluga mark|z90-3|kii-l05|k32c36|s90-t|g628-tl00|discovery 2+|6039h|le x528|life one m|primo zx|d693ar|blade a511|nova x70|gsmart akta a4|g735-l23|blade a5|6039s|e471|d816|vision h3se|h815k|zielo z-500|p7 max|a7600m|d693|a66a|eluga switch|via m1|mi4|s58|studio 5.0 hd lte|r1c|a24|y200|a6020l37|classic pro|diamond v7|y90|preo p2|d5306|note|primo rx2|kiw-cl00|omega|q417|e500|omega pro|e550|p7-l05|6032x|p7-l12|3s m3g|xt1564|prime|blade hn|omega 5.5|primo v1|smart pro|fire plus 3g|x2 soul xtreme|d626w|fire plus 4g|x2 soul pro|note pro|a66s|a75l|x2 soul style plus)'
)
AND carrier IN ('DTAC')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA') and year = 2017 and ((month = 1 and day > 15) or month = 2)
) as dtac ON omo.tapad_id = dtac.tapad_id
WHERE
(user_agent REGEXP '(?i)(a1601|n5111|f1f|x9009|zb551kl|zc550kl (z010d)|cam-l21|v3 max|z00ld|z00ed|r7kf|a601cg (z002)|zd551kl (z00ud)|r7sf|ze550ml (z008d)|vns-l22|r8106|zc520tl (x008d)|desire 816|desire eye|a7010a48|d6503|one|a7020a48|blade s6|rio-l02|d855|ze500cl (z00d)|e5653|d6653|ze601kl|c6902|e5553|h818|m2 note|f3116|blade v6|d690|blade v0720|ze500kg (z00rd)|s90-a|mi max|mi 5|chc-u23|gra-ul00|desire 820s|z90a40|p70-a|t00g|a6020a46|s60-a|pf500kl (t00n)|p7-l10|t06|rm-1096|a6020a40|7d-501u|h790|d5303|ale-l21|zc551kl (z01bdc)|n5116|e2353|gem-702l|d5503|redmi note 2|g7-l03|fire phone|s1a40|z017d|s850|mi 4 lte|one e8 dual sim|p1a42|d5803|d5833|c6903|pb1-750m|816g|a3000|rm-937|h540|moto g (3rd gen)|one e9+|gra-ul10|x3a40|one e8|vns-l31|mi 4c|m681h|d6603|desire 826|mi note lte|blade s6 plus|plk-l01|redmi note 4|d5322|mx5|s59|2pst610|maya max|rm-1116|xt1562|redmi 3s|hm note 1lte|redmi pro|nx511j|m10h|redmi 3|mi note pro|hm note 1w|mi 4i|a300|gra-l09|frd-l09|k50-t5|h60-l04|rm-1118|maya|mi 4s|k50a40|x2-ap|le x820|e6553|2ps6500|d826w|z10|chc-u01|one m8s|d826d|che2-ul00|tb3-710f|marathon m5|one a9|m910x|che2-l11|b2016|nx531j|a311|e5603|kiw-l21|e1003|x600-lte|rio-l01|k50-t3s|f5121|vns-l21|nem-l21|f3111|g7-l01|d816x|hm note 1s|le x620|d852|k520|e380|chm-u01|h1611|d820u|d6502|desire 820|crr-l09|one a9u|x500|kii-l21|desire 816 dual sim|d958|d728x|le x821|m1 note|lumia 640 xl lte|m3s|f3311|note 1 lte w|tit-al00|tit-l01|one x9 dual sim|d820f|desire 728 dual sim|plus|6039y|d850|desire 628 dual sim|6032|smart ultra 6|desire 626g plus|blade a910|a916|h955|pro 5|redmi 3x|m1 metal|h812|blade a452|power|vs985|k420|s60-t|820g+|d851|x900+|h60-l02|a806|sm-j700t|one me dual sim|desire 626|k220|r7sm|p7000|h60-l01|h811|x501|k535|vs986|ale-l04|e5353|a936|noir x900|z90-7|x2-eu|s90-u|ao5510|blade a510|d955|h950|life one x|lgl24|r7s|desire 728g dual sim|ms631|desire 828|h631|h810|desire 626 dual sim|x5004|s551|x600|d820us|k580|f180|d820pi|us991|lgv32|life one x2|desire 820q|che-tl00h|vivo 5|life s5004|vivo xl|d950|p55|d6563|lgv31|ls990|xm50h|h60-l12|k500|ze600kl (z00md)|k500n|e5303|life xl|d802tr|s6s|e5306|h635a|one e9 dual sim|kiw-tl00h|s308|chm-tl00h|f650k|d628u|d816d|c6916|a6020l36|p5000|ls996|d820t|v7a|h60-l03|ls775|che-tl00|ls991|a350|d6643|d802t|a1001|s950|f1fw|ath-tl00|v10|chm-tl00|e455|blade l6|elife e7 mini|s550|diamond s|vns-dl00|blade x9|one e9s dual sim|rm-1074|kiw-tl00|d722ar|d816w|k550|note 3|f3213|p55 novo|t84|d5316|c6906|e481|rm-1067|r7s plus|x2-to|chc-u03|6039a|c6943|0pja2|e313|rio-l03|e484|e5506|ms550|highway 4g|ls675|h520|r106|vns-l23|r7c|a310|p70-t|e311|tit-tl00|c55|t50|blade v580|grand 2|yu5530|a2015|g900w|vivo 5r|zero|h630d|h815t|u10|yu5510|e5606|irulu v3|c2016|spark +|r8201|h542|f650l|d816t|d693n|nem-ul10|crr-ul20|noir z8|x509|discovery 2|a7020a40|blade a475|life one xl|c20|le max|a808t|hercules|emax|primo s2|kii-l23|d693tr|eluga icon|padfone x|blade s|k540|ls660|cynus f7|s60-w|d6543|ice 2|6039j|blade v220|smart 505|d690n|a1p|vns-l53|y29l|smart e4|l39h|vs880pp|r7t|diamond 2 plus|prime 558|studio 6.0 lte|2pq93|6039k|g735-l03|diamond plus|5.0q|fire2 lte|storm|m2 mini|6607|s800c|p81|vs835|iris x5 4g|eluga u|life mark|iris x5|f3 pro|max2 plus|e485|l60|d852g|primo rm2|pure xl|a6020a41|z100|max2|gsmart mika m2|primo z|sc-ul10|us610|e352|n9518|k530|l82vl|up ultra|x2 soul|a316|6032a|x5001|blade l3 plus|eluga mark|z90-3|kii-l05|k32c36|s90-t|g628-tl00|discovery 2+|6039h|le x528|life one m|primo zx|d693ar|blade a511|nova x70|gsmart akta a4|g735-l23|blade a5|6039s|e471|d816|vision h3se|h815k|zielo z-500|p7 max|a7600m|d693|a66a|eluga switch|via m1|mi4|s58|studio 5.0 hd lte|r1c|a24|y200|a6020l37|classic pro|diamond v7|y90|preo p2|d5306|note|primo rx2|kiw-cl00|omega|q417|e500|omega pro|e550|p7-l05|6032x|p7-l12|3s m3g|xt1564|prime|blade hn|omega 5.5|primo v1|smart pro|fire plus 3g|x2 soul xtreme|d626w|fire plus 4g|x2 soul pro|note pro|a66s|a75l|x2 soul style plus)'
)
AND carrier IN ('True Move', 'AIS', 'TOT')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA')
AND dtac.tapad_id IS NULL;

/*

+--------------------------------+
| facebook mobile advertiser ids |
+--------------------------------+
| 576940                         |
+--------------------------------+

*/

/*cluster 1_SS*/

SELECT COUNT(DISTINCT omo.tapad_id) as 'Facebook Mobile Advertiser Ids' FROM apollo.dtac_vertical_dataset omo
LEFT JOIN (SELECT tapad_id FROM apollo.dtac_vertical_dataset WHERE
(user_agent REGEXP '(?i)(sm-j700f|sm-j710f|sm-g610f|sm-a500f|sm-a700fd|sm-a510f|sm-j500g|sm-a800f|sm-j510fn|sm-e700f|sm-a800i|sm-j500h|sm-g570f|sm-g750f|sm-a310f|sm-g903f|sm-j500fn|sm-j700h|sm-a500fu|shv-e300s|sm-j710gn|sm-j500f|shv-e300k|sm-j5007|sm-a500h|sm-g531y|sm-j510gn|sm-e700h|sm-g930s|sm-g720n0|sm-a500yz|sm-a800iz|sm-a500g|sm-c7000|sm-a5000|sm-g930k|sm-j5108|sm-a8000|ek-gc100|sm-g930l|sm-a800yz|sm-n916s|shv-e300l|sm-j7108|sm-j700m|sm-a500l|sm-a800s|sm-j500m|sm-c5000|sm-j320yz|sm-j500n0|sm-n916l|sm-a500y|sm-j710k|sm-g903w|sm-g930fd|sm-g6000|sm-a500s|sm-j500y|sm-j510h|sm-j700k|sm-a500k|sm-g903m|sm-a5100|sm-a510y|sm-g891a|sm-g930u|sm-a310n0|sm-a500m|sm-j5008|sm-g7202|sm-j510l|sm-j710mn|sm-j510s|sm-a510s|sm-j710fn|sm-a500w|sm-a510k|sm-a510l|sm-j7008|scv32|sm-j510k|sm-j700t1|sm-j510mn|sm-g7200|sm-a500|sm-a310y|sm-j320w8|sm-a7009|sm-j700p|ek-gc110|sm-j710fq|sm-a310m|sm-g930az|sm-s902l|sm-e700m|sm-t285m|sgh-s970g|sm-e7009)'
)
AND carrier IN ('DTAC')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA') and year = 2017 and ((month = 1 and day > 15) or month = 2)
) as dtac ON omo.tapad_id = dtac.tapad_id
WHERE
(user_agent REGEXP '(?i)(sm-j700f|sm-j710f|sm-g610f|sm-a500f|sm-a700fd|sm-a510f|sm-j500g|sm-a800f|sm-j510fn|sm-e700f|sm-a800i|sm-j500h|sm-g570f|sm-g750f|sm-a310f|sm-g903f|sm-j500fn|sm-j700h|sm-a500fu|shv-e300s|sm-j710gn|sm-j500f|shv-e300k|sm-j5007|sm-a500h|sm-g531y|sm-j510gn|sm-e700h|sm-g930s|sm-g720n0|sm-a500yz|sm-a800iz|sm-a500g|sm-c7000|sm-a5000|sm-g930k|sm-j5108|sm-a8000|ek-gc100|sm-g930l|sm-a800yz|sm-n916s|shv-e300l|sm-j7108|sm-j700m|sm-a500l|sm-a800s|sm-j500m|sm-c5000|sm-j320yz|sm-j500n0|sm-n916l|sm-a500y|sm-j710k|sm-g903w|sm-g930fd|sm-g6000|sm-a500s|sm-j500y|sm-j510h|sm-j700k|sm-a500k|sm-g903m|sm-a5100|sm-a510y|sm-g891a|sm-g930u|sm-a310n0|sm-a500m|sm-j5008|sm-g7202|sm-j510l|sm-j710mn|sm-j510s|sm-a510s|sm-j710fn|sm-a500w|sm-a510k|sm-a510l|sm-j7008|scv32|sm-j510k|sm-j700t1|sm-j510mn|sm-g7200|sm-a500|sm-a310y|sm-j320w8|sm-a7009|sm-j700p|ek-gc110|sm-j710fq|sm-a310m|sm-g930az|sm-s902l|sm-e700m|sm-t285m|sgh-s970g|sm-e7009)'
)
AND carrier IN ('True Move', 'AIS', 'TOT')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA')
AND dtac.tapad_id IS NULL;

/*
+--------------------------------+
| facebook mobile advertiser ids |
+--------------------------------+
| 162870                         |
+--------------------------------+
*/

/*cluster 2_OEM*/

SELECT COUNT(DISTINCT omo.tapad_id) as 'Facebook Mobile Advertiser Ids' FROM apollo.dtac_vertical_dataset omo
LEFT JOIN (SELECT tapad_id FROM apollo.dtac_vertical_dataset WHERE
(user_agent REGEXP '(?i)(smart 4g speedy 4.0|smart max 4.0|iris 810|iris 700|iris 600|iris 510|iris 550|iris 360m|iris 708|iris 800|i-style 7.7 dtv|smart 4.0|blade a110|smart speedy 4g 4.0|iris 500|smart 3.5 touch|blade d6 lite 3g|blade q3|smart 3.5|iris 405+|i-style 710|blade l3|blade d6 lite 4g|i-style 217|i-style 2.4a|v815w|v830|i-style 216|v811w|smart 5.0|blade l2|blade vec 4g|v993w|blade v7|open c|blade a310|blade q lux|blade q1|i-style 4|blade l5|z812|iris x8q|iris x9|iris 550q|iris 705|blade a462|z717vl|iris fuel 50|blade c341|z818l|blade a476|z828|z831|iris x1|blade l2 plus|iris x1 atom|z955a|blade a112|z820|iris x8|iris x1 selfie|n9130|n9132|t311|iris x1 grand|iris atom 2x|iris x1+|blade apex 3|z850|blade q mini|beeline smart 2|blade q maxi|q505t|z832|iris 250|iris fuel 60|z936l|blade a506|t816|iris fuel f1|iris 406q|blade a5 pro|z933|blade b112|iris alfa l|iris x1 mini|iris atom x|t221|kis 3 max|z797c|blade a465|iris x1 beats|blade a430|blade a470|iris pro 30|blade a450|iris 460|iris selfie 50|iris alfa|iris fuel f1 mini|iris pro 20|iris fuel 25|iris 400s|iris 352|iris 325 style)'
)
AND carrier IN ('DTAC')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA') and year = 2017 and ((month = 1 and day > 15) or month = 2)
) as dtac ON omo.tapad_id = dtac.tapad_id
WHERE
(user_agent REGEXP '(?i)(smart 4g speedy 4.0|smart max 4.0|iris 810|iris 700|iris 600|iris 510|iris 550|iris 360m|iris 708|iris 800|i-style 7.7 dtv|smart 4.0|blade a110|smart speedy 4g 4.0|iris 500|smart 3.5 touch|blade d6 lite 3g|blade q3|smart 3.5|iris 405+|i-style 710|blade l3|blade d6 lite 4g|i-style 217|i-style 2.4a|v815w|v830|i-style 216|v811w|smart 5.0|blade l2|blade vec 4g|v993w|blade v7|open c|blade a310|blade q lux|blade q1|i-style 4|blade l5|z812|iris x8q|iris x9|iris 550q|iris 705|blade a462|z717vl|iris fuel 50|blade c341|z818l|blade a476|z828|z831|iris x1|blade l2 plus|iris x1 atom|z955a|blade a112|z820|iris x8|iris x1 selfie|n9130|n9132|t311|iris x1 grand|iris atom 2x|iris x1+|blade apex 3|z850|blade q mini|beeline smart 2|blade q maxi|q505t|z832|iris 250|iris fuel 60|z936l|blade a506|t816|iris fuel f1|iris 406q|blade a5 pro|z933|blade b112|iris alfa l|iris x1 mini|iris atom x|t221|kis 3 max|z797c|blade a465|iris x1 beats|blade a430|blade a470|iris pro 30|blade a450|iris 460|iris selfie 50|iris alfa|iris fuel f1 mini|iris pro 20|iris fuel 25|iris 400s|iris 352|iris 325 style)'
)
AND carrier IN ('True Move', 'AIS', 'TOT')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA')
AND dtac.tapad_id IS NULL;

/*
+--------------------------------+
| facebook mobile advertiser ids |
+--------------------------------+
| 51222                          |
+--------------------------------+
*/


/*cluster 2_OTH*/

SELECT COUNT(DISTINCT omo.tapad_id) as 'Facebook Mobile Advertiser Ids' FROM apollo.dtac_vertical_dataset omo
LEFT JOIN (SELECT tapad_id FROM apollo.dtac_vertical_dataset WHERE
(user_agent REGEXP '(?i)(t00j|a37f|y51|a33w|1201|r2001|iris 750|a11w|a33f|a51f|y28|y31l|r831k|r1001|cun-l21|zc500tg|a2010|y31|a450cg (t00q)|one m8|a6000|lua-l21|zc451cg (z007)|t00i|a2020a40|a859|p1ma40|r831l|z520|r831|redmi note 3|zb452kg (x014d)|y541-u02|a1000|a536|y625-u43|rm-1013|scl-u23|z150|lumia 535|hol-u19|a526|z205|r1011|rm-1075|magic|d335|z500|y600-u20|rm-1068|d2533|rm-1040|rm-1141|g6-u251|h502|a6010|d2303|g620s-l02|z528/t07|s800|y600-u151|pro|d618|me572cl (k00r)|omega lite 2|zc451cg (t00p)|rm-974|s660|t00f|d325|z220|z160|highway signs|p780|a316i|y520-u33|c230w|desire 620g|h324|rm-1031|g6-u10|desire 310|z330|p10|z410|d2302|e39|d2403|lumia 640 dual sim|e2303|d2305|wax|y520-u22|rm-1152|hongmi 2|d5103|a889|slate6 voicetab|lumia 550|aqua n8|xt1068|y625-u32|scc-u21|fire j1|d2005|d2306|d2004|desire 526g plus|d2203|goa|hm 1sw|moto e2|spin mini|a1000s|hm 1s|hol-u10|y336-u02|g730-u30|h525n|lua-u22|rm-1017|g620-l72|g620s-ul00|scl-l21|lumia 430|start 2|xt1039|a51w|h440n|d722|smart 9|a606|d620|x12|e400|smart 3|h340n|xt1072|y330-u11|desire 510|a319|k350|fire hd 6|d2212|h735|marathon m4|d2202|desire 620|d331|a600|d405|d320|a502cg (t00k)|note s|ms323|305sh|s856|d170|526g|m11|smart 5|z200|h635|sm-g530t|y330-c00|k10000|smart 6|xt1022|xt1021|y51l|t11|p11|desire 626s|k120|d816h|d724|a106|a3600-d|m210|sm-g386t|zc451tg (z00sd)|a37m|a33m|desire 320|p5w|x210|hm 1sc|h500|a65a|r831s|ms330|ms345|sm-g360v|x2-cu|sm-g360p|d295|d290|note ds6|ascend p7 mini|x405|y51a|a102|h420|a396|a399|sm-j320vpp|elife s5.1|d280|sm-j320p|g535-l11|a616|desire 210 dual sim|d380|h440ar|desire 526|ls770|k30-w|aq5001|desire 501 dual sim|x145|y340-u081|a33|y635-cl00|marathon m5 lite|d315|cam-tl00h|a3600|q380|k130|y560-u23|d2104|rm-1090|smart 4|y31a|y560-u02|h340|yu5010a|8079|y530-u051|a109|ls740|k100|sm-g530p|lua-l02|n9000w|x200|lumia 735 lte|h340ar|h634|d160|d390n|6030a|q1010i|306sh|studio 5.0 c hd|studio energy|520 s hd|s898t+|d723|vivo air lte|e560|studio energy 2|benefit m5 plus|h30-l01m|h343|a177|studio 6.0 hd|y28l|6030d|h736|a120|y360-u03|6033x|k332|y560-l02|k30-t|scl-l03|aq4501|f370l|t815|q450|life 8xl|f370k|h440|h30-c00|f540s|a104|g13|a121|eluga i2|g6-t00|startrail 7|a529|y33|xt1023|power five|g620s-l03|y518-t00|d285|a107|d337|power five pro|m2 3g|emax mini|m9 plus|power ice|scl-cl00|s401|birdy|padfone x mini|p5001|sm-g360az|q-smart qs16|y635-l03|h630|yu5010|k121|a116|a290|q391|fresh|t012|yu4711|sm-j320az|ms395|cynus e5|smart fly x50|rm-1114|y360-u72|marathon m5 mini|r831t|forward young|xt1042|g30|q-smart qs550t|p713tr|6030x|y550-l03|omega xl|y516-t00|rono|pro5043|lumia 535 dual sim|d2206|smart 35|g31|l-ite 502|x220|y560-cl00|n907|q372|sc-cl00|a190|scl-l04|k210|d725|omega hd 3|eluga i|d2406|eluga s|rm-1072|h525|discovery 2 mini|j620|d370|d335e|ls751|hol-t00|a788t|eluga a2|s660w|d610ar|402lg|smart 11|l34c|e5001|a2010-a|sm-g800a|w3500|6036a|pure f|rm-1113|pgn518|e100t|power rage|cun-l02|e86|402zt|smart sprint|t40|m310|hs-u601|noir i9|q700s|rio play|rook|e501_eu|s580a|s520|n9005|g620-a2|h342|power four|a358t|3320a|s29|q386|d5106|a564c|q-smart qs17|t3100|neo 4.5|y330-u07|y520-u03|p5004|vivo air|y520-u12|h731|a092|s307|mega|d2243|h442|g6-l33|st-551|president smart 5|aq4502|y536a1|p66|p7-l11|d221|d213|y221-u12|a338t|k9 smart|xt1524|intouch 3|k450|xt1019|a328t|note delight 1|hd new|y360-u93|rm-1039|h636|ls755|c230|a5z|d2-f|noir s1|l33l|dash 5.0|7576u|v12|winwin|primo x3 mini|r2010|hs-g610|be x|ls-5008|xt830c|honor classic p6|501o|e5004|a26|smart start|e51|a208t|x002|q371|q426|be pure|a74c|bolt s300|a74d|s19|sm-g530r4|d722j|a708t|q385|rm-977|d390|benefit m2|x198|smart mini s620|bolt q324|opl4100|x507|d722v|noir i5i|primo gm mini|prime 351d|d105|rm-1092|evo 4g|primo gm|a1900|go984|dash j|noir x30|sm-j320r4|a093|president smart 1|d390ar|x135|y360-u82|noir i5 3g|mobile volt|i777|up hd|q1001|easyphone 6|connect 501|life play mini|y625-u13|d393|xt1025|a94|fire 2|eluga l 4g|primo rh2|ixion m4|d385|smart run 4g|noir s3|ixion ml 4.5|0p6b140|x500a|smart a80|studio m hd|q350|a70|f540l|a37|vs876|p41|d320ar|q345|a74a|noir i7|d107|x2 soul lite|prime s|k200|d375ar|x60|color|bravo z11|sm-g3568v|d100ar|y538|president smart 2|lua-l01|y600-u40|q327|primo ef|magic swift|pro 2|primo gh2|life 8|life play s|y635-tl00|easy smart f2|777|prime 5.0 plus|x250|primo f4|q390|a1010|a108|primo s3 mini|primo gh+|gsmart rey r3|i504|a118r|gsmart arty a3|d120|a218t|a3900|primo h3|gsmart saga s3|desire 620 dual sim|smart e5|a114r|primo hm mini|a50c+|p500m|president smart a98|a330|gsmart t4|primo hm|lgl43al|pro 3|joy smart a6|y330-u15|6036x|500d|storm x450|note e|i503|butterfly m1|v4005|s4012|imsmart a501|a851l|s7503|a37b|smart a75|sm-g800r4|primo e2|smart|y336-a1|p5005a)'
)
AND carrier IN ('DTAC')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA') and year = 2017 and ((month = 1 and day > 15) or month = 2)
) as dtac ON omo.tapad_id = dtac.tapad_id
WHERE
(user_agent REGEXP '(?i)(t00j|a37f|y51|a33w|1201|r2001|iris 750|a11w|a33f|a51f|y28|y31l|r831k|r1001|cun-l21|zc500tg|a2010|y31|a450cg (t00q)|one m8|a6000|lua-l21|zc451cg (z007)|t00i|a2020a40|a859|p1ma40|r831l|z520|r831|redmi note 3|zb452kg (x014d)|y541-u02|a1000|a536|y625-u43|rm-1013|scl-u23|z150|lumia 535|hol-u19|a526|z205|r1011|rm-1075|magic|d335|z500|y600-u20|rm-1068|d2533|rm-1040|rm-1141|g6-u251|h502|a6010|d2303|g620s-l02|z528/t07|s800|y600-u151|pro|d618|me572cl (k00r)|omega lite 2|zc451cg (t00p)|rm-974|s660|t00f|d325|z220|z160|highway signs|p780|a316i|y520-u33|c230w|desire 620g|h324|rm-1031|g6-u10|desire 310|z330|p10|z410|d2302|e39|d2403|lumia 640 dual sim|e2303|d2305|wax|y520-u22|rm-1152|hongmi 2|d5103|a889|slate6 voicetab|lumia 550|aqua n8|xt1068|y625-u32|scc-u21|fire j1|d2005|d2306|d2004|desire 526g plus|d2203|goa|hm 1sw|moto e2|spin mini|a1000s|hm 1s|hol-u10|y336-u02|g730-u30|h525n|lua-u22|rm-1017|g620-l72|g620s-ul00|scl-l21|lumia 430|start 2|xt1039|a51w|h440n|d722|smart 9|a606|d620|x12|e400|smart 3|h340n|xt1072|y330-u11|desire 510|a319|k350|fire hd 6|d2212|h735|marathon m4|d2202|desire 620|d331|a600|d405|d320|a502cg (t00k)|note s|ms323|305sh|s856|d170|526g|m11|smart 5|z200|h635|sm-g530t|y330-c00|k10000|smart 6|xt1022|xt1021|y51l|t11|p11|desire 626s|k120|d816h|d724|a106|a3600-d|m210|sm-g386t|zc451tg (z00sd)|a37m|a33m|desire 320|p5w|x210|hm 1sc|h500|a65a|r831s|ms330|ms345|sm-g360v|x2-cu|sm-g360p|d295|d290|note ds6|ascend p7 mini|x405|y51a|a102|h420|a396|a399|sm-j320vpp|elife s5.1|d280|sm-j320p|g535-l11|a616|desire 210 dual sim|d380|h440ar|desire 526|ls770|k30-w|aq5001|desire 501 dual sim|x145|y340-u081|a33|y635-cl00|marathon m5 lite|d315|cam-tl00h|a3600|q380|k130|y560-u23|d2104|rm-1090|smart 4|y31a|y560-u02|h340|yu5010a|8079|y530-u051|a109|ls740|k100|sm-g530p|lua-l02|n9000w|x200|lumia 735 lte|h340ar|h634|d160|d390n|6030a|q1010i|306sh|studio 5.0 c hd|studio energy|520 s hd|s898t+|d723|vivo air lte|e560|studio energy 2|benefit m5 plus|h30-l01m|h343|a177|studio 6.0 hd|y28l|6030d|h736|a120|y360-u03|6033x|k332|y560-l02|k30-t|scl-l03|aq4501|f370l|t815|q450|life 8xl|f370k|h440|h30-c00|f540s|a104|g13|a121|eluga i2|g6-t00|startrail 7|a529|y33|xt1023|power five|g620s-l03|y518-t00|d285|a107|d337|power five pro|m2 3g|emax mini|m9 plus|power ice|scl-cl00|s401|birdy|padfone x mini|p5001|sm-g360az|q-smart qs16|y635-l03|h630|yu5010|k121|a116|a290|q391|fresh|t012|yu4711|sm-j320az|ms395|cynus e5|smart fly x50|rm-1114|y360-u72|marathon m5 mini|r831t|forward young|xt1042|g30|q-smart qs550t|p713tr|6030x|y550-l03|omega xl|y516-t00|rono|pro5043|lumia 535 dual sim|d2206|smart 35|g31|l-ite 502|x220|y560-cl00|n907|q372|sc-cl00|a190|scl-l04|k210|d725|omega hd 3|eluga i|d2406|eluga s|rm-1072|h525|discovery 2 mini|j620|d370|d335e|ls751|hol-t00|a788t|eluga a2|s660w|d610ar|402lg|smart 11|l34c|e5001|a2010-a|sm-g800a|w3500|6036a|pure f|rm-1113|pgn518|e100t|power rage|cun-l02|e86|402zt|smart sprint|t40|m310|hs-u601|noir i9|q700s|rio play|rook|e501_eu|s580a|s520|n9005|g620-a2|h342|power four|a358t|3320a|s29|q386|d5106|a564c|q-smart qs17|t3100|neo 4.5|y330-u07|y520-u03|p5004|vivo air|y520-u12|h731|a092|s307|mega|d2243|h442|g6-l33|st-551|president smart 5|aq4502|y536a1|p66|p7-l11|d221|d213|y221-u12|a338t|k9 smart|xt1524|intouch 3|k450|xt1019|a328t|note delight 1|hd new|y360-u93|rm-1039|h636|ls755|c230|a5z|d2-f|noir s1|l33l|dash 5.0|7576u|v12|winwin|primo x3 mini|r2010|hs-g610|be x|ls-5008|xt830c|honor classic p6|501o|e5004|a26|smart start|e51|a208t|x002|q371|q426|be pure|a74c|bolt s300|a74d|s19|sm-g530r4|d722j|a708t|q385|rm-977|d390|benefit m2|x198|smart mini s620|bolt q324|opl4100|x507|d722v|noir i5i|primo gm mini|prime 351d|d105|rm-1092|evo 4g|primo gm|a1900|go984|dash j|noir x30|sm-j320r4|a093|president smart 1|d390ar|x135|y360-u82|noir i5 3g|mobile volt|i777|up hd|q1001|easyphone 6|connect 501|life play mini|y625-u13|d393|xt1025|a94|fire 2|eluga l 4g|primo rh2|ixion m4|d385|smart run 4g|noir s3|ixion ml 4.5|0p6b140|x500a|smart a80|studio m hd|q350|a70|f540l|a37|vs876|p41|d320ar|q345|a74a|noir i7|d107|x2 soul lite|prime s|k200|d375ar|x60|color|bravo z11|sm-g3568v|d100ar|y538|president smart 2|lua-l01|y600-u40|q327|primo ef|magic swift|pro 2|primo gh2|life 8|life play s|y635-tl00|easy smart f2|777|prime 5.0 plus|x250|primo f4|q390|a1010|a108|primo s3 mini|primo gh+|gsmart rey r3|i504|a118r|gsmart arty a3|d120|a218t|a3900|primo h3|gsmart saga s3|desire 620 dual sim|smart e5|a114r|primo hm mini|a50c+|p500m|president smart a98|a330|gsmart t4|primo hm|lgl43al|pro 3|joy smart a6|y330-u15|6036x|500d|storm x450|note e|i503|butterfly m1|v4005|s4012|imsmart a501|a851l|s7503|a37b|smart a75|sm-g800r4|primo e2|smart|y336-a1|p5005a)'
)
AND carrier IN ('True Move', 'AIS', 'TOT')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA')
AND dtac.tapad_id IS NULL;

/*

+--------------------------------+
| facebook mobile advertiser ids |
+--------------------------------+
| 149499                         |
+--------------------------------+

*/



/*cluster 2_SS*/

SELECT COUNT(DISTINCT omo.tapad_id) as 'Facebook Mobile Advertiser Ids' FROM apollo.dtac_vertical_dataset omo
LEFT JOIN (SELECT tapad_id FROM apollo.dtac_vertical_dataset WHERE
(user_agent REGEXP '(?i)(sm-j200gu|sm-g530f|sm-g360h|sm-j100ml|sm-g360hu|sm-e500h|sm-j120h|sm-g355h|sm-g355m|sm-j105b|sm-g361hu|sm-g532|sm-g313m|sm-g313hu|sm-g530fz|sm-g318mz|sm-g316hu|sm-g316m|sm-g130hn|sm-g800f|sm-g530h|gt-i9060|sm-g313ml|sm-g313hz|sm-g130m|sm-a300fu|sm-g313h|sm-j100h|gt-i9060i|sm-g530y|sm-g531f|sm-g531h|sm-j320fn|sm-j110g|sm-g357fz|sm-j320f|sm-j111f|sm-g361f|sm-a300f|gt-i8200n|sm-g360g|sm-g388f|sm-j320g|sm-j200h|sm-g360f|sm-g800h|sm-j200g|sm-a300h|sm-j120g|sm-g5308w|sm-g389f|sm-j200y|sm-j200f|sm-j110h|sm-j320h|sm-g600fy|sm-j120fn|sm-j100f|sm-g386f|sm-j120f|gt-i8200|sm-j110f|sm-g5500|sm-j100y|sm-g710k|sm-g530w|sm-a300y|sm-g355hn|sm-g710s|sm-g530az|sm-j210f|sm-j320a|sm-g800y|sm-j3109|sm-j105h|sm-g710|sm-a300g|sm-a510m|sm-g530m|sm-g3586v|sm-a300yz|sm-g7108v|sm-j200m|sm-g386w|sm-g313hn|sm-g313f|sm-a300m|sm-g710l|sm-g360t|sm-g360gy|sm-g550t|sm-j105f|sm-g360m|sm-j100fn|gt-i9060c|sm-g530t1|sm-j110m|sm-g531m|sm-j120zn|gt-s7580l|sm-g386t1|sm-a3000|gt-s7390g|sm-j111m|sm-g350e|sm-j320m|sm-j320n|sm-g530mu|gt-i9060l|sm-g150ns|gt-i8200l|sm-g550fy|sm-g550t1|sm-j320zn|sm-j200bt|sm-g310hn|sm-j120a|sm-g110h|sm-s820l|sm-j320y|gt-i9060m|sm-j100mu|sm-j120w|sm-g7109|sph-l710t|shw-m570s|sm-g530bt|sm-g600s|gt-i8200q|sm-e500yz|sm-g360bt|sm-g313mu|sch-i939i|sm-j120az|sm-j100vpp|sm-g7108 td|gt-i8580|sm-g357m|sm-g800m|sm-j110l|sm-j105m|sm-e500m|sm-g531bt|sm-g5306w|sm-j120m|sm-g3518|sch-i679|sm-j100m|sch-i939d|sm-z200f|sm-a3009|sm-g800hq|sm-z300h|sm-g130e|sm-s765c|sm-g313u|sm-g3858|sm-s550tl|sm-g360fy|sm-g550t2|sm-z130h|sm-g3502c|sm-s120vl|sm-g316ml|sm-g5309w|sm-g313hy|sm-g3502l|sm-g3812b|sm-s766c|sm-g355hq|sm-g316u|sm-g316my|sm-g800x|sm-g3609|sm-g530r7|sm-g3139d|sm-g386u|sm-g310r5|sm-g3556d|sm-g130bt)'
)
AND carrier IN ('DTAC')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA') and year = 2017 and ((month = 1 and day > 15) or month = 2)
) as dtac ON omo.tapad_id = dtac.tapad_id
WHERE
(user_agent REGEXP '(?i)(sm-j200gu|sm-g530f|sm-g360h|sm-j100ml|sm-g360hu|sm-e500h|sm-j120h|sm-g355h|sm-g355m|sm-j105b|sm-g361hu|sm-g532|sm-g313m|sm-g313hu|sm-g530fz|sm-g318mz|sm-g316hu|sm-g316m|sm-g130hn|sm-g800f|sm-g530h|gt-i9060|sm-g313ml|sm-g313hz|sm-g130m|sm-a300fu|sm-g313h|sm-j100h|gt-i9060i|sm-g530y|sm-g531f|sm-g531h|sm-j320fn|sm-j110g|sm-g357fz|sm-j320f|sm-j111f|sm-g361f|sm-a300f|gt-i8200n|sm-g360g|sm-g388f|sm-j320g|sm-j200h|sm-g360f|sm-g800h|sm-j200g|sm-a300h|sm-j120g|sm-g5308w|sm-g389f|sm-j200y|sm-j200f|sm-j110h|sm-j320h|sm-g600fy|sm-j120fn|sm-j100f|sm-g386f|sm-j120f|gt-i8200|sm-j110f|sm-g5500|sm-j100y|sm-g710k|sm-g530w|sm-a300y|sm-g355hn|sm-g710s|sm-g530az|sm-j210f|sm-j320a|sm-g800y|sm-j3109|sm-j105h|sm-g710|sm-a300g|sm-a510m|sm-g530m|sm-g3586v|sm-a300yz|sm-g7108v|sm-j200m|sm-g386w|sm-g313hn|sm-g313f|sm-a300m|sm-g710l|sm-g360t|sm-g360gy|sm-g550t|sm-j105f|sm-g360m|sm-j100fn|gt-i9060c|sm-g530t1|sm-j110m|sm-g531m|sm-j120zn|gt-s7580l|sm-g386t1|sm-a3000|gt-s7390g|sm-j111m|sm-g350e|sm-j320m|sm-j320n|sm-g530mu|gt-i9060l|sm-g150ns|gt-i8200l|sm-g550fy|sm-g550t1|sm-j320zn|sm-j200bt|sm-g310hn|sm-j120a|sm-g110h|sm-s820l|sm-j320y|gt-i9060m|sm-j100mu|sm-j120w|sm-g7109|sph-l710t|shw-m570s|sm-g530bt|sm-g600s|gt-i8200q|sm-e500yz|sm-g360bt|sm-g313mu|sch-i939i|sm-j120az|sm-j100vpp|sm-g7108 td|gt-i8580|sm-g357m|sm-g800m|sm-j110l|sm-j105m|sm-e500m|sm-g531bt|sm-g5306w|sm-j120m|sm-g3518|sch-i679|sm-j100m|sch-i939d|sm-z200f|sm-a3009|sm-g800hq|sm-z300h|sm-g130e|sm-s765c|sm-g313u|sm-g3858|sm-s550tl|sm-g360fy|sm-g550t2|sm-z130h|sm-g3502c|sm-s120vl|sm-g316ml|sm-g5309w|sm-g313hy|sm-g3502l|sm-g3812b|sm-s766c|sm-g355hq|sm-g316u|sm-g316my|sm-g800x|sm-g3609|sm-g530r7|sm-g3139d|sm-g386u|sm-g310r5|sm-g3556d|sm-g130bt)'
)
AND carrier IN ('True Move', 'AIS', 'TOT')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA')
AND dtac.tapad_id IS NULL;

/*
+--------------------------------+
| facebook mobile advertiser ids |
+--------------------------------+
| 89400                          |
+--------------------------------+
*/

/*cluster 3*/

SELECT COUNT(DISTINCT omo.tapad_id) as 'Facebook Mobile Advertiser Ids' FROM apollo.dtac_vertical_dataset omo
LEFT JOIN (SELECT tapad_id FROM apollo.dtac_vertical_dataset WHERE
(user_agent REGEXP '(?i)(gt-i8552b|gt-i8552|gt-s7582l|gt-s7582|i-style q4|i-style 2|iris 353|gt-s7270l|gt-s7270|gt-i8262|gt-s7562|gt-i8262b|r815t|a680|i5-77|iris 456|rm-914|iris 505|i-style 7.1|gt-i9100|blade c310|r821|gt-i8190n|rm-941|i-style 7.5|rm-821|gt-i8190t|gt-i9100t|i-style 7.3|gt-i8160l|rm-885|c5302|iris 354|z130|gt-i9070|gt-s5310|gt-i8160|xt1032|gt-s5360|rm-846|gt-s5310b|a369i|gt-i9250|rm-825|gt-s6310|gt-s5360b|gt-s6810p|adr6400l|gt-s6310b|shv-e210s|a56|a390|gt-s6102|y511-u251|gt-i8190|y511-u30|gt-s7500l|shv-e210k|shv-e160s|start 1|gt-s7500|c2105|g610-u20|shv-e160k|a850|c1905|gt-s5830|lt22i|a706|gt-s5830i|i-style 8|c2104|p880|p768|c1505|shv-e160l|shv-e270k|gt-i9105p|a516|gt-s7580|gt-s5300b|life|gt-s5570|gt-s5300|q360|gt-s5830t|c1904|v370|q10|gt-s7275r|i-style q3|g510-0251|a269i|c5303|a44|desire 500|xt1033|gt-i8730|hongmi|a850+|s510e|smart 8|g610-u00|g610-t11|p714|one v|gt-s5660|a48|gt-i9001|gt-i9003|p760|sm-g350|e430|e451g|shv-e210l|shv-e270s|gt-i9100g|gt-i8260|rm-823|st26i|shv-e275k|9860|shv-e120s|c110e|p713|rm-917|gt-i8350|st26a|gt-s7710|shv-e220s|sm-g3815|xt910|shv-e275s|desire 601|sp ai|gt-s5380|st23i|i-style 7.2|gt-s7560|shv-e110s|y330-u01|a850i|e612|i-style 6|mi 1|mt25i|sgh-t989|gt-e3300v|500|i-style 3|y511-t00|603|f160l|f160k|st27i|gt-i9103|gt-s7392|a47|gt-e3309i|gt-s7262|a502|gt-i9105|p895|w100|st25i|e440|yp-g70|gt-s7390|gt-s3850|shv-e270l|e425|mt620|xt535|gt-i8730t|p710|st27a|mt27i|iris 455|a11 (t00c)|z120|s510b|gt-s6310n|p716|st21i|gt-i8190l|gt-s7275t|lu6200|a75|gt-s8600|gt-i8150|desire 300|i-style q1|a702 andy|a78|f160s|k800|gt-s7272|xt912|hongmi 4g|p720|liquid z3|e615|gt-i9100p|mb865|gt-i9210|gt-s3800w|i-style 5|p765|sgh-t989d|a320e|iris 405|e610|gt-s7583t|f160lv|sgh-i777|a560|rm-835|e460|pg76110|y511-u00|gt-s5301|e425f|p715|gt-i8160p|f200|p769|a74|gt-i8260l|gt-s5690|rm-820|s750|sgh-t599n|gt-s5312|sph-d710|sgh-t399|x515m|u8815|gt-s5570i|gt-c3312|gt-s6810|875|gt-s6312|smart 7|gt-s5839i|sgh-t399n|xt320|sgh-i577|y320-u01|pioneer p2|y511-u10|t31|q1000|gt-i8550|ms500|a820|y320-u30|st21a|t375|lu5400|a369|sm-g3812|mb860|gt-s6500|d505|sgh-t769|liyang z9|g510-0200|iris 351|sch-r530m|cynus e1|shw-m340k|a390t|gt-i9210t|a19|gt-c3312r|e445|gt-i9070p|shw-m240s|9380|jy-f1|f120l|l-05d|sgh-t959v|sgh-i997|noir a12|a61|gt-s6790n|vs930|a28|q-smart qs550|mi 2sc|g610-u15|mi 1s|gt-s7275y|gt-i8262d|xt1028|e435|f120s|blade q|c510e|u8655|a690|gt-i8550l|x40|d500|as7|sph-d710bst|sch-r530u|sph-d720|velocity 4g|sm-g3819|y300-0000|i-style q5a|gt-s7572|desire 500 dual sim|gt-e2202|a238t|smart 4g|sgh-i547|sensation 4g|pm36100|c5306|evo v 4g|t8951|p659|q-smart s16|102e|w1-u00|st21i2|infinity power|y320-c00|u9202l|dash 3.5|ms770|a868|q800|iris 402|star jr|u8665|sm-g3509|sky s777|su870|m032|xt894|sch-i200|gt-i9100m|sch-r830|v970|sch-r530c|ms870|studio 5.5 s|gt-s5310i|rm-878|ms769|ls720|mi one plus|u8510|g1305 boston|yp-gb1|g526-l22|gt-i9082c|y325-t00|u8825|n820|gt-e3309|gt-b5330|x515e|sch-i939|sm-g730w8|sgh-i437p|975n|y220-u00|gt-s6810l|limo x2|9810|x710e|kis ii max|mb855|gt-s5280|e465g|yp-gi1c|y210-0200|a25|sch-r740c|v880g|pm23300|gt-s7390l|603e|y300-0151|y11i t|sgpt211jp|9060|gt-s6790l|3.5|desire u|sch-i930|y320-u10|smartpro|y310-t10|e120|p936|gt-i9023|sch-i739|sm-g3502u|sgh-t679|r821t|blade iii|evo design 4g|studio 5.0 s ii|sm-g3502t|a50|g526-l11|a1+|vs840|y210-0010|gsmart roma r2|s560|gt-s5369|sgh-i437|gt-s6812|sm-g3508j|su880|ms659|sgh-t699|u8860|t83|gt-s6102b|u8650|a54|km-e100|h881c|u8652|y330-u05|gt-s5830l|x1000|sch-s738c|xt875|gt-s7500t|lt900|gt-i8268|gt-s6790|v793|g610-u30|sph-m840|p690f|rm-889|gt-s5830v|r815|h7500+|yp-gi|gt-s5312b|yp-gs1|sgh-i437z|sm-g350l|lu6800|5890|n909|vs890|y220-u10|adr6350|shw-m460d|101+|e4002|8.52|w125|s330|a66|sch-i200pp|a88|vs840pp|gt-e3210b|gt-s7562c|shw-m220l|sch-i415|c201|sm-g3502i|e4004|765|t9500|blade v|gt-e3210|gt-s5301b|m15|5879|z750c|c8825d|m670|noir a65|c525c|gt-i8150t|e400r|a828 tc|dash 3.5 ce|d271|ms3a|gt-s5380d|rm-915|sgh-t599|sgh-t499|gt-s6812i|evo 4g lte|x301|vs870|v807|c625a|q-smart qs05|gt-s3570|gt-s5830d|rm-887|vs410pp|a111|x310e|sch-r760x|u8687|w2-u00|sgh-i847|c625b|x2-02|e502|gt-s5303|gt-i8558|a378t|xt321|mb870|gt-s5220|gt-s7250d|8.52 8g|a630|gt-c3590|gt-b7510|7235|sgh-t759|975|e610v|sgh-i997r|blade apex|a376|xt862|startrail iii|858|v788b|gt-s7275b|101k|gt-b5330l|6990 lvw|gt-c3310|s11t|a760|v790|gt-s7566|a92|ku5900|sm-g3502|t9189|e77|lumia 625h|sch-r920|vs910|vs920|g526-l33|iris 352e|primo f2|gt-b5512|shw-m340s|a320t|prime 812 mini|gt-s5830m|sgh-t959p|gt-e2652w|gt-s5260|gt-s5310m|gt-c3510t|gt-s5831i|shw-m290s|gt-s5830b|gt-s6812b|v889f|s720t|gt-s5830g|e435f|gt-s6310l|is11lg|gt-c3330|gt-s6313t|gt-c3222|gt-c6712|mi 1 c1|sch-i509|e425g|sch-i759|shw-m290k|sm-b360e|mismart smile w1|v889m|gt-i8550e|smart ii|sch-r820|sch-w789|prime 500d|primo c3|iris 356|sch-r720|gt-s5310l|f200ls|primo d3|gt-c3260|a115|v889s|i508|primo r2|a199|smart xl|u100|sph-m830|e455f|a704|smart pro 2|sgh-s730g|gt-c3520|a1+ duple|8085|n909d|sm-b313e|sgh-i677|gt-i699i|x402|a90s|gt-s3353|a630t|as780|k210v|gt-i8150b|iris 502|a228t|x2-05|gt-s6792l)'
)
AND carrier IN ('DTAC')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA') and year = 2017 and ((month = 1 and day > 15) or month = 2)
) as dtac ON omo.tapad_id = dtac.tapad_id
WHERE
(user_agent REGEXP '(?i)(gt-i8552b|gt-i8552|gt-s7582l|gt-s7582|i-style q4|i-style 2|iris 353|gt-s7270l|gt-s7270|gt-i8262|gt-s7562|gt-i8262b|r815t|a680|i5-77|iris 456|rm-914|iris 505|i-style 7.1|gt-i9100|blade c310|r821|gt-i8190n|rm-941|i-style 7.5|rm-821|gt-i8190t|gt-i9100t|i-style 7.3|gt-i8160l|rm-885|c5302|iris 354|z130|gt-i9070|gt-s5310|gt-i8160|xt1032|gt-s5360|rm-846|gt-s5310b|a369i|gt-i9250|rm-825|gt-s6310|gt-s5360b|gt-s6810p|adr6400l|gt-s6310b|shv-e210s|a56|a390|gt-s6102|y511-u251|gt-i8190|y511-u30|gt-s7500l|shv-e210k|shv-e160s|start 1|gt-s7500|c2105|g610-u20|shv-e160k|a850|c1905|gt-s5830|lt22i|a706|gt-s5830i|i-style 8|c2104|p880|p768|c1505|shv-e160l|shv-e270k|gt-i9105p|a516|gt-s7580|gt-s5300b|life|gt-s5570|gt-s5300|q360|gt-s5830t|c1904|v370|q10|gt-s7275r|i-style q3|g510-0251|a269i|c5303|a44|desire 500|xt1033|gt-i8730|hongmi|a850+|s510e|smart 8|g610-u00|g610-t11|p714|one v|gt-s5660|a48|gt-i9001|gt-i9003|p760|sm-g350|e430|e451g|shv-e210l|shv-e270s|gt-i9100g|gt-i8260|rm-823|st26i|shv-e275k|9860|shv-e120s|c110e|p713|rm-917|gt-i8350|st26a|gt-s7710|shv-e220s|sm-g3815|xt910|shv-e275s|desire 601|sp ai|gt-s5380|st23i|i-style 7.2|gt-s7560|shv-e110s|y330-u01|a850i|e612|i-style 6|mi 1|mt25i|sgh-t989|gt-e3300v|500|i-style 3|y511-t00|603|f160l|f160k|st27i|gt-i9103|gt-s7392|a47|gt-e3309i|gt-s7262|a502|gt-i9105|p895|w100|st25i|e440|yp-g70|gt-s7390|gt-s3850|shv-e270l|e425|mt620|xt535|gt-i8730t|p710|st27a|mt27i|iris 455|a11 (t00c)|z120|s510b|gt-s6310n|p716|st21i|gt-i8190l|gt-s7275t|lu6200|a75|gt-s8600|gt-i8150|desire 300|i-style q1|a702 andy|a78|f160s|k800|gt-s7272|xt912|hongmi 4g|p720|liquid z3|e615|gt-i9100p|mb865|gt-i9210|gt-s3800w|i-style 5|p765|sgh-t989d|a320e|iris 405|e610|gt-s7583t|f160lv|sgh-i777|a560|rm-835|e460|pg76110|y511-u00|gt-s5301|e425f|p715|gt-i8160p|f200|p769|a74|gt-i8260l|gt-s5690|rm-820|s750|sgh-t599n|gt-s5312|sph-d710|sgh-t399|x515m|u8815|gt-s5570i|gt-c3312|gt-s6810|875|gt-s6312|smart 7|gt-s5839i|sgh-t399n|xt320|sgh-i577|y320-u01|pioneer p2|y511-u10|t31|q1000|gt-i8550|ms500|a820|y320-u30|st21a|t375|lu5400|a369|sm-g3812|mb860|gt-s6500|d505|sgh-t769|liyang z9|g510-0200|iris 351|sch-r530m|cynus e1|shw-m340k|a390t|gt-i9210t|a19|gt-c3312r|e445|gt-i9070p|shw-m240s|9380|jy-f1|f120l|l-05d|sgh-t959v|sgh-i997|noir a12|a61|gt-s6790n|vs930|a28|q-smart qs550|mi 2sc|g610-u15|mi 1s|gt-s7275y|gt-i8262d|xt1028|e435|f120s|blade q|c510e|u8655|a690|gt-i8550l|x40|d500|as7|sph-d710bst|sch-r530u|sph-d720|velocity 4g|sm-g3819|y300-0000|i-style q5a|gt-s7572|desire 500 dual sim|gt-e2202|a238t|smart 4g|sgh-i547|sensation 4g|pm36100|c5306|evo v 4g|t8951|p659|q-smart s16|102e|w1-u00|st21i2|infinity power|y320-c00|u9202l|dash 3.5|ms770|a868|q800|iris 402|star jr|u8665|sm-g3509|sky s777|su870|m032|xt894|sch-i200|gt-i9100m|sch-r830|v970|sch-r530c|ms870|studio 5.5 s|gt-s5310i|rm-878|ms769|ls720|mi one plus|u8510|g1305 boston|yp-gb1|g526-l22|gt-i9082c|y325-t00|u8825|n820|gt-e3309|gt-b5330|x515e|sch-i939|sm-g730w8|sgh-i437p|975n|y220-u00|gt-s6810l|limo x2|9810|x710e|kis ii max|mb855|gt-s5280|e465g|yp-gi1c|y210-0200|a25|sch-r740c|v880g|pm23300|gt-s7390l|603e|y300-0151|y11i t|sgpt211jp|9060|gt-s6790l|3.5|desire u|sch-i930|y320-u10|smartpro|y310-t10|e120|p936|gt-i9023|sch-i739|sm-g3502u|sgh-t679|r821t|blade iii|evo design 4g|studio 5.0 s ii|sm-g3502t|a50|g526-l11|a1+|vs840|y210-0010|gsmart roma r2|s560|gt-s5369|sgh-i437|gt-s6812|sm-g3508j|su880|ms659|sgh-t699|u8860|t83|gt-s6102b|u8650|a54|km-e100|h881c|u8652|y330-u05|gt-s5830l|x1000|sch-s738c|xt875|gt-s7500t|lt900|gt-i8268|gt-s6790|v793|g610-u30|sph-m840|p690f|rm-889|gt-s5830v|r815|h7500+|yp-gi|gt-s5312b|yp-gs1|sgh-i437z|sm-g350l|lu6800|5890|n909|vs890|y220-u10|adr6350|shw-m460d|101+|e4002|8.52|w125|s330|a66|sch-i200pp|a88|vs840pp|gt-e3210b|gt-s7562c|shw-m220l|sch-i415|c201|sm-g3502i|e4004|765|t9500|blade v|gt-e3210|gt-s5301b|m15|5879|z750c|c8825d|m670|noir a65|c525c|gt-i8150t|e400r|a828 tc|dash 3.5 ce|d271|ms3a|gt-s5380d|rm-915|sgh-t599|sgh-t499|gt-s6812i|evo 4g lte|x301|vs870|v807|c625a|q-smart qs05|gt-s3570|gt-s5830d|rm-887|vs410pp|a111|x310e|sch-r760x|u8687|w2-u00|sgh-i847|c625b|x2-02|e502|gt-s5303|gt-i8558|a378t|xt321|mb870|gt-s5220|gt-s7250d|8.52 8g|a630|gt-c3590|gt-b7510|7235|sgh-t759|975|e610v|sgh-i997r|blade apex|a376|xt862|startrail iii|858|v788b|gt-s7275b|101k|gt-b5330l|6990 lvw|gt-c3310|s11t|a760|v790|gt-s7566|a92|ku5900|sm-g3502|t9189|e77|lumia 625h|sch-r920|vs910|vs920|g526-l33|iris 352e|primo f2|gt-b5512|shw-m340s|a320t|prime 812 mini|gt-s5830m|sgh-t959p|gt-e2652w|gt-s5260|gt-s5310m|gt-c3510t|gt-s5831i|shw-m290s|gt-s5830b|gt-s6812b|v889f|s720t|gt-s5830g|e435f|gt-s6310l|is11lg|gt-c3330|gt-s6313t|gt-c3222|gt-c6712|mi 1 c1|sch-i509|e425g|sch-i759|shw-m290k|sm-b360e|mismart smile w1|v889m|gt-i8550e|smart ii|sch-r820|sch-w789|prime 500d|primo c3|iris 356|sch-r720|gt-s5310l|f200ls|primo d3|gt-c3260|a115|v889s|i508|primo r2|a199|smart xl|u100|sph-m830|e455f|a704|smart pro 2|sgh-s730g|gt-c3520|a1+ duple|8085|n909d|sm-b313e|sgh-i677|gt-i699i|x402|a90s|gt-s3353|a630t|as780|k210v|gt-i8150b|iris 502|a228t|x2-05|gt-s6792l)'
)
AND carrier IN ('True Move', 'AIS', 'TOT')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA')
AND dtac.tapad_id IS NULL;

/*

+--------------------------------+
| facebook mobile advertiser ids |
+--------------------------------+
| 319279                         |
+--------------------------------+

*/



/*cluster 4*/

SELECT COUNT(DISTINCT omo.tapad_id) as 'Facebook Mobile Advertiser Ids' FROM apollo.dtac_vertical_dataset omo
LEFT JOIN (SELECT tapad_id FROM apollo.dtac_vertical_dataset WHERE
(user_agent REGEXP '(?i)(sm-g7102|gt-n7100|gt-i9082l|gt-i9082|sm-n900|gt-i9500|x9006|gt-i9152|sm-n9005|gt-i9300|gt-i9300t|d820|r829|sm-n7502|x9076|gt-i9200|l110|r827|gt-n7000|u707|h30-u10|3006|d802|gt-i9505|r819|e960|c6602|gt-i9190|shv-e250s|gt-n7000b|c2305|x909|rm-892|mi 4w|a328|sm-c101|g750-u10|shv-e250k|gt-n7105|c6603|lt26i|s920|s930|a500kl (t00p)|pe-tl10|s650|gt-i9195|one max|k900|u7015|c6833|shv-e330s|gt-i9205|gt-i9305|c6802|mi 3w|htl21|sm-n900s|g730-u27|g730-u10|sm-n900k|gt-i8750|sm-n900a|r8113|gt-i9506|mt1-u06|gt-i9301i|s820|p6-u06|one x|d410|d2502|h815|c5502|s860|gt-i9515|shw-m440s|lt26w|c6502|d686|alpha gt|lumia 830|one mini|one s|sm-n7505|shv-e250l|sgh-i337|e989|d680|lt28h|sm-a700yd|shv-e330k|me560cg (k00g)|g730-u00|gt-i9295|elife e7|e988|desire 600|sm-n900l|sm-n750|e975|s510|e975k|d682|sm-g7105|c2004|sm-n900t|i9500|c2005|sky|gt-i9300i|x9077|sm-n7507|sm-e7000|gt-i9192|sgh-t999|v8 plus|r819t|gt-i9195i|sgh-i337m|sgh-i747|gt-i9502|shv-e330l|sgh-t889|sm-n900v|sch-i535|gt-n7105t|gt-i9507|801e|sm-n9006|shv-e370k|shw-m250s|jk g9200|g700-u10|g730-c00|gt-i9305t|sm-a7000|sm-g750h|sgh-i747m|sm-g720ax|d838|f320l|f320s|s939|sm-a700f|sm-a700l|gt-n7102|sgh-n075t|sm-n9002|f240l|sch-i605|shv-e500s|sm-a700k|f240s|butterfly|sm-a700s|sm-g7508q|sm-c105|desire 610|shv-e170k|sm-a700h|sm-g7106|lt30p|xt1052|f320k|sm-n750s|sm-n750k|sch-s968c|sm-n900p|mi 3c|sm-n750l|shv-e470s|sgh-i317m|v3+|sm-e500f|sm-g318h|sm-n9008v|f240k|sgh-t999l|sm-n9000q|butterfly s|mi 2s|f310l|xt1053|xt1058|gt-n7108|e980|f180l|shv-e500l|sm-n9008|v887|c5503|sph-l710|sm-n9009|one xl|d800|desire 700 dual sim|f180s|mi 3|f180k|sch-n719|sgh-i527|so-03d|sch-i435|xt1060|gt-i9508|f100l|lt26ii|f410s|d801|sgh-m919v|gt-i9152p|c6503|padfone|desire 601 dual sim|gt-i9195t|sm-n7508v|yp-gp1|a500l|vs980|gt-i9192i|s890|f310lr|sgh-m919n|v987|lgl23|sgh-t999v|e970|sm-n9008s|c6806|gt-i9197|d605|sgh-i257m|q3000|xt926|m353|a240|sch-i959|ls980|g750-t00|sm-s975l|x5 max|gt-1313|802w|gt-i9515l|amazing a7|sch-r970|gt-i9505g|sch-p709|gt-i9301q|sm-g7108|p6-c00|sc-01h|rm-845|f240|pn07120|sm-n9000|g730-t00|gl07s|sm-n7500q|x5v|lt28i|hn3-u01|x5l|302sh|s5.5|lt30a|shw-m250l|sgh-i257|d959|x9070|nx503a|a114|nx403a|is12s|g700-u20|g750-t20|lt30at|e986|sky nano|gt-i9082i|sch-i879|f470s|sgh-t889v|c6506|xt1049|sgh-m819n|gt-i9220|sm-g5108q|6600lvw|gt-i9508v|d682tr|one 3g hd|sky hd9500|m351|sm-n7506v|gt-i9507v|m040|xt1055|mt2-l05|lgl21|d631|sgh-i337z|primo nx2|s720|g700-t00|802t|sgh-i727r|sm-g7105l|t008|gt-n7108d|d805|sgh-i527m|vs880|sm-g750a|gt-i9080l|9190-t00|r829t|l-04e|802d|s720e|gn810|k860|rocket|infinity lotus|sch-i829|lg870|ls995|sm-g3606|801a|d806|e500a|gt-i9150|g928|sm-g730v|sm-w2014|life pure mini|sm-z9005|6515lvw|gt-i9168i|e971|sm-g3608|grand s flex|sch-i869|a110q|d950g|lt28at|s898t|x003|nx501|sm-n900j|gt-s7278|d803|u985|life one|v9815|a117|grand s|601e|c2000|gs01|u707t|r827t|sm-g5108|mt2-l01|primo h2|mini one|infinity gn708|gt-n7100t|gt-i9128v|e973|sch-i545pp|p9070|n9100|gt-i9063t|s806|vivo|102p|5952|a9+|sgh-t899m|sch-r970c|a250|838|gt-i9235|sgh-t999n|gt-i9118|sch-p709e|grand s lite|hn3-u00|x1st|d685|8056|u9510e|ls970|gt-i9505x|up lite|xt1050|infinity lite|sm-g7102t|ascend g740|793|xt1056|e9003|f320|blaster|l930|sch-i535pp|f301|s1+|q503u|gt-i9108|sch-r950|primo n1|a255|r823t|stx ultra|sm-c105a|nx402|z813|p-07d|w8555|gt-i9128e|sch-r890|g906|u9510|s970|zeus-hd|gsmart maya m1 v2|centurion 3|sm-n7505l|a5s|gt-i9128i|d820mu|5950|sch-i929|gt-i9168|p9090|xplorer zii|gt-i9128|g906 plus|mt2-l02|d681|skyfire 2.0|vivo 4.8 hd|mx56|a116i|sch-r760|sch-i545l|gt-i9230|gt-s7273t|gt-i9208|sm-g9092|vivo iv|sch-s960l|k900t|sgh-i407|a500s|801e|gt-i9195l|sch-i889|a500s ips|gt-n7i00|gt-i9080|8750|prof700|vivo 4.3|vivo selfie|sch-r530x|sch-w2013|x2 turbo|sm-s978l)'
)
AND carrier IN ('DTAC')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA') and year = 2017 and ((month = 1 and day > 15) or month = 2)
) as dtac ON omo.tapad_id = dtac.tapad_id
WHERE
(user_agent REGEXP '(?i)(sm-g7102|gt-n7100|gt-i9082l|gt-i9082|sm-n900|gt-i9500|x9006|gt-i9152|sm-n9005|gt-i9300|gt-i9300t|d820|r829|sm-n7502|x9076|gt-i9200|l110|r827|gt-n7000|u707|h30-u10|3006|d802|gt-i9505|r819|e960|c6602|gt-i9190|shv-e250s|gt-n7000b|c2305|x909|rm-892|mi 4w|a328|sm-c101|g750-u10|shv-e250k|gt-n7105|c6603|lt26i|s920|s930|a500kl (t00p)|pe-tl10|s650|gt-i9195|one max|k900|u7015|c6833|shv-e330s|gt-i9205|gt-i9305|c6802|mi 3w|htl21|sm-n900s|g730-u27|g730-u10|sm-n900k|gt-i8750|sm-n900a|r8113|gt-i9506|mt1-u06|gt-i9301i|s820|p6-u06|one x|d410|d2502|h815|c5502|s860|gt-i9515|shw-m440s|lt26w|c6502|d686|alpha gt|lumia 830|one mini|one s|sm-n7505|shv-e250l|sgh-i337|e989|d680|lt28h|sm-a700yd|shv-e330k|me560cg (k00g)|g730-u00|gt-i9295|elife e7|e988|desire 600|sm-n900l|sm-n750|e975|s510|e975k|d682|sm-g7105|c2004|sm-n900t|i9500|c2005|sky|gt-i9300i|x9077|sm-n7507|sm-e7000|gt-i9192|sgh-t999|v8 plus|r819t|gt-i9195i|sgh-i337m|sgh-i747|gt-i9502|shv-e330l|sgh-t889|sm-n900v|sch-i535|gt-n7105t|gt-i9507|801e|sm-n9006|shv-e370k|shw-m250s|jk g9200|g700-u10|g730-c00|gt-i9305t|sm-a7000|sm-g750h|sgh-i747m|sm-g720ax|d838|f320l|f320s|s939|sm-a700f|sm-a700l|gt-n7102|sgh-n075t|sm-n9002|f240l|sch-i605|shv-e500s|sm-a700k|f240s|butterfly|sm-a700s|sm-g7508q|sm-c105|desire 610|shv-e170k|sm-a700h|sm-g7106|lt30p|xt1052|f320k|sm-n750s|sm-n750k|sch-s968c|sm-n900p|mi 3c|sm-n750l|shv-e470s|sgh-i317m|v3+|sm-e500f|sm-g318h|sm-n9008v|f240k|sgh-t999l|sm-n9000q|butterfly s|mi 2s|f310l|xt1053|xt1058|gt-n7108|e980|f180l|shv-e500l|sm-n9008|v887|c5503|sph-l710|sm-n9009|one xl|d800|desire 700 dual sim|f180s|mi 3|f180k|sch-n719|sgh-i527|so-03d|sch-i435|xt1060|gt-i9508|f100l|lt26ii|f410s|d801|sgh-m919v|gt-i9152p|c6503|padfone|desire 601 dual sim|gt-i9195t|sm-n7508v|yp-gp1|a500l|vs980|gt-i9192i|s890|f310lr|sgh-m919n|v987|lgl23|sgh-t999v|e970|sm-n9008s|c6806|gt-i9197|d605|sgh-i257m|q3000|xt926|m353|a240|sch-i959|ls980|g750-t00|sm-s975l|x5 max|gt-1313|802w|gt-i9515l|amazing a7|sch-r970|gt-i9505g|sch-p709|gt-i9301q|sm-g7108|p6-c00|sc-01h|rm-845|f240|pn07120|sm-n9000|g730-t00|gl07s|sm-n7500q|x5v|lt28i|hn3-u01|x5l|302sh|s5.5|lt30a|shw-m250l|sgh-i257|d959|x9070|nx503a|a114|nx403a|is12s|g700-u20|g750-t20|lt30at|e986|sky nano|gt-i9082i|sch-i879|f470s|sgh-t889v|c6506|xt1049|sgh-m819n|gt-i9220|sm-g5108q|6600lvw|gt-i9508v|d682tr|one 3g hd|sky hd9500|m351|sm-n7506v|gt-i9507v|m040|xt1055|mt2-l05|lgl21|d631|sgh-i337z|primo nx2|s720|g700-t00|802t|sgh-i727r|sm-g7105l|t008|gt-n7108d|d805|sgh-i527m|vs880|sm-g750a|gt-i9080l|9190-t00|r829t|l-04e|802d|s720e|gn810|k860|rocket|infinity lotus|sch-i829|lg870|ls995|sm-g3606|801a|d806|e500a|gt-i9150|g928|sm-g730v|sm-w2014|life pure mini|sm-z9005|6515lvw|gt-i9168i|e971|sm-g3608|grand s flex|sch-i869|a110q|d950g|lt28at|s898t|x003|nx501|sm-n900j|gt-s7278|d803|u985|life one|v9815|a117|grand s|601e|c2000|gs01|u707t|r827t|sm-g5108|mt2-l01|primo h2|mini one|infinity gn708|gt-n7100t|gt-i9128v|e973|sch-i545pp|p9070|n9100|gt-i9063t|s806|vivo|102p|5952|a9+|sgh-t899m|sch-r970c|a250|838|gt-i9235|sgh-t999n|gt-i9118|sch-p709e|grand s lite|hn3-u00|x1st|d685|8056|u9510e|ls970|gt-i9505x|up lite|xt1050|infinity lite|sm-g7102t|ascend g740|793|xt1056|e9003|f320|blaster|l930|sch-i535pp|f301|s1+|q503u|gt-i9108|sch-r950|primo n1|a255|r823t|stx ultra|sm-c105a|nx402|z813|p-07d|w8555|gt-i9128e|sch-r890|g906|u9510|s970|zeus-hd|gsmart maya m1 v2|centurion 3|sm-n7505l|a5s|gt-i9128i|d820mu|5950|sch-i929|gt-i9168|p9090|xplorer zii|gt-i9128|g906 plus|mt2-l02|d681|skyfire 2.0|vivo 4.8 hd|mx56|a116i|sch-r760|sch-i545l|gt-i9230|gt-s7273t|gt-i9208|sm-g9092|vivo iv|sch-s960l|k900t|sgh-i407|a500s|801e|gt-i9195l|sch-i889|a500s ips|gt-n7i00|gt-i9080|8750|prof700|vivo 4.3|vivo selfie|sch-r530x|sch-w2013|x2 turbo|sm-s978l)'
)
AND carrier IN ('True Move', 'AIS', 'TOT')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA')
AND dtac.tapad_id IS NULL;

/*
+--------------------------------+
| facebook mobile advertiser ids |
+--------------------------------+
| 186841                         |
+--------------------------------+

*/




/*cluster 5*/

SELECT COUNT(DISTINCT omo.tapad_id) as 'Facebook Mobile Advertiser Ids' FROM apollo.dtac_vertical_dataset omo
LEFT JOIN (SELECT tapad_id FROM apollo.dtac_vertical_dataset WHERE
(user_agent REGEXP '(?i)(sm-n920c|sm-a710f|sm-n910c|sm-g935f|sm-g900f|sm-a910f|sm-g925f|sm-g920f|ze551ml (z00ad)|eva-l19|a7000-a|sm-g928c|vie-l29|sm-g850f|nexus 6|r7 plus f|n5206|sm-n915f|e6853|nxt-l29|e6653|sm-c111|f3216|x5max|rm-1045|mt7-l09|f5122|sm-n910f|k910l|p8max|sm-n9208|nexus 6p|a0001|mt7-tl10|sm-g900i|sm-g920i|sm-g925i|sm-g928f|sm-n910g|e5823|a3003|sm-n920i|s960|sm-g9350|sm-n9200|one m9 plus|sm-g900h|sm-g901f|e5803|xt1650|mla-l01|eva-l09|luxury 3|sm-n910u|f5321|one m9|pixel|sm-n930f|sm-n910h|sm-g900v|sm-n910s|sm-g920|sm-n920s|sm-g900a|xt1572|h860|sm-g900fd|sm-n9100|pixel xl|sm-a9000|sm-n900u|sm-g9250|sm-g900k|sm-n915g|stv100|h961n|sm-g900s|sm-g900t|sm-g920v|sm-n910k|sm-g920s|h850|sm-g900l|nxt-al10|eva-al10|sm-g900w8|sm-g935s|sm-g9200|h962|sm-n920k|sch-i545|sm-g935t|sm-g9287|sm-n910a|sm-g925s|sm-g906s|sm-g920k|sm-g928i|sm-n910t|sm-n910l|sm-n920l|h840|sm-g935v|sm-g920w8|sm-g930w8|xt1575|h990|sm-n915fy|d6633|sm-g920t|sm-g920l|sm-g906k|sm-n910v|sm-g9287c|sm-n920v|sm-g935k|sm-g900p|sm-g925k|vie-al10|one mini 2|z012da|sm-g935a|sm-n920t|mx4 pro|sm-g9208|sm-n920g|sm-a7100|sm-n915s|sm-n920a|sm-g920a|vie-l09|sm-g906l|sm-g920p|sm-a9100|sm-g935l|sm-g935w8|sm-g925l|mx4|sm-g900m|sm-a710y|sm-g925a|sm-g925v|sm-g870f|sm-g925w8|sm-g870a|elite 5|sm-n900w8|sm-g928t|p9000|ultra|sm-g925t|sm-c115|h960|gt-i9158v|k920|sm-n910w8|sm-g928s|sm-g935p|sm-n910p|sm-g928g|sm-n920p|sm-n9300|sm-g928v|sm-g900|sm-n9007|sm-g9006v|sm-g850s|sm-n915k|sm-g9280|ek-gc200|sm-g928a|sm-n930p|sm-n920w8|sm-g360t1|r7 plus|sm-n9150|sm-g850k|sm-g928k|sm-a710k|sm-g9008v td|sm-s920l|sm-g925p|sm-g935u|sm-a710s|sm-g850a|sm-n920f|sm-n9108v|sm-n915l|sm-g850y|sm-n915t|sm-g900fq|sm-g910s|sm-g9300|sm-n915a|x5 max pro|sm-g850l|one m9e|x7 plus|sm-g9009d|d6708|p9000 lite|vs987|sm-g928l|h901|sm-g900t1|sm-c115l|eva-l29|sm-g870w|dmc-cm1|pixel v2|h830|elite x3 telstra|sm-g928w8|elite 1|vs990|sm-g9008w|sm-n920r4|sm-n910t3|sm-n9109w|sm-g900r4|f3212|gt-i9308|sm-n915v|h918|h900|ze552kl (z012s)|sm-g920r4|x5s l|ls992|sm-g928p|e6603|eva-al00|sm-g900md|sm-g930t1|sm-n915p|h820|grand s ii|sm-g900az|sm-g850w|n5207|sm-g920az|vs995|nx508j|elite 4|sm-g9308|dav-703l|sm-a710m|note 2 lte|sm-n9106w|sm-a7108|sm-g850fq|x5m|sm-g900r7|sm-g920t1|sm-g9209|zenfone 2e|sm-a710l|m8minx|sm-g9006w|s960s|x5 max+|pixel v2+|sm-g850m|sm-n915w8|elife s7|ls997|h910|pixel v1|rs988|sm-a5009|elite 2|xplay5s|2015|sm-g900t3|sm-g8508s|sm-n930s|elite 3|824|sm-g925r4|vns-tl00|sm-g928x|sm-g900j|d10|gt-i9158|ultra plus|slate 7 voice tab ultra|sm-n930t|infinity 2 lite|sm-g935r4|sm-n930k|us996|sm-n930v|x5max l|sm-n930l|sm-g850x|h915|note 2|sm-n910r4|sm-g920x|sm-c111m|sm-n900r4|gt-i9308i|xt1250|sm-c115m|x5max v|n5209|sm-g920r6|ultra latitude|sm-a5108|sky pro|sonic|sm-n930a|max plus 5.0|be one|sm-g930r7|sm-n910x|zen|elite|sm-n910t2|sm-g930r6|sm-g900x|sm-g900r6|sm-n920r6|elite 6.0l|gt-i9158p|elite plus|sm-n910|ultra air|blaster 2)'
)
AND carrier IN ('DTAC')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA') and year = 2017 and ((month = 1 and day > 15) or month = 2)
) as dtac ON omo.tapad_id = dtac.tapad_id
WHERE
(user_agent REGEXP '(?i)(sm-n920c|sm-a710f|sm-n910c|sm-g935f|sm-g900f|sm-a910f|sm-g925f|sm-g920f|ze551ml (z00ad)|eva-l19|a7000-a|sm-g928c|vie-l29|sm-g850f|nexus 6|r7 plus f|n5206|sm-n915f|e6853|nxt-l29|e6653|sm-c111|f3216|x5max|rm-1045|mt7-l09|f5122|sm-n910f|k910l|p8max|sm-n9208|nexus 6p|a0001|mt7-tl10|sm-g900i|sm-g920i|sm-g925i|sm-g928f|sm-n910g|e5823|a3003|sm-n920i|s960|sm-g9350|sm-n9200|one m9 plus|sm-g900h|sm-g901f|e5803|xt1650|mla-l01|eva-l09|luxury 3|sm-n910u|f5321|one m9|pixel|sm-n930f|sm-n910h|sm-g900v|sm-n910s|sm-g920|sm-n920s|sm-g900a|xt1572|h860|sm-g900fd|sm-n9100|pixel xl|sm-a9000|sm-n900u|sm-g9250|sm-g900k|sm-n915g|stv100|h961n|sm-g900s|sm-g900t|sm-g920v|sm-n910k|sm-g920s|h850|sm-g900l|nxt-al10|eva-al10|sm-g900w8|sm-g935s|sm-g9200|h962|sm-n920k|sch-i545|sm-g935t|sm-g9287|sm-n910a|sm-g925s|sm-g906s|sm-g920k|sm-g928i|sm-n910t|sm-n910l|sm-n920l|h840|sm-g935v|sm-g920w8|sm-g930w8|xt1575|h990|sm-n915fy|d6633|sm-g920t|sm-g920l|sm-g906k|sm-n910v|sm-g9287c|sm-n920v|sm-g935k|sm-g900p|sm-g925k|vie-al10|one mini 2|z012da|sm-g935a|sm-n920t|mx4 pro|sm-g9208|sm-n920g|sm-a7100|sm-n915s|sm-n920a|sm-g920a|vie-l09|sm-g906l|sm-g920p|sm-a9100|sm-g935l|sm-g935w8|sm-g925l|mx4|sm-g900m|sm-a710y|sm-g925a|sm-g925v|sm-g870f|sm-g925w8|sm-g870a|elite 5|sm-n900w8|sm-g928t|p9000|ultra|sm-g925t|sm-c115|h960|gt-i9158v|k920|sm-n910w8|sm-g928s|sm-g935p|sm-n910p|sm-g928g|sm-n920p|sm-n9300|sm-g928v|sm-g900|sm-n9007|sm-g9006v|sm-g850s|sm-n915k|sm-g9280|ek-gc200|sm-g928a|sm-n930p|sm-n920w8|sm-g360t1|r7 plus|sm-n9150|sm-g850k|sm-g928k|sm-a710k|sm-g9008v td|sm-s920l|sm-g925p|sm-g935u|sm-a710s|sm-g850a|sm-n920f|sm-n9108v|sm-n915l|sm-g850y|sm-n915t|sm-g900fq|sm-g910s|sm-g9300|sm-n915a|x5 max pro|sm-g850l|one m9e|x7 plus|sm-g9009d|d6708|p9000 lite|vs987|sm-g928l|h901|sm-g900t1|sm-c115l|eva-l29|sm-g870w|dmc-cm1|pixel v2|h830|elite x3 telstra|sm-g928w8|elite 1|vs990|sm-g9008w|sm-n920r4|sm-n910t3|sm-n9109w|sm-g900r4|f3212|gt-i9308|sm-n915v|h918|h900|ze552kl (z012s)|sm-g920r4|x5s l|ls992|sm-g928p|e6603|eva-al00|sm-g900md|sm-g930t1|sm-n915p|h820|grand s ii|sm-g900az|sm-g850w|n5207|sm-g920az|vs995|nx508j|elite 4|sm-g9308|dav-703l|sm-a710m|note 2 lte|sm-n9106w|sm-a7108|sm-g850fq|x5m|sm-g900r7|sm-g920t1|sm-g9209|zenfone 2e|sm-a710l|m8minx|sm-g9006w|s960s|x5 max+|pixel v2+|sm-g850m|sm-n915w8|elife s7|ls997|h910|pixel v1|rs988|sm-a5009|elite 2|xplay5s|2015|sm-g900t3|sm-g8508s|sm-n930s|elite 3|824|sm-g925r4|vns-tl00|sm-g928x|sm-g900j|d10|gt-i9158|ultra plus|slate 7 voice tab ultra|sm-n930t|infinity 2 lite|sm-g935r4|sm-n930k|us996|sm-n930v|x5max l|sm-n930l|sm-g850x|h915|note 2|sm-n910r4|sm-g920x|sm-c111m|sm-n900r4|gt-i9308i|xt1250|sm-c115m|x5max v|n5209|sm-g920r6|ultra latitude|sm-a5108|sky pro|sonic|sm-n930a|max plus 5.0|be one|sm-g930r7|sm-n910x|zen|elite|sm-n910t2|sm-g930r6|sm-g900x|sm-g900r6|sm-n920r6|elite 6.0l|gt-i9158p|elite plus|sm-n910|ultra air|blaster 2)'
)
AND carrier IN ('True Move', 'AIS', 'TOT')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA')
AND dtac.tapad_id IS NULL;

/*

+--------------------------------+
| facebook mobile advertiser ids |
+--------------------------------+
| 192367                         |
+--------------------------------+

*/

/*Cluster 6*/

SELECT COUNT(DISTINCT omo.tapad_id) as 'Facebook Mobile Advertiser Ids' FROM apollo.dtac_vertical_dataset omo
LEFT JOIN (SELECT tapad_id FROM apollo.dtac_vertical_dataset WHERE
(user_agent REGEXP '(?i)(gt-n5100|sm-t111|gt-p3100|sm-t231|sm-t211|sm-t111m|gt-p3100b|sm-g318hz|sm-t110|fe170cg (k012)|sm-t311|ideatab a3000-h|gt-p6200|rm-994|me371mg|gt-p3110|a1000-g|fe375cg (k019)|a7-30hc|a1-830|me173x (k00u)|s7-721u|b6000-hv|a100|sm-t235y|sm-t210|b1-a71|me172v|sm-t310|a7-20f|a1-810|sm-t230|a880|a7-10f|gt-n5110|gt-p3113|b1-730|b1-710|sm-t230nu|b6000-f|gt-n5120|tablet s|a1-811|smart touch 710|3840|d50|me180a (k00l)|sgpt12|shogun 10|sm-t315|sm-t235|b1-730hd|sm-t113|305|b1-720|sm-t215|shw-m500w|fire hd 7|s7-104|slate7 extreme|sm-t210r|slate 7|shv-e310s|shv-e310l|sm-t113nu|shv-e310k|700|sm-t2105|tablet|mid-756|v400|gt-p7310|venue 8|sm-t315t|smart tab 4|playbook|m733w|sgh-i467|shv-e150s|sm-t217a|a1000-f|p300|306|venue 7|smart tab iii 7|sm-t210l|s5000-h|s7-601u|p700|7b g3|a800|k-book|a7-30dc|gt-p6210|a7-30gc|m10|p650|308|gt-p7320|smart d71|pro 7|a101|a7-30f|a7-30h|700p11a|m74|dual s|b1-721|adtab 7|me176c (k013)|a1000l-f|7 plus|sch-i800|a088|t500|s5000-f|p1640|at7-b|at1s0|tab 7|sm-g318ml|n90|gt-p6201|p4100|lifetab e7312|777tpc|a2107|m785|s6600|gt-n5105|a8i|smart tab ii 7|v900|s7-201w|l701 tv|mid-744|l-06c|gt-p7320t|v88|lifetab e731x|me171|sgh-i467m|sch-i957|smart tab iii 10|a2107a-f|connect 7 pro|diamond|tab 7t|a2207|primo 78|pad 6|at7-c|mypad p4 lite|a8w|a8ic|m742g|p703|sgh-i957r|xperia tablet|8088|v909|p17|phoenix|s-534|sm-t111nq|mt7a|p515e|tab 7i 8gb 3g|a88x|a7is|touchbook 7.0|v9a|m15gc702p|tab 7hd|mid08|smart tab 7800|t8002|primo walpad 8|sgh-t849|mid990|primo 76|p701+|mid0738|sch-i815|mid74c|a10h|v88s|mid801|lifetab e7310|primo 81|tablet pc 4|a2107a-h|mid713|mid8127|a10t|sgh-i987|a1020-t|smartpad 750 3g|mid7022|v105|sm-t230nt|p410i|mid0714)'
)
AND carrier IN ('DTAC')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA') and year = 2017 and ((month = 1 and day > 15) or month = 2)
) as dtac ON omo.tapad_id = dtac.tapad_id
WHERE
(user_agent REGEXP '(?i)(gt-n5100|sm-t111|gt-p3100|sm-t231|sm-t211|sm-t111m|gt-p3100b|sm-g318hz|sm-t110|fe170cg (k012)|sm-t311|ideatab a3000-h|gt-p6200|rm-994|me371mg|gt-p3110|a1000-g|fe375cg (k019)|a7-30hc|a1-830|me173x (k00u)|s7-721u|b6000-hv|a100|sm-t235y|sm-t210|b1-a71|me172v|sm-t310|a7-20f|a1-810|sm-t230|a880|a7-10f|gt-n5110|gt-p3113|b1-730|b1-710|sm-t230nu|b6000-f|gt-n5120|tablet s|a1-811|smart touch 710|3840|d50|me180a (k00l)|sgpt12|shogun 10|sm-t315|sm-t235|b1-730hd|sm-t113|305|b1-720|sm-t215|shw-m500w|fire hd 7|s7-104|slate7 extreme|sm-t210r|slate 7|shv-e310s|shv-e310l|sm-t113nu|shv-e310k|700|sm-t2105|tablet|mid-756|v400|gt-p7310|venue 8|sm-t315t|smart tab 4|playbook|m733w|sgh-i467|shv-e150s|sm-t217a|a1000-f|p300|306|venue 7|smart tab iii 7|sm-t210l|s5000-h|s7-601u|p700|7b g3|a800|k-book|a7-30dc|gt-p6210|a7-30gc|m10|p650|308|gt-p7320|smart d71|pro 7|a101|a7-30f|a7-30h|700p11a|m74|dual s|b1-721|adtab 7|me176c (k013)|a1000l-f|7 plus|sch-i800|a088|t500|s5000-f|p1640|at7-b|at1s0|tab 7|sm-g318ml|n90|gt-p6201|p4100|lifetab e7312|777tpc|a2107|m785|s6600|gt-n5105|a8i|smart tab ii 7|v900|s7-201w|l701 tv|mid-744|l-06c|gt-p7320t|v88|lifetab e731x|me171|sgh-i467m|sch-i957|smart tab iii 10|a2107a-f|connect 7 pro|diamond|tab 7t|a2207|primo 78|pad 6|at7-c|mypad p4 lite|a8w|a8ic|m742g|p703|sgh-i957r|xperia tablet|8088|v909|p17|phoenix|s-534|sm-t111nq|mt7a|p515e|tab 7i 8gb 3g|a88x|a7is|touchbook 7.0|v9a|m15gc702p|tab 7hd|mid08|smart tab 7800|t8002|primo walpad 8|sgh-t849|mid990|primo 76|p701+|mid0738|sch-i815|mid74c|a10h|v88s|mid801|lifetab e7310|primo 81|tablet pc 4|a2107a-h|mid713|mid8127|a10t|sgh-i987|a1020-t|smartpad 750 3g|mid7022|v105|sm-t230nt|p410i|mid0714)'
)
AND carrier IN ('True Move', 'AIS', 'TOT')
AND id_type IN ('HARDWARE_ANDROID_AD_ID', 'HARDWARE_IDFA')
AND dtac.tapad_id IS NULL;

/*

+--------------------------------+
| facebook mobile advertiser ids |
+--------------------------------+
| 144874                         |
+--------------------------------+

*/