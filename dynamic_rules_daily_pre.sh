####################################################################################################
#! /bin/bash
####################################################################################################
# Version Control:
#  2016/12/15 RS: Initial Version
# tr -d '\r' <dynamic_rules_daily_pre.sh > dynamic_rules_daily.sh
# rm dynamic_rules_daily_pre.sh
# chmod 777 dynamic_rules_daily.sh
####################################################################################################

today_year=$(date --d="today"  +"%Y")
today_month=$(date --d="today"  +"%m")
today_day=$(date --d="today" +"%d") #today is the day we run the initial table before appending

today_date="$today_year-$today_month-$today_day"
echo $today_date
	

impala-shell -i impala.prd.sg1.tapad.com:21000 --var=year=$today_year --var=month=$today_month --var=day=$today_day -f /local/home/rata.suwantong/3get_rules.sql

impala-shell -i impala.prd.sg1.tapad.com:21000 -B -o /local/home/rata.suwantong/prerules_$today_date.csv --output_delimiter=',' -q "select * from sgmt_rules.dynamic_rules_daily where year=$today_year and month=$today_month and day=$today_day order by source asc,uc1_priority asc"

echo $'CHANNEL, OFFER, PRIORITY ORDER, RULE, YEAR, MONTH, DAY' | cat - prerules_$today_date.csv > rules_$today_date.csv

mail -s "Rules for $year-$month-$day" ratasuwantong@gmail.com < /local/home/rata.suwantong/rules_$today_date.csv
