#!/bin/bash
###############################################################
# Name: feedback_tbl
# Description: Create an initial Impala table of essential feedback table for segmentation rules
# Input: taps & tracked_events
# Version:
#   2016/12/06 RS: Initial version
# To run the code on Linux:
# tr -d '\r' <init_feedback_tbl_pre.sh > init_feedback_tbl.sh
# chmod 777 init_feedback_tbl.sh
# /local/home/rata.suwantong/init_feedback_tbl.sh
###############################################################

queryinit=$"drop table if exists sgmt_rules.feedback_tbl;


create table sgmt_rules.feedback_tbl
(sighted_date STRING, offer STRING, source STRING, hl_platform STRING, dvc_techname STRING, 
imps BIGINT, clicks BIGINT, landings BIGINT, submits BIGINT ) partitioned by (year INT, month INT, day INT) stored as PARQUET;"

impala-shell -i impala.prd.sg1.tapad.com:21000 -q "$queryinit"

n=1 # How delay we will get the data
today_year=$(date --d="today"  +"%Y")
today_month=$(date --d="today"  +"%m")
today_day=$(date --d="-$n days" +"%d") #today is the day we run the initial table before appending


year=2016 #first day of Tapad data in SG1
month=8
day=15
arr31=("1" "3" "5" "7" "8" "10" "12")
arr30=("4" "6" "9" "11")

while ([ $day -le $today_day ] && [ $month == $today_month ] && [ $year == $today_year ]) || ([ $month -lt $today_month ] && [ $year == $today_year ]); do
 
echo "$year $month $day is running"
	
	impala-shell -i impala.prd.sg1.tapad.com:21000 --var=year=$year --var=month=$month --var=day=$day -f /local/home/rata.suwantong/2get_feedback.sql

checkleap=$((2016-$year))

case $month in

	(1|3|5|7|8|10|12)	if [ $day == 31 ]; then
							day=1
							month=$(($month+1))
						else 
							day=$(($day+1))
						fi
						;;
	(4|6|9|11)		if [ $day == 30 ]; then
							day=1
							month=$(($month+1))
						else 
							day=$(($day+1))
						fi
						;;
	2 )					if [[ ( $day == 28 ) && ( $month == 2 ) && ( `expr $checkleap % 4` -ne 0 )]]; then 
							day=1
							month=$(($month+1))
						elif [[ ( $day == 29 ) && ( $month == 2 ) && ( `expr $checkleap % 4` == 0 )]]; then 	
							day=1
							month=$(($month+1))
						else 
							day=$(($day+1))
						fi
esac
						
	if [ $day == 31 ] && [ $month == 12 ]; then
		year=$(($year+1))
	fi
	
done

