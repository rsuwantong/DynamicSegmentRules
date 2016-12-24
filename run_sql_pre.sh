####################################################################################################
#! /bin/bash
####################################################################################################
# Universal shell script to call Impala sql script
# The SQL needs to develop in such ways that it accepts the parameters from the shell
####################################################################################################
# Version Control (MM/DD/YYYY):
#  12/06/2016 SN: Initial Version
#  12/23/2016 RS: Extend date validility to all years, months, days (only with data delay, n = 1 day)
# tr -d '\r' <run_sql_pre.sh > run_sql.sh
# rm run_sql_pre.sh
# chmod 777 run_sql.sh
####################################################################################################

# Initialize the value
n=1 # How delay we will get the data
year=$(date --d="today"  +"%Y")
month=$(date --d="today"  +"%m")
day=$(date --d="today" +"%d")

checkleap=$((2016-$year))

case $month in

	(2|4|6|8|9|11)	if [ $day == 1 ]; then
							day=31
							month=$(($month-1))
						else 
							day=$(($day-1))
					fi
						;;
	(5|7|10|12)		if [ $day == 1 ]; then
							day=30
							month=$(($month-1))
						else 
							day=$(($day-1))
					fi
						;;
	1)				if [ $day == 1 ]; then
							day=31
							month=12
							year=$(($year-1))
						else 
							day=$(($day-1))
					fi
						;;
	3 )				if [[ ( $day == 1 ) && ( `expr $checkleap % 4` -ne 0 )]]; then 
							day=28
							month=$(($month-1))
						elif [[ ( $day == 1 ) && ( `expr $checkleap % 4` == 0 )]]; then 	
							day=29
							month=$(($month-1))
						else 
							day=$(($day-1))
					fi
	
esac
						

for job_sql in "$@"
do
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] Start run Impala SQL : $job_sql"
        impala-shell -i impala.prd.sg1.tapad.com --var=year=$year --var=month=$month --var=day=$day -f $job_sql
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] $job_sql run completed"
done


####################################################################################################
# Example of SQL script:
# select * from default.id_syncs where year=${var:year} and month=${var:month} and day=${var:day} limit 10
####################################################################################################


## Example of crontab usage (crontab -e)
#00 12 6 * * run_sql.sh test.sql