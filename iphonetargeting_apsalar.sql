select model, user_agent, count(distinct tapad_id) as reach from apollo.dtac_vertical_dataset where id_type = "HARDWARE_IDFA" and vendor = 'Apple' and event_source ='Apsalar' and year = 2017 group by model, user_agent order by reach desc;


/*
Query submitted at: 2017-02-10 03:17:18 (Coordinator: None)
Query progress can be monitored at: None/query_plan?query_id=794a05a59ac94405:e20683d6b9e58e92
+----------------------+---------------------+-------+
| model                | user_agent          | reach |
+----------------------+---------------------+-------+
| iPhone 6             | iOS Apple iPhone7,2 | 40945 |
| iPhone 5S            | iOS Apple iPhone6,2 | 30571 |
| iPhone 6S            | iOS Apple iPhone8,1 | 30369 |
| iPhone 5             | iOS Apple iPhone5,2 | 20454 |
| iPhone 6 Plus        | iOS Apple iPhone7,1 | 16483 |
| iPhone 6S Plus       | iOS Apple iPhone8,2 | 13531 |
| iPhone 7 Plus        | iOS Apple iPhone9,4 | 13374 |
| iPhone 7             | iOS Apple iPhone9,3 | 12415 |
| iPhone 4S            | iOS Apple iPhone4,1 | 11387 |
| iPhone SE            | iOS Apple iPhone8,4 | 7912  |
| iPhone 5S            | iOS Apple iPhone6,1 | 6453  |
| iPad mini            | iOS Apple iPad2,7   | 6164  |
| iPad /retina display | iOS Apple iPad3,3   | 5818  |
| iPad mini            | iOS Apple iPad2,2   | 5285  |
| iPad mini            | iOS Apple iPad2,5   | 4920  |
| iPad Air 2           | iOS Apple iPad5,4   | 4593  |
| iPhone 4             | iOS Apple iPhone3,1 | 4408  |
| iPad /retina display | iOS Apple iPad3,6   | 4354  |
| iPad Air 2           | iOS Apple iPad5,3   | 4018  |
| iPhone               | iOS Apple iPhone5,4 | 2444  |
| iPhone               | iOS Apple iPhone5,1 | 2316  |
| iPad mini            | iOS Apple iPad2,1   | 2216  |
| iPad Air 2           | iOS Apple iPad5,2   | 2172  |
| iPad /retina display | iOS Apple iPad3,4   | 2167  |
| iPad /retina display | iOS Apple iPad3,1   | 1861  |
| iPad Air 2           | iOS Apple iPad5,1   | 1848  |
| iPhone 7             | iOS Apple iPhone9,1 | 1632  |
| iPhone 7 Plus        | iOS Apple iPhone9,2 | 1600  |
| iPhone 4             | iOS Apple iPhone3,2 | 934   |
| iPod Touch 5         | iOS Apple iPod5,1   | 539   |
| iPhone 5C            | iOS Apple iPhone5,3 | 525   |
| iPad mini            | iOS Apple iPad2,4   | 488   |
| iPod Touch 5         | iOS Apple iPod7,1   | 208   |
| iPod Touch 5         | iOS Apple iPod4,1   | 116   |
| iPad mini            | iOS Apple iPad2,6   | 85    |
| iPhone               | iOS Apple iPhone2,1 | 72    |
| iPad /retina display | iOS Apple iPad3,5   | 70    |
| iPad /retina display | iOS Apple iPad3,2   | 20    |
| iPad mini            | iOS Apple iPad2,3   | 16    |
| iPhone 4             | iOS Apple iPhone3,3 | 12    |
| iPhone               | iOS Apple iPhone8,3 | 11    |
| iPhone               | iOS Apple iPhone7,3 | 11    |
+----------------------+---------------------+-------+
Fetched 42 row(s) in 3.45s
*/