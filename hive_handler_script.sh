#!/bin/bash
set mapred.job.map.memory.mb=6000;
set mapred.job.reduce.memory.mb=4000;
export HADOOP_HEAPSIZE=50000

#!/bin/bash

result=$(hive -e 'select 1 from zeyodb.weblog_user_web_dlm_analytics limit 1')

if [[ $result = "0" ]]; then
   echo "first run....."$result
hive -e 'INSERT INTO TABLE zeyodb.Weblog_user_web_DLM_analytics_handler 
SELECT id, username, amount, ip, createdt, value, score, regioncode, status, method, key, count, type, site,
statuscode, gender, title, firstname, lastname, location_street, location_city, location_state, location_zip,
email, password, salt, md5, sha1, sha256, registered, dob, phone, cell, picture_large, picture_medium, thumbnail FROM zeyodb.Weblog_user_web_DLM_analytics'
else
   echo "incremental run"$result
hive -e  'INSERT INTO TABLE zeyodb.Weblog_user_web_DLM_analytics_handler 
SELECT id, username, amount, ip, createdt, value, score, regioncode, status, method, key, count, type, site,
statuscode, gender, title, firstname, lastname, location_street, location_city, location_state, location_zip,
email, password, salt, md5, sha1, sha256, registered, dob, phone, cell, picture_large, picture_medium, thumbnail FROM zeyodb.Weblog_user_web_DLM_analytics
where id > 999995'
fi
