#!/bin/bash
echo "STARTED JOB FOR DOWNLOADING DATA FROM RDS"

spark-submit --jars "/home/cloudera/Desktop/bala/ext-jars/*" --conf "spark.driver.extraClassPath=/home/cloudera/Desktop/bala/ext-jars/*" --master local[*] /home/cloudera/Desktop/bala/project1/PullRDSData.py jdbc jdbc:mysql://database-1.cpiyydvxgr0g.ap-south-1.rds.amazonaws.com/zeyo com.mysql.cj.jdbc.Driver web_increment_data_src_log root Adityausa908

echo "COMPLETED DOWNLOADING SUCCESSFULLY FROM RDS"

echo "GOING TO START THE SPARK INCREMENTAL DATA INGESTION JOB"

spark-submit --conf "spark.driver.extraClassPath=/home/cloudera/Desktop/bala/ext-jars/*" --master local[*] /home/cloudera/Desktop/bala/project1/App.py json jdbc:mysql://database-1.cpiyydvxgr0g.ap-south-1.rds.amazonaws.com/zeyo com.mysql.jdbc.Driver web_increment_data_src_log root Adityausa908 https://randomuser.me/api/0.8/?results=10 50 overwrite /user/cloudera/aws/rdbms/last_update_record/ com.databricks.spark.avro overwrite com.databricks.spark.avro overwrite /user/cloudera/project1/ /user/cloudera/project1/ /home/cloudera/Desktop/bala/rd2/aws/data2/rds2-weblogs-part2.txt

echo "JOB COMPLETED SUCCESSFULLY....."
