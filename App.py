import urllib.request
import json
from pyspark import SparkContext
from pyspark.sql import functions as f
from pyspark.sql import *
from sys import *
from datetime import date


def main():
    sc = SparkContext(master='local', appName='test')
    sc.setLogLevel('ERROR')
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

    schemaCatalog = ''.join("""{
     "table":{"namespace":"DLM", "name":"Weblog_user_view_DLM"},
     "rowkey":"key",
     "columns":{
        "id":{"cf":"rowkey", "col":"key", "type":"string"},
        "username":{"cf":"view_cf", "col":"username", "type":"string"},
        "amount":{"cf":"view_cf", "col":"amount", "type":"string"},
        "ip":{"cf":"view_cf", "col":"ip", "type":"string"},
        "createdt":{"cf":"view_cf", "col":"createdt", "type":"string"},
        "value":{"cf":"view_cf", "col":"value", "type":"string"},
        "score":{"cf":"view_cf", "col":"score", "type":"string"},
        "regioncode":{"cf":"view_cf", "col":"regioncode", "type":"string"},
        "status":{"cf":"view_cf", "col":"status", "type":"string"},
        "method":{"cf":"view_cf", "col":"method", "type":"string"},
        "key":{"cf":"view_cf", "col":"key", "type":"string"},
        "count":{"cf":"view_cf", "col":"count", "type":"string"},
        "type":{"cf":"view_cf", "col":"type", "type":"string"},
        "site":{"cf":"view_cf", "col":"site", "type":"string"},
        "statuscode":{"cf":"view_cf", "col":"statuscode", "type":"string"}
      }
      }""".split())

    # Helper method used to removeDigits from String.
    def removeDigits(s):
        return ''.join(filter(lambda x: not x.isdigit(), s))

    # get the RandomUsers from https://randomuser.me/api/0.8/?results=10
    def getRandomUserWebApiDataFrame():

        final_list = []
        user_set = set()
        results_json = dict()
        randomuserurl = argv[7]
        ALLOWED_RANDOM_USER = int(argv[8])

        while True:
            resp = json.loads(urllib.request.urlopen(randomuserurl).read().decode('utf-8'))
            for randomuser in resp['results']:
                username = removeDigits(randomuser.get('user').get('username'))
                if username not in user_set:
                    user_set.add(username)
                    final_list.append(randomuser)
                if len(user_set) == ALLOWED_RANDOM_USER:
                    break

            if len(user_set) == ALLOWED_RANDOM_USER:
                break

        results_json["results"] = final_list
        json_df = spark.read.json(sc.parallelize([results_json]))
        users = json_df.withColumn("results", f.explode(f.col("results"))) \
            .withColumn("r_username", f.regexp_replace("results.user.username", '[0-9]', '')) \
            .withColumn("dob", f.from_unixtime(f.unix_timestamp(f.col('results.user.dob').cast('timestamp')), 'dd-MM-yyyy')) \
            .withColumn("user_current_date", f.current_date()) \
            .select("r_username", "results.user.gender", "results.user.name.title",
                    f.col("results.user.name.first").alias("firstname"),
                    f.col("results.user.name.last").alias("lastname"),
                    f.col("results.user.location.street").alias("location_street"),
                    f.col("results.user.location.city").alias("location_city"),
                    f.col("results.user.location.state").alias("location_state"),
                    f.col("results.user.location.zip", ).alias("location_zip"),
                    "results.user.email", "results.user.password", "results.user.salt", "results.user.md5",
                    "results.user.sha1", "results.user.sha256", "results.user.registered", "dob", "results.user.phone",
                    "results.user.cell", f.col("results.user.picture.large").alias("picture_large"),
                    f.col("results.user.picture.medium").alias("picture_medium"), "results.user.picture.thumbnail",
                    "user_current_date")

        return users

    # get the data from Rdbms
    def getReadRDBMSDataFrame():

        from_format = "dd/MMM/yyyy:HH:mm:ss"
        to_format = 'dd-MM-yyyy'
        storeIncrementalValue(0, "ignore")

        df = spark.read.format("csv") \
            .option("inferSchema", True) \
            .option("header", True) \
            .load(argv[17])

        df = df.filter(df.createdt.isNotNull() & (df.createdt!="null"))

        result = df.withColumn("createdt", f.from_unixtime(f.unix_timestamp(f.col("createdt"), from_format), to_format)) \
            .withColumn("value",f.col("value").cast(f.StringType()))\
            .withColumn("score",f.col("score").cast(f.StringType()))\
            .withColumn("regioncode",f.col("regioncode").cast(f.StringType()))\
            .withColumn("status",f.col("status").cast(f.StringType()))\
            .withColumn("count",f.col("count").cast(f.StringType()))\
            .withColumn("statuscode",f.col("statuscode").cast(f.StringType()))\
            .select("id", "username", "amount", "ip", "createdt", "value", "score", "regioncode", "status", "method",
                    "key", "count", "type", "site", "statuscode") \
            .withColumn("rdbms_current_date", f.current_date()) \
            .filter(f.col("id") > getIncrementalValue())

        result = result.withColumn("id",f.col("id").cast(f.StringType())).na.fill("Not_Applicable")

        max_value  = df.agg({"id": "max"}).collect()[0]
        storeIncrementalValue(max_value["max(id)"], "overwrite")

        result.printSchema()

        return result

    # Helper method used to store the last updated value. argv[10]
    def getIncrementalValue():
        df = spark.read.format("csv").option("header", True).load(argv[10])
        value = df.agg({"last_updated_value": "max"}).collect()[0]
        id = value["max(last_updated_value)"]
        print("following incremental value retrieved from hdfs "+str(id))
        return id

    # Helper method used to store the last updated value. argv[9]
    def storeIncrementalValue(s, modetype):
        if modetype == "overwrite":
         print("following id get stored for incremental "+ str(s))
        df = (sc.parallelize([(str(date.today()), s)]).toDF(["date", "last_updated_value"]))
        df.coalesce(1).write.format("csv") \
            .option("inferSchema", True) \
            .option("header", True) \
            .mode(modetype)\
            .save(argv[10])

    # Helper method used to store the last updated value. argv[9]
    def storeIncrementalValuetoHive(s):
        df = (sc.parallelize([(str(date.today()), s)]).toDF(["last_update_date", "last_updated_id"]))
        df.coalesce(1).write.format("hive") \
            .mode("overwrite")\
            .saveAsTable("zeyodb.weblog_user_web_dlm_analytics_counter")

    df1 = getReadRDBMSDataFrame()
    df2 = getRandomUserWebApiDataFrame()

    df1.show(5)
    df2.show(5)

    # raw rdbms data write into hdfs directory
    df1.write.format(argv[11]).mode(argv[12]) \
        .partitionBy("rdbms_current_date") \
        .save(argv[16] + str(date.today()) + "/raw_dml_data")

    # write df1 to the hbase table
    hbase_df = df1
    hbase_df = hbase_df.drop("day").drop("month").drop("year").drop("rdbms_current_date")
    hbase_df.show(2)
    hbase_df.printSchema()
    hbase_df.write.options(catalog=schemaCatalog,newTable = "5") \
        .format("org.apache.spark.sql.execution.datasources.hbase") \
        .save()

    # raw webapi data write into hdfs directory
    df2.write.format(argv[13]).mode(argv[14]) \
        .partitionBy("user_current_date") \
        .save(argv[15] + str(date.today()) + "/raw_web_data")

    # final dataframe abfter join df1 and df2
    cond = [df1.username == df2.r_username]
    df3 = df1.join(df2, cond, how='left').na.fill("Not_Applicable")
    df3 = df3.drop(df3.r_username).drop(df3.user_current_date).drop(df3.rdbms_current_date)
    df3 = df3.withColumn("year",  df3["createdt"].substr(7, 4)) \
             .withColumn("month", df3["createdt"].substr(4, 2)) \
             .withColumn("day",   df3["createdt"].substr(0, 2)) \

    df3.show(5)
    df3.printSchema()

    df3.write.format("hive") \
        .mode("append") \
        .insertInto("zeyodb.weblog_user_web_dlm_analytics")

    storeIncrementalValuetoHive(getIncrementalValue())

if __name__ == "__main__":
    main()

