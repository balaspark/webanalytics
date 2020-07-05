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
    spark = SparkSession.builder.getOrCreate()
    

    # get the data from Rdbms
    def getReadRDBMSDataFrame():

        rdbms = spark.read.format(argv[1]) \
            .option("url", argv[2]) \
            .option("driver", argv[3]) \
            .option("dbtable", argv[4]) \
            .option("user", argv[5]) \
            .option("password", argv[6]) \
            .option("inferSchema", True) \
            .load()

        rdbms.show(3)
 
        print(rdbms.count())

        rdbms.printSchema()

        rdbms.write.format("csv") \
            .mode("overwrite") \
            .option("header", "true") \
            .save("file:///home/cloudera/Desktop/bala/rd2/aws/data2/rds2-weblogs-part2.txt")
  
        print("completed now ...")


     
    getReadRDBMSDataFrame()


    

if __name__ == "__main__":
    main()

