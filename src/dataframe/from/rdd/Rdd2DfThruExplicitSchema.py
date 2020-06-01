from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import unix_timestamp,approx_count_distinct,sum
from pyspark.sql.types import StructType,StructField, IntegerType, LongType,DoubleType,StringType,TimestampType
import os.path
import yaml


if __name__ == '__main__':
    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .config('spark.jars.packages', 'com.amazonaws:aws-java-sdk:1.7.4') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .master('local[*]') \
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    print("\nConvert RDD to Dataframe using SparkSession.createDataframe(),")
    #Creating RDD of Row
    txnFctRdd = sparkSession.sparkContext.textFile("/00_MyDrive/ApacheSpark/AWS_data/roshith-bucket/txn_fct.csv")\
        .filter(lambda record: record.find("txn_id"))\
        .map(lambda record: record.split("|"))\
        .map(lambda record: Row(
                            int(record[0]),
                            int(record[1]),
                            float(record[2]),
                            int(record[3]),
                            int(record[4]),
                            int(record[5]),
                            record[6])
             )
        #RDD[Row[Long, Long, Double, Long, Int, Long, String]]

    # Creating the schema
    txnFctSchema = StructType([
        StructField("txn_id", LongType(), False),
        StructField("created_time_str", LongType(), False),
        StructField("amount", DoubleType(), True),
        StructField("cust_id", LongType(), True),
        StructField("status", IntegerType(), True),
        StructField("merchant_id", LongType(), True),
        StructField("created_time_ist", StringType(), True)
        ])

    txnFctDf = sparkSession.createDataFrame(txnFctRdd, txnFctSchema)
    txnFctDf.printSchema()
    txnFctDf.show(5, False)

    # Applying tranformation on dataframe using DSL (Domain Specific Language)
    txnFctDf = txnFctDf\
        .withColumn("created_time_ist", unix_timestamp(txnFctDf["created_time_ist"], "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))

    txnFctDf.printSchema()
    txnFctDf.show(5, False)

    print("# of records = " + str(txnFctDf.count()))
    print("# of merchants = " + str(txnFctDf.select(txnFctDf["merchant_id"]).distinct().count()))

    txnAggDf = txnFctDf\
        .repartition(10, txnFctDf["merchant_id"])\
        .groupBy("merchant_id")\
        .agg(sum("amount"), approx_count_distinct("status"))

    txnAggDf.show(5, False)

    txnAggDf\
        .withColumnRenamed("sum(amount)", "total_amount")\
        .withColumnRenamed("approx_count_distinct(status)", "dist_status_count")\

    txnAggDf.show(5, False)