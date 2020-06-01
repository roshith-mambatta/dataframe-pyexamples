from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, date_format, expr

if __name__ == '__main__':
    # Create the SparkSession
    # https://mvnrepository.com/artifact/org.apache.spark/spark-avro_2.12/2.4.5
    # Run on command: >pyspark --packages com.databricks:spark-avro_2.11-4.0.0
    sparkSession = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .master('local[*]') \
        .getOrCreate()

    # write into NYC_OMO_YEAR_WISE
    #org.apache.spark.sql.functions.date_format
    dfFromParquet=sparkSession\
        .read\
        .format("parquet")\
        .load("/00_MyDrive/ApacheSpark/AWS_data/roshith-bucket/NYC_OMO")\
        .withColumn("OrderYear",date_format("OMOCreateDate","YYYY"))\
        .repartition(5)

    #sparkSession.sparkContext.hadoopConfiguration.set("spark.sql.parquet.filterPushdown", "true")

    dfFromParquet.printSchema()
    dfFromParquet.show(5,False)

    dfFromParquet\
        .write\
        .partitionBy("OrderYear")\
        .mode("overwrite")\
        .parquet("/00_MyDrive/ApacheSpark/AWS_data/roshith-bucket/NYC_OMO_YEAR_WISE")

    nycOmoDf = sparkSession.read\
        .parquet("/00_MyDrive/ApacheSpark/AWS_data/roshith-bucket/NYC_OMO_YEAR_WISE")\
        .repartition(5)

    parquetExplianPlan =nycOmoDf \
        .select("OMOID","OMONumber","BuildingID") \
        .filter((col("OrderYear") == "2018") & (col("Lot") > "50"))

    print("spark.sql.parquet.filterPushdown:", sparkSession.conf.get("spark.sql.parquet.filterPushdown"))
    print("spark.sql.parquet.mergeSchema:", sparkSession.conf.get("spark.sql.parquet.mergeSchema"))

    parquetExplianPlan.explain()
    #parquetExplianPlan.explain(True)

    # turn on Parquet push-down, stats filtering, and dictionary filtering
    sparkSession.conf.set('spark.sql.parquet.filterPushdown',"true")
    print("spark.sql.parquet.filterPushdown", sparkSession.conf.get("spark.sql.parquet.filterPushdown"))
    sparkSession.conf.set('parquet.filter.statistics.enabled',"true")
    sparkSession.conf.set('parquet.filter.dictionary.enabled',"true")

    #use the non-Hive read path
    sparkSession.conf.set("spark.sql.hive.convertMetastoreParquet", "true")

    # turn off schema merging, which turns off push-down
    sparkSession.conf.set("spark.sql.parquet.mergeSchema", "false")
    sparkSession.conf.set("spark.sql.hive.convertMetastoreParquet.mergeSchema","false")

    parquetExplianPlan1 =nycOmoDf \
        .select("OMOID","OMONumber","BuildingID") \
        .filter((col("OrderYear") == "2018") & (col("Lot") > "50"))

    parquetExplianPlan1.explain()