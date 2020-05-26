from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

if __name__ == '__main__':
    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .getOrCreate()

    print("\nCreating dataframe from parquet file using 'SparkSession.read.parquet()',")
    nycOmoDf = sparkSession.read\
        .parquet("/00_MyDrive/ApacheSpark/AWS_data/roshith-bucket/NYC_OMO")\
        .repartition(5)

    print("# of records = " + str(nycOmoDf.count()))
    print("# of partitions = " + str(nycOmoDf.rdd.getNumPartitions))

    nycOmoDf.printSchema()

    print("Summery of NYC Open Market Order (OMO) charges dataset,")
    nycOmoDf.describe().show()

    print("OMO frequency distribution of different Boroughs,")
    nycOmoDf.groupBy("Boro")\
        .agg({"Boro" : "count"})\
        .withColumnRenamed("count(Boro)", "OrderFrequency")\
        .show()

    print("OMO's Zip & Borough list,")

    boroZipDf = nycOmoDf\
        .select("Boro", nycOmoDf["Zip"].cast(IntegerType()))\
        .groupBy("Boro")\
        .agg({"Zip" :"collect_set"})\
        .withColumnRenamed("collect_set(Zip)", "ZipList")\
        .withColumn("ZipCount", F.size("ZipList"))

    boroZipDf\
        .select("Boro", "ZipCount", "ZipList")\
        .show()

    # Window functions

    omoCreateDatePartitionWindow = Window.partitionBy("OMOCreateDate")
    omoDailyFreq = nycOmoDf\
        .withColumn("OMODailyFreq", F.count("OMOID").over(omoCreateDatePartitionWindow).alias("OMODailyFreq"))

    print("# of partitions in window'ed OM dataframe = " + str(omoDailyFreq.count()))
    omoDailyFreq.show(5)


    omoDailyFreq.select("OMOCreateDate", "OMODailyFreq")\
        .distinct()\
        .show(5)

    omoDailyFreq\
        .repartition(10)\
        .write\
        .mode("overwrite")\
        .parquet("/00_MyDrive/ApacheSpark/AWS_data/roshith-bucket/savedOutput/nyc_omo_data")\
