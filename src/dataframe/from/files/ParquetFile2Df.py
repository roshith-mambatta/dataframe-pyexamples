from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import os.path
import yaml

if __name__ == '__main__':
    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("Read Files") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4,org.apache.spark:spark-avro_2.11:2.4.5') \
        .getOrCreate()

    current_dir = os.path.abspath(os.path.dirname(__file__))
    appConfigFilePath = os.path.abspath(current_dir + "/../../../"+"application.yml")

    with open(appConfigFilePath) as conf:
        doc = yaml.load(conf,Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = sparkSession.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", doc["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", doc["s3_conf"]["secret_access_key"])
    hadoop_conf.set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")

    print("\nCreating dataframe from parquet file using 'SparkSession.read.parquet()',")
    nycOmoDf = sparkSession.read\
        .parquet("s3a://"+doc["s3_conf"]["s3_bucket"]+"/NYC_OMO")\
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
        .show(5)

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
        .parquet("s3a://"+doc["s3_conf"]["s3_bucket"]+"/nyc_omo_data")\
