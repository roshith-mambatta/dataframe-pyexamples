from pyspark.sql import SparkSession,Row
from distutils.util import strtobool
import os.path
import yaml

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("RDD examples") \
        .master('local[*]') \
        .getOrCreate()

    current_dir = os.path.abspath(os.path.dirname(__file__))
    appConfigFilePath = os.path.abspath(current_dir + "/../"+"application.yml")

    with open(appConfigFilePath) as conf:
        doc = yaml.load(conf,Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = sparkSession.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", doc["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", doc["s3_conf"]["secret_access_key"])
    hadoop_conf.set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")

    #call to DataFrame creation.
    demographicsRDD = sparkSession.sparkContext.textFile("s3a://"+doc["s3_conf"]["s3_bucket"]+"/demographic.csv")
    financesRDD = sparkSession.sparkContext.textFile("s3a://"+doc["s3_conf"]["s3_bucket"]+"/finances.csv")
    coursesRDD = sparkSession.sparkContext.textFile("s3a://"+doc["s3_conf"]["s3_bucket"]+"/course.csv")
    print(demographicsRDD.count())
    print(demographicsRDD.take(5))
    mappedDemographicsRDD= demographicsRDD.map(lambda l: l.split(",")) \
        .map(lambda row: Row(id=int(row[0]),
                             age=int(row[1]),
                             codingBootcamp=strtobool(row[2]),
                             country=row[3],
                             gender=row[4],
                             isEthnicMinority=strtobool(row[5]),
                             servedInMilitary=strtobool(row[6]),
                             courseId=int(row[7]))
             )

    print("\nConvert to DF:Method 1")
    DemographicsDF= sparkSession.createDataFrame(mappedDemographicsRDD)
    DemographicsDF.show()
    #OR
    print("\nConvert to DF:Method 2")
    mappedDemographicsRDD.toDF().show()

    print("\nDataFrame Schema")
    DemographicsDF.printSchema()
    print("\nDataFrame filter")
    DemographicsDF.filter("courseId=2").show()
