from pyspark.sql import SparkSession
import os.path
import yaml

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    #OR
    # add -->.config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4')
    # while creating SparkSession

    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .master('local[*]') \
        .getOrCreate()

    current_dir = os.path.abspath(os.path.dirname(__file__))
    appConfigFilePath = os.path.abspath(current_dir + "/../../../../"+"application.yml")

    with open(appConfigFilePath) as conf:
        doc = yaml.load(conf,Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = sparkSession.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", doc["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", doc["s3_conf"]["secret_access_key"])
    hadoop_conf.set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")

    employeeDf = sparkSession.read \
        .json("s3a://"+ doc["s3_conf"]["s3_bucket"]+"/cart_sample_small.txt")

    employeeDf.printSchema()
    employeeDf.show(5,False)