from pyspark.sql import SparkSession,Row
import os.path
import yaml

if __name__ == '__main__':
    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .master('local[*]') \
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    current_dir = os.path.abspath(os.path.dirname(__file__))
    appConfigFilePath = os.path.abspath(current_dir + "/../../../"+"application.yml")

    with open(appConfigFilePath) as conf:
        doc = yaml.load(conf,Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf=sparkSession.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", doc["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", doc["s3_conf"]["secret_access_key"])
    hadoop_conf.set('fs.s3a.endpoint','s3.eu-west-1.amazonaws.com')

    txnFctRdd=sparkSession.sparkContext.textFile("s3a://"+ doc["s3_conf"]["s3_bucket"]+"/txn_fct.csv") \
        .filter(lambda record:record.find("txn_id"))\
        .map(lambda record:record.split("|"))\
        .map(lambda record:(int(record[0]),
                            record[1],
                            float(record[2]),
                            record[3],
                            record[4],
                            record[5],
                            record[6])
            )
    # RDD[(Long, Long, Double, Long, Int, Long, String)]
    print(txnFctRdd.take(5))

    print("\nConvert RDD to Dataframe using toDF() - without column names,")
    txnDfNoColNames = txnFctRdd.toDF()
    txnDfNoColNames.printSchema()
    txnDfNoColNames.show(5, False)

    print("\nCreating Dataframe out of RDD without column names using createDataframe(),")
    txnDfNoColNames2 = sparkSession.createDataFrame(txnFctRdd)
    txnDfNoColNames2.printSchema()
    txnDfNoColNames2.show(5,False)

    print("\nConvert RDD to Dataframe using toDF(colNames: String*) - with column names,")
    txnDfWithColName = txnFctRdd.toDF(["txn_id",
                                      "created_time_string",
                                      "amount",
                                      "cust_id",
                                      "status",
                                      "merchant_id",
                                      "created_time_ist"
                                      ])
    txnDfWithColName.printSchema()
    txnDfWithColName.show(5,False)
