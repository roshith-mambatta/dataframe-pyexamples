from pyspark.sql import SparkSession,Row
from distutils.util import strtobool

if __name__ == '__main__':
    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .getOrCreate()

    # SparkContext from the SparkSession
    sc = sparkSession._sc

    # SQLContext instantiated with Java components
    sqlContext = sparkSession._wrapped

    # Here we call our Scala function by accessing it from the JVM, and
    # then convert the resulting DataFrame to a Python DataFrame. We need
    # to pass the Scala function the JVM version of the SparkContext, as
    # well as our string parameter, as we're using the SparkContext to read
    # in the input data in our Scala function. In order to create the Python
    # DataFrame, we must provide the JVM version of the SQLContext during the
    #call to DataFrame creation.
    demographicsRDD = sparkSession.sparkContext.textFile("/00_MyDrive/ApacheSpark/AWS_data/roshith-bucket/demographic.csv")
    financesRDD = sparkSession.sparkContext.textFile("/00_MyDrive/ApacheSpark/AWS_data/roshith-bucket/finances.csv")
    coursesRDD = sparkSession.sparkContext.textFile("/00_MyDrive/ApacheSpark/AWS_data/roshith-bucket/course.csv")
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
    DemographicsDF= sqlContext.createDataFrame(mappedDemographicsRDD)
    DemographicsDF.show()
    #OR
    print("\nConvert to DF:Method 2")
    mappedDemographicsRDD.toDF().show()

    print("\nDataFrame Schema")
    DemographicsDF.printSchema()
    print("\nDataFrame filter")
    DemographicsDF.filter("courseId=2").show()
