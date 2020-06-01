from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col

if __name__ == '__main__':
    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .master('local[*]') \
        .getOrCreate()

    employeeDf = sparkSession.read\
        .json("/00_MyDrive/ApacheSpark/AWS_data/roshith-bucket/cart_sample_small.txt")

    employeeDf.printSchema()
    employeeDf.show(5,False)

    employeeDf.select(col("cart.swid").alias("cust_id")).show(5,False)
    employeeDf.select(explode("cart.vacationOffer.package.room").alias("vacation_room")).show(5,False)
