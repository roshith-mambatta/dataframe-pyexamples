from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, BooleanType,DoubleType

if __name__ == '__main__':
    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .getOrCreate()

    print("\nCreating dataframe from CSV file using 'SparkSession.read.format()',")

    finSchema = StructType()\
        .add("id", IntegerType(),True)\
        .add("has_debt", BooleanType(),True)\
        .add("has_financial_dependents", BooleanType(),True)\
        .add("has_student_loans", BooleanType(),True)\
        .add("income", DoubleType(),True)

    finDf = sparkSession.read\
        .option("header", "false")\
        .option("delimiter", ",")\
        .format("csv") \
        .schema(finSchema)\
        .load("/00_MyDrive/ApacheSpark/AWS_data/roshith-bucket/finances.csv")

    finDf.printSchema()
    finDf.show()

    print("Creating dataframe from CSV file using 'SparkSession.read.csv()',")

    financeDf = sparkSession.read\
        .option("mode", "DROPMALFORMED")\
        .option("header", "false")\
        .option("delimiter", ",")\
        .option("inferSchema", "true")\
        .csv("/00_MyDrive/ApacheSpark/AWS_data/roshith-bucket/finances.csv")\
        .toDF("id", "has_debt", "has_financial_dependents", "has_student_loans", "income")

    print("Number of partitions = " + str(finDf.rdd.getNumPartitions))
    financeDf.printSchema()
    financeDf.show()

    financeDf\
        .repartition(2)\
        .write\
        .partitionBy("id")\
        .mode("overwrite")\
        .option("header", "true")\
        .option("delimiter", "~")\
        .csv("/00_MyDrive/ApacheSpark/AWS_data/roshith-bucket/savedOutput/fin")
