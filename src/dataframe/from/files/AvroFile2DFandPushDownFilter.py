from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, BooleanType,DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

if __name__ == '__main__':
    # Create the SparkSession
    # https://mvnrepository.com/artifact/org.apache.spark/spark-avro_2.12/2.4.5
    # Run on command: >pyspark --packages com.databricks:spark-avro_2.11-4.0.0
    sparkSession = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .master('local[*]') \
        .config('spark.jars.packages', 'com.databricks:spark-avro_2.11:4.0.0') \
        .getOrCreate()

    print("\nCreating DF from csv and write as avro 'SparkSession.read.format(csv)',")
    finSchema = StructType()\
        .add("id", IntegerType(),True)\
        .add("has_debt", BooleanType(),True)\
        .add("has_financial_dependents", BooleanType(),True)\
        .add("has_student_loans", BooleanType(),True)\
        .add("income", DoubleType(),True)

    print("\n Check Pushdown filter for CSV,")

    finDf = sparkSession.read\
        .option("header", "false")\
        .option("delimiter", ",")\
        .format("csv")\
        .schema(finSchema)\
        .load("/00_MyDrive/ApacheSpark/AWS_data/roshith-bucket/finances.csv")

    csvExplianPlan =finDf.select("id","income").filter("has_debt=true")._jdf.queryExecution().toString()# or .simpleString()
    print(" avroExplianPlan: " + csvExplianPlan)

    print("\n Check Pushdown filter for Avro")

    finAvroDF = sparkSession\
        .read\
        .format("com.databricks.spark.avro")\
        .load("/00_MyDrive/ApacheSpark/AWS_data/roshith-bucket/finAvro")

    """
    avroExplianPlan =finAvroDF.select("id","income").filter("has_debt=true")._jdf.queryExecution().toString()
    print(" avroExplianPlan: " + avroExplianPlan)
    #finAvroDF.printSchema()

    myWindow = Window.partitionBy("has_debt").orderBy(col("income").desc())
    #Option 1: //    finAvroDF.withColumn("ranked",row_number() over myWindow).show(false)
    finAvroDF\
        .withColumn("ranked",row_number().over(Window.partitionBy("has_debt").orderBy(col("income").desc())))\
        .withColumns("windowedSum",sum("income").over(Window.partitionBy("has_debt")))\
        .withColumn("windowedRunningTotal",sum("income").over(Window.partitionBy("has_debt").orderBy(col("income").desc())))\
        .show(5,False)
    """