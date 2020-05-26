from pyspark.sql import SparkSession,Row
from distutils.util import strtobool

if __name__ == '__main__':
    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .getOrCreate()

    demographicsRDD = sparkSession.sparkContext.textFile("/00_MyDrive/ApacheSpark/AWS_data/roshith-bucket/demographic.csv")
    financesRDD = sparkSession.sparkContext.textFile("/00_MyDrive/ApacheSpark/AWS_data/roshith-bucket/finances.csv")
    coursesRDD = sparkSession.sparkContext.textFile("/00_MyDrive/ApacheSpark/AWS_data/roshith-bucket/course.csv")

    demographicsPairedRdd= demographicsRDD.map(lambda l: l.split(",")) \
        .map(lambda row: Row(id=int(row[0]),
                             age=int(row[1]),
                             codingBootcamp=strtobool(row[2]),
                             country=row[3],
                             gender=row[4],
                             isEthnicMinority=strtobool(row[5]),
                             servedInMilitary=strtobool(row[6]),
                             courseId=int(row[7]))
             ) \
        .map(lambda demographic: (demographic.id,demographic)) #Pair RDD (id, demographics)

    financesPairedRdd= financesRDD.map(lambda l: l.split(",")) \
        .map(lambda row: Row(id=int(row[0]),
                             hasDebt=strtobool(row[1]),
                             hasFinancialDependents=strtobool(row[2]),
                             hasStudentLoans=strtobool(row[3]),
                             income=int(row[4]))
             ) \
        .map(lambda finance: (finance.id, finance)) #Pair RDD (id, finance)

    joinPairedRdd=demographicsPairedRdd.cartesian(financesPairedRdd)\
        .filter(lambda rec: rec[0][0]==rec[1][0])  \
        .filter(lambda rec: (rec[0][1].country=="Switzerland") and (rec[1][1].hasDebt) and (rec[1][1].hasFinancialDependents)) \
        .map(lambda rec: (rec[0][0],(rec[0][1],rec[1][1]))) \

    print(joinPairedRdd.take(5))
    #OR
    joinPairedRdd.foreach(print)

