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
        .map(lambda demographic: (demographic.id,demographic)) \
        .filter(lambda r: r[1].country == "Switzerland") #Pair RDD (id, demographics)

    financesPairedRdd= financesRDD.map(lambda l: l.split(",")) \
        .map(lambda row: Row(id=int(row[0]),
                             hasDebt=strtobool(row[1]),
                             hasFinancialDependents=strtobool(row[2]),
                             hasStudentLoans=strtobool(row[3]),
                             income=int(row[4]))
             ) \
        .map(lambda finance: (finance.id, finance)) \
        .filter(lambda r: r[1].hasDebt and r[1].hasFinancialDependents)  ##Pair RDD (id, finance)

    joinPairedRdd=demographicsPairedRdd.join(financesPairedRdd)

    print(joinPairedRdd.take(5))
