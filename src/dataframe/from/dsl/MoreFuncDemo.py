from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col,first,trim,lower,ltrim,initcap,format_string,coalesce,lit
from src.model.Person import Person

if __name__ == '__main__':
    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .master('local[*]') \
        .getOrCreate()

    peopleDf = sparkSession.createDataFrame(List(
        Person("Sidhartha", "Ray", 32, None, Some("Programmer")),
        Person("Pratik", "Solanki", 22, Some(176.7), None),
        Person("Ashok ", "Pradhan", 62, None, None),
        Person(" ashok", "Pradhan", 42, Some(125.3), Some("Chemical Engineer")),
        Person("Pratik", "Solanki", 22, Some(222.2), Some("Teacher"))
    ))

    peopleDf.show()
    peopleDf.groupBy("firstName").agg(first("weightInLbs")).show()
    peopleDf.groupBy(trim(lower("firstName"))).agg(first("weightInLbs")).show()
    peopleDf.groupBy(trim(lower("firstName"))).agg(first("weightInLbs", True)).show()
    peopleDf.sort("weightInLbs".desc).groupBy(trim(lower("firstName"))).agg(first("weightInLbs", True)).show()
    peopleDf.sort("weightInLbs".asc_nulls_last).groupBy(trim(lower("firstName"))).agg(first("weightInLbs", True)).show()

    correctedPeopleDf = peopleDf\
        .withColumn("firstName", initcap("firstName"))\
        .withColumn("firstName", ltrim(initcap("firstName")))\
        .withColumn("firstName", trim(initcap("firstName")))\
        correctedPeopleDf.groupBy("firstName").agg(first("weightInLbs")).show()

    correctedPeopleDf = correctedPeopleDf\
        .withColumn("fullName", format_string("%s %s", "firstName", "lastName"))\
        correctedPeopleDf.show()

    correctedPeopleDf = correctedPeopleDf\
        .withColumn("weightInLbs", coalesce($"weightInLbs", lit(0)))\
        correctedPeopleDf.show()

    correctedPeopleDf\
        .filter(lower($"jobType").contains("engineer"))\
        .show()

    correctedPeopleDf\
        .filter(lower($"jobType").isin(["chemical engineer", "teacher"]:_*)) # List un-listing
        .filter(lower($"jobType").isin("chemical engineer", "teacher")) #without un-listing
        .show()
