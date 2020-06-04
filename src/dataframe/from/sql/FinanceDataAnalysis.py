from pyspark.sql import SparkSession,Row,functions
from pyspark.sql.types import StructType, IntegerType, BooleanType,DoubleType
import os.path
import yaml

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("DSL examples") \
        .master('local[*]') \
        .getOrCreate()

    current_dir = os.path.abspath(os.path.dirname(__file__))
    appConfigFilePath = os.path.abspath(current_dir + "/../../../"+"application.yml")

    with open(appConfigFilePath) as conf:
        doc = yaml.load(conf,Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = sparkSession.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", doc["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", doc["s3_conf"]["secret_access_key"])
    hadoop_conf.set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")

    finFilePath = "s3a://"+doc["s3_conf"]["s3_bucket"]+"/finances-small"
    financeDf = sparkSession.sql("select * from parquet.`{}`".format(finFilePath))

    financeDf.printSchema()
    financeDf.show(5,False)
    financeDf.createOrReplaceTempView("finances")

    sparkSession.sql("select * from finances order by amount").show(5,False)

    sparkSession.sql("select concat_ws(' - ', AccountNumber, Description) as AccountDetails from finances").show(5,False)

    aggFinanceDf = sparkSession.sql("""
        select
        AccountNumber,
        sum(Amount) as TotalTransaction,
        count(Amount) as NumberOfTransaction,
        max(Amount) as MaxTransaction,
        min(Amount) as MinTransaction,
        collect_set(Description) as UniqueTransactionDescriptions
        from
        finances
        group by
        AccountNumber
        """)

    aggFinanceDf.show(5,False)
    aggFinanceDf.createOrReplaceTempView("agg_finances")

    sparkSession.sql(
        """
        select
        AccountNumber,
        UniqueTransactionDescriptions,
        sort_array(UniqueTransactionDescriptions, false) as OrderedUniqueTransactionDescriptions,
        size(UniqueTransactionDescriptions) as CountOfUniqueTransactionTypes,
        array_contains(UniqueTransactionDescriptions, 'Movies') as WentToMovies
        from
        agg_finances
        """
                    )\
        .show(5, False)

    companiesJson = [
        """{"company":"NewCo","employees":[{"firstName":"Sidhartha","lastName":"Ray"},{"firstName":"Pratik","lastName":"Solanki"}]}""",
        """{"company":"FamilyCo","employees":[{"firstName":"Jiten","lastName":"Pupta"},{"firstName":"Pallavi","lastName":"Gupta"}]}""",
        """{"company":"OldCo","employees":[{"firstName":"Vivek","lastName":"Garg"},{"firstName":"Nitin","lastName":"Gupta"}]}""",
        """{"company":"ClosedCo","employees":[]}"""
                    ]
    companiesRDD = sparkSession.sparkContext.parallelize(companiesJson)
    companiesDF = sparkSession.read.json(companiesRDD)
    companiesDF.createOrReplaceTempView("companies")
    companiesDF.show(5,False)
    companiesDF.printSchema()

    employeeDfTemp = sparkSession.sql("select company, explode(employees) as employee from companies")
    employeeDfTemp.show()
    employeeDfTemp.createOrReplaceTempView("employees")
    employeeDfTemp2 = sparkSession.sql("select company, posexplode(employees) as (employeePosition, employee) from companies")
    employeeDfTemp2.show()

    sparkSession.sql("""
              select
                company,
                employee.firstName as firstName,
                case
                  when company = 'FamilyCo' then 'Premium'
                  when company = 'OldCo' then 'Legacy'
                  else 'Standard'
                end as Tier
              from
                employees
            """) \
        .show(5,False)


