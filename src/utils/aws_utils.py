import os.path
import yaml

def getRedshiftJdbcUrl(redshiftConfig: dict):
    host = redshiftConfig["redshift_conf"]["host"]
    port = redshiftConfig["redshift_conf"]["port"]
    database = redshiftConfig["redshift_conf"]["database"]
    username = redshiftConfig["redshift_conf"]["username"]
    password = redshiftConfig["redshift_conf"]["password"]
    return f"jdbc:redshift://{host}:{port}/{database}?user={username}&password={password}"

def getMysqlJdbcUrl(mysqlConfig: dict):
    host = mysqlConfig["mysql_conf"]["hostname"]
    port = mysqlConfig["mysql_conf"]["port"]
    database = mysqlConfig["mysql_conf"]["database"]
    return f"jdbc:mysql://{host}:{port}/{database}?autoReconnect=true&useSSL=false"