from pyspark.sql import SparkSession
from pyspark.sql import functions
import yaml
import os.path
import utils.aws_utils as ut

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    jdbc_params = {"url": ut.get_mysql_jdbc_url(app_secret),
                  "lowerBound": "1",
                  "upperBound": "100",
                  "dbtable": app_conf["mysql_conf"]["dbtable"],
                  "numPartitions": "2",
                  "partitionColumn": app_conf["mysql_conf"]["partition_column"],
                  "user": app_secret["mysql_conf"]["username"],
                  "password": app_secret["mysql_conf"]["password"]
                   }
    # print(jdbcParams)

    # use the ** operator/un-packer to treat a python dictionary as **kwargs
    print("\nReading data from MySQL DB using SparkSession.read.format(),")
    txn_df = spark\
        .read.format("jdbc")\
        .option("driver", "com.mysql.cj.jdbc.Driver")\
        .options(**jdbc_params)\
        .load() \
        .withColumn("ins_dt", functions.current_date())

    txn_df2 = spark.read.format("com.springml.spark.sftp")\
        .option("host", app_secret["sftp_conf"]["hostname"])\
        .option("port", app_secret["sftp_conf"]["port"])\
        .option("username", app_secret["sftp_conf"]["username"])\
        .option("pem", os.path.abspath(current_dir + "/../../../../" + app_secret["sftp_conf"]["pem"]))\
        .option("fileType", "csv")\
        .option("delimiter", "|")\
        .load(app_conf["sftp_conf"]["directory"] + "/TransactionSync.csv")

    txn_df.show()

    txn_df.write \
        .partitionBy("ins_dt") \
        .mode("overwrite") \
        .option("header", "true") \
        .option("delimiter", "~") \
        .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/staging/SB")



# spark-submit --packages "mysql:mysql-connector-java:8.0.15,org.apache.hadoop:hadoop-aws:2.7.4,com.springml:spark-sftp_2.11:1.1.1" com/unilever/source_data_loading.py
