from pyspark.sql import SparkSession
from pyspark.sql import functions
import yaml
import os.path
import utils.aws_utils as ut

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"]) \
        .config("spark.mongodb.output.uri", app_secret["mongodb_config"]["uri"]) \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    src_list = app_conf["source_data_list"]

    for src in src_list:
        src_conf = app_conf[src]
        if src == "SB":
            print("\nReading SB data from MySQL DB >>")
            jdbc_params = {"url": ut.get_mysql_jdbc_url(app_secret),
                           "lowerBound": "1",
                           "upperBound": "100",
                           "dbtable": src_conf["mysql_conf"]["dbtable"],
                           "numPartitions": "2",
                           "partitionColumn": src_conf["mysql_conf"]["partition_column"],
                           "user": app_secret["mysql_conf"]["username"],
                           "password": app_secret["mysql_conf"]["password"]
                           }
            txn_df = spark \
                .read.format("jdbc") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .options(**jdbc_params) \
                .load() \
                .withColumn("ins_dt", functions.current_date())
            txn_df.show()

            txn_df.write \
                .mode('overwrite') \
                .partitionBy("INS_DT") \
                .option("header", "true") \
                .option("delimiter", "~") \
                .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/staging/SB")

            print("\nWriting SB data to S3 <<")

        elif src == "OL":
            print("\nReading OL data from sftp >>")
            txn_df2 = spark.read.format("com.springml.spark.sftp") \
                .option("host", app_secret["sftp_conf"]["hostname"]) \
                .option("port", app_secret["sftp_conf"]["port"]) \
                .option("username", app_secret["sftp_conf"]["username"]) \
                .option("pem", os.path.abspath(current_dir + "/../../" + app_secret["sftp_conf"]["pem"])) \
                .option("fileType", "csv") \
                .option("delimiter", "|") \
                .load(src_conf["sftp_conf"]["directory"] + src_conf["sftp_conf"]["filename"]) \
                .withColumn("ins_dt", functions.current_date())
            txn_df2.show(5)

            txn_df2.write \
                .mode('overwrite') \
                .partitionBy("INS_DT") \
                .option("header", "true") \
                .option("delimiter", "|") \
                .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/staging/OL")

            print("\nWriting OL data to S3 <<")

        elif src == "CP":
            print("Reading CP data from S3 bucket name {}".format(src_conf["s3_conf"]["s3_bucket"]))
            txn_df3 = spark.read \
                .format("csv") \
                .option("delimiter", "|") \
                .load("s3a://" + src_conf["s3_conf"]["s3_bucket"] + "/" + src_conf["s3_conf"]["filename"]) \
                .withColumn("ins_dt", functions.current_date())
            txn_df3.show()

            txn_df3.write \
                .mode('overwrite') \
                .partitionBy("INS_DT") \
                .option("header", "true") \
                .option("delimiter", "~") \
                .csv("s3a://" + src_conf["s3_conf"]["s3_bucket"] + "/staging/CP")
            print('Writing to S3')

        elif src == "addr":
            print("Reading from Mongo")
            customer = spark \
                .read \
                .format("com.mongodb.spark.sql.DefaultSource") \
                .option("database", src_conf["mongodb_config"]["database"]) \
                .option("collection", src_conf["mongodb_config"]["collection"]) \
                .load()

            customer_df = customer.select(functions.col('consumer_id'), functions.col('address.street').alias('Street'),
                                          functions.col('address.city').alias('city'),
                                          functions.col('address.state').alias('State'))
            customer_df = customer_df.withColumn("ins_dt", functions.current_date())
            customer_df.show()

            customer_df.write \
                .mode('overwrite') \
                .partitionBy("INS_DT") \
                .json("s3a://" + src_conf["s3_conf"]["s3_bucket"] + "/staging/addr")
            print('Writing to S3')








# spark-submit --packages "mysql:mysql-connector-java:8.0.15,org.apache.hadoop:hadoop-aws:2.7.4,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1,com.springml:spark-sftp_2.11:1.1.1" com/unilever/source_data_loading.py
