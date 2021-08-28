from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os.path
import yaml
import utils.aws_utils as ut

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DSL examples") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    src_list = app_conf['source_list']
    staging_dir = app_conf["s3_conf"]["staging_dir"]
    for src in src_list:
        src_conf = app_conf[src]
        stg_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + staging_dir + "/" + src
        if src == 'SB':
            jdbc_params = {"url": ut.get_mysql_jdbc_url(app_secret),
                              "lowerBound": "1",
                              "upperBound": "100",
                              "dbtable": src_conf["mysql_conf"]["query"],
                              "numPartitions": "2",
                              "partitionColumn": src_conf["mysql_conf"]["partition_column"],
                              "user": app_secret["mysql_conf"]["username"],
                              "password": app_secret["mysql_conf"]["password"]
                               }
            # use the ** operator/un-packer to treat a python dictionary as **kwargs
            print("\nReading data from MySQL DB using SparkSession.read.format(),")
            sb_df = spark\
                .read.format("jdbc")\
                .option("driver", "com.mysql.cj.jdbc.Driver")\
                .options(**jdbc_params)\
                .load()\
                .withColumn("run_dt", current_date())

            sb_df.show()

            sb_df.write\
                .partitionBy("run_dt")\
                .mode("overwrite")\
                .parquet(stg_path)

        elif src == 'OL':
            ol_df = spark.read\
                .format("com.springml.spark.sftp")\
                .option("host", app_secret["sftp_conf"]["hostname"])\
                .option("port", app_secret["sftp_conf"]["port"])\
                .option("username", app_secret["sftp_conf"]["username"])\
                .option("pem", os.path.abspath(current_dir + "/../../" + app_secret["sftp_conf"]["pem"]))\
                .option("fileType", "csv")\
                .option("delimiter", "|")\
                .load(src_conf["sftp_conf"]["directory"] + "/" + src_conf["sftp_conf"]["filename"])\
                .withColumn("run_dt", current_date())

            ol_df.show()

            ol_df.write\
                .partitionBy("run_dt")\
                .mode("overwrite")\
                .parquet(stg_path)







# spark-submit --packages "mysql:mysql-connector-java:8.0.15" dataframe/ingestion/others/systems/mysql_df.py
# spark-submit --packages "com.springml:spark-sftp_2.11:1.1.1" dataframe/ingestion/others/systems/sftp_df.py

