from pyspark.sql import SparkSession, Row
import argparse
import os
from pyspark.sql import functions as F
from pyspark.sql.functions import concat, lit, to_timestamp, col, when , to_date, date_format, StringType
from datetime import datetime
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/bigdata/ojdbc7.jar,/home/bigdata/mhs/Persian_Date-1.0-SNAPSHOT-jar-with-dependencies.jar pyspark-shell'

parser = argparse.ArgumentParser()
parser.add_argument('arguments', nargs='+',help='an integer for the accumulator')
args = parser.parse_args()
sys_arg = vars(args).get('arguments')
PARAM_DATA_DAY = sys_arg[0]
param_master = sys_arg[1]
PARAM_HDFS_ADDR = sys_arg[2]

spark = SparkSession.builder.appName('ADDRESS_DAILY_YYYYMMdd_YYYYMMDDHH24MISS').master(param_master).getOrCreate()

spark.udf.registerJavaFunction("Decryption","Decryption",StringType())

inf_address = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/INPUT_DATA/INF_ADDRESS/" + \
            PARAM_DATA_DAY + "/**")

result = inf_address.selectExpr("party_id","addr_id","Decryption(addr_2) as region_name","Decryption(addr_3) as city_name", "eff_date", "exp_date", "modify_time")\
        .withColumn("modify_time_dt",to_date(col("modify_time"),"yyyyMMdd"))\
        .withColumn("eff_date_dt",to_date(col("eff_date"),"yyyyMMdd"))\
        .withColumn("exp_date_dt",to_date(col("exp_date"),"yyyyMMdd"))\
        .withColumn("PARAM_DATA_DAY_dt",to_timestamp(lit(PARAM_DATA_DAY),"yyyyMMdd")[0:10])\
        .where(
            (col("modify_time_dt") == col("PARAM_DATA_DAY_dt")) | 
            (col("eff_date_dt") == col("PARAM_DATA_DAY_dt")) | 
            (col("exp_date_dt") == col("PARAM_DATA_DAY_dt"))
            ).drop("modify_time_dt","eff_date_dt","exp_date_dt","PARAM_DATA_DAY_dt")

result\
    .coalesce(1)\
    .write\
    .option("header", True)\
    .mode('overwrite')\
    .csv("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/RESULT_DATA/ADDRESS_DAILY/" + PARAM_DATA_DAY)