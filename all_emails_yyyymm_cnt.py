from pyspark.sql import SparkSession, Row
import argparse
import os
from persiantools.jdatetime import JalaliDateTime
from pyspark.sql import functions as F
from pyspark.sql.functions import concat, lit, to_timestamp, col, expr, when, expr , to_date,date_sub
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/bigdata/ojdbc7.jar,/home/bigdata/mhs/Persian_Date-1.0-SNAPSHOT-jar-with-dependencies.jar pyspark-shell'
from datetime import timedelta, datetime

parser = argparse.ArgumentParser()
parser.add_argument('arguments', nargs='+',help='an integer for the accumulator')
args = parser.parse_args()
sys_arg = vars(args).get('arguments')
PARAM_DATA_DAY = sys_arg[0]
param_master = sys_arg[1]
PARAM_HDFS_ADDR = sys_arg[2]

spark = SparkSession.builder.appName('all_emails_yyyymm_cnt').master(param_master).getOrCreate()

tbl_bc_offerng_inst_email = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CBS/INPUT_DATA/*.BC_OFFERING_INST/" + \
            PARAM_DATA_DAY + "/*")\
            .select(col("OWNER_ID"), col("O_ID"),col("OWNER_TYPE"),col("EFF_DATE"),col("EXP_DATE"))\
            .withColumn("ins_date",expr("to_date('" + PARAM_DATA_DAY + "','yyyyMMdd')"))\
            .where((col("o_id") == lit("201653104")) & (col("exp_date") > col("ins_date")))

bc_acct = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CBS/INPUT_DATA/*.BC_ACCT/" + \
            PARAM_DATA_DAY + "/*").select(col("*"),col("EXP_DATE").alias("EXP_DATE_bc_acct"),col("eff_date").alias("eff_date_bc_acct")).drop("EXP_DATE","eff_date")

bc_contact = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CBS/INPUT_DATA/*.BC_CONTACT/" + \
            PARAM_DATA_DAY + "/*").select(col("*"),col("CONTACT_ID").alias("CONTACT_ID_bc_cont")).drop("CONTACT_ID")

join1 = tbl_bc_offerng_inst_email.join(bc_acct, bc_acct.ACCT_ID == tbl_bc_offerng_inst_email.OWNER_ID)\
    .select(col("ACCT_CODE"),col("owner_id"),col("eff_date"),col("EXP_DATE"),col("tp_acct_key"),col("CONTACT_ID"))\

join2 = join1.join(bc_contact, bc_contact.CONTACT_ID_bc_cont == join1.CONTACT_ID )\
    .select(col("ACCT_CODE"),col("owner_id"),col("EMAIL"),col("mobile_phone").alias("msisdn"),col("eff_date"),col("EXP_DATE"),col("tp_acct_key"))

tbl_bill_sent_mail_all = join2.select(col("acct_code").alias("ACCT_CODE"), col("email").alias("EMAIL"), col("msisdn").alias("MSISDN"))

tbl_bill_sent_mail_all\
    .coalesce(1)\
    .write\
    .option("header", True)\
    .mode('overwrite')\
    .csv("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/RESULT_DATA/all_emails_yyyymm_cnt/" + PARAM_DATA_DAY)