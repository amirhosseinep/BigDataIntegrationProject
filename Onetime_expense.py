from pyspark.sql import SparkSession, Row
import argparse
import os
from persiantools.jdatetime import JalaliDateTime
from pyspark.sql import functions as F
from pyspark.sql.functions import concat, lit, col, when, expr, coalesce, get_json_object,sum , to_date
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/bigdata/ojdbc7.jar,/home/bigdata/mhs/Persian_Date-1.0-SNAPSHOT-jar-with-dependencies.jar pyspark-shell'

parser = argparse.ArgumentParser()
parser.add_argument('arguments', nargs='+',help='an integer for the accumulator')
args = parser.parse_args()
sys_arg = vars(args).get('arguments')
PARAM_DATA_DAY = sys_arg[0]
BILL_CYCLE_ID = sys_arg[1]
param_master = sys_arg[2]
PARAM_HDFS_ADDR = sys_arg[3]
PARAM_ORACLE_ADDR = sys_arg[4]
PARAM_ORACLE_USER = sys_arg[5]
PARAM_ORACLE_PASS = sys_arg[6]
PARAM_ReportTableName = sys_arg[7]

spark = SparkSession.builder.appName('ONETIME_EXPENSE_394').master(param_master).getOrCreate()

CM_CHARGE_CODE_LANG = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/INTERMEDIATE/VW_CM_CHARGE_CODE/" + \
            PARAM_DATA_DAY + "/**")

AR_INVOICE_DETAIL = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CBS/INPUT_DATA/AR_INVOICE_DETAIL/" + \
            PARAM_DATA_DAY + "/**/**")

innerOne = AR_INVOICE_DETAIL.select(col("ext_property"),
    col("CHARGE_CODE_ID").alias("CHARGE_CODE_ID2"),
    lit(1).alias("CNT"),
    col("BILL_CYCLE_ID"),
    col("CHARGE_CODE_TYPE"),
    col("ext_property"),
    col("invoice_detail_id"),
    ((coalesce(col("CHARGE_AMT"),lit(0)))/lit(1000)).alias("CHARGE_FEE")
    ).withColumn("json_0bj",get_json_object(col("ext_property"),"$.USAGE_SERVICETYPE"))\
    .withColumn('USAGE_SERVICETYPE', \
                      when(
                      (
                      (col("CHARGE_CODE_TYPE") == lit('C04')) &\
                      (~(col("CHARGE_CODE_ID2").isin('800002')))
                      ) |\
                      (
                      (col("CHARGE_CODE_TYPE") == lit('C02')) &\
                      (col("CHARGE_CODE_ID2") != lit('98000200')) &\
                      (col("CHARGE_CODE_ID2") != lit('98000201'))
                      ), lit('1520')
                      ).otherwise(lit('-1'))
                )\
    .where(col("USAGE_SERVICETYPE") == lit("1520")).drop("json_0bj").drop("ext_property").drop("CHARGE_CODE_TYPE")

joined = innerOne.join(CM_CHARGE_CODE_LANG, innerOne.CHARGE_CODE_ID2 == CM_CHARGE_CODE_LANG.CHARGE_CODE_ID, "inner")\
        .drop("CHARGE_CODE_ID2")

result = joined.select(
        col("BILL_CYCLE_ID"),
        col("CHARGE_CODE_ID"),
        col("CHARGE_CODE_NAME").alias("One_time_service_name"),
        col("CHARGE_FEE"),
        col("CNT")
)

result2 = result.groupBy(col("One_time_service_name"),col("CHARGE_CODE_ID"),col("BILL_CYCLE_ID"))\
        .agg(F.sum(col("CHARGE_FEE")).alias("Fee_Amount"),F.sum(col("CNT")).alias("CHARGE_CNT"))

result3 = result2.select(col("CHARGE_CNT"),col("Fee_Amount").alias("FEE_AMOUNT"),col("One_time_service_name").alias("ONE_TIME_SERVICE_NAME"),col("BILL_CYCLE_ID")[0:6].alias("BILL_CYCLE_ID"))\
        .withColumn("DATA_DAY",to_date(lit(PARAM_DATA_DAY),'yyyyMMdd'))

result4 = result3.persist()

result4\
    .coalesce(1)\
    .write\
    .option("header", True)\
    .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")\
    .mode('overwrite')\
    .csv("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/RESULT_DATA/ONETIME_EXPENSE_394_csv/" + PARAM_DATA_DAY)

result4\
.write\
.format("jdbc")\
.mode("append")\
.option("driver", "oracle.jdbc.driver.OracleDriver")\
.option("user", PARAM_ORACLE_USER)\
.option("password", PARAM_ORACLE_PASS)\
.option("url", "jdbc:oracle:thin:@//" + PARAM_ORACLE_ADDR)\
.option("dbtable", PARAM_ReportTableName)\
.save()