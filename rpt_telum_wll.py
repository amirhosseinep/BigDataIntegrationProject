from pyspark.sql import SparkSession, Row
import argparse
import os
from pyspark.sql import functions as F
from pyspark.sql.functions import concat, lit, to_timestamp,to_utc_timestamp, col, expr, when, expr , to_date,date_sub, date_format, get_json_object, regexp_replace
from pyspark.sql.types import StructType,StructField, StringType
from datetime import timedelta, datetime
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/bigdata/ojdbc7.jar,/home/bigdata/mhs/Persian_Date-1.0-SNAPSHOT-jar-with-dependencies.jar pyspark-shell'

parser = argparse.ArgumentParser()
parser.add_argument('arguments', nargs='+',help='an integer for the accumulator')
args = parser.parse_args()
sys_arg = vars(args).get('arguments')
PARAM_DATA_DAY = sys_arg[0]
param_master = sys_arg[1]
PARAM_HDFS_ADDR = sys_arg[2]

spark = SparkSession.builder.appName('rpt_telnum_wll_344').master(param_master).getOrCreate()

IM_ENTI_TELNUM = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/INPUT_DATA/IM_ENTI_TELNUM/" + \
            PARAM_DATA_DAY + "/**").where((col("inv_status").isin("Block","Available")) &\
                          (col("wh_id")!=lit("1006")) &\
                          (col("pay_mode").isNull()) &\
                          (col("be_id")==lit("10101")))

MV_REGION = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/INTERMEDIATE/MV_REGION/" + \
            PARAM_DATA_DAY + "/**").select(col("PARENT_REGION_ID"),col("REGION_NAME"))\
            .where(col("PARENT_REGION_ID") != lit("101")) #name check

MV_REGION.createOrReplaceTempView("MV_REGION")

insertesh = spark.sql("select 101 as PARENT_REGION_ID,'ایران' as REGION_NAME")
MV_REG = MV_REGION.unionAll(insertesh)

im_itemtype_level = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/INPUT_DATA/IM_ITEMTYPE_LEVEL/" + \
            PARAM_DATA_DAY + "/**")

IM_WAREHOUSE = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/INPUT_DATA/IM_WAREHOUSE/" + \
            PARAM_DATA_DAY + "/**")

sys_datadict_item = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/INPUT_DATA/SYS_DATADICT_ITEM/" + \
            PARAM_DATA_DAY + "/**").where(col("dict_code")==lit("INV_USE_TYPE")).drop("ITEM_NAME")

sys_datadict_item_lang = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/INPUT_DATA/SYS_DATADICT_ITEM_LANG/" + \
            PARAM_DATA_DAY + "/**").where(col("locale")==lit("en_US"))

Join1 = IM_ENTI_TELNUM.join(MV_REG,  MV_REG.PARENT_REGION_ID == IM_ENTI_TELNUM.C_EX_FIELD2, 'left_outer')
Join2 = Join1.join(im_itemtype_level,Join1.LEVEL_ID == im_itemtype_level.LEVEL_ID, 'right_outer')
Join3 = Join2.join(IM_WAREHOUSE, Join2.WH_ID == IM_WAREHOUSE.IM_RWH_ID, 'left_outer')
datadict_Join = sys_datadict_item_lang.join(sys_datadict_item, sys_datadict_item.REL_ID == sys_datadict_item_lang.ITEM_REL_ID)

Join4 = Join3.join(datadict_Join, datadict_Join.ITEM_CODE == Join3.USE_TYPE)\
        .select(col("MSISDN"),col("REGION_NAME"),col("ITEM_NAME").alias("PURPOSE"),col("LEVEL_NAME")\
             ,col("PAY_MODE"),col("INV_STATUS"),col("IM_RWH_NAME"))

Join4\
    .coalesce(1)\
    .write\
    .option("header", True)\
    .option('emptyValue','')\
    .mode('overwrite')\
    .csv("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/REPORT_RESULT/rpt_telnum_wll/" + PARAM_DATA_DAY)