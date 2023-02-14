from pyspark.sql import SparkSession, Row
import argparse
import os
from persiantools.jdatetime import JalaliDateTime
from pyspark.sql import functions as F
from pyspark.sql.functions import concat, lit, to_timestamp, col, expr, when, expr , to_date,date_sub, date_format, get_json_object, regexp_replace, to_utc_timestamp
from pyspark.sql.types import StructType,StructField, StringType
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/bigdata/ojdbc7.jar,/home/bigdata/mhs/Persian_Date-1.0-SNAPSHOT-jar-with-dependencies.jar pyspark-shell'

parser = argparse.ArgumentParser()
parser.add_argument('arguments', nargs='+',help='an integer for the accumulator')
args = parser.parse_args()
sys_arg = vars(args).get('arguments')
PARAM_DATA_DAY = sys_arg[0]
param_master = sys_arg[1]
PARAM_HDFS_ADDR = sys_arg[2]

spark = SparkSession.builder.appName('BankFileCollectionReversal_266').master(param_master).getOrCreate()

spark.udf.registerJavaFunction("persian_date", "Persian_Date", StringType())

ar_batch_log = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CBS/INPUT_DATA/AR_BATCH_LOG/" + \
            PARAM_DATA_DAY + "/**").withColumn("created_time_utc",to_utc_timestamp(col('created_time'), 'Asia/Tehran'))

e_ar_batch_registpay = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CBS/INPUT_DATA/E_AR_BATCH_REGISTPAY/" + \
            PARAM_DATA_DAY + "/**")

t_d_dim_ar_bank_info = spark.read\
            .option("header", True)\
            .csv("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/dimensions/T_D_DIM_AR_BANK_INFO.csv").select(col("*"),col("SAP_ITEM_CODE").alias("SAP_ITEM_CODE2"),col("SAP_ITEM_DESCRIPTION").alias("SAP_ITEM_DESCRIPTION2"))\
            .drop("CREATED_TIME").drop("PROCS_TIME").drop("SAP_ITEM_CODE").drop("SAP_ITEM_DESCRIPTION")

t_l_rpt_A = ar_batch_log.select(
    lit('').alias("reference_number"),
    lit('5').alias("posting_code"),
    lit('1').alias("sap_customer_code"),
    lit('').alias("sap_vendor_code"),
    lit('').alias("sap_profit_centre"),
    lit('115').alias("sap_item_code"),
    lit('active postpaid usage- normal subscribers').alias("sap_item_description"),              
    lit('Item').alias("Measure_Unit"),
    lit('').alias("vat_code"),
    lit('').alias("vat_amount"),
    lit('').alias("duties_amount"),
    lit('').alias("baseline_date"),
    lit('').alias("cost_centre"),
    col("TOTAL_AMOUNT"),
    col("total_count"),
    col("ext_property"),
    col("trans_type"),
    col("created_time")
    ).where(
        (col("trans_type") == lit("CPR")) &\
        (regexp_replace(col("created_time_utc")[1:10],'-','') == PARAM_DATA_DAY)  
    ).withColumn("date_table",expr("persian_date("+PARAM_DATA_DAY+",'yyyyMMdd')"))\
     .withColumn("procs_time",to_date(lit(PARAM_DATA_DAY),'yyyyMMdd'))\
     .withColumn("event_date",expr("persian_date("+PARAM_DATA_DAY+",'yyyyMMdd')"))\
     .withColumn("year_table",expr("persian_date("+PARAM_DATA_DAY+",'yyyyMMdd')")[1:4])\
     .withColumn("period",expr("persian_date("+PARAM_DATA_DAY+",'yyyyMMdd')")[6:2]
     ).withColumn("json_SENDDATE",get_json_object(col("ext_property"),"$.SENDDATE"))\
     .withColumn("json_Bank_Code",get_json_object(col("ext_property"),"$.BANK_CODE"))\
     .withColumn("created_time_agg",col("created_time")[1:10])\
     .where(col("json_SENDDATE") == regexp_replace(col("event_date")[3:8],'-',''))    

t_l_rpt_All = t_l_rpt_A.join(t_d_dim_ar_bank_info, t_l_rpt_A.json_Bank_Code == t_d_dim_ar_bank_info.BANK_CODE, 'left')

t_l_rpt_bankfilereversal_d1 = t_l_rpt_All.select(
            col("procs_time").alias("data_day"),
            col("reference_number"),
            col("posting_code"),
            col("sap_customer_code"),
            col("sap_vendor_code"),
            col("sap_profit_centre"),
            col("sap_item_code"),
            col("sap_item_description"),
            col("measure_unit"),
            col("vat_code"),
            col("vat_amount"),
            col("duties_amount"),
            col("year_table"),
            col("period"),
            col("baseline_date"),
            col("cost_centre"),
            col("event_date"),
            col("date_table"),
            col("procs_time"),
            col("TOTAL_AMOUNT"),
            col("total_count")
        )\
        .groupBy(col("data_day"),col("date_table"),
            col("reference_number"),
            col("posting_code"),
            col("sap_customer_code"),
            col("sap_vendor_code"),
            col("sap_profit_centre"),
            col("sap_item_code"),
            col("sap_item_description"),
            col("measure_unit"),
            col("vat_code"),
            col("vat_amount"),
            col("duties_amount"),
            col("year_table"),
            col("period"),
            col("baseline_date"),
            col("cost_centre"),
            col("event_date"),
            col("date_table"),
            col("procs_time"),
            col("TOTAL_AMOUNT"),
            col("total_count"))\
        .agg(F.sum(col("TOTAL_AMOUNT")).alias("amount"),F.sum(col("total_count")).alias("quantity"))

t_l_rpt_bankfilereversal_d1_final = t_l_rpt_bankfilereversal_d1.select(
            col("data_day"),
            col("reference_number"),
            col("posting_code"),
            col("sap_customer_code"),
            col("sap_vendor_code"),
            col("sap_profit_centre"),
            col("sap_item_code"),
            col("sap_item_description"),
            col("amount"),
            col("quantity"),
            col("measure_unit"),
            col("vat_code"),
            col("vat_amount"),
            col("duties_amount"),
            col("year_table"),
            col("period"),
            col("baseline_date"),
            col("cost_centre"),
            col("event_date"),
            col("date_table"),
            col("procs_time")
)

a2_t_l_rpt_A = ar_batch_log.select(
    #col("data_day"), dataday nadare!
    lit('').alias("reference_number"),
    lit('5').alias("posting_code"),
    lit('').alias("sap_customer_code"),
    lit('').alias("sap_vendor_code"),
    lit('').alias("sap_profit_centre"),
    #lit('115').alias("sap_item_code"),
    #lit('active postpaid usage- normal subscribers').alias("sap_item_description"),              
    lit('Item').alias("Measure_Unit"),
    lit('').alias("vat_code"),
    lit('').alias("vat_amount"),
    lit('').alias("duties_amount"),
    lit('').alias("baseline_date"),
    lit('').alias("cost_centre"),
    col("TOTAL_AMOUNT"),
    col("total_count"),
    col("ext_property"),
    col("trans_type"),
    col("created_time")
    ).where(
        (col("trans_type") == lit("CPR")) & \
        (regexp_replace(col("created_time_utc")[1:10],'-','') == PARAM_DATA_DAY)      
    ).withColumn("date_table",expr("persian_date("+PARAM_DATA_DAY+",'yyyyMMdd')"))\
     .withColumn("procs_time",to_date(lit(PARAM_DATA_DAY),'yyyyMMdd'))\
     .withColumn("event_date",expr("persian_date("+PARAM_DATA_DAY+",'yyyyMMdd')"))\
     .withColumn("year_table",expr("persian_date("+PARAM_DATA_DAY+",'yyyyMMdd')")[1:4])\
     .withColumn("period",expr("persian_date("+PARAM_DATA_DAY+",'yyyyMMdd')")[6:2]
     ).withColumn("json_SENDDATE",get_json_object(col("ext_property"),"$.SENDDATE"))\
     .withColumn("json_Bank_Code",get_json_object(col("ext_property"),"$.BANK_CODE"))\
     .withColumn("created_time_agg",col("created_time")[1:10])\
          .where((col("json_SENDDATE") == regexp_replace(col("event_date")[3:8],'-','')) &\
            (col("json_Bank_Code") != lit('30')))

a2_t_l_rpt_All = a2_t_l_rpt_A.join(t_d_dim_ar_bank_info, a2_t_l_rpt_A.json_Bank_Code == t_d_dim_ar_bank_info.BANK_CODE, 'left')

a2_t_l_rpt_bankfilereversal_d1 = a2_t_l_rpt_All.select(
            col("procs_time").alias("data_day"),
            col("reference_number"),
            col("posting_code"),
            col("sap_customer_code"),
            col("sap_vendor_code"),
            col("sap_profit_centre"),
            col("sap_item_code2"),
            col("sap_item_description2"),
            col("measure_unit"),
            col("vat_code"),
            col("vat_amount"),
            col("duties_amount"),
            col("year_table"),
            col("period"),
            col("baseline_date"),
            col("cost_centre"),
            col("event_date"),
            col("date_table"),
            col("procs_time"),
            col("TOTAL_AMOUNT"),
            col("total_count")
        )\
        .groupBy(col("data_day"),
            col("sap_item_code2"),
            col("sap_item_description2"),
            col("date_table"),
            col("reference_number"),
            col("posting_code"),
            col("sap_customer_code"),
            col("sap_vendor_code"),
            col("sap_profit_centre"),
            col("measure_unit"),
            col("vat_code"),
            col("vat_amount"),
            col("duties_amount"),
            col("year_table"),
            col("period"),
            col("baseline_date"),
            col("cost_centre"),
            col("event_date"),
            col("date_table"),
            col("procs_time"),
            col("TOTAL_AMOUNT"),
            col("total_count"))\
        .agg(F.sum(col("TOTAL_AMOUNT")).alias("amount"),F.sum(col("total_count")).alias("quantity"))

a2_t_l_rpt_bankfilereversal_d1_final = a2_t_l_rpt_bankfilereversal_d1.select(
            col("data_day"),
            col("reference_number"),
            col("posting_code"),
            col("sap_customer_code"),
            col("sap_vendor_code"),
            col("sap_profit_centre"),
            col("sap_item_code2"),
            col("sap_item_description2"),
            col("amount"),
            col("quantity"),
            col("measure_unit"),
            col("vat_code"),
            col("vat_amount"),
            col("duties_amount"),
            col("year_table"),
            col("period"),
            col("baseline_date"),
            col("cost_centre"),
            col("event_date"),
            col("date_table"),
            col("procs_time")
)

a3_t_l_rpt_A = ar_batch_log.select(
    #col("data_day"), dataday nadare!
    lit('').alias("reference_number"),
    lit('5').alias("posting_code"),
    lit('34').alias("sap_customer_code"),
    lit('').alias("sap_vendor_code"),
    lit('').alias("sap_profit_centre"),
    lit('82').alias("sap_item_code"),
    lit('Banks controlling account for revenues/ postpaid usage - Jiring usage').alias("sap_item_description"),              
    lit('Item').alias("Measure_Unit"),
    lit('').alias("vat_code"),
    lit('').alias("vat_amount"),
    lit('').alias("duties_amount"),
    lit('').alias("baseline_date"),
    lit('').alias("cost_centre"),
    col("TOTAL_AMOUNT"),
    col("total_count"),
    col("ext_property"),
    col("trans_type"),
    col("created_time")
    ).where(
        (col("trans_type") == lit("CPR")) & \
        (regexp_replace(col("created_time_utc")[1:10],'-','') == PARAM_DATA_DAY)       
    ).withColumn("date_table",expr("persian_date("+PARAM_DATA_DAY+",'yyyyMMdd')"))\
     .withColumn("procs_time",to_date(lit(PARAM_DATA_DAY),'yyyyMMdd'))\
     .withColumn("event_date",expr("persian_date("+PARAM_DATA_DAY+",'yyyyMMdd')"))\
     .withColumn("year_table",expr("persian_date("+PARAM_DATA_DAY+",'yyyyMMdd')")[1:4])\
     .withColumn("period",expr("persian_date("+PARAM_DATA_DAY+",'yyyyMMdd')")[6:2]
     ).withColumn("json_SENDDATE",get_json_object(col("ext_property"),"$.SENDDATE"))\
     .withColumn("json_Bank_Code",get_json_object(col("ext_property"),"$.BANK_CODE"))\
     .withColumn("created_time_agg",col("created_time")[1:10])\
     .where((col("json_SENDDATE") == regexp_replace(col("event_date")[3:8],'-','')) & (col("json_Bank_Code") == lit('30')))

a3_t_l_rpt_All = a3_t_l_rpt_A.join(t_d_dim_ar_bank_info, a3_t_l_rpt_A.json_Bank_Code == t_d_dim_ar_bank_info.BANK_CODE, 'left')

a3_t_l_rpt_bankfilereversal_d1 = a3_t_l_rpt_All.select(
            col("procs_time").alias("data_day"),
            col("reference_number"),
            col("posting_code"),
            col("sap_customer_code"),
            col("sap_vendor_code"),
            col("sap_profit_centre"),
            col("sap_item_code"),
            col("sap_item_description"),
            col("measure_unit"),
            col("vat_code"),
            col("vat_amount"),
            col("duties_amount"),
            col("year_table"),
            col("period"),
            col("baseline_date"),
            col("cost_centre"),
            col("event_date"),
            col("date_table"),
            col("procs_time"),
            col("TOTAL_AMOUNT"),
            col("total_count")
        )\
        .groupBy(col("data_day"),col("date_table"),
            col("reference_number"),
            col("posting_code"),
            col("sap_customer_code"),
            col("sap_vendor_code"),
            col("sap_profit_centre"),
            col("sap_item_code"),
            col("sap_item_description"),
            col("measure_unit"),
            col("vat_code"),
            col("vat_amount"),
            col("duties_amount"),
            col("year_table"),
            col("period"),
            col("baseline_date"),
            col("cost_centre"),
            col("event_date"),
            col("date_table"),
            col("procs_time"),
            col("TOTAL_AMOUNT"),
            col("total_count"))\
        .agg(F.sum(col("TOTAL_AMOUNT")).alias("amount"),F.sum(col("total_count")).alias("quantity"))

a3_t_l_rpt_bankfilereversal_d1_final = a3_t_l_rpt_bankfilereversal_d1.select(
            col("data_day"),
            col("reference_number"),
            col("posting_code"),
            col("sap_customer_code"),
            col("sap_vendor_code"),
            col("sap_profit_centre"),
            col("sap_item_code"),
            col("sap_item_description"),
            col("amount"),
            col("quantity"),
            col("measure_unit"),
            col("vat_code"),
            col("vat_amount"),
            col("duties_amount"),
            col("year_table"),
            col("period"),
            col("baseline_date"),
            col("cost_centre"),
            col("event_date"),
            col("date_table"),
            col("procs_time")
)                               
                    
a4_t_l_rpt_A = e_ar_batch_registpay.select(
    lit('').alias("reference_number"),
    lit('5').alias("posting_code"),
    (col("registe_amount")/1000).alias("sap_customer_code"),
    lit('').alias("sap_vendor_code"),
    lit('').alias("sap_profit_centre"),
    lit('116').alias("sap_item_code"),
    lit('incoming payment for credit charge sales in prepaid subscribers lines').alias("sap_item_description"),              
    lit('Item').alias("Measure_Unit"),
    lit('').alias("vat_code"),
    lit('').alias("vat_amount"),
    lit('').alias("duties_amount"),
    lit('').alias("baseline_date"),
    lit('').alias("cost_centre"),
    col("registe_amount"),
    col("registe_count"),
    col("batch_no")
    ).withColumn("Data_Day",to_date(lit(PARAM_DATA_DAY),'yyyyMMdd'))\
     .withColumn("date_table",expr("persian_date("+PARAM_DATA_DAY+",'yyyyMMdd')"))\
     .withColumn("procs_time",to_date(lit(PARAM_DATA_DAY),'yyyyMMdd'))\
     .withColumn("event_date",expr("persian_date("+PARAM_DATA_DAY+",'yyyyMMdd')"))\
     .withColumn("year_table",expr("persian_date("+PARAM_DATA_DAY+",'yyyyMMdd')")[1:4])\
     .withColumn("period",expr("persian_date("+PARAM_DATA_DAY+",'yyyyMMdd')")[6:2]
     )

a4_ar_batch_log = ar_batch_log.withColumn("bank_file_date",get_json_object(col("ext_property"),"$.SENDDATE"))\
     .where((col("trans_type") == lit("CPR")))\
     .select(col("batch_no").alias("batch_no2"),col("bank_file_date"))\
     .groupBy(col("batch_no2"),col("bank_file_date"))\
     .agg(F.count(col("batch_no2")))\
     .drop(col("count(batch_no2)")).drop("batch_no")
#GroupBy used instead of distinct. then spark needed agg so we added it and dropped it later.

a4_t_l_rpt_All = a4_t_l_rpt_A.join(a4_ar_batch_log, a4_t_l_rpt_A.batch_no == a4_ar_batch_log.batch_no2,'inner')\
    .where(col("bank_file_date") == regexp_replace(col("event_date"),'-','')[3:8])

a4_t_l_rpt_bankfilereversal_d1 = a4_t_l_rpt_All.select(
            col("Data_Day"),
            col("reference_number"),
            col("posting_code"),
            col("sap_customer_code"),
            col("sap_vendor_code"),
            col("sap_profit_centre"),
            col("sap_item_code"),
            col("sap_item_description"),
            col("measure_unit"),
            col("vat_code"),
            col("vat_amount"),
            col("duties_amount"),
            col("year_table"),
            col("period"),
            col("baseline_date"),
            col("cost_centre"),
            col("event_date"),
            col("date_table"),
            col("procs_time"),
            col("registe_amount"),
            col("registe_count"),
            col("batch_no")
        )\
        .groupBy(col("data_day"),col("sap_customer_code"),col("event_date"),
            col("reference_number"),
            col("posting_code"),
            col("sap_vendor_code"),
            col("sap_profit_centre"),
            col("sap_item_code"),
            col("sap_item_description"),
            col("measure_unit"),
            col("vat_code"),
            col("vat_amount"),
            col("duties_amount"),
            col("year_table"),
            col("period"),
            col("baseline_date"),
            col("cost_centre"),
            col("date_table"),
            col("procs_time"),
            col("registe_amount"),
            col("registe_count"),
            col("batch_no"))\
        .agg(F.sum((col("registe_amount")*col("registe_count"))/1000).alias("amount"),F.sum(col("registe_count")).alias("quantity"))

a4_t_l_rpt_bankfilereversal_d1_final = a4_t_l_rpt_bankfilereversal_d1.select(
            col("data_day"),
            col("reference_number"),
            col("posting_code"),
            col("sap_customer_code"),
            col("sap_vendor_code"),
            col("sap_profit_centre"),
            col("sap_item_code"),
            col("sap_item_description"),
            col("amount"),
            col("quantity"),
            col("measure_unit"),
            col("vat_code"),
            col("vat_amount"),
            col("duties_amount"),
            col("year_table"),
            col("period"),
            col("baseline_date"),
            col("cost_centre"),
            col("event_date"),
            col("date_table"),
            col("procs_time")
)

u1 = t_l_rpt_bankfilereversal_d1_final.unionAll(a2_t_l_rpt_bankfilereversal_d1_final)
u2 = u1.unionAll(a3_t_l_rpt_bankfilereversal_d1_final)
u3 = u2.unionAll(a4_t_l_rpt_bankfilereversal_d1_final)

u3\
    .coalesce(1)\
    .write\
    .option("header", True)\
    .mode('overwrite')\
    .csv("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/RESULT_DATA/BankFileCollectionReversal_n/" + PARAM_DATA_DAY)