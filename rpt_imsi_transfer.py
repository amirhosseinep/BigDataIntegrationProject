from pyspark.sql import SparkSession, Row
import argparse
import os
from persiantools.jdatetime import JalaliDateTime
from pyspark.sql import functions as F
from pyspark.sql.functions import concat, lit, col, when, expr, coalesce, get_json_object,sum , to_date, regexp_replace
from pyspark.sql.types import StructType,StructField, StringType
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/bigdata/ojdbc7.jar,/home/bigdata/mhs/Persian_Date-1.0-SNAPSHOT-jar-with-dependencies.jar pyspark-shell'

parser = argparse.ArgumentParser() 
parser.add_argument('arguments', nargs='+',help='an integer for the accumulator')
args = parser.parse_args()
sys_arg = vars(args).get('arguments')
PARAM_DATA_DAY = sys_arg[0]A
last_cycle_first_day = sys_arg[1]
param_master = sys_arg[2]
PARAM_HDFS_ADDR = sys_arg[3]

spark = SparkSession.builder.appName('rpt_imsi_transfer_315').master(param_master).getOrCreate()
spark.udf.registerJavaFunction("persian_date", "Persian_Date", StringType())

Im_Warehouse = spark.read\
    .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/INPUT_DATA/IM_WAREHOUSE/" + \
    PARAM_DATA_DAY + "/**")

VW_WAREHOUSE = Im_Warehouse.select(
                col("IM_RWH_ID"),
                col("IM_RWH_CODE"),
                col("IM_RWH_NAME"),
                col("OU_ID"),
                col("IS_DEFAULT"),
                col("STATUS"),
                col("OU_CODE"),
                col("OWNER_TYPE"),
                col("OWNER_PROLE_ID"),
                col("OWNER_OU_ID"),
                col("BE_ID"),
                col("BE_CODE"),
                col("TOP_BE_CODE"),
                col("REMARK"),
                col("PARTY_ROLE"),
                col("PARTY_ROLE_TYPE"),
                col("MANAGER_ROLE_ID"),
                col("OWNER_PROLE_TYPE"),
                col("STORE_TYPE"),
                col("STORE_LEVEL"),
                col("CREATE_PROLE_TYPE"),
                col("CREATE_PROLE_ID"),
                col("CREATE_DEPT_ID"),
                col("CREATE_TIME"),
                col("MODIFY_PROLE_TYPE"),
                col("MODIFY_PROLE_ID"),
                col("MODIFY_DEPT_ID"),
                col("MODIFY_TIME")
        )
VW_IM_ENTI_IMSI = spark.read\
    .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/INPUT_DATA/IM_ENTI_IMSI/" + \
    PARAM_DATA_DAY + "/**").select(col("IMSI").alias("IMSI_Imsii"),
                                col("INV_STATUS").alias("INV_STATUS_Imsii"),
                                col("WH_ID").alias("WH_ID_Imsii"),
                                col("SKU_CODE").alias("SKU_CODE_Imsii"),
                                col("REGION_ID"),
                                #.alias("REGION_ID_Imsii")
                                col("ICCID")
                                #.alias("ICCID_Imsiit")
                                )

IM_BP_NOTE_INST_T = spark.read\
    .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/INPUT_DATA/IM_BP_NOTE_INST/" + \
    PARAM_DATA_DAY + "/**")\
            .select(col("IM_OPER_TYPE_CODE"),col("submit_time"),col("CREATOR_NAME"),col("INV_OPER_ID"),col("STATUS"),col("CREATE_TIME"),col("IS_VALID"))\
            .withColumn("dt_last_cycle_first_day", to_date(lit(last_cycle_first_day),'yyyyMMdd'))\
            .withColumn("shamsi_create_time_yearmonth", expr("persian_date(to_date(CREATE_TIME),'yyyyMMdd')")[0:7])\
            .withColumn("shamsi_last_cycle_yearmonth", expr("persian_date(to_date(dt_last_cycle_first_day),'yyyyMMdd')")[0:7])\
            .where((col("STATUS")==lit("completed")) &\
                   (col("shamsi_create_time_yearmonth") == col("shamsi_last_cycle_yearmonth")) &\
                   (col("IM_OPER_TYPE_CODE").isin(lit("SIMTransfer"), lit("Transfer"), lit("SIMStockIn"), lit("SIMStockOut"), lit("SIMRecall")))
                  ) #15804

IM_INV_OP_ORDER_K = spark.read\
    .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/INPUT_DATA/IM_INV_OP_ORDER/" + \
    PARAM_DATA_DAY + "/**").select( col("AUDIT_SKU_ID"),
                            col("ORI_PLAYER_ID"),
                            col("DEST_PLAYER_ID"),
                            col("ORI_PLAYER_TYPE_ID"),
                            col("DEST_PLAYER_TYPE_ID"),
                            col("ITEM_SKU_CODE"),
                            col("CREATE_PROLE_ID"),
                            col("CREATE_DEPT_ID"),
                            col("CREATE_TIME").alias("CREATE_TIME_k"),
                            col("INV_OPER_ID").alias("INV_OPER_ID2")
                          )

TBL_FZ_IMSI_TRANSFER_1 = IM_INV_OP_ORDER_K.join(IM_BP_NOTE_INST_T, IM_BP_NOTE_INST_T.INV_OPER_ID == IM_INV_OP_ORDER_K.INV_OPER_ID2 )\
        .select(
                col("INV_OPER_ID"),
                col("IM_OPER_TYPE_CODE"),
                col("IS_VALID"),
                col("CREATOR_NAME"),
                col("SUBMIT_TIME"),
                col("AUDIT_SKU_ID"),
                col("ORI_PLAYER_ID"),
                col("DEST_PLAYER_ID"),
                col("ORI_PLAYER_TYPE_ID"),
                col("DEST_PLAYER_TYPE_ID"),
                col("ITEM_SKU_CODE"),
                col("CREATE_PROLE_ID").alias("CREATE_PROLE_ID2"),
                col("CREATE_DEPT_ID").alias("CREATE_DEPT_ID2"),
                col("CREATE_TIME_k")
            )

TBL_FZ_IMSI_TRANSFER_2 = TBL_FZ_IMSI_TRANSFER_1\
    .join(VW_WAREHOUSE, TBL_FZ_IMSI_TRANSFER_1.DEST_PLAYER_ID == VW_WAREHOUSE.IM_RWH_ID, 'left_outer')\
    .select(
        col("inv_oper_id"),
        col("im_oper_type_code"),
        col("is_valid"),
        col("creator_name"),
        col("submit_time"),
        col("audit_sku_id"),
        col("ori_player_id"),
        col("dest_player_id"),
        col("ori_player_type_id"),
        col("dest_player_type_id"),
        col("item_sku_code"),
        col("CREATE_PROLE_ID2"),
        col("CREATE_DEPT_ID2"),
        col("create_time").alias("create_time2"),
        col("IM_RWH_NAME").alias("IM_RWH_NAME2")
    ).withColumn("WH_NAME_DEST", get_json_object(col("IM_RWH_NAME2"),"$.fa_IR"))

TBL_FZ_IMSI_TRANSFER_3 = TBL_FZ_IMSI_TRANSFER_2\
    .join(VW_WAREHOUSE, TBL_FZ_IMSI_TRANSFER_2.ori_player_id == VW_WAREHOUSE.IM_RWH_ID, 'left_outer')\
    .select(
        col("inv_oper_id"),
        col("im_oper_type_code"),
        col("is_valid"),
        col("creator_name"),
        col("submit_time"),
        col("audit_sku_id"),
        col("ori_player_id"),
        col("dest_player_id"),
        col("ori_player_type_id"),
        col("dest_player_type_id"),
        col("item_sku_code"),
        col("CREATE_PROLE_ID2"),
        col("CREATE_DEPT_ID2"),
        col("create_time2"),
        col("IM_RWH_NAME").alias("IM_RWH_NAME3"),
        col("WH_NAME_DEST")
    ).withColumn("WH_NAME_SOURCE", get_json_object(col("IM_RWH_NAME3"),"$.fa_IR")) #16023

SYS_ORG = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/INPUT_DATA/SYS_ORG/" + \
            PARAM_DATA_DAY + "/**").select(col("*"), col("ORG_NAME").alias("ORG_NAME_2"), col("ORG_NAME").alias("ORG_NAME_3"), col("ORG_NAME").alias("ORG_NAME_4"))

MV_REGION = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/INTERMEDIATE/MV_REGION/" + \
            PARAM_DATA_DAY + "/**")

TBL_FZ_IMSI_TRANSFER_4_j1 = TBL_FZ_IMSI_TRANSFER_3\
    .join(VW_WAREHOUSE, TBL_FZ_IMSI_TRANSFER_3.dest_player_id == VW_WAREHOUSE.IM_RWH_ID, 'left_outer')\
    .select(
        col("inv_oper_id"),
        col("im_oper_type_code"),
        col("is_valid"),
        col("creator_name"),
        col("submit_time"),
        col("audit_sku_id"),
        col("ori_player_id"),
        col("dest_player_id"),
        col("ori_player_type_id"),
        col("dest_player_type_id"),
        col("item_sku_code"),
        col("CREATE_PROLE_ID2"),
        col("CREATE_DEPT_ID2"),
        col("create_time2"),
        col("IM_RWH_NAME"),
        col("WH_NAME_DEST"),
        col("WH_NAME_SOURCE"),
        col("OU_CODE")
    )

TBL_FZ_IMSI_TRANSFER_4_j2 = TBL_FZ_IMSI_TRANSFER_4_j1\
    .join(SYS_ORG, TBL_FZ_IMSI_TRANSFER_4_j1.OU_CODE == SYS_ORG.ORG_CODE, 'left_outer')\
    .select(
        col("inv_oper_id"),
        col("im_oper_type_code"),
        col("is_valid"),
        col("creator_name"),
        col("submit_time"),
        col("audit_sku_id"),
        col("ori_player_id"),
        col("dest_player_id"),
        col("ori_player_type_id"),
        col("dest_player_type_id"),
        col("item_sku_code"),
        col("CREATE_PROLE_ID2"),
        col("CREATE_DEPT_ID2"),
        col("create_time2"),
        col("IM_RWH_NAME"),
        col("WH_NAME_DEST"),
        col("WH_NAME_SOURCE"),
        SYS_ORG.ORG_NAME,
        SYS_ORG.B_REGION_ID
    )

TBL_FZ_IMSI_TRANSFER_4 = TBL_FZ_IMSI_TRANSFER_4_j2\
    .join(MV_REGION, TBL_FZ_IMSI_TRANSFER_4_j2.B_REGION_ID == MV_REGION.region_id, 'left_outer')\
    .select(
        col("inv_oper_id"),
        col("im_oper_type_code"),
        col("is_valid"),
        col("creator_name"),
        col("submit_time"),
        col("audit_sku_id"),
        col("ori_player_id"),
        col("dest_player_id"),
        col("ori_player_type_id"),
        col("dest_player_type_id"),
        col("item_sku_code"),
        col("CREATE_PROLE_ID2"),
        col("CREATE_DEPT_ID2"),
        col("create_time2"),
        col("WH_NAME_DEST"),
        col("WH_NAME_SOURCE"),
        MV_REGION.REGION_NAME.alias("REGION_DEST")
    ) #16023

TBL_FZ_IMSI_TRANSFER_5 = TBL_FZ_IMSI_TRANSFER_4.join(SYS_ORG, TBL_FZ_IMSI_TRANSFER_4.CREATE_DEPT_ID2 == SYS_ORG.ORG_ID, 'left_outer')\
    .select(    col("INV_OPER_ID"),
                col("IM_OPER_TYPE_CODE"),
                col("IS_VALID"),
                col("CREATOR_NAME"),
                col("SUBMIT_TIME"),
                col("AUDIT_SKU_ID"),
                col("ORI_PLAYER_ID"),
                col("DEST_PLAYER_ID"),
                col("ORI_PLAYER_TYPE_ID"),
                col("DEST_PLAYER_TYPE_ID"),
                col("ITEM_SKU_CODE"),
                col("CREATE_PROLE_ID2"),
                col("CREATE_DEPT_ID2"),
                col("create_time2"),
                col("WH_NAME_DEST"),
                col("WH_NAME_SOURCE"),
                col("REGION_DEST"),
                col("ORG_NAME_2")
            ).withColumn("OPERATOR_OFFICE_NAME", when(col("ORG_NAME_2").like('fa_IR'),\
                            get_json_object(col("ORG_NAME_2"),"$.fa_IR")).otherwise(col("ORG_NAME_2")))

TBL_FZ_IMSI_TRANSFER_6_j1 = TBL_FZ_IMSI_TRANSFER_5.join(VW_WAREHOUSE, TBL_FZ_IMSI_TRANSFER_5.ORI_PLAYER_ID == VW_WAREHOUSE.IM_RWH_ID, 'left_outer')\
.select(     col("INV_OPER_ID"),
            col("IM_OPER_TYPE_CODE"),
            col("IS_VALID"),
            col("CREATOR_NAME"),
            col("SUBMIT_TIME"),
            col("AUDIT_SKU_ID"),
            col("ORI_PLAYER_ID"),
            col("DEST_PLAYER_ID"),
            col("ORI_PLAYER_TYPE_ID"),
            col("DEST_PLAYER_TYPE_ID"),
            col("ITEM_SKU_CODE"),
            col("CREATE_PROLE_ID2"),
            col("CREATE_DEPT_ID2"),
            col("create_time2"),
            col("WH_NAME_DEST"),
            col("WH_NAME_SOURCE"),
            col("REGION_DEST"),
            col("ORG_NAME_2"),
            col("OPERATOR_OFFICE_NAME"),
            col("IM_RWH_CODE").alias("WH_CODE_SOURCE"),
            col("OU_CODE")
        ) #16023
TBL_FZ_IMSI_TRANSFER_6 = TBL_FZ_IMSI_TRANSFER_6_j1.alias("j1").join(SYS_ORG.alias("t"), TBL_FZ_IMSI_TRANSFER_6_j1.OU_CODE == SYS_ORG.ORG_CODE, 'left_outer')\
    .select(    col("j1.INV_OPER_ID"),
                col("j1.IM_OPER_TYPE_CODE"),
                col("j1.IS_VALID"),
                col("j1.CREATOR_NAME"),
                col("j1.SUBMIT_TIME"),
                col("j1.AUDIT_SKU_ID"),
                col("j1.ORI_PLAYER_ID"),
                col("j1.DEST_PLAYER_ID"),
                col("j1.ORI_PLAYER_TYPE_ID"),
                col("j1.DEST_PLAYER_TYPE_ID"),
                col("j1.ITEM_SKU_CODE"),
                col("j1.CREATE_PROLE_ID2"),
                col("j1.CREATE_DEPT_ID2"),
                col("j1.create_time2"),
                col("j1.WH_NAME_DEST"),
                col("j1.WH_NAME_SOURCE"),
                col("j1.REGION_DEST"),
                col("t.ORG_NAME_3"),
                col("j1.OPERATOR_OFFICE_NAME"),
                col("j1.WH_CODE_SOURCE"),
                col("t.ORG_BIZ_CODE").alias("OFFICE_CODE_SOURCE"),
                col("OU_CODE").alias("OU_CODE2")
            ).withColumn("ORG_NAME_SOURCE", when(col("t.ORG_NAME_3").like('fa_IR'),\
                            get_json_object(col("t.ORG_NAME_3"),"$.fa_IR")).otherwise(col("t.ORG_NAME_3"))) #16023

TBL_FZ_IMSI_TRANSFER_7_j1 = TBL_FZ_IMSI_TRANSFER_6.join(VW_WAREHOUSE, TBL_FZ_IMSI_TRANSFER_6.DEST_PLAYER_ID == VW_WAREHOUSE.IM_RWH_ID, 'left_outer')\
.select(    col("INV_OPER_ID"),
            col("IM_OPER_TYPE_CODE"),
            col("IS_VALID"),
            col("CREATOR_NAME"),
            col("SUBMIT_TIME"),
            col("AUDIT_SKU_ID"),
            col("ORI_PLAYER_ID"),
            col("DEST_PLAYER_ID"),
            col("ORI_PLAYER_TYPE_ID"),
            col("DEST_PLAYER_TYPE_ID"),
            col("ITEM_SKU_CODE"),
            col("CREATE_PROLE_ID2"),
            col("CREATE_DEPT_ID2"),
            col("create_time2"),
            col("WH_NAME_DEST"),
            col("WH_NAME_SOURCE"),
            col("REGION_DEST"),
            col("OPERATOR_OFFICE_NAME"),
            col("OFFICE_CODE_SOURCE"),
            col("ORG_NAME_SOURCE"),
            col("WH_CODE_SOURCE"),
            col("OU_CODE"),
            col("IM_RWH_CODE").alias("WH_CODE_DEST")
)

TBL_FZ_IMSI_TRANSFER_7 = TBL_FZ_IMSI_TRANSFER_7_j1.join(SYS_ORG, TBL_FZ_IMSI_TRANSFER_7_j1.OU_CODE == SYS_ORG.ORG_CODE, 'left_outer')\
.select(    col("INV_OPER_ID"),
            col("IM_OPER_TYPE_CODE"),
            col("IS_VALID"),
            col("CREATOR_NAME"),
            col("SUBMIT_TIME"),
            col("AUDIT_SKU_ID"),
            col("ORI_PLAYER_ID"),
            col("DEST_PLAYER_ID"),
            col("ORI_PLAYER_TYPE_ID"),
            col("DEST_PLAYER_TYPE_ID"),
            col("ITEM_SKU_CODE"),
            col("CREATE_PROLE_ID2").alias("CREATE_PROLE_ID"),
            col("CREATE_DEPT_ID2").alias("CREATE_DEPT_ID"),
            col("create_time2").alias("CREATE_TIME"),
            col("WH_NAME_DEST"),
            col("WH_NAME_SOURCE"),
            col("REGION_DEST"),
            col("OPERATOR_OFFICE_NAME"),
            col("OFFICE_CODE_SOURCE"),
            col("ORG_NAME_SOURCE"),
            col("WH_CODE_SOURCE"),
            col("ORG_BIZ_CODE").alias("OFFICE_CODE_DEST"),
            col("OU_CODE"),
            col("ORG_NAME_2"),
            col("WH_CODE_DEST")
).withColumn("ORG_NAME_DEST", when(col("ORG_NAME_2").like('fa_IR'),\
                get_json_object(col("ORG_NAME_2"),"$.fa_IR")).otherwise(col("ORG_NAME_2"))) #16023

TBL_FZ_IMSI_TRANSFER_7 = TBL_FZ_IMSI_TRANSFER_7\
.select(
        col("INV_OPER_ID"),
        col("IM_OPER_TYPE_CODE"),
        col("IS_VALID"),
        col("CREATOR_NAME"),
        col("SUBMIT_TIME"),
        col("AUDIT_SKU_ID"),
        col("ORI_PLAYER_ID"),
        col("DEST_PLAYER_ID"),
        col("ORI_PLAYER_TYPE_ID"),
        col("DEST_PLAYER_TYPE_ID"),
        col("ITEM_SKU_CODE"),
        col("CREATE_PROLE_ID"),
        col("CREATE_DEPT_ID"),
        col("CREATE_TIME"),
        col("WH_NAME_DEST"),
        col("WH_NAME_SOURCE"),
        col("REGION_DEST"),
        col("OPERATOR_OFFICE_NAME"),
        col("OFFICE_CODE_SOURCE"),
        col("ORG_NAME_SOURCE"),
        col("WH_CODE_SOURCE"),
        col("OFFICE_CODE_DEST"),
        col("ORG_NAME_DEST"),
        col("WH_CODE_DEST")
) #16023

IM_INV_AUDIT_ITEM = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/INPUT_DATA/IM_INV_AUDIT_ITEM/" + \
            PARAM_DATA_DAY + "/**")

TBL_FZ_IMSI_TRANSFER_8 = TBL_FZ_IMSI_TRANSFER_7.join(IM_INV_AUDIT_ITEM, IM_INV_AUDIT_ITEM.AUDIT_SKU_ID == TBL_FZ_IMSI_TRANSFER_7.AUDIT_SKU_ID )\
.select(
    col("INV_OPER_ID"),
    col("IM_OPER_TYPE_CODE"),
    col("IS_VALID"),
    col("CREATOR_NAME"),
    col("SUBMIT_TIME"),
    TBL_FZ_IMSI_TRANSFER_7.AUDIT_SKU_ID,
    col("ORI_PLAYER_ID"),
    col("DEST_PLAYER_ID"),
    col("ORI_PLAYER_TYPE_ID"),
    col("DEST_PLAYER_TYPE_ID"),
    col("ITEM_SKU_CODE"),
    TBL_FZ_IMSI_TRANSFER_7.CREATE_PROLE_ID,
    TBL_FZ_IMSI_TRANSFER_7.CREATE_DEPT_ID,
    TBL_FZ_IMSI_TRANSFER_7.CREATE_TIME,
    col("OFFICE_CODE_DEST"),
    col("REGION_DEST"),
    col("OFFICE_CODE_SOURCE"),
    col("ORG_NAME_SOURCE"),
    col("ORG_NAME_DEST"),
    col("OPERATOR_OFFICE_NAME"),
    col("WH_NAME_DEST"),
    col("WH_NAME_SOURCE"),
    col("wh_code_dest"),
    col("wh_code_source"),
    col("MAJOR_ID")
) #4514400

TBL_FZ_IMSI_TRANSFER_9 = TBL_FZ_IMSI_TRANSFER_8.join(VW_IM_ENTI_IMSI, TBL_FZ_IMSI_TRANSFER_8.MAJOR_ID == VW_IM_ENTI_IMSI.IMSI_Imsii, 'left_outer')\
    .select(
       col("INV_OPER_ID"),
       col("IM_OPER_TYPE_CODE"),
       col("IS_VALID"),
       col("CREATOR_NAME"),
       col("SUBMIT_TIME"),
       col("AUDIT_SKU_ID"),
       col("ORI_PLAYER_ID"),
       col("DEST_PLAYER_ID"),
       col("ORI_PLAYER_TYPE_ID"),
       col("DEST_PLAYER_TYPE_ID"),
       col("ITEM_SKU_CODE"),
       col("CREATE_PROLE_ID"),
       col("CREATE_DEPT_ID"),
       col("CREATE_TIME"),
       col("MAJOR_ID"),
       lit(' ').alias("EMPLOYEE_DEST").cast("string"),
       col("OFFICE_CODE_DEST"),
       col("REGION_DEST"),
       col("OFFICE_CODE_SOURCE"),
       col("ORG_NAME_SOURCE"),
       col("ORG_NAME_DEST"),
       col("OPERATOR_OFFICE_NAME"),
       col("WH_NAME_DEST"),
       col("WH_NAME_SOURCE"),
       col("WH_CODE_DEST"),
       col("WH_CODE_SOURCE"),
       VW_IM_ENTI_IMSI.INV_STATUS_Imsii,
       VW_IM_ENTI_IMSI.WH_ID_Imsii.alias("WH_ID_Imsii_9"),
       VW_IM_ENTI_IMSI.SKU_CODE_Imsii.alias("SKU_CODE_Imsii_9"),
       VW_IM_ENTI_IMSI.REGION_ID.alias("REGION_ID_9")
    ) #4514400
    
    

TBL_FZ_IMSI_TRANSFER_10 = TBL_FZ_IMSI_TRANSFER_9.alias('a').join(VW_IM_ENTI_IMSI.alias('b'), TBL_FZ_IMSI_TRANSFER_9.MAJOR_ID == VW_IM_ENTI_IMSI.ICCID, 'left_outer')\
    .select(
        TBL_FZ_IMSI_TRANSFER_9.INV_OPER_ID,
        TBL_FZ_IMSI_TRANSFER_9.IM_OPER_TYPE_CODE,
        TBL_FZ_IMSI_TRANSFER_9.IS_VALID,
        TBL_FZ_IMSI_TRANSFER_9.CREATOR_NAME,
        TBL_FZ_IMSI_TRANSFER_9.SUBMIT_TIME,
        TBL_FZ_IMSI_TRANSFER_9.AUDIT_SKU_ID,
        TBL_FZ_IMSI_TRANSFER_9.ORI_PLAYER_ID,
        TBL_FZ_IMSI_TRANSFER_9.DEST_PLAYER_ID,
        TBL_FZ_IMSI_TRANSFER_9.ORI_PLAYER_TYPE_ID,
        TBL_FZ_IMSI_TRANSFER_9.DEST_PLAYER_TYPE_ID,
        TBL_FZ_IMSI_TRANSFER_9.ITEM_SKU_CODE,
        TBL_FZ_IMSI_TRANSFER_9.CREATE_PROLE_ID,
        TBL_FZ_IMSI_TRANSFER_9.CREATE_DEPT_ID,
        TBL_FZ_IMSI_TRANSFER_9.CREATE_TIME,
        TBL_FZ_IMSI_TRANSFER_9.MAJOR_ID,
        TBL_FZ_IMSI_TRANSFER_9.EMPLOYEE_DEST,
        TBL_FZ_IMSI_TRANSFER_9.OFFICE_CODE_DEST,
        TBL_FZ_IMSI_TRANSFER_9.REGION_DEST,
        TBL_FZ_IMSI_TRANSFER_9.OFFICE_CODE_SOURCE,
        TBL_FZ_IMSI_TRANSFER_9.ORG_NAME_SOURCE,
        TBL_FZ_IMSI_TRANSFER_9.ORG_NAME_DEST,
        TBL_FZ_IMSI_TRANSFER_9.OPERATOR_OFFICE_NAME,
        TBL_FZ_IMSI_TRANSFER_9.WH_NAME_DEST,
        TBL_FZ_IMSI_TRANSFER_9.WH_NAME_SOURCE,
        TBL_FZ_IMSI_TRANSFER_9.WH_CODE_DEST,
        TBL_FZ_IMSI_TRANSFER_9.WH_CODE_SOURCE,
        TBL_FZ_IMSI_TRANSFER_9.INV_STATUS_Imsii.alias("INV_STATUS2"),
        TBL_FZ_IMSI_TRANSFER_9.WH_ID_Imsii_9,
        TBL_FZ_IMSI_TRANSFER_9.SKU_CODE_Imsii_9,
        TBL_FZ_IMSI_TRANSFER_9.REGION_ID_9,
        col('b.INV_STATUS_Imsii'),
        col('b.WH_ID_Imsii'),
        col('b.SKU_CODE_Imsii'),
        col('b.REGION_ID')
    ).withColumn("WH_ID_IMSI", when(col("INV_STATUS2").isNull(), col("b.WH_ID_Imsii")).otherwise(col("WH_ID_Imsii_9")))\
    .withColumn("SKU_CODE_IMSI", when(col("INV_STATUS2").isNull(), col("b.SKU_CODE_Imsii")).otherwise(col("SKU_CODE_Imsii_9")))\
    .withColumn("REGION_ID_IMSI", when(col("INV_STATUS2").isNull(), col("b.REGION_ID")).otherwise(col("REGION_ID_9")))\
    .withColumn("INV_STATUS", coalesce(col("INV_STATUS2"), col("b.INV_STATUS_Imsii")))\
    .withColumn("OPERATOR_TIME", regexp_replace((expr("persian_date(to_date(SUBMIT_TIME),'yyyy/MM/dd')")),'-','/'))


final = TBL_FZ_IMSI_TRANSFER_10.select(
    col("MAJOR_ID").alias("IMSI"),
    col("ORI_PLAYER_ID").alias("WH_ID_SOURCE"),
    col("WH_CODE_SOURCE"),
    col("WH_NAME_SOURCE"),
    col("DEST_PLAYER_ID").alias("WH_ID_DEST"),
    col("WH_CODE_DEST"),
    col("WH_NAME_DEST"),
    col("REGION_DEST"),
    col("IM_OPER_TYPE_CODE").alias("OPERATION_TYPE"),
    col("OPERATOR_TIME"),
    col("CREATOR_NAME").alias("OPERATOR_NAME"),
    col("OPERATOR_OFFICE_NAME"),
    col("OFFICE_CODE_SOURCE"),
    col("OFFICE_CODE_DEST").alias("OFFICE_CODE_DEST"),
    col("ORG_NAME_SOURCE"),
    col("ORG_NAME_DEST"),
    col("INV_STATUS"),
    col("SKU_CODE_IMSI").alias("SKU_CODE")
)

final\
    .coalesce(1)\
    .write\
    .option("header", True)\
    .option('emptyValue', '')\
    .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")\
    .mode('overwrite')\
    .csv("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/RESULT_DATA/rpt_imsi_transfer_YYYYMMDDHH24MISS/" + PARAM_DATA_DAY)