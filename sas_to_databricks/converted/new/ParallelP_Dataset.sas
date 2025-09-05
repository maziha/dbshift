from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

spark = SparkSession.builder.appName("SAS_to_PySpark_Conversion").getOrCreate()

PRODTEST = "PROD"
FCST_V1 = "/Workspace/SASProd/SASProjects/salesforecast/1_DemandForecast_Production/SASProgram"

def print_dtstamp(message):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} : {message}")

# Set up environment
%run f"{FCST_V1}/FolderSetup_SAS2"
%run f"{FCST_V1}/DTStamp.sas"

print_dtstamp("set=Y")

# JDBC connection properties (placeholders)
jdbc_url = "jdbc:teradata://your_teradata_host/database"
jdbc_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.teradata.jdbc.TeraDriver"
}

# Date filter
sql_query_insert = """
SELECT Current_Date AS upd_date, Max(a.startDate) AS startDate, Max(wk_end_dt) AS endDate,Count(DISTINCT wk_end_dt) AS nweeks
FROM
(
    SELECT max(endDate) +1 AS startDate 
    FROM  fcst_update_dtinf AS a 
) AS a,
vprod_dim.dt_inf AS b 

WHERE wk_end_dt BETWEEN startDate AND Current_Date
"""

df_to_insert = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"({sql_query_insert}) as subq") \
    .options(**jdbc_properties) \
    .load()

df_to_insert.write \
    .format("jdbc") \
    .mode("append") \
    .option("url", jdbc_url) \
    .option("dbtable", "fcst_update_dtinf") \
    .options(**jdbc_properties) \
    .save()


# Date filter for rebuilding/updating datasets from 2016 (commented out as in original)
# sql_query_insert_rebuild = """
# SELECT Current_Date AS upd_date, (date '2016-01-01') AS startDate, (date '2020-10-10') AS endDate,Count(DISTINCT wk_end_dt) AS nweeks
# FROM
# (
#     SELECT  (date '2016-01-01') AS startDate
#     FROM  fcst_update_dtinf AS a
# ) AS a,
# vprod_dim.dt_inf AS b
# WHERE wk_end_dt BETWEEN startDate AND Current_Date
# """
# df_to_insert_rebuild = spark.read \
#     .format("jdbc") \
#     .option("url", jdbc_url) \
#     .option("dbtable", f"({sql_query_insert_rebuild}) as subq") \
#     .options(**jdbc_properties) \
#     .load()
# df_to_insert_rebuild.write \
#     .format("jdbc") \
#     .mode("append") \
#     .option("url", jdbc_url) \
#     .option("dbtable", "fcst_update_dtinf") \
#     .options(**jdbc_properties) \
#     .save()


df_fcst_update_dtinf = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "adva.fcst_update_dtinf") \
    .options(**jdbc_properties) \
    .load()

df_fcst_update_dtinf.createOrReplaceTempView("fcst_update_dtinf")

df_dates = spark.sql("""
    SELECT
        CONCAT("'", CAST(DATE(MAX(startDate)) AS STRING), "'") as st_dt,
        CONCAT("'", CAST(DATE(MAX(endDate)) AS STRING), "'") as end_dt,
        DATE_FORMAT(MAX(startDate), 'ddMMMyyyy') as st_dt2,
        DATE_FORMAT(MAX(endDate), 'ddMMMyyyy') as end_dt2
    FROM adva.fcst_update_dtinf
""")

# where upd_date = '20OCT2020'd; (commented as in original)

date_row = df_dates.collect()[0]
MinDt = date_row["st_dt"]
MaxDt = date_row["end_dt"]
MinDt_SAS = date_row["st_dt2"]
MaxDT_SAS = date_row["end_dt2"]

print(MinDt)
print(MaxDt)
print(MinDt_SAS)
print(MaxDT_SAS)

print("NOTE: rc=0")
workpath = "In-memory temporary views"
print(f"NOTE: work path is {workpath}")

# The creation of FCSTEnvironment.sas is replaced by Python functions
# that will be called within each parallel task.

# Commented out blocks from original SAS script
# Creating the CNT table is not necessary with the delete and insert statements below
# spark.sql("DROP TABLE adva.MJR_P_CLS_HL_P_CNT")
# sql_create_cnt_table = """
# CREATE SET TABLE DL_CNTL_ADVA.MJR_P_CLS_HL_P_CNT ,FALLBACK ,
#      NO BEFORE JOURNAL,
#      NO AFTER JOURNAL,
#      CHECKSUM = DEFAULT,
#      DEFAULT MERGEBLOCKRATIO,
#      MAP = TD_MAP2
#      (
#       MJR_P_CLS_ID VARCHAR(15) CHARACTER SET Latin NOT CaseSpecific NOT NULL,
#       ROW_CNT DECIMAL(18) NOT NULL )
# UNIQUE PRIMARY INDEX ( MJR_P_CLS_ID )
# """
# # This would require a direct JDBC execute call not standard in pyspark.read/write
# print(f"Placeholder for executing: {sql_create_cnt_table}")

# spark.sql("DELETE FROM DL_CNTL_ADVA.MJR_P_CLS_HL_P_CNT")
# sql_insert_cnt_table = """
# INSERT INTO DL_CNTL_ADVA.MJR_P_CLS_HL_P_CNT
# SELECT MJR_P_CLS_ID, Count(*)
# 		from
# 		vprod_mds_rplnm.dly_ut_fcst_hm_loc_cap_hst as a
# 		join
# 		fcst_p_upc_inf as b
# 			on a.p_id = b.p_id
# GROUP BY 1
# """
# # This would require a direct JDBC execute call
# print(f"Placeholder for executing: {sql_insert_cnt_table}")

# sql_collect_stats_cnt = "COLLECT STATISTICS COLUMN (MJR_P_CLS_ID) ON dl_cntl_adva.MJR_P_CLS_HL_P_CNT"
# print(f"Placeholder for executing: {sql_collect_stats_cnt}")

# spark.sql("DROP TABLE adva.new_mpk_flg")
# sql_create_mpk_flg = """
# CREATE TABLE dl_cntl_adva.new_mpk_flg AS
# (
# 	SELECT
# 		p.p_id
# 		, b.day_dt
# 		, 1 AS mpk_cpn_flg
#
# 	FROM
# 	(
# 		SELECT *
# 		FROM vmarket_basket.mb_cmkt_promo_id_inf AS a
# 		WHERE a.mperk_ind_flg = 'Y'
# 		AND a.mperk_ofr_tgt_typ_ct = 1
# 	) AS a
#
# 	JOIN VPROD_OPTM_PROMO.PROMO_UPC_HST AS c
# 		ON a.cmkt_promo_id = c.cmkt_promo_id
#
# 	JOIN vprod.p_upc_p_hcy_inf AS P
# 		ON p.P_upc_id = c.P_UPC_ID
#
# 	JOIN vprod_dim.dt_inf AS b
# 		ON b.DAY_DT BETWEEN cmkt_cpn_dsp_strt_dt AND cmkt_cpn_dsp_end_dt
#
# 	WHERE cmkt_cpn_dsp_end_dt >= date '2015-01-01'
#
# 	GROUP BY 1,2,3
# ) WITH DATA UNIQUE PRIMARY INDEX (p_id, day_dt)
# """
# # This would require a direct JDBC execute call
# print(f"Placeholder for executing: {sql_create_mpk_flg}")

# sql_collect_stats_mpk = "COLLECT stats dl_cntl_adva.new_mpk_flg INDEX (p_id, day_dt)"
# print(f"Placeholder for executing: {sql_collect_stats_mpk}")

# spark.sql("DROP TABLE adva.fcst_hook_flag2")
# sql_create_hook_flag = """
# create table dl_cntl_adva.fcst_hook_flag2, no fallback as
# (
# 					sel day_dt, ut_id,
# 					max(mpk_hook_flg) as mpk_hook_flg ,
# 					max(mcc_hook_flg) as mcc_hook_flg,
# 					max(bucks_hook_flg) as bucks_hook_flg,
# 					max(other_hook_flg) as other_hook_flg,
# 					max(mPk_cpn_flg) as mPk_cpn_flg
# 					from
# 					 dl_mds_tier3_share.bskt_ad_hst_vw
# 					 where day_dt ge date '2016-01-01'
# 					 group by 1,2
#  ) with data unique primary index (ut_id, day_dt)
# """
# # This would require a direct JDBC execute call
# print(f"Placeholder for executing: {sql_create_hook_flag}")
# sql_collect_stats_hook = "COLLECT stats dl_cntl_adva.fcst_hook_flag2 INDEX (ut_id, day_dt)"
# print(f"Placeholder for executing: {sql_collect_stats_hook}")


print("NOTE: Log output directed to /dbfs/FileStore/SASLogs/Update/Daily/DatasetDLY.txt")

# Define task functions to simulate rsubmit blocks

def setup_task_environment():
    workpath = "In-memory temporary views"
    print(f"NOTE: work path is {workpath}")
    print('NOTE: running test;')
    %run f"{FCST_V1}/FolderSetup_SAS2"
    %run f"{FCST_V1}/DTStamp.sas"
    %run f"{FCST_V1}/CalcError"
    %run f"{FCST_V1}/GetFileList"

def task_daily_create_gro():
    setup_task_environment()
    L6_id = "'L6-000005'"
    %run f"{FCST_V1}/SASDataset_CreateTable.sas"
    print_dtstamp("End Dataset Grocery")

def task_daily_create_pets():
    setup_task_environment()
    L6_id = "'L6-000013'"
    %run f"{FCST_V1}/SASDataset_CreateTable.sas"
    %run f"{FCST_V1}/SortDataset.sas"
    print_dtstamp("End Dataset Pets")

def task_daily_create_hbc():
    setup_task_environment()
    L6_id = "'L6-000001'"
    %run f"{FCST_V1}/SASDataset_CreateTable.sas"
    print_dtstamp("End Dataset HBC")

def task_daily_sort_gro1():
    setup_task_environment()
    L6_id = "'L6-000005'"
    %run f"{FCST_V1}/SortDataset.sas"
    print_dtstamp("End Dataset Grocery")

def task_daily_sort_gro2():
    setup_task_environment()
    L6_id = "'L6-000005'"
    %run f"{FCST_V1}/SortDataset_V2.sas"
    print_dtstamp("End Dataset Gro_Sort")

def task_daily_sort_hbc1():
    setup_task_environment()
    L6_id = "'L6-000001'"
    %run f"{FCST_V1}/SortDataset.sas"
    print_dtstamp("End Dataset HBC_Sort")

def task_daily_sort_hbc2():
    setup_task_environment()
    L6_id = "'L6-000001'"
    %run f"{FCST_V1}/SortDataset_V2.sas"
    print_dtstamp("End Dataset HBC")

# Execute daily create tasks in parallel
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = [
        executor.submit(task_daily_create_gro),
        executor.submit(task_daily_create_pets),
        executor.submit(task_daily_create_hbc)
    ]
    for future in futures:
        future.result()  # Wait for all to complete

# Execute daily sort tasks in parallel
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [
        executor.submit(task_daily_sort_gro1),
        executor.submit(task_daily_sort_gro2),
        executor.submit(task_daily_sort_hbc1),
        executor.submit(task_daily_sort_hbc2)
    ]
    for future in futures:
        future.result()  # Wait for all to complete

# --- Email Notification 1 ---
def send_email(subject, recipients, body_html):
    smtp_server = "your.smtp.server.com" # Placeholder
    from_addr = "sarah.george@meijer.com"
    reply_to = "xxxxx@xxxx.com"

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_addr
    msg['To'] = ", ".join(recipients)
    msg.add_header('reply-to', reply_to)
    
    msg.attach(MIMEText(body_html, 'html'))
    
    try:
        with smtplib.SMTP(smtp_server) as server:
            server.send_message(msg)
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")

recipients_weekly = [
    "advanced.analytics@meijer.com", "justin.kaukonen@meijer.com",
    "aldijana.avdic@meijer.com", "Victor.Vardan@meijer.com",
    "Thomas.Eldridge@meijer.com", "Loretta.Davidson@meijer.com",
    "Sarah.George@meijer.com", "Joel.Smith@meijer.com"
]
tdate = datetime.now().strftime('%d%b%Y').upper()
email_body_weekly = f"""
<p>Good Morning,</p>
<p>GROCERY, PETS, & HBC: Daily Datesets Updated; Weekly Datasets will commence</p>
<p>Have a Great Day!!!</p>
<p>{tdate}</p>
"""
send_email("Dataset Creation Update", recipients_weekly, email_body_weekly)
print(tdate)

# --- Weekly Dataset Update ---
print("NOTE: Log output directed to /dbfs/FileStore/SASLogs/Update/Weekly/DatasetWkly.txt")

def task_weekly_create_gro():
    setup_task_environment()
    L6_id = "'L6-000005'"
    %run f"{FCST_V1}/SASDatasets_Wkly.sas"
    print_dtstamp("End Dataset Grocery")

def task_weekly_create_pets():
    setup_task_environment()
    L6_id = "'L6-000013'"
    %run f"{FCST_V1}/SASDatasets_Wkly.sas"
    print_dtstamp("End Dataset task2")

def task_weekly_create_hbc():
    setup_task_environment()
    L6_id = "'L6-000001'"
    %run f"{FCST_V1}/SASDatasets_Wkly.sas"
    print_dtstamp("End Dataset HBC")

with ThreadPoolExecutor(max_workers=3) as executor:
    futures = [
        executor.submit(task_weekly_create_gro),
        executor.submit(task_weekly_create_pets),
        executor.submit(task_weekly_create_hbc)
    ]
    for future in futures:
        future.result()

print_dtstamp("End of Dataset Update")

# --- Email Notification 2 ---
recipients_regional = [
    "advanced.analytics@meijer.com", "aldijana.avdic@meijer.com",
    "justin.kaukonen@meijer.com", "Victor.Vardan@meijer.com",
    "Thomas.Eldridge@meijer.com", "Loretta.Davidson@meijer.com",
    "Sarah.George@meijer.com", "Joel.Smith@meijer.com"
]
email_body_regional = f"""
<p>Good Morning,</p>
<p>GROCERY, PETS, & HBC: Weekly Datesets Updated; Regional Dataset Updates will commence</p>
<p>Have a Great Day!!!</p>
<p>{tdate}</p>
"""
send_email("Dataset Creation Update", recipients_regional, email_body_regional)
print(tdate)

# --- Regional Dataset Update ---
print("NOTE: Log output directed to /dbfs/FileStore/SASLogs/Update/DatasetRgnl.txt")

def task_regional_create_gro():
    setup_task_environment()
    L6_id = "'L6-000005'"
    %run f"{FCST_V1}/SASDataset_Rgnl.sas"
    print_dtstamp("End Dataset Grocery")

def task_regional_create_pets():
    setup_task_environment()
    L6_id = "'L6-000013'"
    %run f"{FCST_V1}/SASDataset_Rgnl.sas"
    print_dtstamp("End Dataset task2")

def task_regional_create_hbc():
    setup_task_environment()
    L6_id = "'L6-000001'"
    %run f"{FCST_V1}/SASDataset_Rgnl.sas"
    print_dtstamp("End Dataset HBC")

with ThreadPoolExecutor(max_workers=3) as executor:
    futures = [
        executor.submit(task_regional_create_gro),
        executor.submit(task_regional_create_pets),
        executor.submit(task_regional_create_hbc)
    ]
    for future in futures:
        future.result()

print_dtstamp("End of Dataset Update")

# --- Email Notification 3 ---
recipients_datacheck = ["sarah.george@meijer.com", "justin.kaukonen@meijer.com"]
email_body_datacheck = f"""
<p>Good Morning,</p>
<p>GROCERY & HBC & PETS: Daily, Weekly, and Regional datasets have finished updating.; Datacheck will now commence.</p>
<p>Have a Great Day!!!</p>
<p>{tdate}</p>
"""
send_email("Data Check", recipients_datacheck, email_body_datacheck)
print(tdate)

# --- Final Data Check ---
%run f"{FCST_V1}/SASDataCHECK.sas"
print_dtstamp("End of Dataset Check")
#End-DBShift