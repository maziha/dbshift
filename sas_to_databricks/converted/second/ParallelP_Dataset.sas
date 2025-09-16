import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

spark = SparkSession.builder.appName("sas_update_datasets").getOrCreate()

PRODTEST = "PROD"
FCST_V1 = "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram"

%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/FolderSetup_SAS2"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/DTStamp.sas"
print(f"DTSTAMP: SET=Y - {datetime.datetime.now()}")

jdbc_url_adva = "jdbc:teradata://your_server/database=adva"
jdbc_url_vprod_dim = "jdbc:teradata://your_server/database=vprod_dim"
connection_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.teradata.jdbc.TeraDriver"
}

df_fcst_update_dtinf_source = spark.read.jdbc(url=jdbc_url_adva, table="fcst_update_dtinf", properties=connection_properties)
start_date_result = df_fcst_update_dtinf_source.agg(F.max("endDate").alias("max_end_date")).collect()[0]
start_date = start_date_result["max_end_date"] + datetime.timedelta(days=1)

df_dt_inf = spark.read.jdbc(url=jdbc_url_vprod_dim, table="dt_inf", properties=connection_properties)

df_to_insert = df_dt_inf.filter(
    (F.col("wk_end_dt") >= F.lit(start_date)) & (F.col("wk_end_dt") <= F.current_date())
).agg(
    F.max("wk_end_dt").alias("endDate"),
    F.countDistinct("wk_end_dt").alias("nweeks")
).withColumn(
    "upd_date", F.current_date()
).withColumn(
    "startDate", F.lit(start_date)
).select("upd_date", "startDate", "endDate", "nweeks")

df_to_insert.write.jdbc(
    url=jdbc_url_adva,
    table="fcst_update_dtinf",
    mode="append",
    properties=connection_properties
)

df_fcst_update_dtinf_updated = spark.read.jdbc(url=jdbc_url_adva, table="adva.fcst_update_dtinf", properties=connection_properties)
df_fcst_update_dtinf_updated.createOrReplaceTempView("fcst_update_dtinf")

dates_df = spark.sql("""
    SELECT
        MAX(startDate) as max_start_date,
        MAX(endDate) as max_end_date
    FROM fcst_update_dtinf
""")

dates_row = dates_df.collect()[0]
max_start_date_val = dates_row["max_start_date"]
max_end_date_val = dates_row["max_end_date"]

MinDt = f"'{max_start_date_val.strftime('%Y-%m-%d')}'"
MaxDt = f"'{max_end_date_val.strftime('%Y-%m-%d')}'"
MinDt_SAS = max_start_date_val.strftime('%d%b%Y').upper()
MaxDT_SAS = max_end_date_val.strftime('%d%b%Y').upper()

print(MinDt)
print(MaxDt)
print(MinDt_SAS)
print(MaxDT_SAS)

WorkPath = "/dbfs/tmp/saswork"
print(f"NOTE: work path is {WorkPath}")

def send_email(to_list, cc_list, subject, html_body, from_addr, reply_to):
    smtp_server = "smtp.meijer.com"
    smtp_port = 25
    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = ", ".join(to_list)
    if cc_list:
        msg['Cc'] = ", ".join(cc_list)
    msg['Subject'] = subject
    msg.add_header('reply-to', reply_to)
    msg.attach(MIMEText(html_body, 'html'))
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.sendmail(from_addr, to_list + (cc_list or []), msg.as_string())
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")

print("--- Starting block: gro ---")
WorkPath_gro = "/dbfs/tmp/gro"
print(f"NOTE: work path is {WorkPath_gro}")
print("NOTE: running test")
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/FolderSetup_SAS2"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/DTStamp.sas"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/CalcError"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/GetFileList"
L6_id = "'L6-000005'"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/SASDataset_CreateTable.sas"
print(f"DTSTAMP: End Dataset Grocery - {datetime.datetime.now()}")

print("--- Starting block: pets ---")
WorkPath_pets = "/dbfs/tmp/pets"
print(f"NOTE: work path is {WorkPath_pets}")
print("NOTE: running test")
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/FolderSetup_SAS2"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/DTStamp.sas"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/CalcError"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/GetFileList"
L6_id = "'L6-000013'"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/SASDataset_CreateTable.sas"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/SortDataset.sas"
print(f"DTSTAMP: End Dataset Pets - {datetime.datetime.now()}")

print("--- Starting block: hbc ---")
WorkPath_hbc = "/dbfs/tmp/hbc"
print(f"NOTE: work path is {WorkPath_hbc}")
print("NOTE: running test")
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/FolderSetup_SAS2"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/DTStamp.sas"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/CalcError"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/GetFileList"
L6_id = "'L6-000001'"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/SASDataset_CreateTable.sas"
print(f"DTSTAMP: End Dataset HBC - {datetime.datetime.now()}")

print("--- Starting block: gro1 ---")
WorkPath_gro1 = "/dbfs/tmp/gro1"
print(f"NOTE: work path is {WorkPath_gro1}")
print("NOTE: running test")
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/FolderSetup_SAS2"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/DTStamp.sas"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/CalcError"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/GetFileList"
L6_id = "'L6-000005'"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/SortDataset.sas"
print(f"DTSTAMP: End Dataset Grocery - {datetime.datetime.now()}")

print("--- Starting block: gro2 ---")
WorkPath_gro2 = "/dbfs/tmp/gro2"
print(f"NOTE: work path is {WorkPath_gro2}")
print("NOTE: running test")
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/FolderSetup_SAS2"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/DTStamp.sas"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/CalcError"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/GetFileList"
L6_id = "'L6-000005'"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/SortDataset_V2.sas"
print(f"DTSTAMP: End Dataset Gro_Sort - {datetime.datetime.now()}")

print("--- Starting block: HBC1 ---")
WorkPath_HBC1 = "/dbfs/tmp/HBC1"
print(f"NOTE: work path is {WorkPath_HBC1}")
print("NOTE: running test")
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/FolderSetup_SAS2"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/DTStamp.sas"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/CalcError"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/GetFileList"
L6_id = "'L6-000001'"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/SortDataset.sas"
print(f"DTSTAMP: End Dataset HBC_Sort - {datetime.datetime.now()}")

print("--- Starting block: HBC2 ---")
WorkPath_HBC2 = "/dbfs/tmp/HBC2"
print(f"NOTE: work path is {WorkPath_HBC2}")
print("NOTE: running test")
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/FolderSetup_SAS2"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/DTStamp.sas"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/CalcError"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/GetFileList"
L6_id = "'L6-000001'"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/SortDataset_V2.sas"
print(f"DTSTAMP: End Dataset HBC - {datetime.datetime.now()}")

to_list_1 = ["advanced.analytics@meijer.com", "justin.kaukonen@meijer.com", "aldijana.avdic@meijer.com", "Victor.Vardan@meijer.com", "Thomas.Eldridge@meijer.com", "Loretta.Davidson@meijer.com", "Sarah.George@meijer.com", "Joel.Smith@meijer.com"]
from_addr_1 = "sarah.george@meijer.com"
reply_to_1 = "xxxxx@xxxx.com"
subject_1 = "Dataset Creation Update"
tdate_1 = datetime.datetime.now().strftime('%d%b%Y').upper()
print(tdate_1)
html_body_1 = f"""
<html><body>
<p>Good Morning,</p>
<p>GROCERY, PETS, & HBC: Daily Datesets Updated; Weekly Datasets will commence</p>
<p>Have a Great Day!!!</p>
<p>{tdate_1}</p>
</body></html>
"""
send_email(to_list_1, None, subject_1, html_body_1, from_addr_1, reply_to_1)

print("--- Starting block: task1 (Weekly) ---")
WorkPath_task1_wkly = "/dbfs/tmp/task1_wkly"
print(f"NOTE: work path is {WorkPath_task1_wkly}")
print("NOTE: running test")
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/FolderSetup_SAS2"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/DTStamp.sas"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/CalcError"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/GetFileList"
L6_id = "'L6-000005'"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/SASDatasets_Wkly.sas"
print(f"DTSTAMP: End Dataset Grocery - {datetime.datetime.now()}")

print("--- Starting block: task2 (Weekly) ---")
WorkPath_task2_wkly = "/dbfs/tmp/task2_wkly"
print(f"NOTE: work path is {WorkPath_task2_wkly}")
print("NOTE: running test")
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/FolderSetup_SAS2"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/DTStamp.sas"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/CalcError"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/GetFileList"
L6_id = "'L6-000013'"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/SASDatasets_Wkly.sas"
print(f"DTSTAMP: End Dataset task2 - {datetime.datetime.now()}")

print("--- Starting block: task4 (Weekly) ---")
WorkPath_task4_wkly = "/dbfs/tmp/task4_wkly"
print(f"NOTE: work path is {WorkPath_task4_wkly}")
print("NOTE: running test")
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/FolderSetup_SAS2"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/DTStamp.sas"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/CalcError"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/GetFileList"
L6_id = "'L6-000001'"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/SASDatasets_Wkly.sas"
print(f"DTSTAMP: End Dataset HBC - {datetime.datetime.now()}")
print(f"DTSTAMP: End of Dataset Update - {datetime.datetime.now()}")

to_list_2 = ["advanced.analytics@meijer.com", "aldijana.avdic@meijer.com", "justin.kaukonen@meijer.com", "Victor.Vardan@meijer.com", "Thomas.Eldridge@meijer.com", "Loretta.Davidson@meijer.com", "Sarah.George@meijer.com", "Joel.Smith@meijer.com"]
from_addr_2 = "aldijana.avdic@meijer.com"
reply_to_2 = "xxxxx@xxxx.com"
subject_2 = "Dataset Creation Update"
tdate_2 = datetime.datetime.now().strftime('%d%b%Y').upper()
print(tdate_2)
html_body_2 = f"""
<html><body>
<p>Good Morning,</p>
<p>GROCERY, PETS, & HBC: Weekly Datesets Updated; Regional Dataset Updates will commence</p>
<p>Have a Great Day!!!</p>
<p>{tdate_2}</p>
</body></html>
"""
send_email(to_list_2, None, subject_2, html_body_2, from_addr_2, reply_to_2)

print("--- Starting block: task1 (Rgnl) ---")
WorkPath_task1_rgnl = "/dbfs/tmp/task1_rgnl"
print(f"NOTE: work path is {WorkPath_task1_rgnl}")
print("NOTE: running test")
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/FolderSetup_SAS2"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/DTStamp.sas"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/CalcError"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/GetFileList"
L6_id = "'L6-000005'"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/SASDataset_Rgnl.sas"
print(f"DTSTAMP: End Dataset Grocery - {datetime.datetime.now()}")

print("--- Starting block: task2 (Rgnl) ---")
WorkPath_task2_rgnl = "/dbfs/tmp/task2_rgnl"
print(f"NOTE: work path is {WorkPath_task2_rgnl}")
print("NOTE: running test")
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/FolderSetup_SAS2"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/DTStamp.sas"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/CalcError"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/GetFileList"
L6_id = "'L6-000013'"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/SASDataset_Rgnl.sas"
print(f"DTSTAMP: End Dataset task2 - {datetime.datetime.now()}")

print("--- Starting block: task4 (Rgnl) ---")
WorkPath_task4_rgnl = "/dbfs/tmp/task4_rgnl"
print(f"NOTE: work path is {WorkPath_task4_rgnl}")
print("NOTE: running test")
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/FolderSetup_SAS2"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/DTStamp.sas"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/CalcError"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/GetFileList"
L6_id = "'L6-000001'"
%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/SASDataset_Rgnl.sas"
print(f"DTSTAMP: End Dataset HBC - {datetime.datetime.now()}")
print(f"DTSTAMP: End of Dataset Update - {datetime.datetime.now()}")

to_list_3 = ["sarah.george@meijer.com", "justin.kaukonen@meijer.com"]
from_addr_3 = "sarah.george@meijer.com"
reply_to_3 = "xxxxx@xxxx.com"
subject_3 = "Data Check"
tdate_3 = datetime.datetime.now().strftime('%d%b%Y').upper()
print(tdate_3)
html_body_3 = f"""
<html><body>
<p>Good Morning,</p>
<p>GROCERY & HBC & PETS: Daily, Weekly, and Regional datasets have finished updating.; Datacheck will now commence.</p>
<p>Have a Great Day!!!</p>
<p>{tdate_3}</p>
</body></html>
"""
send_email(to_list_3, None, subject_3, html_body_3, from_addr_3, reply_to_3)

%run "/Workspace/salesforecast/1_DemandForecast_Production/SASProgram/SASDataCHECK.sas"
print(f"DTSTAMP: End of Dataset Check - {datetime.datetime.now()}")

#End-DBShift