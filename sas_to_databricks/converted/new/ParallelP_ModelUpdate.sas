import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("model_update_driver").getOrCreate()

%run "/Workspace/us/meijer/com/mjrdata/SASProd/SASProjects/salesforecast/SASProgram/FinalSASPrograms/Autoexec_test"

PRODTEST = "PROD"
FCST_V1 = "/Workspace/us/meijer/com/mjrdata/SASProd/SASProjects/salesforecast/1_DemandForecast_Production/SASProgram"
fcst_log = "/dbfs/us.meijer.com/mjrdata/Sdrive/ShopperMarketing/Analytics/Staff Folders/Aldijana Lelak Avdic/Corp Ad Hoc/SalesForecast/SASLog"
fcst_coef = "/dbfs/us.meijer.com/mjrdata/SASProd/SASProjects/salesforecast/1_DemandForecast_Production/ModelCoef"
pred_promo = "/dbfs/us.meijer.com/mjrdata/SASProd/SASProjects/salesforecast/1_DemandForecast_Production/ModelPredPromo"
pred_base = "/dbfs/us.meijer.com/mjrdata/SASProd/SASProjects/salesforecast/1_DemandForecast_Production/ModelPredBase"
fcst = "/dbfs/us.meijer.com/mjrdata/Sdrive/ShopperMarketing/Analytics/Staff Folders/Aldijana Lelak Avdic/Corp Ad Hoc/SalesForecast/ModelPredBase"
fcst_promo = "/dbfs/us.meijer.com/mjrdata/Sdrive/ShopperMarketing/Analytics/Staff Folders/Aldijana Lelak Avdic/Corp Ad Hoc/SalesForecast/ModelPredPromo"
eval = "/dbfs/us.meijer.com/mjrdata/SASProd/SASProjects/salesforecast/1_DemandForecast_Production/Eval"
archive = "/dbfs/us.meijer.com/mjrdata/Sdrive/ShopperMarketing/Analytics/Staff Folders/Aldijana Lelak Avdic/Corp Ad Hoc/SalesForecast/ADM/Archive/Model Coeff"

%run f"{FCST_V1}/FolderSetup_SAS2"
%run f"{FCST_V1}/DTStamp"
%run f"{FCST_V1}/GetFileList"

dtstamp(set='Y')

Forecast = "N"
Update = "Y"
Train = "N"
Score = "Y"

td_user = "your_username"
td_password = "your_password"
td_url = "jdbc:teradata://your_teradata_host/database"

sql_query_dateinf = """
(SELECT MIN(day_dt) as startDate, MAX(day_dt) as endDate, COUNT(DISTINCT wk_end_dt) as nweeks
 FROM vprod_dim.dt_inf
 WHERE day_dt BETWEEN '20150101' AND '20220319')
"""

df_dateinf = spark.read.format("jdbc") \
    .option("url", td_url) \
    .option("dbtable", sql_query_dateinf) \
    .option("user", td_user) \
    .option("password", td_password) \
    .load()

df_dateinf = df_dateinf

dateinf_row = df_dateinf.first()
if dateinf_row:
    FirstPreWeek = dateinf_row["startDate"]
    LastPreWeek = dateinf_row["endDate"]
    nweeks = dateinf_row["nweeks"]

df_fcst_update_dtinf = spark.table("adva.fcst_update_dtinf")

result_row = df_fcst_update_dtinf.agg(
    F.max("startDate").alias("max_startDate"),
    F.max("endDate").alias("max_endDate")
).first()

if result_row:
    MinDt = f"'{result_row['max_startDate'].strftime('%Y-%m-%d')}'"
    MaxDt = f"'{result_row['max_endDate'].strftime('%Y-%m-%d')}'"
    max_tr_dt = f"'{result_row['max_endDate'].strftime('%Y%m%d')}'"

print(LastPreWeek)
print(MinDt)
print(MaxDt)
print(max_tr_dt)

mo_dt = datetime.strptime('31MAR2022', '%d%b%Y').date()
print(mo_dt)

ValidWeeks = 0
ValidWks = 0
ForecastWeeks = 10

sql_insert_modelprocess = "INSERT INTO DL_CNTL_ADVA.MODELPROCESS SELECT Current_Date AS DAY_DT, 'Update' AS PROCESS, 'ALL' AS HIERARCHY, 'InProgress' AS STATUS"
df_to_insert = spark.sql("SELECT Current_Date() AS DAY_DT, 'Update' AS PROCESS, 'ALL' AS HIERARCHY, 'InProgress' AS STATUS")

df_to_insert.write.format("jdbc") \
    .option("url", td_url) \
    .option("dbtable", "DL_CNTL_ADVA.MODELPROCESS") \
    .option("user", td_user) \
    .option("password", td_password) \
    .mode("append") \
    .save()

L6_id = 'L6-000005'
Part = 1
dir = 'asc'
%run f"{FCST_V1}/FolderSetup_SAS2"
%run f"{FCST_V1}/DTStamp"
%run f"{FCST_V1}/CalcError"
%run f"{FCST_V1}/GetFileList"
%run f"{FCST_V1}/Poisson"
%run f"{FCST_V1}/BkwrdReg_SPD"
%run f"{FCST_V1}/Reg_PW"
%run f"{FCST_V1}/RegScale"
%run f"{FCST_V1}/RALSO_PL"
%run f"{FCST_V1}/Poisson_ZIP"
%run f"{FCST_V1}/RegPromo"
dtstamp(f"End Poisson1 {L6_id}")

L6_id = 'L6-000005'
Part = 2
dir = 'desc'
%run f"{FCST_V1}/FolderSetup_SAS2"
%run f"{FCST_V1}/DTStamp"
%run f"{FCST_V1}/CalcError"
%run f"{FCST_V1}/GetFileList"
%run f"{FCST_V1}/Poisson"
%run f"{FCST_V1}/RALSO_PL"
%run f"{FCST_V1}/RandomForest"
%run f"{FCST_V1}/Poisson_ZIP"
%run f"{FCST_V1}/RegAct"
%run f"{FCST_V1}/ElasticN"
%run f"{FCST_V1}/RForestW"
%run f"{FCST_V1}/RegScale"
dtstamp(f"End Poisson1 {L6_id}")

L6_id = 'L6-000005'
Part = 1
dir = 'asc'
%run f"{FCST_V1}/FolderSetup_SAS2"
%run f"{FCST_V1}/DTStamp"
%run f"{FCST_V1}/CalcError"
%run f"{FCST_V1}/GetFileList"
%run f"{FCST_V1}/RegMPerk"
%run f"{FCST_V1}/RForestW"
%run f"{FCST_V1}/ElasticN"
%run f"{FCST_V1}/RegSP"
%run f"{FCST_V1}/StepReg_SPD"
%run f"{FCST_V1}/Reg_PL"
%run f"{FCST_V1}/StepRegR"
%run f"{FCST_V1}/RALSO_RG"
dtstamp(f"End Poisson1 {L6_id}")

L6_id = 'L6-000001'
Part = 1
dir = 'asc'
%run f"{FCST_V1}/FolderSetup_SAS2"
%run f"{FCST_V1}/DTStamp"
%run f"{FCST_V1}/CalcError"
%run f"{FCST_V1}/GetFileList"
%run f"{FCST_V1}/RegScale"
%run f"{FCST_V1}/RALSO_RG"
%run f"{FCST_V1}/RForestW"
%run f"{FCST_V1}/RALSO_PL"
%run f"{FCST_V1}/ElasticN"
%run f"{FCST_V1}/Reg_PL"
%run f"{FCST_V1}/RandomForest"
%run f"{FCST_V1}/Poisson"
dtstamp(f"End Poisson1 {L6_id}")

L6_id = 'L6-000001'
Part = 2
dir = 'desc'
%run f"{FCST_V1}/FolderSetup_SAS2"
%run f"{FCST_V1}/DTStamp"
%run f"{FCST_V1}/CalcError"
%run f"{FCST_V1}/GetFileList"
%run f"{FCST_V1}/StepReg_SPD"
%run f"{FCST_V1}/RALSO_PL"
%run f"{FCST_V1}/RForestW"
%run f"{FCST_V1}/BkwrdReg_SPD"
%run f"{FCST_V1}/RegAct"
%run f"{FCST_V1}/Poisson_ZIP"
%run f"{FCST_V1}/Poisson"
dtstamp(f"End Poisson1 {L6_id}")

L6_id = 'L6-000001'
Part = 1
dir = 'desc'
%run f"{FCST_V1}/FolderSetup_SAS2"
%run f"{FCST_V1}/DTStamp"
%run f"{FCST_V1}/CalcError"
%run f"{FCST_V1}/GetFileList"
%run f"{FCST_V1}/Reg_PDL"
%run f"{FCST_V1}/Poisson_ZIP"
%run f"{FCST_V1}/ElasticN"
%run f"{FCST_V1}/RegScale"
%run f"{FCST_V1}/RegPromo"
%run f"{FCST_V1}/RegSPD"
%run f"{FCST_V1}/RegMPerk"
%run f"{FCST_V1}/Reg_PL"
%run f"{FCST_V1}/RegSPD"
%run f"{FCST_V1}/RegMPerk"
%run f"{FCST_V1}/Reg_PW"
%run f"{FCST_V1}/RegSP"
dtstamp(f"End Poisson1 {L6_id}")

L6_id = 'L6-000013'
Part = 1
dir = 'asc'
%run f"{FCST_V1}/FolderSetup_SAS2"
%run f"{FCST_V1}/DTStamp"
%run f"{FCST_V1}/CalcError"
%run f"{FCST_V1}/GetFileList"
%run f"{FCST_V1}/Reg_PSW"
%run f"{FCST_V1}/RegSPD"
%run f"{FCST_V1}/RegMPerk"
%run f"{FCST_V1}/RegScale"
%run f"{FCST_V1}/StepRegR"
%run f"{FCST_V1}/Reg_PW"
%run f"{FCST_V1}/RegAct"
%run f"{FCST_V1}/RALSO_PL"
%run f"{FCST_V1}/Reg_PL"
%run f"{FCST_V1}/StepReg_SPD"
%run f"{FCST_V1}/ElasticN"
%run f"{FCST_V1}/Poisson"
dtstamp(f"End Poisson1 {L6_id}")

L6_id = 'L6-000013'
Part = 2
dir = 'desc'
%run f"{FCST_V1}/FolderSetup_SAS2"
%run f"{FCST_V1}/DTStamp"
%run f"{FCST_V1}/CalcError"
%run f"{FCST_V1}/GetFileList"
%run f"{FCST_V1}/RForestW"
%run f"{FCST_V1}/RegPDRg"
%run f"{FCST_V1}/RALSO_PL"
%run f"{FCST_V1}/Poisson_ZIP"
%run f"{FCST_V1}/Poisson"
%run f"{FCST_V1}/ElasticN"
dtstamp(f"End Poisson1 {L6_id}")

L6_id = 'L6-000013'
Part = 1
dir = 'asc'
%run f"{FCST_V1}/FolderSetup_SAS2"
%run f"{FCST_V1}/DTStamp"
%run f"{FCST_V1}/CalcError"
%run f"{FCST_V1}/GetFileList"
%run f"{FCST_V1}/RegPromo"
%run f"{FCST_V1}/RegSP"
%run f"{FCST_V1}/Reg_PDL"
%run f"{FCST_V1}/Poisson_ZIP"
%run f"{FCST_V1}/BkwrdReg_SPD"
%run f"{FCST_V1}/RForestW"
dtstamp(f"End Poisson1 {L6_id}")

L6_id = 'L6-000014'
Part = 1
dir = 'asc'
%run f"{FCST_V1}/FolderSetup_SAS2"
%run f"{FCST_V1}/DTStamp"
%run f"{FCST_V1}/CalcError"
%run f"{FCST_V1}/GetFileList"
%run f"{FCST_V1}/RegSub"
%run f"{FCST_V1}/RegSP"
%run f"{FCST_V1}/BkwrdReg_SPD"
%run f"{FCST_V1}/Reg_PDL"
%run f"{FCST_V1}/Poisson"
%run f"{FCST_V1}/Poisson_ZIP"
dtstamp(f"End Poisson1 {L6_id}")

L6_id = 'L6-000014'
Part = 2
dir = 'asc'
%run f"{FCST_V1}/FolderSetup_SAS2"
%run f"{FCST_V1}/DTStamp"
%run f"{FCST_V1}/CalcError"
%run f"{FCST_V1}/GetFileList"
%run f"{FCST_V1}/RALSO_PL"
%run f"{FCST_V1}/RegPromo"
%run f"{FCST_V1}/RegSPD"
%run f"{FCST_V1}/Reg_PW"
%run f"{FCST_V1}/ElasticN"
%run f"{FCST_V1}/RForestW"
%run f"{FCST_V1}/RandomForest"
%run f"{FCST_V1}/RForestW"
dtstamp(f"End Poisson1 {L6_id}")

L6_id = 'L6-000014'
Part = 2
dir = 'asc'
%run f"{FCST_V1}/FolderSetup_SAS2"
%run f"{FCST_V1}/DTStamp"
%run f"{FCST_V1}/CalcError"
%run f"{FCST_V1}/GetFileList"
%run f"{FCST_V1}/Poisson"
%run f"{FCST_V1}/Poisson_ZIP"
%run f"{FCST_V1}/PSW_ACT"
%run f"{FCST_V1}/Reg_PSW"
%run f"{FCST_V1}/RandomForest"
dtstamp(f"End Poisson1 {L6_id}")

tdate = datetime.now().strftime("%d%m%Y")
print(tdate)

#End-DBShift