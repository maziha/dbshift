import pyspark.sql.functions as F
import datetime

get_ipython().run_line_magic('run', '"/Workspace/SASProd/SASProjects/salesforecast/SASProgram/FinalSASPrograms/Autoexec_test"')

PRODTEST = "PROD"
FCST_V1 = "/mnt/mjrdata/SASProd/SASProjects/salesforecast/1_DemandForecast_Production/SASProgram"
fcst_log = "/mnt/Sdrive/ShopperMarketing/Analytics/Staff Folders/Aldijana Lelak Avdic/Corp Ad Hoc/SalesForecast/SASLog"
fcst_coef = "/mnt/mjrdata/SASProd/SASProjects/salesforecast/1_DemandForecast_Production/ModelCoef"
pred_promo = "/mnt/mjrdata/SASProd/SASProjects/salesforecast/1_DemandForecast_Production/ModelPredPromo"
pred_base = "/mnt/mjrdata/SASProd/SASProjects/salesforecast/1_DemandForecast_Production/ModelPredBase"
fcst = "/mnt/Sdrive/ShopperMarketing/Analytics/Staff Folders/Aldijana Lelak Avdic/Corp Ad Hoc/SalesForecast/ModelPredBase"
fcst_promo = "/mnt/Sdrive/ShopperMarketing/Analytics/Staff Folders/Aldijana Lelak Avdic/Corp Ad Hoc/SalesForecast/ModelPredPromo"
eval_path = "/mnt/mjrdata/SASProd/SASProjects/salesforecast/1_DemandForecast_Production/Eval"
archive = "/mnt/Sdrive/ShopperMarketing/Analytics/Staff Folders/Aldijana Lelak Avdic/Corp Ad Hoc/SalesForecast/ADM/Archive/Model Coeff"

get_ipython().run_line_magic('run', f'"{FCST_V1}/FolderSetup_SAS2"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/DTStamp"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/GetFileList"')
dtstamp(set='Y')

Forecast = 'N'
Update = 'Y'
Train = 'N'
Score = 'Y'

td_driver = "com.teradata.jdbc.TeraDriver"
td_url = "jdbc:teradata://your_teradata_host/DATABASE=your_db"
td_user = "your_username"
td_password = "your_password"

sql_dateinf = """
(SELECT
    MIN(day_dt) AS startDate,
    MAX(day_dt) AS endDate,
    COUNT(DISTINCT wk_end_dt) AS nweeks
FROM vprod_dim.dt_inf
WHERE day_dt BETWEEN CAST('2015-01-01' AS DATE) AND CAST('2022-03-19' AS DATE)
) as dateinf_query
"""
df_dateinf = spark.read \
    .format("jdbc") \
    .option("driver", td_driver) \
    .option("url", td_url) \
    .option("dbtable", sql_dateinf) \
    .option("user", td_user) \
    .option("password", td_password) \
    .load()

dateinf_row = df_dateinf.first()
FirstPreWeek = dateinf_row["startDate"]
LastPreWeek = dateinf_row["endDate"]
nweeks = dateinf_row["nweeks"]

sql_fcst_update_dtinf = "(SELECT MAX(startDate) as max_start, MAX(endDate) as max_end FROM adva.fcst_update_dtinf) as fcst_update_query"
df_fcst_update_dtinf = spark.read \
    .format("jdbc") \
    .option("driver", td_driver) \
    .option("url", td_url) \
    .option("dbtable", sql_fcst_update_dtinf) \
    .option("user", td_user) \
    .option("password", td_password) \
    .load()

fcst_update_row = df_fcst_update_dtinf.select(
    F.concat(F.lit("'"), F.date_format(F.col("max_start"), "yyyy-MM-dd"), F.lit("'")).alias("st_dt"),
    F.concat(F.lit("'"), F.date_format(F.col("max_end"), "yyyy-MM-dd"), F.lit("'")).alias("end_dt"),
    F.concat(F.lit("'"), F.date_format(F.col("max_end"), "yyyyMMdd"), F.lit("'")).alias("end_dt2")
).first()

MinDt = fcst_update_row["st_dt"]
MaxDt = fcst_update_row["end_dt"]
max_tr_dt = fcst_update_row["end_dt2"]

print(LastPreWeek)
print(MinDt)
print(MaxDt)
print(max_tr_dt)
mo_dt = datetime.date(2022, 3, 31)
print(mo_dt)
ValidWeeks = 0
ValidWks = 0
ForecastWeeks = 10

insert_df = spark.createDataFrame(
    [('Update', 'ALL', 'InProgress')],
    ['PROCESS', 'HIERARCHY', 'STATUS']
).withColumn("DAY_DT", F.current_date())

insert_df.write \
    .format("jdbc") \
    .option("driver", td_driver) \
    .option("url", td_url) \
    .option("dbtable", "DL_CNTL_ADVA.MODELPROCESS") \
    .option("user", td_user) \
    .option("password", td_password) \
    .mode("append") \
    .save()

print("Log output would be directed to: \\\\us.meijer.com\\mjrdata\\Sdrive\\ShopperMarketing\\Analytics\\Staff Folders\\Aldijana Lelak Avdic\\Corp Ad Hoc\\SalesForecast\\SASLog\\Update\\ModelUpdate.txt")

L6_id = "'L6-000005'"
Part = 1
dir_val = 'asc'
get_ipython().run_line_magic('run', f'"{FCST_V1}/Poisson"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/BkwrdReg_SPD"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Reg_PW"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegScale"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RALSO_PL"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Poisson_ZIP"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegPromo"')
dtstamp(f"End Poisson1 {L6_id}")

L6_id = "'L6-000005'"
Part = 2
dir_val = 'desc'
get_ipython().run_line_magic('run', f'"{FCST_V1}/Poisson"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RALSO_PL"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RandomForest"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Poisson_ZIP"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegAct"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/ElasticN"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RForestW"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegScale"')
dtstamp(f"End Poisson1 {L6_id}")

L6_id = "'L6-000005'"
Part = 1
dir_val = 'asc'
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegMPerk"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RForestW"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/ElasticN"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegSP"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/StepReg_SPD"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Reg_PL"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/StepRegR"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RALSO_RG"')
dtstamp(f"End Poisson1 {L6_id}")

L6_id = "'L6-000001'"
Part = 1
dir_val = 'asc'
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegScale"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RALSO_RG"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RForestW"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RALSO_PL"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/ElasticN"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Reg_PL"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RandomForest"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Poisson"')
dtstamp(f"End Poisson1 {L6_id}")

L6_id = "'L6-000001'"
Part = 2
dir_val = 'desc'
get_ipython().run_line_magic('run', f'"{FCST_V1}/StepReg_SPD"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RALSO_PL"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RForestW"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/BkwrdReg_SPD"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegAct"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Poisson_ZIP"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Poisson"')
dtstamp(f"End Poisson1 {L6_id}")

L6_id = "'L6-000001'"
Part = 1
dir_val = 'desc'
get_ipython().run_line_magic('run', f'"{FCST_V1}/Reg_PDL"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Poisson_ZIP"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/ElasticN"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegScale"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegPromo"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegSPD"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegMPerk"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Reg_PL"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegSPD"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegMPerk"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Reg_PW"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegSP"')
dtstamp(f"End Poisson1 {L6_id}")

L6_id = "'L6-000013'"
Part = 1
dir_val = 'asc'
get_ipython().run_line_magic('run', f'"{FCST_V1}/Reg_PSW"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegSPD"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegMPerk"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegScale"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/StepRegR"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Reg_PW"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegAct"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RALSO_PL"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Reg_PL"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/StepReg_SPD"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/ElasticN"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Poisson"')
dtstamp(f"End Poisson1 {L6_id}")

L6_id = "'L6-000013'"
Part = 2
dir_val = 'desc'
get_ipython().run_line_magic('run', f'"{FCST_V1}/RForestW"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegPDRg"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RALSO_PL"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Poisson_ZIP"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Poisson"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/ElasticN"')
dtstamp(f"End Poisson1 {L6_id}")

L6_id = "'L6-000013'"
Part = 1
dir_val = 'asc'
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegPromo"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegSP"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Reg_PDL"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Poisson_ZIP"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/BkwrdReg_SPD"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RForestW"')
dtstamp(f"End Poisson1 {L6_id}")

L6_id = "'L6-000014'"
Part = 1
dir_val = 'asc'
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegSub"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegSP"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/BkwrdReg_SPD"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Reg_PDL"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Poisson"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Poisson_ZIP"')
dtstamp(f"End Poisson1 {L6_id}")

L6_id = "'L6-000014'"
Part = 2
dir_val = 'asc'
get_ipython().run_line_magic('run', f'"{FCST_V1}/RALSO_PL"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegPromo"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RegSPD"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Reg_PW"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/ElasticN"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RForestW"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RandomForest"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RForestW"')
dtstamp(f"End Poisson1 {L6_id}")

L6_id = "'L6-000014'"
Part = 2
dir_val = 'asc'
get_ipython().run_line_magic('run', f'"{FCST_V1}/Poisson"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Poisson_ZIP"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/PSW_ACT"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/Reg_PSW"')
get_ipython().run_line_magic('run', f'"{FCST_V1}/RandomForest"')
dtstamp(f"End Poisson1 {L6_id}")

tdate = datetime.date.today().strftime("%d-%m-%Y")
print(tdate)
email_to = "Sarah.George@meijer.com"
email_cc = "Sarah.George@meijer.com"
email_from = "Sarah.George@meijer.com"
email_subject = "Model Update"
email_body_html = f"""
<p>Hello,</p>
<p>Model Updates finished running. Run Model Track to determine progress.</p>
<p>Have a Great Day!!!</p>
<p>{tdate}</p>
"""
print("--- SENDING EMAIL ---")
print(f"TO: {email_to}")
print(f"CC: {email_cc}")
print(f"FROM: {email_from}")
print(f"SUBJECT: {email_subject}")
print("BODY (HTML):")
print(email_body_html)
print("---------------------")

#End-DBShift