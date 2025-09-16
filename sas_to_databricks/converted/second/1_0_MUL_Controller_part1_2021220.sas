import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import datetime, date, timedelta
import calendar

# This is a helper function to get dbutils in a Databricks environment.
# In a real Databricks notebook, dbutils is already available.
def get_dbutils():
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark.sparkContext)
    except ImportError:
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]

dbutils = get_dbutils()

# Conversion of SAS %LET statements to Python variables
runtype = 'Weekly'  # or 'Monthly'
fndncurr = 'K1390'
fndnminus1 = 'J8626'
fndnminus2 = 'J6093'
acxiomdate = '09JUN2022'
acxiomrefreshfilezipped = 'Jun2022'
email_feed_end_dt = '15NOV2018'

# Conversion of SAS %GLOBAL statements
rename1 = None
rename2 = None
INTERMEDIATE_DIR = None
FILE = None
FILE2 = None
MULDATE = None
MULDATE2 = None
MonthsSent = None
create_dt = None
year = None
enddt = None
twobegdt = None
onebegdt = None
drvs_maxdt = None
MULDATEF = None
MULDATEF3 = None
MULDATEFdt = None
MULDATEF3dt = None
gunzip = None
gzip = None
pm_muldate = None
monthly_mul = None
epsilon_datasets_ed = None
p_acxdate = None
p_custdate = None
thurdate = None
prev_fridate = None

# SAS file paths
DATA_DIR1 = "/vg01/aarp_sas/ftp/incoming/mdsm"
Y = "Yes"
N = "No"
val1 = "Data Present"
val2 = "Blank/Null"

# Include external configuration
# The creds file likely contains sensitive information and connection details.
# In a Databricks context, this would be managed via secrets and configurations.
%run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/Creds"

def setup():
    global rename1, rename2, INTERMEDIATE_DIR, FILE, FILE2, MULDATE, MULDATE2, MonthsSent, create_dt, year, enddt, twobegdt, onebegdt, drvs_maxdt, MULDATEF, MULDATEF3, MULDATEFdt, MULDATEF3dt, gunzip, gzip, pm_muldate, monthly_mul, epsilon_datasets_ed, p_acxdate, p_custdate, thurdate, prev_fridate, bl_file

    today = date.today()

    def get_nth_weekday_of_month(n, weekday_iso, target_date):
        first_day_of_month = target_date.replace(day=1)
        # weekday_iso: Monday is 0, Sunday is 6
        days_to_weekday = (weekday_iso - first_day_of_month.weekday() + 7) % 7
        first_weekday = first_day_of_month + timedelta(days=days_to_weekday)
        return first_weekday + timedelta(weeks=n - 1)
        
    def get_nth_sas_weekday_of_month(n, weekday_sas, year, month):
        # SAS weekday: 1=Sun, 2=Mon, ..., 7=Sat
        # Python weekday: 0=Mon, ..., 6=Sun
        py_weekday = (weekday_sas - 2 + 7) % 7
        target_dt = date(year, month, 1)
        return get_nth_weekday_of_month(n, py_weekday, target_dt)

    if runtype == 'Monthly':
        INTERMEDIATE_DIR = "/vg04/twalters/Monthly/"
        
        monthly_mul_calc = get_nth_weekday_of_month(3, 0, today) # 3rd Monday of current month
        
        last_month_date = (today.replace(day=1) - timedelta(days=1))
        p_monthly_mul_calc = get_nth_weekday_of_month(3, 0, last_month_date) # 3rd Monday of last month
        
        # SAS logic: if month(monthly_mul)=1 then p_monthly_mul=nwkdom(3,1,12,year("&SYSDATE9"d)-1)+1;
        # This is already handled by the relative date calculation for p_monthly_mul_calc
        
        if today < monthly_mul_calc:
            two_months_ago_date = (last_month_date.replace(day=1) - timedelta(days=1))
            monthly_mul_date = p_monthly_mul_calc
            p_monthly_mul_date = get_nth_weekday_of_month(3, 0, two_months_ago_date)
        else:
            monthly_mul_date = monthly_mul_calc
            p_monthly_mul_date = p_monthly_mul_calc

        last_day_of_mul_month = calendar.monthrange(monthly_mul_date.year, monthly_mul_date.month)[1]
        eom_date = date(monthly_mul_date.year, monthly_mul_date.month, last_day_of_mul_month)
        year2me = eom_date.replace(year=eom_date.year - 2)

        p_fri1 = "n_MUL_" + monthly_mul_date.strftime('%y%m%d') + ".TXT"
        p_sat1 = "n_MUL_" + (monthly_mul_date - timedelta(days=1)).strftime('%y%m%d') + ".TXT"

        gunzip = f"gunzip {DATA_DIR1}/{p_fri1}.gz"
        gzip = f"gzip {DATA_DIR1}/{p_fri1}"
        
        rename1_src = f"/vg01/aarp_sas/aarp_data/model_epsilon_{p_monthly_mul_date.strftime('%d%b%Y').lower()}.sas7bdat"
        rename1_dest = f"/vg01/aarp_sas/aarp_data/model_epsilon_{monthly_mul_date.strftime('%d%b%Y').lower()}.sas7bdat"
        rename1 = f"mv {rename1_src} {rename1_dest}"
        
        rename2_src = f"/vg01/aarp_sas/aarp_data/epsilon_demograph_{p_monthly_mul_date.strftime('%d%b%Y').lower()}.sas7bdat"
        rename2_dest = f"/vg01/aarp_sas/aarp_data/epsilon_demograph_{monthly_mul_date.strftime('%d%b%Y').lower()}.sas7bdat"
        rename2 = f"mv {rename2_src} {rename2_dest}"
        
        rename1 = rename1
        rename2 = rename2
        FILE = p_fri1
        MULDATE = monthly_mul_date.strftime('%d%b%Y').upper()
        MULDATE2 = monthly_mul_date.strftime('%d%b%Y').upper()
        pm_muldate = p_monthly_mul_date.strftime('%d%b%Y').upper()
        MonthsSent = (today + timedelta(days=7)).month 
        create_dt = monthly_mul_date.strftime('%y%m%d')
        year = str(monthly_mul_date.year)
        
        if date(2021, 1, 1) <= monthly_mul_date <= date(2021, 2, 27):
            year = str(monthly_mul_date.year - 1)
        
        enddt = monthly_mul_date.strftime('%d%b%Y').upper()
        twobegdt = monthly_mul_date.replace(year=monthly_mul_date.year - 2).strftime('%d%b%Y').upper()
        onebegdt = monthly_mul_date.replace(year=monthly_mul_date.year - 1).strftime('%d%b%Y').upper()
        drvs_maxdt = year2me.strftime('%d%b%Y').upper()
        monthly_mul = monthly_mul_date.strftime('%s') # Represent as epoch seconds
        
        MULDATEF = "'" + monthly_mul_date.strftime('%m/%d/%Y') + "'"
        mul_minus_3_months_month = (monthly_mul_date.month - 3 - 1) % 12 + 1
        mul_minus_3_months_year = monthly_mul_date.year + (monthly_mul_date.month - 3 - 1) // 12
        MULDATEF3 = "'" + monthly_mul_date.replace(month=mul_minus_3_months_month, year=mul_minus_3_months_year).strftime('%m/%d/%Y') + "'"
        
        MULDATEFdt = str(int(datetime.combine(monthly_mul_date, datetime.min.time()).timestamp()))
        mul_minus_3_date = monthly_mul_date.replace(month=mul_minus_3_months_month, year=mul_minus_3_months_year)
        MULDATEF3dt = str(int(datetime.combine(mul_minus_3_date, datetime.min.time()).timestamp()))

        bl_file = f"{DATA_DIR1}/{FILE}.gz"

        %run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/ReadLibnames"
        
        dbutils.fs.mv(f"dbfs:{rename1_src}", f"dbfs:{rename1_dest}")
        dbutils.fs.mv(f"dbfs:{rename2_src}", f"dbfs:{rename2_dest}")

        %run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/Process_Acxiom_Demo_20201222"

    if runtype == 'Weekly':
        INTERMEDIATE_DIR = "/vg04/twalters/Weekly/"
        %run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/Weekly_Process/GetRecentFileNames"
        
        mondate = today - timedelta(days=today.weekday())
        
        weekd = today.weekday() + 1 # Monday=1, ..., Sunday=7
        if weekd >= 6: # SAS Friday or Saturday
            thurdate_calc = today - timedelta(days=today.weekday() - 3) # Get Thursday of current week
        else:
            thurdate_calc = today - timedelta(days=today.weekday() + 4) # Get Thursday of last week
        prev_fridate_calc = thurdate_calc - timedelta(days=6)

        monthly_mul_calc = get_nth_weekday_of_month(3, 0, mondate) # 3rd Monday
        
        if monthly_mul_calc >= mondate:
            if mondate.month == 1:
                monthly_mul_date = get_nth_sas_weekday_of_month(3, 2, mondate.year - 1, 12)
            else:
                last_month = mondate.replace(day=1) - timedelta(days=1)
                monthly_mul_date = get_nth_weekday_of_month(3, 0, last_month)
        else:
            monthly_mul_date = monthly_mul_calc
            
        last_day_of_mon_month = calendar.monthrange(mondate.year, mondate.month)[1]
        eom_date = date(mondate.year, mondate.month, last_day_of_mon_month)
        year2me = eom_date.replace(year=eom_date.year - 2)

        file1 = f"acxiom_mul_weekly_{p_acxdate.strftime('%y%m%d')}.TXT"
        file2 = f"aarp_cust_weekly_{p_custdate.strftime('%y%m%d')}.TXT"

        FILE = file1
        FILE2 = file2
        MULDATE = mondate.strftime('%d%b%Y').upper()
        MULDATE2 = monthly_mul_date.strftime('%d%b%Y').upper()
        MonthsSent = mondate.month
        create_dt = mondate.strftime('%y%m%d')
        year = str(mondate.year)
        
        if date(2021, 1, 1) <= mondate <= date(2021, 2, 27):
            year = str(mondate.year - 1)
        
        enddt = mondate.strftime('%d%b%Y').upper()
        twobegdt = mondate.replace(year=mondate.year - 2).strftime('%d%b%Y').upper()
        onebegdt = mondate.replace(year=mondate.year - 1).strftime('%d%b%Y').upper()
        drvs_maxdt = year2me.strftime('%d%b%Y').upper()
        
        MULDATEF = "'" + monthly_mul_date.strftime('%m/%d/%Y') + "'"
        mul_minus_3_months_month = (monthly_mul_date.month - 3 - 1) % 12 + 1
        mul_minus_3_months_year = monthly_mul_date.year + (monthly_mul_date.month - 3 - 1) // 12
        MULDATEF3 = "'" + monthly_mul_date.replace(month=mul_minus_3_months_month, year=mul_minus_3_months_year).strftime('%m/%d/%Y') + "'"
        
        MULDATEFdt = str(int(datetime.combine(monthly_mul_date, datetime.min.time()).timestamp()))
        mul_minus_3_date = monthly_mul_date.replace(month=mul_minus_3_months_month, year=mul_minus_3_months_year)
        MULDATEF3dt = str(int(datetime.combine(mul_minus_3_date, datetime.min.time()).timestamp()))
        
        thurdate = thurdate_calc.strftime('%d%b%Y').upper()
        prev_fridate = prev_fridate_calc.strftime('%d%b%Y').upper()
        
        %run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/ReadLibnames"
        
        bl_file = f"{DATA_DIR1}/{FILE}"
        
def miss_zero(df):
    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, (IntegerType, DoubleType, FloatType, LongType, ShortType, ByteType, DecimalType))]
    return df.na.fill(0, subset=numeric_cols)

# Execute the setup logic
setup()

print(f"User: {dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}")

%run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/ReadMikeDirectory_20161228"
%run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/ReadJaredFile"
%run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/ReadEpsilonDirectory_20180305"
%run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/ReadDemoFile"

%run "/vg02/aarp_sas/aarp_formats/epsilon_formats"
%run "/vg04/twalters/Formats_Current"

%run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/ReadLibnames"

# The following programs are called to perform the main ETL and scoring logic.
# These would be other Databricks notebooks.
# %run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/MUL_Extract_20210728"
# %run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/new_rpm_scoring_20181101"
# %run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/Masters_2012_Scoring_Code_20180925"
#End-DBShift