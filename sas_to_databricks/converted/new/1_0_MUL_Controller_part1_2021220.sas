import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import time

runtype = 'Weekly'
fndncurr = 'K1390'
fndnminus1 = 'J8626'
fndnminus2 = 'J6093'
acxiomdate = '09JUN2022'
acxiomrefreshfilezipped = 'Jun2022'
email_feed_end_dt = '15NOV2018'

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

DATA_DIR1 = '/vg01/aarp_sas/ftp/incoming/mdsm'
Y = 'Yes'
N = 'No'
val1 = 'Data Present'
val2 = 'Blank/Null'

%run "/Workspace/aarp_sas/aarp_projects/twalters/Data_Processing/Creds"

def setup():
    global rename1, rename2, INTERMEDIATE_DIR, FILE, FILE2, MULDATE, MULDATE2, MonthsSent, create_dt, year, enddt
    global twobegdt, onebegdt, drvs_maxdt, MULDATEF, MULDATEF3, MULDATEFdt, MULDATEF3dt, gunzip, gzip, pm_muldate
    global monthly_mul, epsilon_datasets_ed, p_acxdate, p_custdate, thurdate, prev_fridate
    global bl_file

    sysdate9 = date.today()

    def get_sas_intnx_week_mon_start(ref_date, num_weeks):
        first_day_of_ref_week = ref_date - timedelta(days=ref_date.weekday())
        return first_day_of_ref_week + timedelta(weeks=num_weeks)

    def get_nth_weekday_in_month(year, month, weekday_to_find, n):
        first_day_of_month = date(year, month, 1)
        first_day_weekday = first_day_of_month.weekday()
        days_to_add = (weekday_to_find - first_day_weekday + 7) % 7
        first_occurrence = first_day_of_month + timedelta(days=days_to_add)
        return first_occurrence + timedelta(weeks=n - 1)

    if runtype == 'Monthly':
        INTERMEDIATE_DIR = '/vg04/twalters/Monthly/'
        
        first_day_current_month = sysdate9.replace(day=1)
        monthly_mul_calc = get_sas_intnx_week_mon_start(first_day_current_month, 3)

        first_day_prev_month = (sysdate9.replace(day=1) - timedelta(days=1)).replace(day=1)
        p_monthly_mul_calc = get_sas_intnx_week_mon_start(first_day_prev_month, 3)

        if monthly_mul_calc.month == 1:
            p_monthly_mul_calc = get_nth_weekday_in_month(sysdate9.year - 1, 12, 0, 3)

        if sysdate9 < monthly_mul_calc:
            monthly_mul_calc = get_sas_intnx_week_mon_start(first_day_prev_month, 3)
            first_day_two_months_ago = (first_day_prev_month - timedelta(days=1)).replace(day=1)
            p_monthly_mul_calc = get_sas_intnx_week_mon_start(first_day_two_months_ago, 3)

        monthly_mul = monthly_mul_calc
        p_monthly_mul = p_monthly_mul_calc
        
        end_of_month_for_mul = monthly_mul + relativedelta(day=31)
        year2me = end_of_month_for_mul - relativedelta(years=2)

        p_fri1 = f"n_MUL_{(monthly_mul - timedelta(days=3)).strftime('%y%m%d')}.TXT"
        p_sat1 = f"n_MUL_{(monthly_mul - timedelta(days=2)).strftime('%y%m%d')}.TXT"

        gunzip = f"gunzip /vg01/aarp_sas/ftp/incoming/mdsm/{p_fri1}.gz"
        gzip = f"gzip /vg01/aarp_sas/ftp/incoming/mdsm/{p_fri1}"
        
        rename1 = f"mv /vg01/aarp_sas/aarp_data/model_epsilon_{p_monthly_mul.strftime('%d%b%Y').lower()}.sas7bdat /vg01/aarp_sas/aarp_data/model_epsilon_{monthly_mul.strftime('%d%b%Y').lower()}.sas7bdat"
        rename2 = f"mv /vg01/aarp_sas/aarp_data/epsilon_demograph_{p_monthly_mul.strftime('%d%b%Y').lower()}.sas7bdat /vg01/aarp_sas/aarp_data/epsilon_demograph_{monthly_mul.strftime('%d%b%Y').lower()}.sas7bdat"
        
        FILE = p_fri1
        MULDATE = monthly_mul.strftime('%d%b%Y').upper()
        MULDATE2 = monthly_mul.strftime('%d%b%Y').upper()
        pm_muldate = p_monthly_mul.strftime('%d%b%Y').upper()
        
        monday_of_current_week = sysdate9 - timedelta(days=sysdate9.weekday())
        MonthsSent = monday_of_current_week.month
        
        create_dt = monthly_mul.strftime('%y%m%d')
        year = str(monthly_mul.year)
        
        if date(2021, 1, 1) <= monthly_mul <= date(2021, 2, 27):
            year = str(monthly_mul.year - 1)
            
        enddt = monthly_mul.strftime('%d%b%Y').upper()
        twobegdt = (monthly_mul - relativedelta(years=2)).strftime('%d%b%Y').upper()
        onebegdt = (monthly_mul - relativedelta(years=1)).strftime('%d%b%Y').upper()
        drvs_maxdt = year2me.strftime('%d%b%Y').upper()
        
        MULDATEF = f"'{monthly_mul.strftime('%m/%d/%Y')}'"
        MULDATEF3 = f"'{(monthly_mul - relativedelta(months=3)).strftime('%m/%d/%Y')}'"
        
        MULDATEFdt = int(time.mktime(monthly_mul.timetuple()))
        MULDATEF3dt = int(time.mktime((monthly_mul - relativedelta(months=3)).timetuple()))
        
        MULDATE4mo = (monthly_mul - timedelta(weeks=17)).strftime('%d%b%Y').lower()
        
        first_day_12m_ago = (sysdate9.replace(day=1) - relativedelta(months=12))
        MULDATE13mo = get_sas_intnx_week_mon_start(first_day_12m_ago, 3).strftime('%d%b%Y').upper()

        %run "/Workspace/aarp_sas/aarp_projects/twalters/Data_Processing/ReadLibnames"

        dbutils.fs.sh(rename1.replace("/vg01/aarp_sas/", "/dbfs/mnt/aarp_sas/"))
        dbutils.fs.sh(rename2.replace("/vg01/aarp_sas/", "/dbfs/mnt/aarp_sas/"))
        
        bl_file_path = f"{DATA_DIR1}/{FILE}.gz"
        bl_file = bl_file_path.replace('/vg01/aarp_sas/', 'dbfs:/mnt/aarp_sas/')

        %run "/Workspace/aarp_sas/aarp_projects/twalters/Data_Processing/Process_Acxiom_Demo_20201222"

    if runtype == 'Weekly':
        INTERMEDIATE_DIR = '/vg04/twalters/Weekly/'
        
        %run "/Workspace/aarp_sas/aarp_projects/twalters/Data_Processing/Weekly_Process/GetRecentFileNames"
        
        mondate = sysdate9 - timedelta(days=sysdate9.weekday())

        # SAS weekday(): Sun=1,..,Fri=6,Sat=7. Python weekday(): Mon=0,..,Fri=4,Sat=5,Sun=6
        if sysdate9.weekday() in [4, 5]: # Friday or Saturday
            sunday_of_current_week = sysdate9 - timedelta(days=sysdate9.isoweekday() % 7)
            thurdate_calc = sunday_of_current_week + timedelta(days=4)
        else:
            sunday_of_current_week = sysdate9 - timedelta(days=sysdate9.isoweekday() % 7)
            thurdate_calc = sunday_of_current_week - timedelta(days=4)

        prev_fridate_calc = thurdate_calc - timedelta(days=6)
        
        monthly_mul_calc = get_nth_weekday_in_month(mondate.year, mondate.month, 0, 3)

        if monthly_mul_calc >= mondate:
            if mondate.month == 1:
                monthly_mul_calc = get_nth_weekday_in_month(mondate.year - 1, 12, 0, 3)
            else:
                monthly_mul_calc = get_nth_weekday_in_month(mondate.year, mondate.month - 1, 0, 3)
        
        monthly_mul = monthly_mul_calc

        end_of_month_for_mon = mondate + relativedelta(day=31)
        year2me = end_of_month_for_mon - relativedelta(years=2)
        
        file1 = f"acxiom_mul_weekly_{p_acxdate}.TXT"
        file2 = f"aarp_cust_weekly_{p_custdate}.TXT"

        FILE = file1
        FILE2 = file2
        MULDATE = mondate.strftime('%d%b%Y').upper()
        MULDATE2 = monthly_mul.strftime('%d%b%Y').upper()
        MonthsSent = mondate.month
        create_dt = mondate.strftime('%y%m%d')
        year = str(mondate.year)

        if date(2021, 1, 1) <= mondate <= date(2021, 2, 27):
            year = str(mondate.year - 1)
            
        enddt = mondate.strftime('%d%b%Y').upper()
        twobegdt = (mondate - relativedelta(years=2)).strftime('%d%b%Y').upper()
        onebegdt = (mondate - relativedelta(years=1)).strftime('%d%b%Y').upper()
        drvs_maxdt = year2me.strftime('%d%b%Y').upper()

        MULDATEF = f"'{monthly_mul.strftime('%m/%d/%Y')}'"
        MULDATEF3 = f"'{(monthly_mul - relativedelta(months=3)).strftime('%m/%d/%Y')}'"
        
        MULDATEFdt = int(time.mktime(monthly_mul.timetuple()))
        MULDATEF3dt = int(time.mktime((monthly_mul - relativedelta(months=3)).timetuple()))
        
        thurdate = thurdate_calc.strftime('%d%b%Y').upper()
        prev_fridate = prev_fridate_calc.strftime('%d%b%Y').upper()
        
        %run "/Workspace/aarp_sas/aarp_projects/twalters/Data_Processing/ReadLibnames"
        
        bl_file_path = f"{DATA_DIR1}/{FILE}"
        bl_file = bl_file_path.replace('/vg01/aarp_sas/', 'dbfs:/mnt/aarp_sas/')

setup()

%run "/Workspace/aarp_sas/aarp_projects/twalters/Data_Processing/ReadMikeDirectory_20161228"
%run "/Workspace/aarp_sas/aarp_projects/twalters/Data_Processing/ReadJaredFile"
%run "/Workspace/aarp_sas/aarp_projects/twalters/Data_Processing/ReadEpsilonDirectory_20180305"
%run "/Workspace/aarp_sas/aarp_projects/twalters/Data_Processing/ReadDemoFile"

%run "/Workspace/aarp_sas/aarp_formats/epsilon_formats"
%run "/Workspace/twalters/Formats_Current"

def MISSZERO(df):
    numeric_cols = [c for c, t in df.dtypes if t in ('int', 'double', 'float', 'bigint', 'smallint', 'tinyint', 'decimal')]
    return df.na.fill(0, subset=numeric_cols)

%run "/Workspace/aarp_sas/aarp_projects/twalters/Data_Processing/ReadLibnames"

# %run "/Workspace/aarp_sas/aarp_projects/twalters/Data_Processing/MUL_Extract_20210617"
# %run "/Workspace/aarp_sas/aarp_projects/twalters/Data_Processing/MUL_Extract_20210728"
# %run "/Workspace/aarp_sas/aarp_projects/twalters/Data_Processing/new_rpm_scoring_20181101"
# %run "/Workspace/aarp_sas/aarp_projects/twalters/Data_Processing/Masters_2012_Scoring_Code_20180925"
#End-DBShift