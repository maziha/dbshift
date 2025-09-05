import pyspark.sql.functions as F
from pyspark.sql.types import *

# This is a placeholder for the connection properties.
# In a real Databricks environment, these should be fetched from Databricks Secrets.
jdbc_url = "jdbc:teradata://your_server/database"
jdbc_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.teradata.jdbc.TeraDriver"
}

# --- Assume these variables are passed from a configuration or a calling notebook ---
# These would be the values for the final call to the main function.
L6_id = "'some_L6_id'" # Example value, should be set appropriately
MinDt_SAS = "2022-01-01" # Example value
MinDT = "'2022-01-01'" # Example value
MaxDT = "'2023-01-01'" # Example value
# ------------------------------------------------------------------------------------


# Execute query to get holiday_dv
sql_holiday_dv = """
select trim(columnname) as col
from dbc.columns
where  tablename = 'HolidayCalendar'
and columnname not in ('DAY_DT', 'JulyFourWKND','JulyFive','JulyTwo','JulyOne', 'FatTues')
and databasename = 'DL_CNTL_ADVA'
order by columnid
"""
df_holiday_dv = spark.read.format("jdbc").options(**jdbc_properties).option("dbtable", f"({sql_holiday_dv}) as subq").load()
holiday_dv_list = [row.col for row in df_holiday_dv.collect()]
holiday_dv = ",".join(holiday_dv_list)
print(holiday_dv)

# Execute query to get dates_dv
sql_dates_dv = """
select trim(columnname) as col
from dbc.columns
where tablename = 'dates'
and columnname <> 'DAY_DT'
and databasename = 'DL_CNTL_ADVA'
order by columnid
"""
df_dates_dv = spark.read.format("jdbc").options(**jdbc_properties).option("dbtable", f"({sql_dates_dv}) as subq").load()
dates_dv_list = [row.col for row in df_dates_dv.collect()]
dates_dv = ",".join(dates_dv_list)
print(dates_dv)

model_var = """
		a.ut_id, a.p_id, a.day_dt, a.mjr_p_cls_id, p_sold_first_dt, p_sold_last_dt,
		p_sell_dur, mkt_bskt_ut_qt, p_bse_pr_am, p_promo_pr_am, atl_p_sold_am, ntl_log_mkt_bskt_ut_qt, ntl_log_bse_pr_am,
		ntl_log_bse_dct_pr_am, ntl_log_bse_promo_dct_pr_am, 
		cast(adv_circ_flg as smallint) as  adv_circ_flg,
		cast(adv_mid_wk_flg as smallint) as  adv_mid_wk_flg,
		cast(adv_super_evnt_flg as smallint) as  adv_super_evnt_flg,
		cast(adv_dgtl_circ_flg as smallint) as  adv_dgtl_circ_flg,
		cast(unadv_flg as smallint) as  unadv_flg,
		cast(pr_drop_flg as smallint) as  pr_drop_flg,
		cast(p_ten_for_ten_flg as smallint) as  p_ten_for_ten_flg,
		ut_same_sto_flg, atl_min_temp_val,
		atl_mean_temp_val, atl_max_temp_val, atl_t_ppt_qt, atl_t_snow_qt,
		day_bef_ppt_temp_val, day_bef_snow_temp_val, day_aft_ppt_temp_val,
		day_aft_snow_temp_val
"""

model_var2 = """
		a.ut_id, a.p_id, a.day_dt, a.mjr_p_cls_id, 
		coalesce(a.p_sold_first_dt,c.p_sold_first_dt) as p_sold_first_dt , 
		coalesce(a.p_sold_last_dt,c.p_sold_last_dt) as p_sold_last_dt , 
		p_sell_dur, mkt_bskt_ut_qt, p_bse_pr_am, p_promo_pr_am, atl_p_sold_am, ntl_log_mkt_bskt_ut_qt, ntl_log_bse_pr_am,
		ntl_log_bse_dct_pr_am, ntl_log_bse_promo_dct_pr_am, 
		cast(adv_circ_flg as smallint) as  adv_circ_flg,
		cast(adv_mid_wk_flg as smallint) as  adv_mid_wk_flg,
		cast(adv_super_evnt_flg as smallint) as  adv_super_evnt_flg,
		cast(adv_dgtl_circ_flg as smallint) as  adv_dgtl_circ_flg,
		cast(unadv_flg as smallint) as  unadv_flg,
		cast(pr_drop_flg as smallint) as  pr_drop_flg,
		cast(p_ten_for_ten_flg as smallint) as  p_ten_for_ten_flg,
		ut_same_sto_flg, atl_min_temp_val,
		atl_mean_temp_val, atl_max_temp_val, atl_t_ppt_qt, atl_t_snow_qt,
		day_bef_ppt_temp_val, day_bef_snow_temp_val, day_aft_ppt_temp_val,
		day_aft_snow_temp_val
"""

def pids_model_sales(L6, min_dt, MinDt_TD, MaxDt_TD):
    
    sql_l2_info = f"""
    select distinct mjr_p_cls_id, L2
    from dl_cntl_adva.fcst_p_upc_inf
    where mjr_mds_are_id = {L6}
    order by L2 desc
    """
    df_l2_info = spark.read.format("jdbc").options(**jdbc_properties).option("dbtable", f"({sql_l2_info}) as subq").load()
    l2_list = df_l2_info.collect()

    for i, row in enumerate(l2_list):
        L2_id = row["mjr_p_cls_id"]
        string2 = row["L2"]
        
        print(i + 1)
        print(string2)
        
        # In PySpark, we don't redirect logs to a file this way. Logging would be configured globally.
        # This line is intentionally omitted as it's an environment-specific command.
        
        sql_cnt = f"""
        select count(*) as cnt from dl_cntl_adva.MJR_P_CLS_HL_P_CNT where mjr_p_cls_id = '{L2_id}'
        """
        df_cnt = spark.read.format("jdbc").options(**jdbc_properties).option("dbtable", f"({sql_cnt}) as subq").load()
        cnt = df_cnt.collect()[0]['cnt']
        print(cnt)

        target_table_name = f"promo.pid_{string2}"
        table_exists = spark.catalog.tableExists(target_table_name)
        
        column_hl_cap_exists = False
        if table_exists:
            try:
                if 'hl_cap' in spark.table(target_table_name).columns:
                    column_hl_cap_exists = True
            except Exception:
                column_hl_cap_exists = False
        
        exist = 1 if column_hl_cap_exists else 0
        print(exist)
        
        # Dynamically build the SQL query
        select_additions = ""
        join_additions = ""

        add_shelf_space_columns = (exist > 0) or (cnt > 0 and not table_exists)
        add_shelf_space_join = (cnt > 0) or (exist > 0)

        if add_shelf_space_columns:
            select_additions = ",coalesce(HM_LOC_CAP_QT,0) as hl_cap ,coalesce(ENDCAP_CAP_QT,0) as end_cap"

        if add_shelf_space_join:
            join_additions = f"""
                left join 
                ( select a.* 
                    from  vprod_mds_rplnm.dly_ut_fcst_hm_loc_cap_hst as a
                    where day_dt between date {MinDt_TD} and date {MaxDt_TD}
                ) as hl
                    on a.day_dt = hl.day_dt 
                    and a.ut_id = hl.ut_id 
                    and a.p_id = hl.p_id
            """

        main_sql = f"""
            select {model_var} 
             ,cast(coalesce(mpk_hook_flg,0) as smallint) as  mpk_hook_flg
             ,cast(coalesce(mcc_hook_flg,0) as smallint) as  mcc_hook_flg
             ,cast(coalesce(bucks_hook_flg,0) as smallint) as  bucks_hook_flg
             ,cast(coalesce(other_hook_flg,0) as smallint) as  other_hook_flg
             ,cast(coalesce(mpk.mPk_cpn_flg,0) as smallint) as  mPk_cpn_flg
             {select_additions}
             , {dates_dv} , {holiday_dv}
            from 
            (select {model_var2}
                from VPROD_MDS_RPLNM.DLY_UT_P_ANLT_SL_HST   as a 
                join 
                    (select p_id 
                    from dl_cntl_adva.fcst_p_upc_inf 
                    where mjr_p_cls_id = '{L2_id}'
                    group by 1) as b 
                    on a.p_id = b.p_id 
                left join dl_cntl_adva.p_sold_first_dt as c 
                    on a.p_id = c.p_id 
                    and a.ut_id = c.ut_id
                where day_dt between date {MinDt_TD} and date {MaxDt_TD}
            ) as a
            join 
            ( select * from dl_cntl_adva.dates
                where day_dt between date {MinDt_TD} and date {MaxDt_TD}
            ) as d 
                on a.day_dt  = d.day_dt 
            join
            ( select * from dl_cntl_adva.HolidayCalendar
                where day_dt between date {MinDt_TD} and date {MaxDt_TD}
            ) as hd 
                on a.day_dt  = hd.day_dt 
            left join dl_cntl_adva.fcst_hook_flag2 as hf
                on a.day_dt = hf.day_dt 
                and a.ut_id = hf.ut_id
            LEFT JOIN dl_cntl_adva.new_mpk_flg AS mpk
                ON a.p_id = mpk.p_id
                AND a.day_dt = mpk.day_dt
            {join_additions}
        """
        
        try:
            df_pid_temp = spark.read.format("jdbc").options(**jdbc_properties).option("dbtable", f"({main_sql}) as subq").load()
        except Exception as e:
            print(f"Could not read data for {string2}. Error: {e}")
            df_pid_temp = None

        is_temp_empty = False
        if df_pid_temp is None:
            is_temp_empty = True
        else:
            if not df_pid_temp.head(1):
                is_temp_empty = True
        
        if not is_temp_empty:
            if table_exists:
                delete_sql = f"DELETE FROM {target_table_name} WHERE day_dt >= '{min_dt}'"
                spark.sql(delete_sql)
            
            # This handles both insert into existing and creation of a new table.
            df_pid_temp.write.format("delta").mode("append").saveAsTable(target_table_name)
        
        # There is no concept of dropping a temp table like in SAS,
        # the DataFrame df_pid_temp will be garbage collected.

# Call the main function with the predefined parameters
pids_model_sales(L6=L6_id, min_dt=MinDt_SAS, MinDt_TD=MinDT, MaxDt_TD=MaxDT)

#End-DBShift