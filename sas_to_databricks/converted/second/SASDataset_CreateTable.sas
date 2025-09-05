import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SASDatasets_Translation").getOrCreate()

# --- Placeholder Variables ---
# These variables would be set by a calling script or configuration in a real environment.
jdbc_url = "jdbc:teradata://your_teradata_host/DATABASE=your_db"
connection_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.teradata.jdbc.TeraDriver"
}
L6_id = "some_l6_id"  # Example value
MinDt_SAS = "2022-01-01" # SAS date format, used for Spark SQL DELETE
MinDT = "2022-01-01"      # Teradata date format
MaxDT = "2022-12-31"      # Teradata date format

# --- Dynamic Variable Creation from Database Metadata ---

# Create holiday_dv
holiday_dv_query = """
(SELECT TRIM(columnname) as columnname
FROM dbc.columns
WHERE tablename = 'HolidayCalendar'
AND columnname NOT IN ('DAY_DT', 'JulyFourWKND','JulyFive','JulyTwo','JulyOne', 'FatTues')
AND databasename = 'DL_CNTL_ADVA'
ORDER BY columnid) as holiday_columns
"""
df_holiday_dv = spark.read.jdbc(url=jdbc_url, dbtable=holiday_dv_query, properties=connection_properties)
holiday_dv_list = [row.columnname for row in df_holiday_dv.collect()]
holiday_dv = ",".join(holiday_dv_list)
print(f"holiday_dv: {holiday_dv}")

# Create dates_dv
dates_dv_query = """
(SELECT TRIM(columnname) as columnname
FROM dbc.columns
WHERE tablename = 'dates'
AND columnname <> 'DAY_DT'
AND databasename = 'DL_CNTL_ADVA'
ORDER BY columnid) as dates_columns
"""
df_dates_dv = spark.read.jdbc(url=jdbc_url, dbtable=dates_dv_query, properties=connection_properties)
dates_dv_list = [row.columnname for row in df_dates_dv.collect()]
dates_dv = ",".join(dates_dv_list)
print(f"dates_dv: {dates_dv}")


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
    l2_info_query = f"""
    (sel distinct mjr_p_cls_id, L2
    from dl_cntl_adva.fcst_p_upc_inf
    where mjr_mds_are_id = '{L6}'
    order by L2 desc) as l2_info
    """
    df_l2_info = spark.read.jdbc(url=jdbc_url, dbtable=l2_info_query, properties=connection_properties)
    l2_records = df_l2_info.collect()

    nL2 = len(l2_records)

    for j, row in enumerate(l2_records):
        L2_id = row['mjr_p_cls_id']
        string2 = row['L2']
        
        print(f"Loop iteration: {j+1}")
        print(f"Processing L2 table: {string2}")

        cnt_query = f"(select count(*) as cnt from dl_cntl_adva.MJR_P_CLS_HL_P_CNT where mjr_p_cls_id = '{L2_id}') as item_count"
        cnt_df = spark.read.jdbc(url=jdbc_url, dbtable=cnt_query, properties=connection_properties)
        cnt = cnt_df.first()['cnt']
        print(f"Count for {L2_id}: {cnt}")

        exist = 0
        final_table_name = f"promo.pid_{string2}"
        if spark.catalog.tableExists(final_table_name):
            if 'hl_cap' in spark.table(final_table_name).columns:
                exist = 1
        print(f"Column 'hl_cap' exists in {final_table_name}: {exist}")
        
        temp_table_name = f"promo.pid_{string2}_temp"

        extra_cols_str = ""
        extra_join_str = ""

        if cnt > 0 or exist > 0:
            extra_cols_list = [
                ",coalesce(HM_LOC_CAP_QT,0) as hl_cap",
                ",coalesce(ENDCAP_CAP_QT,0) as end_cap"
            ]
            extra_cols_str = "\n".join(extra_cols_list)
            
            extra_join_str = f"""
                 left join 
                 ( select a.* 
                    from  vprod_mds_rplnm.dly_ut_fcst_hm_loc_cap_hst as a
                    where day_dt between date '{MinDt_TD}' and date '{MaxDt_TD}'
                 ) as hl
                    on a.day_dt = hl.day_dt 
                    and a.ut_id = hl.ut_id 
                    and a.p_id = hl.p_id
            """
        
        # Construct the main SQL query for Teradata
        passthrough_sql_query = f"""
        (select {model_var} 
             ,cast(coalesce(mpk_hook_flg,0) as smallint) as  mpk_hook_flg
             ,cast(coalesce(mcc_hook_flg,0) as smallint) as  mcc_hook_flg
             ,cast(coalesce(bucks_hook_flg,0) as smallint) as  bucks_hook_flg
             ,cast(coalesce(other_hook_flg,0) as smallint) as  other_hook_flg
             ,cast(coalesce(mpk.mPk_cpn_flg,0) as smallint) as  mPk_cpn_flg
             {extra_cols_str}
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
                        
                        where day_dt between date '{MinDt_TD}' and date '{MaxDt_TD}'
                         
                        ) as a

                join 
                ( select * from dl_cntl_adva.dates
                    where day_dt between date '{MinDt_TD}' and date '{MaxDt_TD}'
                         
                ) as d 
                    on a.day_dt  = d.day_dt 

                join
                ( select * from dl_cntl_adva.HolidayCalendar
                    where day_dt between date '{MinDt_TD}' and date '{MaxDt_TD}'
                         
                ) as hd 
                    on a.day_dt  = hd.day_dt 

                left join dl_cntl_adva.fcst_hook_flag2 as hf
                    on a.day_dt = hf.day_dt 
                    and a.ut_id = hf.ut_id

                LEFT JOIN dl_cntl_adva.new_mpk_flg AS mpk
                    ON a.p_id = mpk.p_id
                    AND a.day_dt = mpk.day_dt
                
                {extra_join_str}
        ) as main_query
        """

        df_temp = spark.read.jdbc(url=jdbc_url, dbtable=passthrough_sql_query, properties=connection_properties)
        df_temp.write.format("delta").mode("overwrite").saveAsTable(temp_table_name)
        
        # Check if temp table was created and has data
        if spark.catalog.tableExists(temp_table_name):
            df_to_process = spark.table(temp_table_name)
            
            # If the final table already exists, delete recent records and append
            if spark.catalog.tableExists(final_table_name):
                spark.sql(f"DELETE FROM {final_table_name} WHERE day_dt >= '{min_dt}'")
                df_to_process.write.format("delta").mode("append").saveAsTable(final_table_name)
            # If the final table does not exist, create it from the temp table
            else:
                df_to_process.write.format("delta").mode("overwrite").saveAsTable(final_table_name)

            # Drop the temporary table
            spark.sql(f"DROP TABLE IF EXISTS {temp_table_name}")
        else:
            print(f"Warning: Temporary table {temp_table_name} was not created or is empty. Skipping insert/create.")

# Call the translated function
pids_model_sales(L6=L6_id, min_dt=MinDt_SAS, MinDt_TD=MinDT, MaxDt_TD=MaxDT)

#End-DBShift