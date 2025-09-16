import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

# This script assumes the following variables are pre-defined in the Databricks environment
# L6_id = "some_value"
# MinDt_SAS = "YYYY-MM-DD"
# MaxDt_SAS = "YYYY-MM-DD"
# jdbc_url = "jdbc:teradata://..."
# td_user = "your_user"
# td_password = "your_password"


def wkly_data(L6, min_dt, max_dt):
    """
    Creates SAS dataset at pid,week, store level for all active products 
    including promo, holiday, and shelf space information.
    """
    
    # Connect to Teradata to get L2 information
    # SAS: proc sql; &tdcon.; select distinct ... into ... from connection to teradata (...)
    teradata_query = f"""
    (SELECT * FROM dl_cntl_adva.fcst_p_upc_inf WHERE mjr_mds_are_id = {L6}) as fcst_p_upc_inf_subset
    """
    
    df_from_teradata = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", teradata_query) \
        .option("user", td_user) \
        .option("password", td_password) \
        .load()

    df_l2_info = df_from_teradata.select("mjr_p_cls_id", "L2").distinct().orderBy(F.col("L2").asc())

    # Collect the results to loop over, simulating SAS macro variables
    l2_results = df_l2_info.collect()
    L2_ids = [row.mjr_p_cls_id for row in l2_results]
    L2_table = [row.L2 for row in l2_results]
    nL2 = len(l2_results)

    # SAS: %do j=1 %to &nL2.;
    for j in range(nL2):
        L2_id = L2_ids[j]
        string2 = L2_table[j]
        
        print(f"{j+1}")
        print(f"{string2}")
        
        # Define table names for the current loop iteration
        promo_table_name = f"promo.pid_{string2}"
        week_table_name = f"week.pid_{string2}"
        promo_ts_table_name = f"promo_ts.pid_{string2}"

        # SAS: proc sql; select count(*) into : exist from dictionary.columns ...
        exist = 0
        if spark.catalog.tableExists(promo_table_name):
            if 'hl_cap' in spark.table(promo_table_name).columns:
                exist = 1
        print(f"{exist}")

        table_exists_in_week = spark.catalog.tableExists(week_table_name)
        
        exist_wk = 0
        if table_exists_in_week:
            if 'hl_cap' in spark.table(week_table_name).columns:
                exist_wk = 1
        print(f"{exist_wk}")
        
        final_df_to_write = None

        if not spark.catalog.tableExists(promo_table_name):
            print(f"Source table {promo_table_name} does not exist. Skipping iteration.")
            continue

        # Prepare for the main aggregation query
        spark.table(promo_table_name).createOrReplaceTempView(f"v_promo_pid_{string2}")
        
        # Dynamically build parts of the SQL query based on column existence
        hl_cap_select_clause = ""
        if exist > 0 and exist_wk > 0:
            hl_cap_select_clause = ", avg(hl_cap) as hl_cap, avg(end_cap) as end_cap"
        elif exist <= 0 and exist_wk > 0:
            hl_cap_select_clause = ", 0 as hl_cap, 0 as end_cap"
        
        # This large SQL query is the direct translation of the main SELECT statement in SAS
        sql_aggregation_query = f"""
        SELECT
            UT_ID,
            P_ID,
            WK_END_DT,
            min(MJR_P_CLS_ID) as MJR_P_CLS_ID,
            min(P_SOLD_FIRST_DT) as P_SOLD_FIRST_DT,
            max(P_SOLD_LAST_DT) as P_SOLD_LAST_DT,
            max(P_SELL_DUR) as P_SELL_DUR,
            sum(MKT_BSKT_UT_QT) as MKT_BSKT_UT_QT,
            avg(P_BSE_PR_AM) as P_BSE_PR_AM,
            CASE WHEN sum(mkt_bskt_ut_qt) > 0 THEN sum(P_PROMO_PR_AM*mkt_bskt_ut_qt) / sum(mkt_bskt_ut_qt) ELSE avg(p_promo_pr_am) END as P_PROMO_PR_AM,
            CASE WHEN sum(MKT_BSKT_UT_QT) > 0 THEN sum(ATL_P_SOLD_AM*mkt_bskt_ut_qt) / sum(mkt_bskt_ut_qt) ELSE avg(ATL_P_SOLD_AM) END as ATL_P_SOLD_AM,
            CASE WHEN sum(mkt_bskt_ut_qt) < 0 THEN LN(1) ELSE LN(sum(MKT_BSKT_UT_QT)+1) END as NTL_LOG_MKT_BSKT_UT_QT,
            LN(avg(P_BSE_PR_AM)) as NTL_LOG_BSE_PR_AM,
            CASE WHEN sum(MKT_BSKT_UT_QT) > 0 THEN LN(sum(ATL_P_SOLD_AM*mkt_bskt_ut_qt) / sum(mkt_bskt_ut_qt)) ELSE LN(avg(ATL_P_SOLD_AM)) END as NTL_LOG_BSE_DCT_PR_AM,
            CASE WHEN sum(MKT_BSKT_UT_QT) > 0 THEN LN(sum(ATL_P_SOLD_AM*mkt_bskt_ut_qt) / sum(mkt_bskt_ut_qt)) - LN(avg(P_BSE_PR_AM)) ELSE LN(avg(ATL_P_SOLD_AM)) - LN(avg(P_BSE_PR_AM)) END as NTL_LOG_BSE_PROMO_DCT_PR_AM,
            CAST(max(adv_circ_flg) AS INT) as adv_circ_flg,
            CAST(max(adv_mid_wk_flg) AS INT) as adv_mid_wk_flg,
            CAST(max(adv_super_evnt_flg) AS INT) as adv_super_evnt_flg,
            CAST(max(adv_dgtl_circ_flg) AS INT) as adv_dgtl_circ_flg,
            CAST(max(unadv_flg) AS INT) as unadv_flg,
            CAST(max(pr_drop_flg) AS INT) as pr_drop_flg,
            CAST(max(p_ten_for_ten_flg) AS INT) as p_ten_for_ten_flg,
            avg(ATL_MIN_TEMP_VAL) as ATL_MIN_TEMP_VAL,
            avg(ATL_MEAN_TEMP_VAL) as ATL_MEAN_TEMP_VAL,
            avg(ATL_MAX_TEMP_VAL) as ATL_MAX_TEMP_VAL,
            avg(ATL_T_PPT_QT) as ATL_T_PPT_QT,
            avg(ATL_T_SNOW_QT) as ATL_T_SNOW_QT,
            avg(DAY_BEF_PPT_TEMP_VAL) as DAY_BEF_PPT_TEMP_VAL,
            avg(DAY_BEF_SNOW_TEMP_VAL) as DAY_BEF_SNOW_TEMP_VAL,
            avg(DAY_AFT_PPT_TEMP_VAL) as DAY_AFT_PPT_TEMP_VAL,
            avg(DAY_AFT_SNOW_TEMP_VAL) as DAY_AFT_SNOW_TEMP_VAL,
            CAST(max(mpk_hook_flg) AS INT) as mpk_hook_flg,
            CAST(max(mcc_hook_flg) AS INT) as mcc_hook_flg,
            CAST(max(bucks_hook_flg) AS INT) as bucks_hook_flg,
            CAST(max(other_hook_flg) AS INT) as other_hook_flg,
            CAST(max(mPk_cpn_flg) AS INT) as mPk_cpn_flg
            {hl_cap_select_clause},
            max(FCL_PER_OF_YR_ID) as FCL_PER_OF_YR_ID,
            max(FCL_WK_OF_YR_ID) as FCL_WK_OF_YR_ID,
            max(FCL_YR_ID) as FCL_YR_ID,
            CAST(max(july4thFri) AS INT) as july4thFri,
            CAST(max(july4thSat) AS INT) as july4thSat,
            CAST(max(wk1) AS INT) as wk1, CAST(max(wk2) AS INT) as wk2, CAST(max(wk3) AS INT) as wk3, CAST(max(wk4) AS INT) as wk4, CAST(max(wk5) AS INT) as wk5,
            CAST(max(wk6) AS INT) as wk6, CAST(max(wk7) AS INT) as wk7, CAST(max(wk8) AS INT) as wk8, CAST(max(wk9) AS INT) as wk9, CAST(max(wk10) AS INT) as wk10,
            CAST(max(wk11) AS INT) as wk11, CAST(max(wk12) AS INT) as wk12, CAST(max(wk13) AS INT) as wk13, CAST(max(wk14) AS INT) as wk14, CAST(max(wk15) AS INT) as wk15,
            CAST(max(wk16) AS INT) as wk16, CAST(max(wk17) AS INT) as wk17, CAST(max(wk18) AS INT) as wk18, CAST(max(wk19) AS INT) as wk19, CAST(max(wk20) AS INT) as wk20,
            CAST(max(wk21) AS INT) as wk21, CAST(max(wk22) AS INT) as wk22, CAST(max(wk23) AS INT) as wk23, CAST(max(wk24) AS INT) as wk24, CAST(max(wk25) AS INT) as wk25,
            CAST(max(wk26) AS INT) as wk26, CAST(max(wk27) AS INT) as wk27, CAST(max(wk28) AS INT) as wk28, CAST(max(wk29) AS INT) as wk29, CAST(max(wk30) AS INT) as wk30,
            CAST(max(wk31) AS INT) as wk31, CAST(max(wk32) AS INT) as wk32, CAST(max(wk33) AS INT) as wk33, CAST(max(wk34) AS INT) as wk34, CAST(max(wk35) AS INT) as wk35,
            CAST(max(wk36) AS INT) as wk36, CAST(max(wk37) AS INT) as wk37, CAST(max(wk38) AS INT) as wk38, CAST(max(wk39) AS INT) as wk39, CAST(max(wk40) AS INT) as wk40,
            CAST(max(wk41) AS INT) as wk41, CAST(max(wk42) AS INT) as wk42, CAST(max(wk43) AS INT) as wk43, CAST(max(wk44) AS INT) as wk44, CAST(max(wk45) AS INT) as wk45,
            CAST(max(wk46) AS INT) as wk46, CAST(max(wk47) AS INT) as wk47, CAST(max(wk48) AS INT) as wk48, CAST(max(wk49) AS INT) as wk49, CAST(max(wk50) AS INT) as wk50,
            CAST(max(wk51) AS INT) as wk51, CAST(max(wk52) AS INT) as wk52,
            CAST(max(SunPreXmas) AS INT) as SunPreXmas,
            CAST(max(RedSat) AS INT) as RedSat,
            CAST(max(BlackFri) AS INT) as BlackFri,
            CAST(max(Turkey) AS INT) as Turkey,
            CAST(max(SaturdayB4Turkey) AS INT) as SaturdayB4Turkey,
            CAST(max(FridayB4Turkey) AS INT) as FridayB4Turkey,
            CAST(max(Veterans) AS INT) as Veterans,
            CAST(max(HalloweenFri) AS INT) as HalloweenFri,
            CAST(max(Halloween) AS INT) as Halloween,
            CAST(max(HalloweenEve) AS INT) as HalloweenEve,
            CAST(max(PreHalo_Sun) AS INT) as PreHalo_Sun,
            CAST(max(PreHalo_Fri) AS INT) as PreHalo_Fri,
            CAST(max(Columbus) AS INT) as Columbus,
            CAST(max(LaborFri) AS INT) as LaborFri,
            CAST(max(LaborSun) AS INT) as LaborSun,
            CAST(max(LaborSat) AS INT) as LaborSat,
            CAST(max(LaborWkEnd) AS INT) as LaborWkEnd,
            CAST(max(Labor) AS INT) as Labor,
            CAST(max(Dad) AS INT) as Dad,
            CAST(max(DadSat) AS INT) as DadSat,
            CAST(max(MemWkend) AS INT) as MemWkend,
            CAST(max(Mom) AS INT) as Mom,
            CAST(max(MomSat) AS INT) as MomSat,
            CAST(max(EasterWk) AS INT) as EasterWk,
            CAST(max(Easter) AS INT) as Easter,
            CAST(max(EasterSat) AS INT) as EasterSat,
            CAST(max(PresDay) AS INT) as PresDay,
            CAST(max(ValentineWknd) AS INT) as ValentineWknd,
            CAST(max(Valentine) AS INT) as Valentine,
            CAST(max(DayB4Valentine) AS INT) as DayB4Valentine,
            CAST(max(SuperBowlSat) AS INT) as SuperBowlSat,
            CAST(max(SuperBowl) AS INT) as SuperBowl,
            CAST(max(MLK) AS INT) as MLK,
            CAST(max(NY) AS INT) as NY,
            CAST(max(JulyFour) AS INT) as JulyFour,
            CAST(max(HalloweenWkEnd) AS INT) as HalloweenWkEnd,
            CAST(max(DecTwoThree) AS INT) as DecTwoThree,
            CAST(max(XMASEVE) AS INT) as XMASEVE,
            CAST(max(SATPNY) AS INT) as SATPNY,
            CAST(max(SUNPNY) AS INT) as SUNPNY,
            CAST(max(Memorial) AS INT) as Memorial,
            CAST(max(PreHalo_Sat) AS INT) as PreHalo_Sat,
            CAST(max(TurkeyWed) AS INT) as TurkeyWed,
            CAST(max(DecTwoSix) AS INT) as DecTwoSix,
            CAST(max(MCC_BLK_OUT) AS INT) as MCC_BLK_OUT,
            CAST(max(NYEVE) AS INT) as NYEVE
        FROM v_promo_pid_{string2}
        WHERE day_dt BETWEEN to_date('{min_dt}', 'yyyy-MM-dd') AND to_date('{max_dt}', 'yyyy-MM-dd')
        GROUP BY 1, 2, 3
        """
        
        df_aggregated_data = spark.sql(sql_aggregation_query)

        if table_exists_in_week:
            # SAS: delete from week.pid_&string2 where wk_end_dt ge "&min_dt."d;
            # This is equivalent to filtering the existing table to keep only older data
            df_existing_data = spark.table(week_table_name)
            df_data_to_keep = df_existing_data.filter(F.col("wk_end_dt") < F.to_date(F.lit(min_dt), 'yyyy-MM-dd'))
            
            # SAS: insert into week.pid_&string2 ...
            # This is equivalent to unioning the old data with the new aggregated data
            final_df_to_write = df_data_to_keep.unionByName(df_aggregated_data)
        else:
            # SAS: create table week.pid_&string2 as ...
            final_df_to_write = df_aggregated_data
        
        # SAS: proc sort ... data = week.pid_&string2.; by p_id ut_id wk_end_dt;
        # Write the final result to the 'week' database, sorted and overwriting
        final_df_to_write.orderBy("p_id", "ut_id", "wk_end_dt") \
            .write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(week_table_name)
            
        # SAS: proc sql; drop table promo_ts.pid_&string2.;
        spark.sql(f"DROP TABLE IF EXISTS {promo_ts_table_name}")
        
        # SAS: proc copy in=week out=promo_ts; select pid_&string2.;
        df_copied = spark.table(week_table_name)
        df_copied.write.format("delta").mode("overwrite").saveAsTable(promo_ts_target_table)

# Execute the main function with predefined variables
# wkly_data(L6=L6_id, min_dt=MinDt_SAS, max_dt=MaxDt_SAS)

#End-DBShift