import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SASDatasets_Wkly_Conversion").getOrCreate()

# Placeholder variables for parameters that would be passed to the script
L6_id = "'L6-000001'"
MinDt_SAS = '2021-05-02'
MaxDt_SAS = '2021-05-29'
MinDT = "'2021-05-02'"
MaxDT = "'2021-05-29'"

# Placeholder for Teradata connection properties
# These should be configured securely, e.g., using Databricks secrets
jdbc_url = "jdbc:teradata://your_teradata_host/database"
jdbc_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.teradata.jdbc.TeraDriver"
}

def rgnl_data(L6, min_dt, max_dt, MinDt_TD, MaxDt_TD):

    # This assumes a 'tdcon' like connection is established via JDBC properties
    
    # First PROC SQL to get L2 information
    df_l2_info = spark.sql(f"""
        SELECT
            mjr_p_cls_id,
            L2
        FROM mtest.fcst_p_upc_inf
        WHERE mjr_mds_are_id = {L6}
        ORDER BY L2 asc
    """)
    
    l2_info_collected = df_l2_info.collect()
    if not l2_info_collected:
        print("No L2 information found for the given L6_id. Exiting.")
        return

    L2_ids = "|".join([row.mjr_p_cls_id for row in l2_info_collected])
    L2_table = "|".join([row.L2 for row in l2_info_collected])
    nL2 = len(l2_info_collected)

    # Drop mtest.rgnl_ut_cnt
    spark.sql("DROP TABLE IF EXISTS mtest.rgnl_ut_cnt")

    # Create mtest.rgnl_ut_cnt from Teradata
    sql_query_rgnl_ut_cnt = """
    select day_dt, ut_st_ad, count(distinct(ut_id)) as ut_cnt
    from 
    vprod.ut_inf as a 
    join 
    vprod_dim.dt_inf as b 
    on day_dt ge ut_opn_dt 
    and day_dt le coalesce(ut_cls_dt, current_date+180)
    and day_dt between '20150101' and current_date+180
    and ut_ct = 'MS'
    group by 1,2
    """
    df_rgnl_ut_cnt = spark.read \
        .format("jdbc") \
        .options(**jdbc_properties) \
        .option("dbtable", f"({sql_query_rgnl_ut_cnt}) as subq") \
        .load()

    df_rgnl_ut_cnt.write.format("delta").mode("overwrite").saveAsTable("mtest.rgnl_ut_cnt")

    # Drop mtest.scale_ten4ten_hst_l2
    spark.sql("DROP TABLE IF EXISTS mtest.scale_ten4ten_hst_l2")

    # Create mtest.scale_ten4ten_hst_l2 from Teradata
    sql_query_scale_ten4ten = f"select * from dl_cntl_adva.scale_ten4ten_hst2 where day_dt between date {MinDt_TD} and date {MaxDt_TD}"
    
    df_scale_ten4ten_hst_l2 = spark.read \
        .format("jdbc") \
        .options(**jdbc_properties) \
        .option("dbtable", f"({sql_query_scale_ten4ten}) as subq") \
        .load()

    df_scale_ten4ten_hst_l2.write.format("delta").mode("overwrite").saveAsTable("mtest.scale_ten4ten_hst_l2")
    
    # PROC SQL; create index ...; is not directly applicable in Databricks/Delta Lake.
    # Z-Ordering is a common optimization, but we will omit the index creation steps.

    # Start of the main loop
    l2_ids_list = L2_ids.split("|")
    l2_table_list = L2_table.split("|")

    for j in range(nL2):
        L2_id = l2_ids_list[j]
        string2 = l2_table_list[j]
        
        print(j + 1)
        print(string2)
        
        # PROC PRINTTO is for SAS logging, not translated.

        # Check if column 'hl_cap' exists in the promo table
        promo_table_name = f"promo.pid_{string2}"
        exist = 0
        if spark.catalog.tableExists(promo_table_name):
            promo_cols = spark.table(promo_table_name).columns
            if 'hl_cap' in promo_cols:
                exist = 1

        # Create local temporary table scale_ten4ten_hst2
        df_scale_ten4ten_hst_l2_full = spark.table("mtest.scale_ten4ten_hst_l2")
        df_scale_ten4ten_hst2 = df_scale_ten4ten_hst_l2_full.filter(F.col("mjr_p_cls_id") == L2_id)
        df_scale_ten4ten_hst2.createOrReplaceTempView("scale_ten4ten_hst2")

        region_table_name = f"region.pid_{string2}"
        region_table_exists = spark.catalog.tableExists(region_table_name)
        promo_table_exists = spark.catalog.tableExists(promo_table_name)

        # Conditional DELETE based on existence of promo table
        if promo_table_exists and region_table_exists:
            spark.sql(f"DELETE FROM {region_table_name} WHERE day_dt >= '{min_dt}'")
        
        # Build the main SQL query
        hl_cap_sql = ""
        if exist > 0:
            hl_cap_sql = """
                ,avg(hl_cap) as hl_cap
                ,avg(end_cap) as end_cap
            """
            
        main_query = f"""
            SELECT
                r.UT_ST_Ad,
                a.P_ID,
                a.DAY_DT,
                min(a.MJR_P_CLS_ID) as MJR_P_CLS_ID,
                min(P_SOLD_FIRST_DT) as P_SOLD_FIRST_DT,
                max(P_SOLD_LAST_DT) as P_SOLD_LAST_DT,
                max(P_SELL_DUR) as P_SELL_DUR,
                sum(MKT_BSKT_UT_QT) as MKT_BSKT_UT_QT,
                avg(P_BSE_PR_AM) as P_BSE_PR_AM,
                case when sum(mkt_bskt_ut_qt) > 0 then sum(P_PROMO_PR_AM*mkt_bskt_ut_qt) / sum(mkt_bskt_ut_qt)
                     else avg(p_promo_pr_am) end as P_PROMO_PR_AM,
                case when sum(MKT_BSKT_UT_QT) > 0 then sum(ATL_P_SOLD_AM*mkt_bskt_ut_qt) / sum(mkt_bskt_ut_qt)
                     else avg(ATL_P_SOLD_AM) end as ATL_P_SOLD_AM,
                case when sum(mkt_bskt_ut_qt) < 0 then LN(1)
                     else LN(sum(MKT_BSKT_UT_QT)+1) end as NTL_LOG_MKT_BSKT_UT_QT,
                LN(avg(P_BSE_PR_AM)) as NTL_LOG_BSE_PR_AM,
                case when sum(MKT_BSKT_UT_QT) > 0 then LN(sum(ATL_P_SOLD_AM*mkt_bskt_ut_qt) / sum(mkt_bskt_ut_qt))
                     else LN(avg(ATL_P_SOLD_AM)) end as NTL_LOG_BSE_DCT_PR_AM,
                case when sum(MKT_BSKT_UT_QT) > 0 then LN(sum(ATL_P_SOLD_AM*mkt_bskt_ut_qt) / sum(mkt_bskt_ut_qt)) - LN(avg(P_BSE_PR_AM))
                     else LN(avg(ATL_P_SOLD_AM)) - LN(avg(P_BSE_PR_AM)) end as NTL_LOG_BSE_PROMO_DCT_PR_AM,
                max(adv_circ_flg) as adv_circ_flg,
                max(adv_mid_wk_flg) as adv_mid_wk_flg,
                max(adv_super_evnt_flg) as adv_super_evnt_flg,
                max(adv_dgtl_circ_flg) as adv_dgtl_circ_flg,
                max(unadv_flg) as unadv_flg,
                max(pr_drop_flg) as pr_drop_flg,
                max(p_ten_for_ten_flg) as p_ten_for_ten_flg,
                max(coalesce(Scale_Flg,0)) as Scale_Flg,
                max(coalesce(Ten4Ten_Flg,0)) as Ten4Ten_Flg,
                max(coalesce(featured_pid,0)) as featured_pid,
                sum(adv_circ_flg) as adv_circ_cnt,
                sum(adv_mid_wk_flg) as adv_mid_wk_cnt,
                sum(adv_super_evnt_flg) as adv_super_evnt_cnt,
                sum(adv_dgtl_circ_flg) as adv_dgtl_circ_cnt,
                sum(unadv_flg) as unadv_cnt,
                sum(pr_drop_flg) as pr_drop_cnt,
                sum(p_ten_for_ten_flg) as p_ten_for_ten_cnt,
                sum(coalesce(Scale_Flg,0)) as Scale_cnt,
                sum(coalesce(Ten4Ten_Flg,0)) as Ten4Ten_cnt,
                sum(coalesce(featured_pid,0)) as featured_cnt,
                avg(ATL_MIN_TEMP_VAL) as ATL_MIN_TEMP_VAL,
                avg(ATL_MEAN_TEMP_VAL) as ATL_MEAN_TEMP_VAL,
                avg(ATL_MAX_TEMP_VAL) as ATL_MAX_TEMP_VAL,
                avg(ATL_T_PPT_QT) as ATL_T_PPT_QT,
                avg(ATL_T_SNOW_QT) as ATL_T_SNOW_QT,
                avg(DAY_BEF_PPT_TEMP_VAL) as DAY_BEF_PPT_TEMP_VAL,
                avg(DAY_BEF_SNOW_TEMP_VAL) as DAY_BEF_SNOW_TEMP_VAL,
                avg(DAY_AFT_PPT_TEMP_VAL) as DAY_AFT_PPT_TEMP_VAL,
                avg(DAY_AFT_SNOW_TEMP_VAL) as DAY_AFT_SNOW_TEMP_VAL,
                max(mpk_hook_flg) as mpk_hook_flg,
                max(mcc_hook_flg) as mcc_hook_flg,
                max(bucks_hook_flg) as bucks_hook_flg,
                max(other_hook_flg) as other_hook_flg,
                max(mPk_cpn_flg) as mPk_cpn_flg,
                sum(mpk_hook_flg) as mpk_hook_cnt,
                sum(mcc_hook_flg) as mcc_hook_cnt,
                sum(bucks_hook_flg) as bucks_hook_cnt,
                sum(other_hook_flg) as other_hook_cnt,
                sum(mPk_cpn_flg) as mPk_cpn_cnt
                {hl_cap_sql},
                max(FCL_PER_OF_YR_ID) as FCL_PER_OF_YR_ID,
                max(FCL_WK_OF_YR_ID) as FCL_WK_OF_YR_ID,
                max(FCL_YR_ID) as FCL_YR_ID,
                max(july4thFri) as july4thFri,
                max(july4thSat) as july4thSat,
                max(wk1) as wk1,
                max(wk2) as wk2,
                max(wk3) as wk3,
                max(wk4) as wk4,
                max(wk5) as wk5,
                max(wk6) as wk6,
                max(wk7) as wk7,
                max(wk8) as wk8,
                max(wk9) as wk9,
                max(wk10) as wk10,
                max(wk11) as wk11,
                max(wk12) as wk12,
                max(wk13) as wk13,
                max(wk14) as wk14,
                max(wk15) as wk15,
                max(wk16) as wk16,
                max(wk17) as wk17,
                max(wk18) as wk18,
                max(wk19) as wk19,
                max(wk20) as wk20,
                max(wk21) as wk21,
                max(wk22) as wk22,
                max(wk23) as wk23,
                max(wk24) as wk24,
                max(wk25) as wk25,
                max(wk26) as wk26,
                max(wk27) as wk27,
                max(wk28) as wk28,
                max(wk29) as wk29,
                max(wk30) as wk30,
                max(wk31) as wk31,
                max(wk32) as wk32,
                max(wk33) as wk33,
                max(wk34) as wk34,
                max(wk35) as wk35,
                max(wk36) as wk36,
                max(wk37) as wk37,
                max(wk38) as wk38,
                max(wk39) as wk39,
                max(wk40) as wk40,
                max(wk41) as wk41,
                max(wk42) as wk42,
                max(wk43) as wk43,
                max(wk44) as wk44,
                max(wk45) as wk45,
                max(wk46) as wk46,
                max(wk47) as wk47,
                max(wk48) as wk48,
                max(wk49) as wk49,
                max(wk50) as wk50,
                max(wk51) as wk51,
                max(wk52) as wk52,
                max(SunPreXmas) as SunPreXmas,
                max(RedSat) as RedSat,
                max(BlackFri) as BlackFri,
                max(Turkey) as Turkey,
                max(SaturdayB4Turkey) as SaturdayB4Turkey,
                max(FridayB4Turkey) as FridayB4Turkey,
                max(Veterans) as Veterans,
                max(HalloweenFri) as HalloweenFri,
                max(Halloween) as Halloween,
                max(HalloweenEve) as HalloweenEve,
                max(PreHalo_Sun) as PreHalo_Sun,
                max(PreHalo_Fri) as PreHalo_Fri,
                max(Columbus) as Columbus,
                max(LaborFri) as LaborFri,
                max(LaborSun) as LaborSun,
                max(LaborSat) as LaborSat,
                max(LaborWkEnd) as LaborWkEnd,
                max(Labor) as Labor,
                max(Dad) as Dad,
                max(DadSat) as DadSat,
                max(MemWkend) as MemWkend,
                max(Mom) as Mom,
                max(MomSat) as MomSat,
                max(EasterWk) as EasterWk,
                max(Easter) as Easter,
                max(EasterSat) as EasterSat,
                max(PresDay) as PresDay,
                max(ValentineWknd) as ValentineWknd,
                max(Valentine) as Valentine,
                max(DayB4Valentine) as DayB4Valentine,
                max(SuperBowlSat) as SuperBowlSat,
                max(SuperBowl) as SuperBowl,
                max(MLK) as MLK,
                max(NY) as NY,
                max(JulyFour) as JulyFour,
                max(HalloweenWkEnd) as HalloweenWkEnd,
                max(DecTwoThree) as DecTwoThree,
                max(XMASEVE) as XMASEVE,
                max(SATPNY) as SATPNY,
                max(SUNPNY) as SUNPNY,
                max(Memorial) as Memorial,
                max(PreHalo_Sat) as PreHalo_Sat,
                max(TurkeyWed) as TurkeyWed,
                max(DecTwoSix) as DecTwoSix,
                max(MCC_BLK_OUT) as MCC_BLK_OUT,
                max(NYEVE) as NYEVE,
                max(Ash_Wednesday) as Ash_Wed,
                max(Lent_Week1) as Lent_Wk1,
                max(Lent_Week2) as Lent_Wk2,
                max(Lent_Week3) as Lent_Wk3,
                max(Lent_Week4) as Lent_Wk4,
                max(Lent_Week5) as Lent_Wk5,
                max(Lent_Week6) as Lent_Wk6,
                max(JulyFourWk ) as JulyFourWk,
                max(rg_IL) as rg_IL,
                max(rg_KY) as rg_KY,
                max(rg_MI) as rg_MI,
                max(rg_OH) as rg_OH,
                max(rg_WI) as rg_WI
            FROM {promo_table_name} as a
            LEFT JOIN scale_ten4ten_hst2 as p
                ON a.p_id = p.p_id
                AND a.ut_id = p.ut_id
                AND a.day_dt = p.day_dt
            LEFT JOIN mtest.holidaycalendar_lent as h
                ON a.day_dt = h.day_dt
            LEFT JOIN mtest.rg_inf_flg as r
                ON a.ut_id = r.ut_id
            WHERE a.day_dt BETWEEN '{min_dt}' AND '{max_dt}'
            GROUP BY 1, 2, 3
        """
        
        # Register other source tables as temp views if they are not already
        if not spark.catalog.tableExists("mtest.holidaycalendar_lent"):
            spark.table("mtest.holidaycalendar_lent").createOrReplaceTempView("mtest_holidaycalendar_lent")
        if not spark.catalog.tableExists("mtest.rg_inf_flg"):
            spark.table("mtest.rg_inf_flg").createOrReplaceTempView("mtest_rg_inf_flg")

        # Execute the main query
        df_result = spark.sql(main_query)
        
        # Write data to the region table
        if not region_table_exists:
            # Drop table if it somehow exists (like in SAS logic) and then create
            spark.sql(f"DROP TABLE IF EXISTS {region_table_name}")
            df_result.write.format("delta").mode("overwrite").saveAsTable(region_table_name)
        else:
            # Append data to existing table (after the conditional delete)
            df_result.write.format("delta").mode("append").saveAsTable(region_table_name)

        # PROC SORT equivalent for performance is OPTIMIZE ZORDER BY in Delta
        df_sorted = spark.table(region_table_name).orderBy("p_id", "ut_st_ad", "day_dt")
        # Overwrite the table with sorted data if needed, or use Z-ORDER for optimization
        # df_sorted.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(region_table_name)
        # A more common pattern is to optimize the table for the sort keys
        spark.sql(f"OPTIMIZE {region_table_name} ZORDER BY (p_id, ut_st_ad, day_dt)")
        
        # Copying data to rgn_ts folder (equivalent to PROC COPY or fcopy)
        rgn_ts_table_name = f"rgn_ts.pid_{string2.lower()}"
        df_to_copy = spark.table(region_table_name)
        df_to_copy.write.format("delta").mode("overwrite").saveAsTable(rgn_ts_table_name)


# Call the main function
rgnl_data(L6=L6_id, min_dt=MinDt_SAS, max_dt=MaxDt_SAS, MinDt_TD=MinDT, MaxDt_TD=MaxDT)
#End-DBShift