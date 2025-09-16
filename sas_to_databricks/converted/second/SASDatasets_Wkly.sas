import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SASDatasets_Wkly_Conversion").getOrCreate()

# These variables are assumed to be defined in the Databricks environment,
# similar to how SAS macro variables would be.
# Example values are provided.
jdbc_url = "jdbc:teradata://<your_teradata_host>/DATABASE=<your_db>"
connection_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.teradata.jdbc.TeraDriver"
}
L6_id = "some_L6_id"
MinDt_SAS = "01JAN2016"
MaxDt_SAS = "08MAY2021"

def wkly_data(L6, min_dt, max_dt):
    sql_query_l2_info = f"""
    (SELECT DISTINCT mjr_p_cls_id, L2
     FROM dl_cntl_adva.fcst_p_upc_inf
     WHERE mjr_mds_are_id = {L6}
     ORDER BY L2 ASC) AS L2_INFO
    """
    
    df_l2_info = spark.read.jdbc(url=jdbc_url, table=sql_query_l2_info, properties=connection_properties)
    l2_info_list = df_l2_info.collect()

    for row in l2_info_list:
        L2_id = row["mjr_p_cls_id"]
        string2 = row["L2"]

        print(f"Processing table suffix: {string2}")

        exist = 0
        try:
            promo_cols = {c.name.lower() for c in spark.catalog.listColumns(f"promo.pid_{string2}")}
            if 'hl_cap' in promo_cols:
                exist = 1
        except Exception:
            exist = 0
        
        print(f"Exist flag: {exist}")

        table_exists = spark.catalog.tableExists(f"week.pid_{string2}")
        exist_wk = 0

        if table_exists:
            try:
                week_cols = {c.name.lower() for c in spark.catalog.listColumns(f"week.pid_{string2}")}
                if 'hl_cap' in week_cols:
                    exist_wk = 1
            except Exception:
                exist_wk = 0
            
            print(f"Exist_wk flag: {exist_wk}")

            df_week_table = spark.table(f"week.pid_{string2}")
            df_filtered = df_week_table.filter(F.col("wk_end_dt") < F.to_date(F.lit(min_dt), 'ddMMMyyyy'))
            df_filtered.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"week.pid_{string2}")
        
        hl_cap_select_clause = ""
        if exist > 0 and exist_wk > 0:
            hl_cap_select_clause = ",avg(hl_cap) as hl_cap ,avg(end_cap) as end_cap"
        elif exist <= 0 and exist_wk > 0:
            hl_cap_select_clause = ",0 as hl_cap ,0 as end_cap"

        
        source_table_for_sql = f"promo.pid_{string2}"
        df_source_data = spark.table(source_table_for_sql)
        df_source_data.createOrReplaceTempView(f"pid_{string2}")

        sql_query_main = f"""
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
            max(mPk_cpn_flg) as mPk_cpn_flg
            {hl_cap_select_clause},
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
            max(NYEVE) as NYEVE
        FROM
            pid_{string2}
        WHERE
            day_dt BETWEEN to_date('{min_dt}', 'ddMMMyyyy') AND to_date('{max_dt}', 'ddMMMyyyy')
        GROUP BY
            1, 2, 3
        """

        df_new_weekly_data = spark.sql(sql_query_main)
        
        df_new_weekly_data.write.format("delta").mode("append").saveAsTable(f"week.pid_{string2}")

        df_sorted = spark.table(f"week.pid_{string2}").orderBy("p_id", "ut_id", "wk_end_dt")
        df_sorted.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"week.pid_{string2}")

        spark.sql(f"DROP TABLE IF EXISTS promo_ts.pid_{string2}")
        
        df_to_copy = spark.table(f"week.pid_{string2}")
        df_to_copy.write.format("delta").mode("overwrite").saveAsTable(f"promo_ts.pid_{string2}")

wkly_data(L6=L6_id, min_dt=MinDt_SAS, max_dt=MaxDt_SAS)
#End-DBShift