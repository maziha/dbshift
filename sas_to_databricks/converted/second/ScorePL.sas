import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from datetime import datetime

ModelType = "Reg_PL"
type = "Reg_PL"
tdate = datetime.now().strftime("%m%d%Y")

def pyspark_proc_score(spark, data_df, score_df, var_cols, id_cols):
    if score_df.count() != 1:
        # In SAS PROC SCORE, if the score dataset has no observations, it stops.
        # If it has more than one, it uses the first one. We'll be stricter.
        print(f"Warning: Score DataFrame does not contain exactly one row. Contained {score_df.count()} rows. Skipping scoring.")
        return None

    coef_dict = score_df.collect()[0].asDict()
    
    intercept = 0.0
    # PROC SCORE uses 'Intercept' for intercept term by default
    if 'Intercept' in coef_dict:
        intercept = coef_dict.get('Intercept', 0.0)
    # Some SAS procs might name it _Intercept_
    elif '_Intercept_' in coef_dict:
        intercept = coef_dict.get('_Intercept_', 0.0)

    pred_expr = F.lit(intercept)

    for col_name in var_cols:
        if col_name in data_df.columns and col_name in coef_dict:
            coef_value = coef_dict[col_name]
            if coef_value is not None:
                pred_expr += F.coalesce(F.col(col_name), F.lit(0)) * F.lit(coef_value)

    select_cols = id_cols + [pred_expr.alias("p_1")]
    
    # Ensure all id_cols are in the dataframe before selecting
    final_id_cols = [c for c in id_cols if c in data_df.columns]
    
    final_df = data_df.withColumn("p_1", pred_expr).select(*final_id_cols, "p_1")
    
    return final_df

def pids_model_score_spd(L6, LastPrWk, ValidWeeks, rescore, Fcst, mo_date, retrain, Update_M):
    spark = SparkSession.builder.getOrCreate()
    
    if Fcst == 'Y':
        score_m_db = f"{fcst_coef}/{type}"
        pred_p_db = f"{pred_promo}/{ModelType}"
        pred_b_db = f"{pred_base}/{ModelType}"
    else:
        score_m_db = f"{fcst_coef}/{type}/Train"
        pred_p_db = f"{fcst_promo}/{ModelType}"
        pred_b_db = f"{fcst_base}/{ModelType}"

    if Fcst == 'Y':
        sql_l6_info = f"""
            SELECT DISTINCT
                TRIM(SUBSTRING(mjr_mds_are_nm_tx, 1, 2)) as l6_tx,
                mjr_p_cls_id
            FROM mtest.fcst_p_upc_inf
            WHERE mjr_mds_are_id = {L6}
        """
        df_l6_info = spark.sql(sql_l6_info)
        l6_info_collected = df_l6_info.collect()
        
        L6tx = l6_info_collected[0]['l6_tx'] if l6_info_collected else ""
        L2 = ",".join([f"'{row['mjr_p_cls_id']}'" for row in l6_info_collected])

        sql_create_future_promo = f"""
            SELECT
                a.*
            FROM promo.fcst_future_pred_set_{L6tx} AS a
            INNER JOIN mtest.st_inf_flg AS b ON a.ut_id = b.ut_id
            WHERE mjr_p_cls_id IN ({L2}) AND (promo_model = "{ModelType}" OR base_model = "{ModelType}")
        """
        df_future_promo = spark.sql(sql_create_future_promo)
        df_future_promo.write.format("delta").mode("overwrite").option("sortby", "p_id, ut_id, cldr_day_of_wk_id").saveAsTable(f"promo.future_{ModelType}_{L6tx}")

    from_clause_l2_info = ""
    where_clause_l2_info = ""
    if Fcst == 'Y':
        from_clause_l2_info = f"""
            INNER JOIN (
                SELECT DISTINCT mjr_p_cls_id FROM promo.future_{ModelType}_{L6tx}
            ) AS b ON a.mjr_p_cls_id = b.mjr_p_cls_id
        """
    else:
        where_clause_l2_info = f"""
            WHERE a.mjr_mds_are_id = {L6}
            AND a.mjr_p_cls_id NOT IN ('L2-010051')
        """

    sql_l2_info = f"""
        SELECT DISTINCT
            a.mjr_p_cls_id,
            a.L2,
            TRIM(SUBSTRING(a.mjr_mds_are_nm_tx, 1, 2)) as L6NM,
            c.seasonal_indx
        FROM mtest.fcst_p_upc_inf AS a
        LEFT JOIN mtest.fcst_seasonal_index AS c ON a.mjr_p_cls_id = c.mjr_p_cls_id
        {from_clause_l2_info}
        {where_clause_l2_info}
        ORDER BY a.L2 ASC
    """
    df_l2_info = spark.sql(sql_l2_info)
    l2_info_collected = df_l2_info.collect()

    L2_ids = "|".join([row['mjr_p_cls_id'] for row in l2_info_collected])
    L2_table = "|".join([row['L2'] for row in l2_info_collected])
    nL2 = len(l2_info_collected)
    L6NM = l2_info_collected[0]['L6NM'] if l2_info_collected else ""
    seasonal_indx = "|".join([str(row['seasonal_indx']) for row in l2_info_collected])

    print(L6NM)
    print(Fcst)

    l2_ids_list = L2_ids.split('|')
    l2_table_list = L2_table.split('|')
    seasonal_indx_list = seasonal_indx.split('|')

    for j in range(nL2):
        L2_id = l2_ids_list[j]
        string2 = l2_table_list[j]
        L2_indx = seasonal_indx_list[j]

        if Fcst == 'Y':
            exist = 0
            nobs = 0
            score_type = "fcst"
        else:
            exist = 0
            nobs = 0
            promo_table_full_name = f"{pred_p_db}.pid_{string2}_{ModelType}_promo"
            if spark.catalog.tableExists(promo_table_full_name):
                try:
                    df_history = spark.sql(f"DESCRIBE HISTORY {promo_table_full_name}")
                    latest_version = df_history.orderBy(F.col("timestamp").desc()).first()
                    if latest_version:
                        modate_ts = latest_version['timestamp']
                        nobs = latest_version['numOutputRows']
                        if modate_ts > datetime.strptime(str(mo_date), '%Y%m%d'):
                             exist = 1
                except Exception as e:
                    print(f"Could not describe history for {promo_table_full_name}: {e}")
            
            print(exist)
            print(nobs)
            score_type = "train"
        
        coef_table_name = f"{score_m_db}.pid_{string2}_coef"
        if spark.catalog.tableExists(coef_table_name) and (rescore == 'Y' or exist != 1 or nobs < 10):
            df_score_coef = spark.table(coef_table_name)
            
            if Fcst == 'Y':
                df_coef_base = df_score_coef
            else:
                df_coef_base = spark.table(f"{score_m_db}.pid_{string2}_coef")

            df_coef_base = df_coef_base.withColumn("ntl_log_bse_promo_dct_pr_am", F.lit(0)) \
                                       .withColumn("adv_circ_flg", F.lit(0)) \
                                       .withColumn("adv_mid_wk_flg", F.lit(0)) \
                                       .withColumn("adv_super_evnt_flg", F.lit(0)) \
                                       .withColumn("adv_dgtl_circ_flg", F.lit(0)) \
                                       .withColumn("unadv_flg", F.lit(0)) \
                                       .withColumn("p_ten_for_ten_flg", F.lit(0))

            temp_pdl_from_clause = ""
            temp_pdl_where_clause = ""
            if Fcst == 'Y':
                temp_pdl_from_clause = f"promo.future_{ModelType}_{L6tx}"
                temp_pdl_where_clause = f"WHERE mjr_p_cls_id = '{L2_id}'"
            else:
                temp_pdl_from_clause = f"""
                    promo.pid_{string2} AS a
                    INNER JOIN mtest.st_inf_flg AS b ON a.ut_id = b.ut_id
                """
                if L2_indx == '1':
                    temp_pdl_where_clause = f"""
                        WHERE (p_sold_first_dt <= {LastPrWk} - 724 AND day_dt BETWEEN {LastPrWk} - 364 AND {LastPrWk} - 364 + ({ValidWeeks} * 7))
                           OR (p_sold_first_dt > {LastPrWk} - 724 AND day_dt BETWEEN {LastPrWk} - ({ValidWeeks} * 7) AND {LastPrWk})
                    """
                else:
                    temp_pdl_where_clause = f"WHERE (day_dt BETWEEN {LastPrWk} - ({ValidWeeks} * 7) AND {LastPrWk})"
            
            sql_temp_pdl = f"""
                SELECT * FROM {temp_pdl_from_clause}
                {temp_pdl_where_clause}
            """
            df_temp_pdl = spark.sql(sql_temp_pdl)
            
            var_list = [
                "ntl_log_bse_pr_am", "ntl_log_bse_promo_dct_pr_am",
                "fcl_per_of_yr_id",
                "wk2", "wk3", "wk4", "wk5", "wk6", "wk7", "wk8", "wk9", "wk10", "wk11", "wk12", "wk13", "wk14", "wk15", "wk16", "wk17", "wk18", "wk19", "wk20", "wk21", "wk22", "wk23", "wk24", "wk25", "wk26", "wk27", "wk28", "wk29", "wk30", "wk31", "wk32", "wk33", "wk34", "wk35", "wk36", "wk37", "wk38", "wk39", "wk40", "wk41", "wk42", "wk43", "wk44", "wk45", "wk46", "wk47", "wk48", "wk49", "wk50", "wk51", "wk52",
                "dow2", "dow3", "dow4", "dow5", "dow6", "dow7",
                "ut19", "ut20", "ut21", "ut22", "ut23", "ut24", "ut25", "ut26", "ut27", "ut28", "ut29", "ut30", "ut31", "ut32", "ut33", "ut34", "ut35", "ut36", "ut37", "ut38", "ut39", "ut40", "ut41", "ut42", "ut43", "ut44", "ut45", "ut46", "ut47", "ut48", "ut49", "ut50", "ut51", "ut52", "ut53", "ut54", "ut55", "ut56", "ut57", "ut58", "ut59", "ut60", "ut61", "ut62", "ut63", "ut64", "ut65", "ut66", "ut67", "ut68", "ut69", "ut70", "ut71", "ut72", "ut73", "ut74", "ut75", "ut76", "ut77", "ut78", "ut79", "ut80", "ut81", "ut82", "ut83", "ut84", "ut85", "ut86", "ut87", "ut88", "ut89", "ut90", "ut91", "ut92", "ut93", "ut94", "ut95", "ut96", "ut97", "ut98", "ut99", "ut100", "ut101", "ut102", "ut103", "ut104", "ut105", "ut106", "ut107", "ut108", "ut109", "ut110", "ut111", "ut112", "ut113", "ut114", "ut115", "ut116", "ut117", "ut118", "ut119", "ut120", "ut121", "ut122", "ut123", "ut124", "ut125", "ut126", "ut127", "ut128", "ut129", "ut130", "ut131", "ut132", "ut133", "ut134", "ut135", "ut136", "ut137", "ut138", "ut139", "ut140", "ut141", "ut142", "ut143", "ut144", "ut145", "ut146", "ut147", "ut148", "ut149", "ut150", "ut151", "ut152", "ut153", "ut154", "ut155", "ut156", "ut157", "ut158", "ut159", "ut160", "ut161", "ut162", "ut163", "ut164", "ut165", "ut166", "ut167", "ut168", "ut169", "ut170", "ut171", "ut172", "ut173", "ut174", "ut175", "ut176", "ut177", "ut178", "ut179", "ut180", "ut181", "ut182", "ut183", "ut184", "ut185", "ut186", "ut187", "ut188", "ut189", "ut190", "ut191", "ut192", "ut193", "ut194", "ut195", "ut196", "ut197", "ut198", "ut199", "ut200", "ut201", "ut202", "ut203", "ut204", "ut205", "ut206", "ut207", "ut208", "ut209", "ut210", "ut211", "ut212", "ut213", "ut214", "ut215", "ut216", "ut217", "ut218", "ut219", "ut220", "ut221", "ut222", "ut223", "ut224", "ut225", "ut226", "ut227", "ut228", "ut229", "ut230", "ut231", "ut232", "ut233", "ut234", "ut235", "ut236", "ut237", "ut238", "ut239", "ut240", "ut241", "ut242", "ut243", "ut244", "ut245", "ut246", "ut247", "ut248", "ut249", "ut250", "ut251", "ut252", "ut253", "ut254", "ut255", "ut256", "ut257", "ut258", "ut259", "ut260", "ut261", "ut262", "ut263", "ut264", "ut265", "ut266", "ut267", "ut268", "ut269", "ut270", "ut271", "ut272", "ut273", "ut274", "ut275", "ut276", "ut277", "ut278", "ut279", "ut280", "ut281", "ut282", "ut283", "ut284", "ut285", "ut286", "ut287", "ut288", "ut289", "ut290", "ut291", "ut292", "ut293", "ut294", "ut295", "ut296", "ut297", "ut298", "ut299", "ut300", "ut301", "ut302", "ut303", "ut304", "ut305", "ut306", "ut307", "ut308", "ut309", "ut310", "ut311", "ut312", "ut313", "ut314", "ut315", "ut316", "ut317", "ut318", "ut319", "ut320", "ut321", "ut322", "ut323", "ut324",
                "adv_circ_flg", "adv_mid_wk_flg", "adv_super_evnt_flg", "adv_dgtl_circ_flg", "unadv_flg", "p_ten_for_ten_flg",
                "NY", "SuperBowlSat", "DayB4Valentine", "Valentine", "Easter", "EasterWk",
                "MomSat", "Mom", "Memorial", "MemWkend", "DadSat", "Dad", "JulyFour", "Labor",
                "LaborWkEnd", "LaborFri", "LaborSat", "LaborSun", "Columbus", "MCC_BLK_OUT",
                "PreHalo_Fri", "PreHalo_Sat", "PreHalo_Sun",
                "HalloweenEve", "HalloweenFri", "Halloween", "Veterans",
                "FridayB4Turkey", "SaturdayB4Turkey", "TurkeyWed", "Turkey", "BlackFri",
                "RedSat", "DecTwoThree", "DecTwoSix", "SunPreXmas", "XMASEVE", "SUNPNY", "SATPNY", "NYEVE"
            ]
            id_list = ["day_dt", "p_id", "mjr_p_cls_id", "ut_id", "ntl_log_bse_pr_am", "ntl_log_bse_promo_dct_pr_am", "fcl_per_of_yr_id", "adv_circ_flg", "adv_mid_wk_flg", "adv_super_evnt_flg", "adv_dgtl_circ_flg"]
            
            if Fcst == 'Y':
                df_to_score_base = df_temp_pdl.filter(F.col("base_model") == ModelType)
                df_base_scored = pyspark_proc_score(spark, df_to_score_base, df_coef_base, var_list, id_list)
                if df_base_scored:
                    df_base_scored.write.format("delta").mode("overwrite").saveAsTable(f"{pred_b_db}.pid_{string2}_{ModelType}_base")
            else:
                df_base_scored = pyspark_proc_score(spark, df_temp_pdl, df_coef_base, var_list, id_list)
                if df_base_scored:
                    df_base_scored.write.format("delta").mode("overwrite").saveAsTable(f"{pred_b_db}.pid_{string2}_{type}_base")

            if Fcst == 'Y':
                df_to_score_promo = df_temp_pdl.filter(F.col("promo_model") == ModelType)
                df_promo_scored = pyspark_proc_score(spark, df_to_score_promo, df_score_coef, var_list, id_list)
                if df_promo_scored:
                    df_promo_scored.write.format("delta").mode("overwrite").saveAsTable(f"{pred_p_db}.pid_{string2}_{ModelType}_promo")
            else:
                df_promo_scored = pyspark_proc_score(spark, df_temp_pdl, df_score_coef, var_list, id_list)
                if df_promo_scored:
                    df_promo_scored.write.format("delta").mode("overwrite").saveAsTable(f"{pred_p_db}.pid_{string2}_{type}_promo")


# The following variables are expected to be defined in the calling environment.
# L6_id = ...
# LastPreWeek = ...
# ValidWks = ...
# score = ...
# Forecast = ...
# mo_dt = ... (e.g., 20231026)
# train = ...
# Update = ...
# fcst_coef = "..."
# pred_promo = "..."
# pred_base = "..."
# fcst_promo = "..."

# pids_model_score_spd(
#     L6=L6_id, 
#     LastPrWk=LastPreWeek, 
#     ValidWeeks=ValidWks,
#     rescore=score, 
#     Fcst=Forecast, 
#     mo_date=mo_dt, 
#     retrain=train, 
#     Update_M=Update
# )
#End-DBShift