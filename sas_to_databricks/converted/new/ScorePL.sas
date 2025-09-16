import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from datetime import datetime

ModelType = "Reg_PL"
type_var = "Reg_PL"
tdate = datetime.now().strftime('%m%d%Y')

L6_id = "YOUR_L6_ID"
LastPreWeek = "YYYY-MM-DD"
ValidWks = 52
score = "Y"
Forecast = "N"
mo_dt = "YYYY-MM-DD"
train = "N"
Update = "N"
fcst_log = "dbfs:/path/to/fcst_log"
fcst_coef = "dbfs:/path/to/fcst_coef"
pred_promo = "dbfs:/path/to/pred_promo"
fcst_promo = "dbfs:/path/to/fcst_promo"
pred_base = "dbfs:/path/to/pred_base"
fcst_base = "dbfs:/path/to/fcst_base"

def pids_model_score_spd(L6, LastPrWk, ValidWeeks, rescore, Fcst, mo_date, retrain, Update_M):
    if Fcst == 'Y':
        print(f"INFO: Log location would be: {fcst_log}/Update/DF_Score/library_setup_{Fcst}_{ModelType}{L6}.txt")
    else:
        print(f"INFO: Log location would be: {fcst_log}/{ModelType}/library_setup_score_{Fcst}{L6}.txt")

    if Fcst == 'Y':
        score_m_db = "score_m"
        pred_p_db = "pred_p"
        pred_b_db = "pred_b"
    else:
        score_m_db = "score_m"
        pred_p_db = "pred_p"
        pred_b_db = "pred_b"

    if Fcst == 'Y':
        df_l6_info = spark.sql(f"""
            SELECT DISTINCT
                TRIM(SUBSTRING(mjr_mds_are_nm_tx, 1, 2)) as L6tx,
                CONCAT("'", TRIM(mjr_p_cls_id), "'") as L2_quoted
            FROM mtest.fcst_p_upc_inf
            WHERE mjr_mds_are_id = {L6}
        """)
        l6_info_collected = df_l6_info.collect()
        
        L6tx = l6_info_collected[0]['L6tx'] if l6_info_collected else ""
        L2_for_in_clause = ",".join([row['L2_quoted'] for row in l6_info_collected])

        spark.sql(f"""
            CREATE OR REPLACE TABLE promo.future_{ModelType}_{L6tx}
            USING DELTA
            AS
            SELECT a.*
            FROM promo.fcst_future_pred_set_{L6tx} as a
            INNER JOIN mtest.st_inf_flg as b ON a.ut_id = b.ut_id
            WHERE a.mjr_p_cls_id IN ({L2_for_in_clause})
              AND (a.promo_model = "{ModelType}" OR a.base_model = "{ModelType}")
        """)

    from_clause_extra = ""
    where_clause = ""
    if Fcst == 'Y':
        from_clause_extra = f"""
        INNER JOIN (
            SELECT DISTINCT mjr_p_cls_id FROM promo.future_{ModelType}_{L6tx}
        ) as b ON a.mjr_p_cls_id = b.mjr_p_cls_id
        """
    else:
        where_clause = f"""
        WHERE a.mjr_mds_are_id = {L6}
        AND a.mjr_p_cls_id NOT IN ('L2-010051')
        """

    sql_query_l2_info = f"""
        SELECT DISTINCT
            a.mjr_p_cls_id,
            a.L2,
            TRIM(SUBSTRING(a.mjr_mds_are_nm_tx, 1, 2)) as L6NM,
            c.seasonal_indx
        FROM mtest.fcst_p_upc_inf AS a
        LEFT JOIN mtest.fcst_seasonal_index AS c ON a.mjr_p_cls_id = c.mjr_p_cls_id
        {from_clause_extra}
        {where_clause}
        ORDER BY a.L2 ASC
    """
    df_l2_info = spark.sql(sql_query_l2_info)
    l2_info_collected = df_l2_info.collect()

    if not l2_info_collected:
        print("No L2 information found. Exiting.")
        return

    L2_ids = "|".join([str(row['mjr_p_cls_id']) for row in l2_info_collected])
    L2_table = "|".join([str(row['L2']) for row in l2_info_collected])
    nL2 = len(l2_info_collected)
    L6NM = l2_info_collected[0]['L6NM']
    seasonal_indx = "|".join([str(row['seasonal_indx']) if row['seasonal_indx'] is not None else "" for row in l2_info_collected])

    print(L6NM)
    print(Fcst)
    
    L2_ids_list = L2_ids.split('|')
    L2_table_list = L2_table.split('|')
    seasonal_indx_list = seasonal_indx.split('|')

    for j in range(nL2):
        L2_id = L2_ids_list[j]
        string2 = L2_table_list[j]
        L2_indx = seasonal_indx_list[j]

        if Fcst == 'Y':
            print(f"INFO: Log location would be: {fcst_log}/{ModelType}/pid_{string2}_score.txt")
        else:
            print(f"INFO: Log location would be: {fcst_base}/{ModelType}/Log/pid_{string2}_score.txt")

        if Fcst == 'Y':
            exist = 0
            nobs = 0
            score_type = "fcst"
        else:
            exist = 0
            nobs = 0
            table_to_check = f"{pred_p_db}.pid_{string2}_{ModelType}_promo"
            if spark.catalog.tableExists(table_to_check):
                try:
                    history_df = spark.sql(f"DESCRIBE HISTORY {table_to_check}")
                    latest_mod_timestamp = history_df.orderBy(F.col("version").desc()).select("timestamp").first()[0]
                    if latest_mod_timestamp.date() > datetime.strptime(mo_date, '%Y-%m-%d').date():
                         df_to_check_count = spark.table(table_to_check)
                         count_res = df_to_check_count.count()
                         exist = 1
                         nobs = count_res
                    else:
                        exist = 0
                        nobs = 0
                except Exception:
                    exist = 0
                    nobs = 0
            
            print(f"Exist: {exist}")
            print(f"Nobs: {nobs}")
            score_type = "train"
        
        coef_table_name = f"{score_m_db}.pid_{string2}_coef"
        coef_table_exists = spark.catalog.tableExists(coef_table_name)
        
        condition_to_score = coef_table_exists and (rescore == 'Y' or exist != 1 or nobs < 10)

        if condition_to_score:
            
            if Fcst == 'Y':
                df_coef_base_source = spark.table(f"{score_m_db}.pid_{string2}_coef")
                df_future_coef_base = df_coef_base_source.withColumn("ntl_log_bse_promo_dct_pr_am", F.lit(0)) \
                                                         .withColumn("adv_circ_flg", F.lit(0)) \
                                                         .withColumn("adv_mid_wk_flg", F.lit(0)) \
                                                         .withColumn("adv_super_evnt_flg", F.lit(0)) \
                                                         .withColumn("adv_dgtl_circ_flg", F.lit(0)) \
                                                         .withColumn("unadv_flg", F.lit(0)) \
                                                         .withColumn("p_ten_for_ten_flg", F.lit(0))
            else:
                df_coef_base_source = spark.table(f"{score_m_db}.pid_{string2}_coef")
                df_pid_coef_base = df_coef_base_source.withColumn("ntl_log_bse_promo_dct_pr_am", F.lit(0)) \
                                                      .withColumn("adv_circ_flg", F.lit(0)) \
                                                      .withColumn("adv_mid_wk_flg", F.lit(0)) \
                                                      .withColumn("adv_super_evnt_flg", F.lit(0)) \
                                                      .withColumn("adv_dgtl_circ_flg", F.lit(0)) \
                                                      .withColumn("unadv_flg", F.lit(0)) \
                                                      .withColumn("p_ten_for_ten_flg", F.lit(0))
            
            if Fcst == 'Y':
                df_temp_pdl = spark.table(f"promo.future_{ModelType}_{L6tx}").filter(F.col("mjr_p_cls_id") == L2_id)
            else:
                df_pid_string2 = spark.table(f"promo.pid_{string2}")
                df_st_inf_flg = spark.table("mtest.st_inf_flg")
                df_temp_pdl_joined = df_pid_string2.join(df_st_inf_flg, "ut_id", "inner")
                
                if L2_indx == '1':
                    df_temp_pdl = df_temp_pdl_joined.filter(
                        ((F.col("p_sold_first_dt") <= F.expr(f"date_sub('{LastPrWk}', 724)")) &
                         (F.col("day_dt").between(F.expr(f"date_sub('{LastPrWk}', 364)"), F.expr(f"date_add(date_sub('{LastPrWk}', 364), {ValidWeeks * 7})")))) |
                        ((F.col("p_sold_first_dt") > F.expr(f"date_sub('{LastPrWk}', 724)")) &
                         (F.col("day_dt").between(F.expr(f"date_sub('{LastPrWk}', {ValidWeeks * 7})"), F.lit(LastPrWk)))))
                else:
                    df_temp_pdl = df_temp_pdl_joined.filter(
                        F.col("day_dt").between(F.expr(f"date_sub('{LastPrWk}', {ValidWeeks * 7})"), F.lit(LastPrWk)))
            
            df_temp_pdl_sorted = df_temp_pdl.orderBy("p_id")
            
            wk_vars = [f"wk{i}" for i in range(2, 53)]
            dow_vars = [f"dow{i}" for i in range(2, 8)]
            ut_vars = [f"ut{i}" for i in range(19, 325)]
            promo_vehicle_vars = ["adv_circ_flg", "adv_mid_wk_flg", "adv_super_evnt_flg", "adv_dgtl_circ_flg", "unadv_flg", "p_ten_for_ten_flg"]
            holidays_vars = ["NY", "SuperBowlSat", "DayB4Valentine", "Valentine", "Easter", "EasterWk", "MomSat", "Mom", "Memorial", "MemWkend", "DadSat", "Dad", "JulyFour", "Labor", "LaborWkEnd", "LaborFri", "LaborSat", "LaborSun", "Columbus", "MCC_BLK_OUT", "PreHalo_Fri", "PreHalo_Sat", "PreHalo_Sun", "HalloweenEve", "HalloweenFri", "Halloween", "Veterans", "FridayB4Turkey", "SaturdayB4Turkey", "TurkeyWed", "Turkey", "BlackFri", "RedSat", "DecTwoThree", "DecTwoSix", "SunPreXmas", "XMASEVE", "SUNPNY", "SATPNY", "NYEVE"]
            other_vars = ["ntl_log_bse_pr_am", "ntl_log_bse_promo_dct_pr_am", "fcl_per_of_yr_id"]
            all_scoring_vars = other_vars + wk_vars + dow_vars + ut_vars + promo_vehicle_vars + holidays_vars
            
            if Fcst == 'Y':
                df_score_input_base = df_temp_pdl_sorted.filter(F.col("base_model") == ModelType)
                coef_row_base = df_future_coef_base.filter(F.col("_TYPE_") == "PARMS").first()
                if coef_row_base:
                    coef_dict_base = coef_row_base.asDict()
                    intercept_base = coef_dict_base.get("Intercept", 0.0)
                    score_expr_base = F.lit(intercept_base)
                    for var in all_scoring_vars:
                        if var.upper() in [k.upper() for k in coef_dict_base.keys()] and var in df_score_input_base.columns:
                            coef_key = [k for k in coef_dict_base.keys() if k.upper() == var.upper()][0]
                            score_expr_base += F.coalesce(F.col(f"`{var}`"), F.lit(0)) * F.lit(coef_dict_base.get(coef_key, 0.0))
                    
                    df_scored_base = df_score_input_base.withColumn("P_1", score_expr_base)
                    df_scored_base.write.format("delta").mode("overwrite").saveAsTable(f"{pred_b_db}.pid_{string2}_{ModelType}_base")
            else:
                df_score_input_base = df_temp_pdl_sorted
                coef_row_base = df_pid_coef_base.filter(F.col("_TYPE_") == "PARMS").first()
                if coef_row_base:
                    coef_dict_base = coef_row_base.asDict()
                    intercept_base = coef_dict_base.get("Intercept", 0.0)
                    score_expr_base = F.lit(intercept_base)
                    for var in all_scoring_vars:
                        if var.upper() in [k.upper() for k in coef_dict_base.keys()] and var in df_score_input_base.columns:
                            coef_key = [k for k in coef_dict_base.keys() if k.upper() == var.upper()][0]
                            score_expr_base += F.coalesce(F.col(f"`{var}`"), F.lit(0)) * F.lit(coef_dict_base.get(coef_key, 0.0))
                    
                    df_scored_base = df_score_input_base.withColumn("P_1", score_expr_base)
                    df_scored_base.write.format("delta").mode("overwrite").saveAsTable(f"{pred_b_db}.pid_{string2}_{type_var}_base")

            if Fcst == 'Y':
                df_score_input_promo = df_temp_pdl_sorted.filter(F.col("promo_model") == ModelType)
                df_pid_coef_promo = spark.table(f"{score_m_db}.pid_{string2}_coef")
                coef_row_promo = df_pid_coef_promo.filter(F.col("_TYPE_") == "PARMS").first()
                if coef_row_promo:
                    coef_dict_promo = coef_row_promo.asDict()
                    intercept_promo = coef_dict_promo.get("Intercept", 0.0)
                    score_expr_promo = F.lit(intercept_promo)
                    for var in all_scoring_vars:
                        if var.upper() in [k.upper() for k in coef_dict_promo.keys()] and var in df_score_input_promo.columns:
                            coef_key = [k for k in coef_dict_promo.keys() if k.upper() == var.upper()][0]
                            score_expr_promo += F.coalesce(F.col(f"`{var}`"), F.lit(0)) * F.lit(coef_dict_promo.get(coef_key, 0.0))

                    df_scored_promo = df_score_input_promo.withColumn("P_1", score_expr_promo)
                    df_scored_promo.write.format("delta").mode("overwrite").saveAsTable(f"{pred_p_db}.pid_{string2}_{ModelType}_promo")
            else:
                df_score_input_promo = df_temp_pdl_sorted
                df_pid_coef_promo = spark.table(f"{score_m_db}.pid_{string2}_coef")
                coef_row_promo = df_pid_coef_promo.filter(F.col("_TYPE_") == "PARMS").first()
                if coef_row_promo:
                    coef_dict_promo = coef_row_promo.asDict()
                    intercept_promo = coef_dict_promo.get("Intercept", 0.0)
                    score_expr_promo = F.lit(intercept_promo)
                    for var in all_scoring_vars:
                        if var.upper() in [k.upper() for k in coef_dict_promo.keys()] and var in df_score_input_promo.columns:
                            coef_key = [k for k in coef_dict_promo.keys() if k.upper() == var.upper()][0]
                            score_expr_promo += F.coalesce(F.col(f"`{var}`"), F.lit(0)) * F.lit(coef_dict_promo.get(coef_key, 0.0))
                            
                    df_scored_promo = df_score_input_promo.withColumn("P_1", score_expr_promo)
                    df_scored_promo.write.format("delta").mode("overwrite").saveAsTable(f"{pred_p_db}.pid_{string2}_{type_var}_promo")

pids_model_score_spd(
    L6=L6_id,
    LastPrWk=LastPreWeek,
    ValidWeeks=ValidWks,
    rescore=score,
    Fcst=Forecast,
    mo_date=mo_dt,
    retrain=train,
    Update_M=Update
)
#End-DBShift