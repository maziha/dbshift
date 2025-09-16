import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType, DateType, TimestampType

spark = SparkSession.builder.appName("SAS_to_PySpark_Conversion").getOrCreate()

# Section: Setup Options and Variables
macro_dir = "/vg06/ryue/Macros"
user_id = "jye_sas"
password = "Zm8fj2P8evyTwAYh"

# JDBC connection properties
# Note: DSN names are used as placeholders. Replace with actual JDBC URLs.
# For example, a Teradata URL might look like: "jdbc:teradata://<hostname>/DATABASE=your_db"
jdbc_url_asi_tar = "jdbc:odbc:HQAACRM03_ASI_TARGET" 
jdbc_url_asi_vita = "jdbc:odbc:HQAACRM03_ASI_ANALYTICS_ARCHIVE"
jdbc_url_asi_did = "jdbc:odbc:HQAACRM03_ASI_DEID_MART"

jdbc_connection_properties = {
    "user": user_id,
    "password": password
    # "driver": "com.teradata.jdbc.TeraDriver" # Example driver
}

ASI_Target_conn_str = f"dsn='HQAACRM03_ASI_TARGET' user={user_id} password={password}"

# DBFS library paths
src_aiq_path = "/vg07/asi_sas/asi_data"
src_aiq2_path = "/vg01/asi_sas/ftp/incoming/Analytics_IQ"
aiqftp_path = "/vg01/asi_sas/ftp/outgoing/Analytics_IQ"
em_path = "/vg06/projects/Emailable_Emarketable/2021"
snapshot_path = "/vg08/btao/Trigger/snapshot"
code_dir = "/vg06/jye/model_score/code"
target_path = "/vg06/jye/model_score/data"
target_dir = "/vg06/jye/model_score/data"
btao_path = "/vg06/btao/model_score/data"

# Parameter Input
version = 80
AIQ_version = 89
AIQ_MMMYYYY = "Jun2021"
AIQ_version2 = 88
AIQ_MMMYYYY2 = "Jan2021"
AIQ_Input = f"aiq_dyn_{AIQ_version}_sp_all_{AIQ_MMMYYYY}"
AIQ_Input_z4 = f"aiq_dyn_{AIQ_version}_sp_all_z4_{AIQ_MMMYYYY}"
AIQ_Input2 = f"aiq_dyn_{AIQ_version2}_{AIQ_MMMYYYY2}_NewVar"

file_name = f"MMS_M{version}"

# *****Input data file with data collection attributes;
df_mms_m = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url_asi_tar) \
    .option("dbtable", f"VITA_MEMBERINFO_m{version}") \
    .options(**jdbc_connection_properties) \
    .load() \
    .filter((F.col("account_stat") == "0") & (F.col("CHID_key") > 0)) \
    .select("chid_key", "mid_key") \
    .withColumnRenamed("chid_key", "chid")

# *** Do Not Change *** ;
# *** 2.A Append Attributes from VITA_ACXIOM Table **** ;
df_mms_m = df_mms_m.orderBy("CHID")

# PROC SQL CONNECT TO ODBC for VITA_ACXIOM
sql_query_vita_acxiom = f"""
select chid_key
, ibx_pasdsf_deliverability_idt as A0001
, ibx_inferred_household_rank as A0002
, ibx_current_affairs_politics_premier as A0003
, ibx_community_charities_premier as A0004
, ibx_religious_inspirational_premier as A0005
, ibx_travel_us_premier as A0006
, ibx_travel_foreign_premier as A0007
, ibx_recreational_vehicles_premier as A0008
, ibx_travel_family_vacations_premier as A0009
, ibx_travel_cruise_vacations_premier as A0010
, ibx_grandchildren_premier as A0011
, ibx_pc_owner_premier as A0012
, ibx_travel_grouping_premier as A0013
, ibx_exercise_health_group_premier as A0014
, ibx_electronics_company_grouping_premier as A0015
, ibx_investing_finance_grouping_premier as A0016
, ibx_soho_idt_premier as A0017
, ibx_home_year_built as A0018
, ibx_vehicle_truck_motorcycle_rv_premier as A0019
, ibx_mail_order_donor as A0020
, ibx_golf as A0021
, ibx_pets_other as A0022
, ibx_outdoors_dimension as A0023
, ibx_mail_order_buyer as A0024
, ibx_movie_music_grouping as A0025
, ibx_home_improvement_grouping as A0026
, ibx_investors_highly_likely as A0027
, ibx_investors_likely as A0028
, ibx_home_loan_dt_1 as A0029
, ibx_home_loan_total_ranges as A0030
, ibx_home_lot_square_footage_ranges as A0031
, ibx_home_lot_built_ranges as A0032
, ibx_home_square_footage_ranges as A0033
, ibx_home_room_count as A0034
, ibx_home_roof_type as A0035
, ibx_home_market_value_deciles as A0036
, ibx_political_party_1st_individual as A0037
, ibx_political_party_2nd_individual as A0038
, ibx_political_party_input_individual as A0039
, ibx_owner_type_detail as A0040
, ibx_home_assessed_value_ranges as A0041
, ibx_home_purchased_amount_ranges as A0042
, ibx_home_lender_name_1 as A0043
, ibx_home_loan_amount_1_ranges as A0044
, ibx_home_loan_type_1 as A0045
, ibx_home_loan_interest_rate_type_1 as A0046
, ibx_home_loan_transaction_type_1 as A0047
, ibx_home_owner as A0048
, ibx_home_purchase_dt_year_month as A0049
, ibx_home_purchase_year as A0050
, ibx_home_property_type_details as A0051
, ibx_home_loan_to_value_ranges as A0052
, ibx_home_equity_available_ranges as A0053
, ibx_home_equity_lendable_ranges as A0054
, ibx_home_owners_insurance_expiration_dt as A0055
, ibx_home_year_built_actual as A0056
, ibx_adult_age_ranges_present_in_household_premier as A0057
, ibx_children_age_ranges_present_in_household_premier as A0058
, ibx_children_household_in_household_premier as A0059
, ibx_occupation_1st_individual_premier as A0060
, ibx_occupation_2nd_individual_premier as A0061
, ibx_home_owner_renter_premier as A0062
, ibx_length_of_residence_premier as A0063
, ibx_dwelling_type as A0064
, IBX_MARITAL_STAT_IN_THE_HOUSEHOLD_PREMIER as A0065
, ibx_age_in_two_year_increments_1st_individual_premier as A0069
, ibx_age_in_two_year_increments_2nd_individual_premier as A0070
, ibx_working_woman_premier as A0071
, ibx_mail_order_responder_premier as A0072
, ibx_credit_card_idt_premier as A0073
, ibx_children_presence_of_household as A0074
, ibx_property_type_premier as A0075
, ibx_adults_number_of_household_premier as A0076
, ibx_household_size_premier as A0077
, ibx_occupation_input_individual_premier as A0078
, ibx_num_of_sources_premier as A0080
, ibx_home_market_value_premier as A0081
, ibx_home_purchase_dt_premier as A0082
, ibx_home_purchase_dt_year_premier as A0083
, ibx_home_purchase_dt_month_premier as A0084
, ibx_vehicle_new_car_buyer_premier as A0085
, ibx_vehicle_known_owned_number_premier as A0086
, ibx_vehicle_dominant_lifestyle_premier as A0087
, ibx_life_stages_cd as A0088
, ibx_home_equity_available_premier as A0090
, ibx_pc_software_buyer_premier as A0091
, ibx_income_estimated_narrow_ranges_premier as A0092
, ibx_presence_of_young_adult_premier as A0093
, ibx_gender_input_individual_premier as A0094
, ibx_presence_of_senior_adult_premier as A0095
, ibx_home_property_type as A0096
, ibx_home_loan_amount_actual as A0097
, ibx_home_equity_lendable_actual as A0098
, ibx_home_equity_available_actual as A0099
, ibx_home_loan_amount_1_actual as A0100
, ibx_trends_for_telecom_internet_user as A0101
, ibx_trends_for_telecom_cellular_user as A0102
, ibx_trends_for_telecom_international_long_distance_user as A0103
, ibx_trends_for_telecom_optional_calling_services as A0104
, ibx_trends_for_telecom_top_20percent_long_distance_user as A0105
, ibx_num_of_lines_of_credit as A0106
, ibx_home_lender_type_1 as A0107
, ibx_ethnic_code_e_tech_2 as A0108
, ibx_religious_affiliation_code_e_tech_2 as A0109
, ibx_ethnic_roll_up_code_e_tech_2 as A0110
, ibx_language_preference_code_e_tech_2 as A0111
, ibx_credit_card_frequency_of_purchase_premier as A0113
, ibx_retail_activity_last_dt as A0114
, ibx_retail_purchases_cat as A0115
, ibx_retail_purchases_most_frequent_cat as A0116
, ibx_retail_purchases_activity_dt_and_cat_1 as A0117
, ibx_retail_purchases_activity_dt_and_cat_2 as A0118
, ibx_retail_purchases_activity_dt_and_cat_3 as A0119
, ibx_retail_purchases_activity_dt_and_cat_4 as A0120
, ibx_retail_purchases_activity_dt_and_cat_5 as A0121
, ibx_personic_cluster as A0122
, ibx_networth_premier as A0124
, ibx_veteran as A0125
, ibx_education_1st_individual as A0126
, ibx_education_second_individual_premier as A0127
, ibx_education_input_individual_premier as A0128
, ibx_race_cd_1st_individual_premier as A0129
, ibx_race_cd_2nd_individual_premier as A0130
, ibx_mail_order_buyer_categories as A0131
, ibx_community_involvement_causes_supported_financially as A0132
, ibx_age_input_individual_default_1st_individual_premier as A0133
, ibx_ethnic_code_e_tech as A0134
, ibx_religious_affiliation_code_e_tech as A0135
, ibx_language_preference_code_e_tech as A0136
, ibx_country_of_origin_code_e_tech as A0137
, ibx_ethnic_roll_up_code_e_tech as A0138
from VITA_ACXIOM_m{version}
where chid_key is not null
"""

df_vita_acxiom = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url_asi_tar) \
    .option("dbtable", f"({sql_query_vita_acxiom}) as subq") \
    .options(**jdbc_connection_properties) \
    .load()

# Create DP1
df_mms_m.createOrReplaceTempView(file_name)
df_vita_acxiom.createOrReplaceTempView("VITA_ACXIOM")

df_dp1 = spark.sql(f"""
    select a.CHID
          ,b.*
    from (select distinct CHID from {file_name} where CHID > 0 and CHID is not null) a
    left join VITA_ACXIOM b on a.CHID = b.chid_key
""")

df_dp1 = df_dp1.withColumn("VITA_ACXIOM_Flg", F.when(F.col("chid_key").isNotNull(), 1).otherwise(0)) \
               .drop("chid_key")

print("Frequency count for VITA_ACXIOM_Flg in dp1:")
df_dp1.groupBy("VITA_ACXIOM_Flg").count().show()

df_mms_m_dp1 = df_dp1.dropDuplicates(["CHID"])

spark.sql("DROP VIEW IF EXISTS VITA_ACXIOM")

# *** 2.B Append Attributes from VITA_MEMBERINFO Table **** ;
sql_query_vita_memberinfo = f"""
select chid_key
, state
, zip
, zip4
, dma_cd
, age_agg_ind as B0007
, initial_gender as B0008
, birth_dt_agg_ind as B0009
, employment_stat as B0010
, marital_stat_agg_ind as B0011
, association_idt as B0012
, account_stat
, kx_create_dt as B0015
, times_renewed_agg_act as B0016
, paid_through_dt as B0017
, corp_member_ind as B0018
, term_agg_act as B0021
, sec_gender_agg_act as B0029
, sec_birth_dt_agg_act as B0030
, sec_age_agg_act as B0031
, addr_move_dt as B0032
, addr_change_src as B0033
, sp_email_eligible_fl_agg_ind as B0035
, health_beauty as B0036
, ethnicity_group_cd as B0037
, health_allergy as B0038
, health_cholesterol as B0039
, health_diabetic as B0040
, health_homeopathic as B0041
, health_organic as B0042
, health_orthopedic as B0043
, business_owner as B0044
, travel_airline as B0045
, adult_age_ranges_present_in_household_plus as B0046
, occupation_detail as B0047
, vacation_travel_us as B0048
, vacation_travel_international as B0049
, vacation_travel_cruise as B0050
, text_messaging as B0051
, green_living as B0052
, business_owner_input_individual as B0053
, cycling as B0054
, donation_contribution as B0055
, dvd_video as B0056
, health_diet as B0057
, health_medical_supplies as B0058
, health_medical_supplies_orthopedic as B0059
, health_nutraceuticals_vitamins as B0060
, health_vitamins_nutrition as B0061
, membership_clubs as B0062
, pets as B0063
, household_income as B0064
, pc_broadband_user as B0065
, credit_card_user as B0066
, renewal_dt_agg_act as B0067
, move_dt as B0068
, response_channel as B0072
, register_ind as B0073
, optin_cat_pref_01 as B0074
, optin_cat_pref_02 as B0075
, optin_cat_pref_03 as B0076
, optin_cat_pref_04 as B0077
, optin_cat_pref_05 as B0078
, optin_cat_pref_06 as B0079
, optin_cat_pref_07 as B0080
, lifestage_segment as B0081
, relationship_seg as B0082
, rpm_score as B0083
, member_tenure_bymonth as B0090
from VITA_MEMBERINFO_m{version}
where chid_key is not null
"""

df_vita_memberinfo = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url_asi_tar) \
    .option("dbtable", f"({sql_query_vita_memberinfo}) as subq") \
    .options(**jdbc_connection_properties) \
    .load()

# Create DP2
df_vita_memberinfo.createOrReplaceTempView("VITA_MEMBERINFO")

df_dp2 = spark.sql(f"""
    select a.CHID
          ,b.*
    from (select distinct CHID from {file_name} where CHID > 0 and CHID is not null) a
    left join VITA_MEMBERINFO b on a.CHID = b.chid_key
""")

df_dp2 = df_dp2.withColumn("VITA_MEMBERINFO_Flg", F.when(F.col("chid_key").isNotNull(), 1).otherwise(0)) \
               .drop("chid_key")

df_mms_m_dp2 = df_dp2.dropDuplicates(["CHID"])

print("Frequency count for VITA_MEMBERINFO_Flg in dp2:")
df_dp2.groupBy("VITA_MEMBERINFO_Flg").count().show()

spark.sql("DROP VIEW IF EXISTS VITA_MEMBERINFO")

# *** 2.C0 Append Attributes from VITA_MISC Table **** ;
sql_query_vita_misc = f"""
select chid_key
, presence_title as C00001
, presence_name_mi as C00002
, presence_last as C00003
, presence_suffix as C00004
, presence_add1 as C00005
, presence_add2 as C00006
, presence_city as C00007
, gender_agg_ind as C00008
, presence_dateofbirth as C00009
, presence_sectitle as C00010
, presence_sec_name_mi as C00011
, presence_sec_last as C00012
, presence_secbirthdate as C00013
, current_order_create_dt_agg_act as C00016
, last_effort_dt as C00017
, effort_key_cd as C00018
, diversity_flag_agg_ind as C00019
, diversity_subgroup_agg_ind as C00020
, emailable_agg_ind as C00034
, num_times_selected_email_agg_ind as C00035
, globally_opted_in_fl as C00036
, active_sprel_overall_agg_act as C00048
, hist_sprel_overall_agg_act as C00049
from VITA_MISC_m{version}
where chid_key is not null
"""

df_vita_misc = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url_asi_tar) \
    .option("dbtable", f"({sql_query_vita_misc}) as subq") \
    .options(**jdbc_connection_properties) \
    .load()

# Create DP3
df_vita_misc.createOrReplaceTempView("VITA_MISC")

df_dp3 = spark.sql(f"""
    select a.CHID
          ,b.*
    from (select distinct CHID from {file_name} where CHID > 0 and CHID is not null) a
    left join VITA_MISC b on a.CHID = b.chid_key
""")

df_dp3 = df_dp3.withColumn("VITA_MISC_Flg", F.when(F.col("chid_key").isNotNull(), 1).otherwise(0)) \
               .drop("chid_key")

print("Frequency count for VITA_MISC_Flg in dp3:")
df_dp3.groupBy("VITA_MISC_Flg").count().show()

df_mms_m_dp3 = df_dp3.dropDuplicates(["CHID"])

spark.sql("DROP VIEW IF EXISTS VITA_MISC")

# **** 2.C1 Append Attributes from VITA_WEBDATA Table **** ;
sql_query_vita_webdata = f"""
select chid_key
, num_web_visits_past_month as C10001
, num_web_visits_past_3months as C10002
, num_asi_web_visits_past_3months as C10003
, num_community_web_visits_past_3months as C10004
, num_entertainment_web_visits_past_3months as C10005
, num_espanol_web_visits_past_3months as C10006
, num_food_web_visits_past_3months as C10007
, num_games_web_visits_past_3months as C10008
, online_dollars as C10009
, online_orders as C10010
, total_online_dollars as C10011
, total_online_purchases as C10012
, weeks_since_last_online_order as C10013
, online_average_amt_per_order as C10014
, num_health_web_visits_past_3months as C10015
, most_freq_web_channel_past_week as C10016
, most_freq_web_channel_past_month as C10017
, num_home_family_web_visits_past_3months as C10018
, num_member_benefits_web_visits_past_3months as C10019
, num_money_web_visits_past_3months as C10020
, num_travel_web_visits_past_3months as C10021
, num_work_web_visits_past_3months as C10022
, num_emails_opened_3_months_agg_ind as C10023
, num_times_clicked_thr_email_3_months_agg_ind as C10024
from VITA_WEBDATA_m{version}
where chid_key is not null
"""

df_vita_webdata = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url_asi_tar) \
    .option("dbtable", f"({sql_query_vita_webdata}) as subq") \
    .options(**jdbc_connection_properties) \
    .load()

# Create DP3b
df_vita_webdata.createOrReplaceTempView("VITA_WEBDATA")

df_dp3b = spark.sql(f"""
    select a.CHID
          ,b.*
    from (select distinct CHID from {file_name} where CHID > 0 and CHID is not null) a
    left join VITA_WEBDATA b on a.CHID = b.chid_key
""")

df_dp3b = df_dp3b.withColumn("VITA_WEBDATA_Flg", F.when(F.col("chid_key").isNotNull(), 1).otherwise(0)) \
                 .drop("chid_key")

print("Frequency count for VITA_WEBDATA_Flg in DP3b:")
df_dp3b.groupBy("VITA_WEBDATA_Flg").count().show()

df_mms_m_dp3b = df_dp3b.dropDuplicates(["CHID"])

spark.sql("DROP VIEW IF EXISTS VITA_WEBDATA")

# *** 2.D Append Attributes from VITA_NMAS Table **** ;
sql_query_vita_nmas = f"""
select chid_key
, nmas_1_hlth_dcl as D0001
, nmas_2_hlth_sec as D0002
, nmas_3_ren_hlth as D0003
, nmas_4_parent as D0004
, nmas_5_life_ins as D0005
, nmas_6_fin_sec as D0006
, nmas_7_consume as D0007
, nmas_8_employ as D0008
, nmas_9_invest as D0009
, nmas_10_credit as D0010
, nmas_11_retire as D0011
, nmas_12_hme_ins as D0012
, nmas_13_car_ins as D0013
, nmas_14_com_inv as D0014
, nmas_15_liv_com as D0015
, nmas_16_local as D0016
, nmas_17_charity as D0017
, nmas_18_advocy as D0018
, nmas_19_volunt as D0019
, nmas_20_travel as D0020
, nmas_21_leisure as D0021
, nmas_22_childrn as D0022
, nmas_23_motorng as D0023
, nmas_24_progsvc as D0024
, nmas_25_attitde as D0025
, nmas_26_webuse as D0026
, nmas_27_pubs as D0027
, nmas_28_media as D0028
, nmas_29_life as D0029
, nmas_30_online as D0030
, nmas_31_drvs as D0031
, nmas_32_job_sec as D0032
, nmas_33_medicar as D0033
, nmas_34_biznes as D0034
, nmas_35_renewal as D0035
, nmas_36_utility as D0036
, nmas_37_ren_ot as D0037
, nmas_38_deals as D0038
, nmas_39_ss as D0039
, nmas_40_ltc as D0040
, nmas_41_mail as D0041
, nmas_42_email as D0042
, nmas_43_phone as D0043
, nmas_44_cell as D0044
, nmas_45_web as D0045
, nmas_46_tech as D0046
, nmas_47_loclent as D0047
, nmas_48_social as D0048
, nmas_49_connect as D0049
, nmas_50_finance as D0050
from VITA_NMAS_m{version}
where chid_key is not null
"""

df_vita_nmas = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url_asi_tar) \
    .option("dbtable", f"({sql_query_vita_nmas}) as subq") \
    .options(**jdbc_connection_properties) \
    .load()

# Create DP4
df_vita_nmas.createOrReplaceTempView("VITA_NMAS")

df_dp4 = spark.sql(f"""
    select a.CHID
          ,b.*
    from (select distinct CHID from {file_name} where CHID > 0 and CHID is not null) a
    left join VITA_NMAS b on a.CHID = b.chid_key
""")

df_dp4 = df_dp4.withColumn("VITA_NMAS_Flg", F.when(F.col("chid_key").isNotNull(), 1).otherwise(0)) \
               .drop("chid_key")

print("Frequency count for VITA_NMAS_Flg in dp4:")
df_dp4.groupBy("VITA_NMAS_Flg").count().show()

df_mms_m_dp4 = df_dp4.dropDuplicates(["CHID"])

spark.sql("DROP VIEW IF EXISTS VITA_NMAS")

# *** 2.E Append Attributes from VITA_CONTACTHIST Table **** ;
sql_query_vita_contacthist = f"""
select chid_key
, num_curr_participation_lifestyle_agg_hhd as E0001
, num_hist_participation_lifestyle_agg_hhd as E0002
, last_participation_lifestyle_dt_agg_hhd as E0003
, orig_participation_lifestyle_dt_agg_hhd as E0004
, orig_participation_travel_dt_agg_hhd as E0006
, last_participation_travel_dt_agg_hhd as E0007
, num_hist_participation_travel_agg_hhd as E0008
, num_curr_participation_travel_agg_hhd as E0009
, orig_participation_financial_dt_agg_hhd as E0013
, last_participation_financial_dt_agg_hhd as E0014
, num_hist_participation_financial_agg_hhd as E0015
, num_curr_participation_financial_agg_hhd as E0016
, orig_participation_technology_dt_agg_hhd as E0017
, last_participation_technology_dt_agg_hhd as E0018
, num_curr_participation_technology_agg_hhd as E0019
, orig_participation_discounts_dt_agg_hhd as E0020
, last_participation_discounts_dt_agg_hhd as E0021
, num_hist_participation_discounts_agg_hhd as E0022
, num_curr_participation_discounts_agg_hhd as E0023
, orig_participation_overall_dt_agg_hhd as E0024
, last_participation_overall_dt_agg_hhd as E0025
, num_hist_participation_overall_agg_hhd as E0026
, num_curr_participation_overall_agg_hhd as E0027
, num_contact_travel_3_months_agg_hhd as E0028
, num_contact_travel_12_months_agg_hhd as E0029
, num_contact_health_3_months_agg_hhd as E0030
, num_contact_health_12_months_agg_hhd as E0031
, num_contact_financial_3_months_agg_hhd as E0032
, num_contact_financial_12_months_agg_hhd as E0033
, num_contact_technology_3_months_agg_hhd as E0034
, num_contact_technology_12_months_agg_hhd as E0035
, num_contact_discounts_3_months_agg_hhd as E0036
, num_contact_discounts_12_months_agg_hhd as E0037
, num_contact_aarp_3_months_agg_hhd as E0038
, num_contact_aarp_12_months_agg_hhd as E0039
, num_contact_overall_3_months_agg_hhd as E0040
, num_contact_overall_12_months_agg_hhd as E0041
, num_times_contacted_prior_to_renewal as E0042
from VITA_CONTACTHIST_m{version}
where chid_key is not null
"""

df_vita_contacthist = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url_asi_tar) \
    .option("dbtable", f"({sql_query_vita_contacthist}) as subq") \
    .options(**jdbc_connection_properties) \
    .load()

# Create DP5
df_vita_contacthist.createOrReplaceTempView("VITA_CONTACTHIST")

df_dp5 = spark.sql(f"""
    select a.CHID
          ,b.*
    from (select distinct CHID from {file_name} where CHID > 0 and CHID is not null) a
    left join VITA_CONTACTHIST b on a.CHID = b.chid_key
""")

df_dp5 = df_dp5.withColumn("VITA_CONTACTHIST_Flg", F.when(F.col("chid_key").isNotNull(), 1).otherwise(0)) \
               .drop("chid_key")

print("Frequency count for VITA_CONTACTHIST_Flg in dp5:")
df_dp5.groupBy("VITA_CONTACTHIST_Flg").count().show()

df_mms_m_dp5 = df_dp5.dropDuplicates(["CHID"])

spark.sql("DROP VIEW IF EXISTS VITA_CONTACTHIST")

# *** 2.F Append Attributes from VITA_CENSUS Table **** ;
sql_query_vita_census = f"""
select chid_key
, geo_cd_2010
, cens_age_hh_percent_with_householder_age_55_64 as F0001
, cens_age_hh_percent_with_householder_age_65_74 as F0002
, cens_age_hh_percent_with_householder_age_75_84 as F0003
, cens_age_hh_percent_with_householder_age_85_plus as F0004
, cens_age_pop_percent_60_64 as F0005
, cens_lang_hh_percent_spanish_speaking as F0006
, cens_homval_home_value_cbsa_index as F0007
, cens_inc_hh_median_household_income as F0008
, cens_move_occhu_median_length_of_residence as F0009
, cens_homval_oohu_median_home_value as F0010
, cens_ethnic_pop_percent_asian_only_hisp as F0011
, cens_ethnic_pop_percent_asian_only as F0012
, cens_ethnic_pop_percent_black_only_hisp as F0013
, cens_ethnic_pop_percent_black_only as F0014
from VITA_CENSUS_m{version}
where chid_key is not null
"""

df_vita_census = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url_asi_tar) \
    .option("dbtable", f"({sql_query_vita_census}) as subq") \
    .options(**jdbc_connection_properties) \
    .load()

# Create DP6
df_vita_census.createOrReplaceTempView("VITA_CENSUS")

df_dp6 = spark.sql(f"""
    select a.CHID
          ,b.*
    from (select distinct CHID from {file_name} where CHID > 0 and CHID is not null) a
    left join VITA_CENSUS b on a.CHID = b.chid_key
""")

df_dp6 = df_dp6.withColumn("VITA_CENSUS_Flg", F.when(F.col("chid_key").isNotNull(), 1).otherwise(0)) \
               .drop("chid_key")

print("Frequency count for VITA_CENSUS_Flg in dp6:")
df_dp6.groupBy("VITA_CENSUS_Flg").count().show()

df_mms_m_dp6 = df_dp6.dropDuplicates(["CHID"])

spark.sql("DROP VIEW IF EXISTS VITA_CENSUS")

# *** 2.G Append Attributes from VITA_TRANSINFO Table **** ;
sql_query_vita_transinfo = f"""
select chid_key
, num_hist_trans_participation_health_agg_hhd as G0001
, last_trans_participation_health_dt_agg_hhd as G0002
, orig_trans_participation_health_dt_agg_hhd as G0003
, num_hist_trans_participation_discounts_agg_hhd as G0004
, last_trans_participation_discounts_dt_agg_hhd as G0005
, orig_trans_participation_discounts_dt_agg_hhd as G0006
, last_trans_participation_travel_dt_agg_hhd as G0013
, orig_trans_participation_travel_dt_agg_hhd as G0014
, num_hist_trans_participation_lifestyle_agg_hhd as G0015
, last_trans_participation_lifestyle_dt_agg_hhd as G0016
, orig_trans_participation_lifestyle_dt_agg_hhd as G0017
, num_hist_trans_participation_travel_agg_hhd as G0018
from VITA_TRANSINFO_m{version}
where chid_key is not null
"""

df_vita_transinfo = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url_asi_tar) \
    .option("dbtable", f"({sql_query_vita_transinfo}) as subq") \
    .options(**jdbc_connection_properties) \
    .load()

# Create DP7
df_vita_transinfo.createOrReplaceTempView("VITA_TRANSINFO")

df_dp7 = spark.sql(f"""
    select a.CHID
          ,b.*
    from (select distinct CHID from {file_name} where CHID > 0 and CHID is not null) a
    left join VITA_TRANSINFO b on a.CHID = b.chid_key
""")

df_dp7 = df_dp7.withColumn("VITA_TRANSINFO_Flg", F.when(F.col("chid_key").isNotNull(), 1).otherwise(0)) \
               .drop("chid_key")

print("Frequency count for VITA_TRANSINFO_Flg in dp7:")
df_dp7.groupBy("VITA_TRANSINFO_Flg").count().show()

df_mms_m_dp7 = df_dp7.dropDuplicates(["CHID"])

spark.sql("DROP VIEW IF EXISTS VITA_TRANSINFO")

# *** 2.H Append Attributes from ACXIOM_DEMOGRAPHICS Table **** ;
sql_query_acxiom_demographics = f"""
select chid_key
, computers as H0014
, home_and_garden_composite as H0028
, date_of_birth_1st_individual_premier as H0069
, date_of_birth_2nd_individual_premier as H0074
, race_code_input_individual_premier as H0135
, email_append_available as H0136
, gaming_lottery as H0137
, gaming_casino as H0138
, sweepstakes_contests as H0139
, dob_input_individual_default_to_1st_individual_premier as H0142
, wireless_product_buyer as H0144
, fashion as H0145
, history_military as H0146
, smoking_tobacco as H0147
, celebrities as H0148
, theater_performing_arts as H0149
, science_space as H0150
, strange_and_unusual as H0151
, career_improvement as H0152
, food_wines as H0153
, arts as H0154
, reading_general as H0155
, reading_best_sellers as H0156
, reading_religious_inspirational as H0157
, reading_science_fiction as H0158
, reading_magazines as H0159
, reading_audio_books as H0160
, cooking_general as H0161
, cooking_gourmet as H0162
, cooking_low_fat as H0163
, foods_vegetarian as H0164
, foods_natural as H0165
, exercise_running_jogging as H0166
, exercise_walking as H0167
, exercise_aerobic as H0168
, crafts as H0169
, photography as H0170
, aviation as H0171
, auto_work as H0172
, sewing_knitting_needlework as H0173
, woodworking as H0174
, games_board_games_puzzles as H0175
, music_home_stereo as H0176
, music_player as H0177
, music_collector as H0178
, music_avid_listener as H0179
, movie_collector as H0180
, tv_cable as H0181
, games_video_games as H0182
, movies_at_home as H0183
, tv_satellite_dish as H0184
, health_medical as H0185
, dieting_weight_loss as H0186
, self_improvement as H0187
, cat_owner as H0188
, dog_owner as H0189
, house_plants as H0190
, Parenting as H0191
, childrens_interests as H0192
, spectator_sports_auto_motorcycle_racing as H0193
, spectator_sports_football as H0194
, Spectator_Sports_Baseball as H0195
, spectator_sports_basketball as H0196
, spectator_sports_hockey as H0197
, spectator_sports_soccer as H0198
, spectator_sports_tennis as H0199
, collectibles_general as H0200
, collectibles_stamps as H0201
, collectibles_coins as H0202
, collectibles_arts as H0203
, collectibles_antiques as H0204
, investments_personal as H0205
, investments_real_estate as H0206
, investments_stocks_bonds as H0207
, pc_internet_online_service_user as H0208
, pc_modem_owner as H0209
, games_computer_games as H0210
, wireless_cellular_phone_owner as H0211
, consumer_electronics as H0212
, Fishing as H0213
, camping_hiking as H0214
, hunting_shooting as H0215
, boating_sailing as H0216
, water_sports as H0217
, scuba_diving as H0218
, biking_mountain_biking as H0219
, environmental_issues as H0220
, tennis as H0221
, snow_skiing as H0222
, motorcycling as H0223
, equestrian as H0224
, home_furnishings_decorating as H0225
, home_improvement as H0226
, gardening as H0227
, sports_grouping as H0228
, reading_grouping as H0229
, cooking_food_grouping as H0230
, collectibles_and_antiques_grouping as H0231
, boat_owner as H0232
, career as H0233
, christian_families as H0234
, collectibles_sports_memorabilia as H0235
, education_online as H0236
, tv_hdtv_satellite_dish as H0237
, investments_foreign as H0238
, nascar as H0239
, reading_financial_newsletter_subscribers as H0240
, beauty_cosmetics as H0241
, home_improvement_do_it_yourselfers as H0242
, money_seekers as H0243
, our_nations_heritage as H0244
, spectator_sports_tv_sports as H0245
, home_video_recording as H0246
, collector_avid as H0247
, home_living as H0248
, diy_living as H0249
, sporty_living as H0250
, upscale_living as H0251
, cultural_artistic_living as H0252
, Highbrow as H0253
, high_tech_living as H0254
, power_boating as H0255
, common_living as H0256
, professional_living as H0257
, broader_living as H0258
, Chiphead as H0259
, tv_guide as H0260
, home_care_maintenance as H0261
, home_care_safety as H0262
, novelty_cats as H0263
, home_heating_cooling as H0264
, political_party_3rd_individual as H0265
, political_party_4th_individual as H0266
, political_party_5th_individual as H0267
, home_heat_source as H0268
, home_pool_present as H0269
, pc_operating_system as H0274
, occupation_3rd_individual as H0275
, occupation_4th_individual as H0276
, occupation_5th_individual as H0277
, age_in_two_year_increments_3rd_individual as H0278
, date_of_birth_3rd_individual_yyyy_mm as H0279
, pc_software_recency_date as H0280
, credit_card_new_issue as H0281
, education_3rd_individual as H0283
, education_4th_individual as H0284
, education_5th_individual as H0285
, height_input_individual as H0286
, race_code_3rd_individual as H0287
, race_code_4th_individual as H0288
, race_code_5th_individual as H0289
, weight_input_individual as H0290
, suppression_mail_dma as H0291
, ts_pa_match_level_indicator as H0292
, ts_pa_overall_match_indicator as H0293
, ts_pa_phone_1 as H0294
, LACConsumer_Link as H0295
, LACAddress_Link as H0296
, health_beauty as H0297
, ethnicity_group_cd as H0298
, dollars_health as H0299
, health_allergy as H0300
, health_cholesterol as H0301
, health_diabetic as H0302
, health_homeopathic as H0303
, health_organic as H0304
, health_orthopedic as H0305
, business_owner as H0306
, occupation_detail as H0307
, travel_airline as H0308
, vacation_travel_cruise as H0309
, vacation_travel_us as H0310
, vacation_travel_international as H0311
, online_orders as H0312
, online_dollars as H0313
, weeks_since_last_online_order as H0314
, online_average_amt_per_order as H0315
, text_messaging as H0316
, green_living as H0317
, business_owner_input_individual as H0318
, cycling as H0319
, donation_contribution as H0320
, dvd_video as H0321
, health_diet as H0322
, health_medical_supplies as H0323
, health_medical_supplies_orthopedic as H0324
, health_nutraceuticals_vitamins as H0325
, health_vitamins_nutrition as H0326
, membership_clubs as H0327
, total_online_dollars as H0328
, total_online_purchases as H0329
, pets as H0330
, household_income as H0331
, pc_broadband_user as H0332
, credit_card_user as H0333
, move_date as H0334
, adult_age_ranges_present_in_household_plus as H0335
from VITA_ACXIOM_DEMOGRAPHICS_m{version}
where chid_key is not null
"""

df_acxiom_demographics = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url_asi_tar) \
    .option("dbtable", f"({sql_query_acxiom_demographics}) as subq") \
    .options(**jdbc_connection_properties) \
    .load()

# Create DP8
df_acxiom_demographics.createOrReplaceTempView("ACXIOM_DEMOGRAPHICS")

df_dp8 = spark.sql(f"""
    select a.CHID
          ,b.*
    from (select distinct CHID from {file_name} where CHID > 0 and CHID is not null) a
    left join ACXIOM_DEMOGRAPHICS b on a.CHID = b.chid_key
""")

df_dp8 = df_dp8.withColumn("ACXIOM_DEMOGRAPHICS_Flg", F.when(F.col("chid_key").isNotNull(), 1).otherwise(0)) \
               .drop("chid_key")

print("Frequency count for ACXIOM_DEMOGRAPHICS_Flg in dp8:")
df_dp8.groupBy("ACXIOM_DEMOGRAPHICS_Flg").count().show()

df_mms_m_dp8 = df_dp8.dropDuplicates(["CHID"])

spark.sql("DROP VIEW IF EXISTS ACXIOM_DEMOGRAPHICS")

# *** 2.I Append Attributes from CENSUS Table **** ;
sql_query_census = """
select CENS_STATE_CODE+CENS_COUNTY_CODE+CENS_CENSUS_TRACT_AND_BLOCK_GROUP as geo_cd_2010_key
, CENS_COUNT_POPULATION as I0001
, CENS_COUNT_POPULATION_AGE_3_PLUS as I0002
, CENS_COUNT_POPULATION_AGE_15_PLUS as I0003
, CENS_COUNT_LABOR_FORCE as I0004
, CENS_COUNT_WORKERS as I0005
, CENS_COUNT_HOUSEHOLDS as I0006
, CENS_COUNT_FAMILY_HOUSEHOLDS as I0007
, CENS_COUNT_HOUSING_UNITS as I0008
, CENS_COUNT_OWNER_OCCUPIED_HOUSING_UNITS as I0009
, CENS_COUNT_RENTAL_UNITS as I0010
, CENS_DENSITY_POPULATION_PER_SQUARE_MILE as I0011
, CENS_DENSITY_PERSONS_PER_HH_FOR_POP_IN_HH as I0012
, CENS_AGE_POP_PERCENT_0_4 as I0013
, CENS_AGE_POP_PERCENT_0_9 as I0014
, CENS_AGE_POP_PERCENT_0_17 as I0015
, CENS_AGE_POP_PERCENT_0_24 as I0016
, CENS_AGE_POP_PERCENT_5_9 as I0017
, CENS_AGE_POP_PERCENT_10_13 as I0018
, CENS_AGE_POP_PERCENT_14_17 as I0019
, CENS_AGE_POP_PERCENT_18_20 as I0020
, CENS_AGE_POP_PERCENT_18_65 as I0021
, CENS_AGE_POP_PERCENT_18_99_PLUS as I0022
, CENS_AGE_POP_PERCENT_21_24 as I0023
, CENS_AGE_POP_PERCENT_25_29 as I0024
, CENS_AGE_POP_PERCENT_25_34 as I0025
, CENS_AGE_POP_PERCENT_30_34 as I0026
, CENS_AGE_POP_PERCENT_35_39 as I0027
, CENS_AGE_POP_PERCENT_35_44 as I0028
, CENS_AGE_POP_PERCENT_40_44 as I0029
, CENS_AGE_POP_PERCENT_45_49 as I0030
, CENS_AGE_POP_PERCENT_45_54 as I0031
, CENS_AGE_POP_PERCENT_50_54 as I0032
, CENS_AGE_POP_PERCENT_55_59 as I0033
, CENS_AGE_POP_PERCENT_55_64 as I0034
, CENS_AGE_POP_PERCENT_65_69 as I0036
, CENS_AGE_POP_PERCENT_65_99_PLUS as I0037
, CENS_AGE_POP_PERCENT_70_74 as I0038
, CENS_AGE_POP_PERCENT_75_79 as I0039
, CENS_AGE_POP_PERCENT_75_99_PLUS as I0040
, CENS_AGE_POP_PERCENT_80_84 as I0041
, CENS_AGE_POP_PERCENT_85_99_PLUS as I0042
, CENS_AGE_POP_MEDIAN_AGE as I0043
, CENS_AGE_POP_MEDIAN_AGE_OF_MALES as I0044
, CENS_AGE_POP_MEDIAN_AGE_OF_FEMALES as I0045
, CENS_AGE_POP_MEDIAN_AGE_OF_ADULTS_18_PLUS as I0046
, CENS_AGE_POP_MEDIAN_AGE_OF_ADULT_MALES_18_PLUS as I0047
, CENS_AGE_POP_MEDIAN_AGE_OF_ADULT_FEMALES_18_PLUS as I0048
, CENS_AGE_HH_PERCENT_WITH_HOUSEHOLDER_AGE_15_24 as I0049
, CENS_AGE_HH_PERCENT_WITH_HOUSEHOLDER_AGE_25_34 as I0050
, CENS_AGE_HH_PERCENT_WITH_HOUSEHOLDER_AGE_35_44 as I0051
, CENS_AGE_HH_PERCENT_WITH_HOUSEHOLDER_AGE_45_54 as I0052
, CENS_BUILT_HU_PERCENT_BUILT_LT1940 as I0057
, CENS_BUILT_HU_PERCENT_BUILT_1940_TO_1949 as I0058
, CENS_BUILT_HU_PERCENT_BUILT_1950_TO_1959 as I0059
, CENS_BUILT_HU_PERCENT_BUILT_1960_TO_1969 as I0060
, CENS_BUILT_HU_PERCENT_BUILT_1970_TO_1979 as I0061
, CENS_BUILT_HU_PERCENT_BUILT_1980_TO_1989 as I0062
, CENS_BUILT_HU_PERCENT_BUILT_1990_TO_1999 as I0063
, CENS_BUILT_HU_PERCENT_BUILT_2000_TO_2004 as I0064
, CENS_BUILT_HU_PERCENT_BUILT_2005_PLUS as I0065
, CENS_BUILT_HU_MEDIAN_HOUSING_UNIT_AGE as I0066
, CENS_CHILD_HH_PERCENT_WITH_PERSONS_LT18 as I0067
, CENS_CHILD_HH_PERCENT_FAM_WITH_PERSONS_LT18 as I0068
, CENS_CHILD_HH_PERCENT_MARR_COUPLE_FAM_WITH_PERSONS_LT18 as I0069
, CENS_CHILD_HH_PERCENT_OTH_FAM_WITH_PERSONS_LT18 as I0070
, CENS_CHILD_HH_PERCENT_MALE_HOH_FAM_WITH_PERSONS_LT18 as I0071
, CENS_CHILD_HH_PERCENT_FEMALE_HOH_FAM_WITH_PERSONS_LT18 as I0072
, CENS_CHILD_HH_PERCENT_NON_FAM_WITH_PERSONS_LT18 as I0073
, CENS_CHILD_HH_PERCENT_MALE_HOH_NFAM_WITH_PERSONS_LT18 as I0074
, CENS_CHILD_HH_PERCENT_FEMALE_HOH_NFAM_WITH_PERSONS_LT18 as I0075
, CENS_CHILD_HH_PERCENT_WITHOUT_PERSONS_LT18 as I0076
, CENS_CHILD_HH_PERCENT_FAM_WITHOUT_PERSONS_LT18 as I0077
, CENS_CHILD_HH_PERCENT_MARR_COUPLE_FAM_WITHOUT_PERSONS_LT18 as I0078
, CENS_CHILD_HH_PERCENT_OTHER_FAM_WITHOUT_PERSONS_LT18 as I0079
, CENS_CHILD_HH_PERCENT_MALE_HOH_FAM_WITHOUT_PERSONS_LT18 as I0080
, CENS_CHILD_HH_PERCENT_FEMALE_HOH_FAM_WITHOUT_PERSONS_LT18 as I0081
, CENS_CHILD_HH_PERCENT_NON_FAM_WITHOUT_PERSONS_LT18 as I0082
, CENS_CHILD_HH_PERCENT_MALE_HOH_NFAM_WITHOUT_PERSONS_LT18 as I0083
, CENS_CHILD_HH_PERCENT_FEMALE_HOH_NFAM_WITHOUT_PERSONS_LT18 as I0084
, CENS_COMMUTE_COMMUTER_PERCENT_TRAV_TO_WORK_LT_30_MIN as I0085
, CENS_COMMUTE_COMMUTER_PERCENT_TRAV_TO_WORK_60_89_MIN as I0086
, CENS_COMMUTE_COMMUTER_PERCENT_TRAV_TO_WORK_90_PLUS_MINS as I0087
, CENS_COMMUTE_COMMUTER_AVG_TRAV_TIME_TO_WORK as I0088
, CENS_COMMUTE_COMMUTER_MEDIAN_TRAV_TIME_TO_WORK as I0089
, CENS_COMMUTE_WRKRS_PERCENT_CARPOOLED_TO_WORK as I0090
, CENS_COMMUTE_WRKRS_PERCENT_DROVE_TO_WORK_ALONE as I0091
, CENS_COMMUTE_WRKRS_PERCENT_PUBLIC_TRANS_TO_WORK as I0092
, CENS_COMMUTE_WRKRS_PERCENT_WORK_AT_HOME as I0093
, CENS_EARN_HH_PERCENT_WITH_EARNINGS as I0094
, CENS_EARN_HH_PERCENT_NO_EARNINGS as I0095
, CENS_EARN_HH_PERCENT_WITH_WAGE_SALARY_INCOME as I0096
, CENS_EARN_HH_PERCENT_NO_WAGE_SALARY_INCOME as I0097
, CENS_EARN_HH_PERCENT_WITH_SELF_EMPLOYMENT_INCOME as I0098
, CENS_EARN_HH_PERCENT_NO_SELF_EMPLOYMENT_INCOME as I0099
, CENS_EARN_HH_PERCENT_WITH_INTEREST_DIV_RENTAL_INCOME as I0100
, CENS_EARN_HH_PERCENT_NO_INTEREST_DIV_RENTAL_INCOME as I0101
, CENS_EARN_HH_PERCENT_WITH_SOCIAL_SECURITY_INCOME as I0102
, CENS_EARN_HH_PERCENT_NO_SOCIAL_SECURITY_INCOME as I0103
, CENS_EARN_HH_PERCENT_WITH_SUPPLEMENTAL_SECURITY_INC as I0104
, CENS_EARN_HH_PERCENT_NO_SUPPLEMENTAL_SECURITY_INC as I0105
, CENS_EARN_HH_PERCENT_WITH_PUBLIC_ASSISTANCE_INCOME as I0106
, CENS_EARN_HH_PERCENT_NO_PUBLIC_ASSISTANCE_INCOME as I0107
, CENS_EARN_HH_PERCENT_WITH_RETIREMENT_INCOME as I0108
, CENS_EARN_HH_PERCENT_NO_RETIREMENT_INCOME as I0109
, CENS_EARN_HH_PERCENT_WITH_OTHER_TYPE_OF_INCOME as I0110
, CENS_EARN_HH_PERCENT_NO_OTHER_TYPE_OF_INCOME as I0111
, CENS_EDUC_POP25_PLUS_PERCENT_GRT_9TH_GRADE as I0112
, CENS_EDUC_POP25_PLUS_PERCENT_9_12TH_GR_NO_DIPLOMA as I0113
, CENS_EDUC_POP25_PLUS_PERCENT_HIGH_SCHOOL_GRAD as I0114
, CENS_EDUC_POP25_PLUS_PERCENT_SOME_COLLEGE as I0115
, CENS_EDUC_POP25_PLUS_PERCENT_ASSOCIATE_DEGREE as I0116
, CENS_EDUC_POP25_PLUS_PERCENT_BACHELOR_DEGREE as I0117
, CENS_EDUC_POP25_PLUS_PERCENT_PROF_DEGREE as I0118
, CENS_EDUC_POP25_PLUS_MEDIAN_EDUCATION_ATTAINED as I0119
, CENS_EMPLOY_LABF_PERCENT_IN_ARMED_FORCES as I0120
, CENS_EMPLOY_LABF_PERCENT_EMPLOYED as I0121
, CENS_EMPLOY_LABF_PERCENT_UNEMPLOYED as I0122
, CENS_EMPLOY_POP16_PLUS_PERCENT_NOT_IN_LABOR_FORCE as I0123
, CENS_EMPLOY_LABF_PERCENT_FEMALE as I0124
, CENS_EMPLOY_POPFEM16_PLUS_PERCENT_IN_LABOR_FORCE as I0125
, CENS_EMPLOY_POP18_PLUS_PERCENT_CIVILIAN_VETS as I0126
, CENS_ENROLL_POP3_PLUS_PERCENT_ENROLLED_IN_PRE_SCHOOL as I0127
, CENS_ENROLL_POP3_PLUS_PERCENT_ENROLLED_IN_KNDERGARTEN as I0128
, CENS_ENROLL_POP3_PLUS_PERCENT_ENROLLED_IN_ELEM_GR_1_8 as I0129
, CENS_ENROLL_POP3_PLUS_PERCENT_ENROLLED_IN_HS_GR_9_12 as I0130
, CENS_ENROLL_POP3_PLUS_PERCENT_ENROLLED_IN_COLLEGE as I0131
, CENS_ENROLL_POP3_PLUS_PERCENT_ENROLLED_IN_GRAD_SCHOOL as I0132
, CENS_ENROLL_POP3_PLUS_PERCENT_NOT_ENROLLED_IN_SCHOOL as I0133
, CENS_ENROLL_POP3_PLUS_PERCENT_ENROLLED_IN_PUBLIC_SCHOOL as I0134
, CENS_ENROLL_POP3_PLUS_PERCENT_ENROLLED_IN_PRIVATE_SCHOOL as I0135
, CENS_ETHNIC_POP_PERCENT_WHITE_ONLY as I0136
, CENS_ETHNIC_POP_PERCENT_AM_IND_AK_NAT_ONLY as I0138
, CENS_ETHNIC_POP_PERCENT_HI_NAT_OTH_PAC_ONLY as I0140
, CENS_ETHNIC_POP_PERCENT_SOME_OTHER_RACE_ONLY as I0141
, CENS_ETHNIC_POP_PERCENT_TWO_OR_MORE_RACES as I0142
, CENS_ETHNIC_POP_PERCENT_NON_HISPANIC as I0143
, CENS_ETHNIC_POP_PERCENT_WHITE_ONLY_NON_HISP as I0144
, CENS_ETHNIC_POP_PERCENT_BLACK_ONLY_NON_HISP as I0145
, CENS_ETHNIC_POP_PERCENT_AM_IND_AK_NAT_ONLY_NON_HISP as I0146
, CENS_ETHNIC_POP_PERCENT_ASIAN_ONLY_NON_HISP as I0147
, CENS_ETHNIC_POP_PERCENT_HI_NAT_OTH_PAC_ONLY_NON_HISP as I0148
, CENS_ETHNIC_POP_PERCENT_SOME_OTHER_RACE_ONLY_NON_HISP as I0149
, CENS_ETHNIC_POP_PERCENT_TWO_OR_MORE_RACES_NON_HISP as I0150
, CENS_ETHNIC_POP_PERCENT_HISPANIC as I0151
, CENS_ETHNIC_POP_PERCENT_WHITE_ONLY_HISP as I0152
, CENS_ETHNIC_POP_PERCENT_AM_IND_AK_NAT_ONLY_HISP as I0154
, CENS_ETHNIC_POP_PERCENT_HI_NAT_OTHER_PAC_ONLY_HISP as I0156
, CENS_ETHNIC_POP_PERCENT_SOME_OTHER_RACE_ONLY_HISP as I0157
, CENS_ETHNIC_POP_PERCENT_TWO_OR_MORE_RACES_HISP as I0158
, CENS_ETHNIC_HH_PERCENT_HOH_NON_HISPANIC as I0159
, CENS_ETHNIC_HH_PERCENT_HOH_NONHISP_WHITE_ONLY as I0160
, CENS_ETHNIC_HH_PERCENT_HOH_NONHISP_BLACK_ONLY as I0161
, CENS_ETHNIC_HH_PERCENT_HOH_NONHISP_AM_IND_AK_NAT_ONLY as I0162
, CENS_ETHNIC_HH_PERCENT_HOH_NONHISP_ASIAN_ONLY as I0163
, CENS_ETHNIC_HH_PERCENT_HOH_NONHISP_HI_NAT_OTH_PI_ONLY as I0164
, CENS_ETHNIC_HH_PERCENT_HOH_NONHISP_SOME_OTH_RACE_ONLY as I0165
, CENS_ETHNIC_HH_PERCENT_HOH_NONHISP_TWO_OR_MORE_RACES as I0166
, CENS_ETHNIC_HH_PERCENT_HOH_HISPANIC as I0167
, CENS_ETHNIC_HH_PERCENT_HOH_HISP_WHITE_ONLY as I0168
, CENS_ETHNIC_HH_PERCENT_HOH_HISP_BLACK_ONLY as I0169
, CENS_ETHNIC_HH_PERCENT_HOH_HISP_AM_IND_AK_NAT_ONLY as I0170
, CENS_ETHNIC_HH_PERCENT_HOH_HISP_ASIAN_ONLY as I0171
, CENS_ETHNIC_HH_PERCENT_HOH_HISP_HI_NAT_OTH_PI_ONLY as I0172
, CENS_ETHNIC_HH_PERCENT_HOH_HISP_SOME_OTHER_RACE_ONLY as I0173
, CENS_ETHNIC_HH_PERCENT_HOH_HISP_TWO_OR_MORE_RACES as I0174
, CENS_GENDER_POP_PERCENT_MALE as I0175
, CENS_GENDER_POP_PERCENT_FEMALE as I0176
, CENS_GRPQTRS_POP_PERCENT_GROUP_QUARTERS as I0177
, CENS_GRPQTRS_POP_PERCENT_INSTITUTIONS as I0178
, CENS_GRPQTRS_POP_PERCENT_CORRECTIONAL_INSTITUTIONS as I0179
, CENS_GRPQTRS_POP_PERCENT_NURSING_HOMES as I0180
, CENS_GRPQTRS_POP_PERCENT_OTH_INSTITUTION_GRP_QTRS as I0181
, CENS_GRPQTRS_POP_PERCENT_NON_INSTITUTIONAL_GRP_QTRS as I0182
, CENS_GRPQTRS_POP_PERCENT_COLLEGE_DORMS as I0183
, CENS_GRPQTRS_POP_PERCENT_MILITARY_QTRS as I0184
, CENS_GRPQTRS_POP_PERCENT_OTH_NON_INSTITUTION_GRP_QTRS as I0185
, CENS_HEAT_OCCHU_PERCENT_UTILITY_GAS_HEAT as I0186
, CENS_HEAT_OCCHU_PERCENT_BOTTLE_OR_TANK_LP_GAS_HEAT as I0187
, CENS_HEAT_OCCHU_PERCENT_ELECTRIC_HEAT as I0188
, CENS_HEAT_OCCHU_PERCENT_OIL_OR_KEROSENE_HEAT as I0189
, CENS_HEAT_OCCHU_PERCENT_COAL_OR_COKE_HEAT as I0190
, CENS_HEAT_OCCHU_PERCENT_WOOD_HEAT as I0191
, CENS_HEAT_OCCHU_PERCENT_SOLAR_HEAT as I0192
, CENS_HEAT_OCCHU_PERCENT_OTHER_HEAT as I0193
, CENS_HEAT_OCCHU_PERCENT_NO_HEAT as I0194
, CENS_HHSIZE_HH_PERCENT_1_PERSON as I0195
, CENS_HHSIZE_HH_PERCENT_2_PERSONS as I0196
, CENS_HHSIZE_HH_PERCENT_3_PERSONS as I0197
, CENS_HHSIZE_HH_PERCENT_4_PERSONS as I0198
, CENS_HHSIZE_HH_PERCENT_5_PERSONS as I0199
, CENS_HHSIZE_HH_PERCENT_6_PERSONS as I0200
, CENS_HHSIZE_HH_PERCENT_7_PLUS_PERSONS as I0201
, CENS_HHSIZE_HH_AVERAGE_HOUSEHOLD_SIZE as I0202
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_UNDER_10K as I0203
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_10_14K as I0204
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_15_19K as I0205
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_20_24K as I0206
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_25_29K as I0207
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_30_34K as I0208
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_35_39K as I0209
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_40_49K as I0210
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_50_59K as I0211
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_60_69K as I0212
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_70_79K as I0213
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_80_89K as I0214
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_90_99K as I0215
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_100_124K as I0216
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_125_149K as I0217
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_150_174K as I0218
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_175_199K as I0219
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_200_249K as I0220
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_250_299K as I0221
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_300_399K as I0222
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_400_499K as I0223
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_500_749K as I0224
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_750_999K as I0225
, CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_1M_OR_MORE as I0226
, CENS_HUSTR_HU_PERCENT_1_UNIT_ATTACHED as I0228
, CENS_HUSTR_HU_PERCENT_1_UNIT_DETACHED as I0229
, CENS_HUSTR_HU_PERCENT_2_UNITS as I0230
, CENS_HUSTR_HU_PERCENT_3_4_UNITS as I0231
, CENS_HUSTR_HU_PERCENT_5_9_UNITS as I0232
, CENS_HUSTR_HU_PERCENT_10_19_UNITS as I0233
, CENS_HUSTR_HU_PERCENT_20_49_UNITS as I0234
, CENS_HUSTR_HU_PERCENT_50_PLUS_UNITS as I0235
, CENS_HUSTR_HU_PERCENT_MOBILE_HOME as I0236
, CENS_HUSTR_HU_PERCENT_BOAT_RV_TENT_ETC as I0237
, CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_UNDER_10K as I0238
, CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_10_14K as I0239
, CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_15_19K as I0240
, CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_20_24K as I0241
, CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_25_29K as I0242
, CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_30_34K as I0243
, CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_35_39K as I0244
, CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_40_44K as I0245
, CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_45_49K as I0246
, CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_50_59K as I0247
, CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_60_74K as I0248
, CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_75_99K as I0249
, CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_100_124K as I0250
, CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_125_149K as I0251
, CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_150_199K as I0252
, CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_200_249K as I0253
, CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_250_499K as I0254
, CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_500K_OR_MORE as I0255
, CENS_INC_HH_MEDIAN_FAMILY_HOUSEHOLD_INCOME as I0257
, CENS_INC_HH_MEDIAN_NON_FAMILY_HOUSEHOLD_INCOME as I0258
, CENS_INC_POP_PER_CAPITA_INCOME as I0259
, CENS_INC_HH_MED_INC_HOUSEHOLDER_AGE_UNDER_25 as I0260
, CENS_INC_HH_MED_INC_HOUSEHOLDER_AGE_25_34 as I0261
, CENS_INC_HH_MED_INC_HOUSEHOLDER_AGE_35_44 as I0262
, CENS_INC_HH_MED_INC_HOUSEHOLDER_AGE_45_54 as I0263
, CENS_INC_HH_MED_INC_HOUSEHOLDER_AGE_55_64 as I0264
, CENS_INC_HH_MED_INC_HOUSEHOLDER_AGE_65_74 as I0265
, CENS_INC_HH_MED_INC_HOUSEHOLDER_AGE_75_PLUS as I0266
, CENS_INDUS_EMPLD_PERCENT_AGRIC_FOREST_FISH_AND_HUNT as I0267
, CENS_INDUS_EMPLD_PERCENT_MINING as I0268
, CENS_INDUS_EMPLD_PERCENT_CONSTRUCTION as I0269
, CENS_INDUS_EMPLD_PERCENT_MANUFACTURING as I0270
, CENS_INDUS_EMPLD_PERCENT_WHOLESALE_TRADE as I0271
, CENS_INDUS_EMPLD_PERCENT_RETAIL_TRADE as I0272
, CENS_INDUS_EMPLD_PERCENT_TRANSPORT_AND_WAREHOUSING as I0273
, CENS_INDUS_EMPLD_PERCENT_UTILITIES as I0274
, CENS_INDUS_EMPLD_PERCENT_INFORMATION as I0275
, CENS_INDUS_EMPLD_PERCENT_FINANCE_AND_INSURANCE as I0276
, CENS_INDUS_EMPLD_PERCENT_REAL_ESTATE_RENTAL_LEASING as I0277
, CENS_INDUS_EMPLD_PERCENT_PROF_SCI_AND_TECH_SVCS as I0278
, CENS_INDUS_EMPLD_PERCENT_MGMT_OF_COS_AND_ENTERPRISES as I0279
, CENS_INDUS_EMPLD_PERCENT_ADMIN_SUPP_AND_WASTE_MGMT as I0280
, CENS_INDUS_EMPLD_PERCENT_EDUCATIONAL_SERVICES as I0281
, CENS_INDUS_EMPLD_PERCENT_HLTH_CARE_SOCIAL_ASSISTANCE as I0282
, CENS_INDUS_EMPLD_PERCENT_ARTS_ENTERTAINMENT_AND_REC as I0283
, CENS_INDUS_EMPLD_PERCENT_ACCOMODATION_AND_FOOD_SVCS as I0284
, CENS_INDUS_EMPLD_PERCENT_OTH_SVCS as I0285
, CENS_INDUS_EMPLD_PERCENT_PUBLIC_ADMINISTRATION as I0286
, CENS_LANG_HH_PERCENT_ENGLISH_SPEAKING as I0287
, CENS_LANG_HH_PERCENT_SPAN_SPEAK_LINGUIST_ISOL as I0289
, CENS_LANG_HH_PERCENT_SPAN_SPEAK_NOT_LINGUIST_ISOL as I0290
, CENS_LANG_HH_PERCENT_NOT_ENGL_SPEAK_LINGUIST_ISOL as I0291
, CENS_MARR_POP15_PLUS_PERCENT_NEVER_MARRIED as I0292
, CENS_MARR_POP15_PLUS_PERCENT_SPOUSE_PRESENT as I0293
, CENS_MARR_POP15_PLUS_PERCENT_SPOUSE_ABSENT as I0294
, CENS_MARR_POP15_PLUS_PERCENT_WIDOWED as I0295
, CENS_MARR_POP15_PLUS_PERCENT_DIVORCED as I0296
, CENS_MORTG_OOHU_PERCENT_FIRST_AND_SECOND_MORTGAGE as I0297
, CENS_MORTG_OOHU_PERCENT_FIRST_MRTG_AND_HOME_EQUITY_LN as I0298
, CENS_MORTG_OOHU_PERCENT_TWO_MRTGS_AND_HOME_EQUITY_LN as I0299
, CENS_MORTG_OOHU_PERCENT_FIRST_MORTGAGE_ONLY as I0300
, CENS_MORTG_OOHU_PERCENT_NO_MORTGAGE as I0301
, CENS_MOVE_OCCHU_PERCENT_MOVED_IN_LT1970 as I0302
, CENS_MOVE_OCCHU_PERCENT_MOVED_IN_1970_1979 as I0303
, CENS_MOVE_OCCHU_PERCENT_MOVED_IN_1980_1989 as I0304
, CENS_MOVE_OCCHU_PERCENT_MOVED_IN_1990_1999 as I0305
, CENS_MOVE_OCCHU_PERCENT_MOVED_IN_2000_2004 as I0306
, CENS_MOVE_OCCHU_PERCENT_MOVED_IN_2005_PLUS as I0307
, CENS_MOVE_OCCHU_PERCENT_TURNOVER_LAST_5_YRS as I0309
, CENS_MOVE_OCCHU_PERCENT_TURNOVER_LAST_10_YRS as I0310
, CENS_MOVE_OCCHU_PERCENT_STABILITY_20_OR_MORE_YEARS as I0311
, CENS_OCCUP_EMPLD_PERCENT_MANAGEMENT as I0312
, CENS_OCCUP_EMPLD_PERCENT_BUS_AND_FINANCIAL_OPS as I0313
, CENS_OCCUP_EMPLD_PERCENT_COMPUTERS_AND_MATH as I0314
, CENS_OCCUP_EMPLD_PERCENT_ARCHITECTURE_AND_ENGINEERING as I0315
, CENS_OCCUP_EMPLD_PERCENT_LIFE_PHYS_AND_SOC_SCIENCES as I0316
, CENS_OCCUP_EMPLD_PERCENT_COMMUNITY_AND_SOCIAL_SVCS as I0317
, CENS_OCCUP_EMPLD_PERCENT_LEGAL as I0318
, CENS_OCCUP_EMPLD_PERCENT_EDUC_TRAINING_AND_LIBRARY as I0319
, CENS_OCCUP_EMPLD_PERCENT_ARTS_DSGN_ENTER_SPORTS_MEDIA as I0320
, CENS_OCCUP_EMPLD_PERCENT_HEALTH_DIAG_AND_TREAT_PRACS as I0321
, CENS_OCCUP_EMPLD_PERCENT_HEALTH_TECHS as I0322
, CENS_OCCUP_EMPLD_PERCENT_HEALTHCARE_SUPP as I0323
, CENS_OCCUP_EMPLD_PERCENT_FIRE_AND_PROT_SVCS_INCL_SUPV as I0324
, CENS_OCCUP_EMPLD_PERCENT_LAW_ENFORCEMENT_INCL_SUPV as I0325
, CENS_OCCUP_EMPLD_PERCENT_FOOD_PREP_AND_SERVING as I0326
, CENS_OCCUP_EMPLD_PERCENT_BLDG_AND_GDS_CLEAN_AND_MTC as I0327
, CENS_OCCUP_EMPLD_PERCENT_PERSONAL_CARE_SVCS as I0328
, CENS_OCCUP_EMPLD_PERCENT_SALES_AND_RELATED as I0329
, CENS_OCCUP_EMPLD_PERCENT_OFFICE_AND_ADMIN_SUPP as I0330
, CENS_OCCUP_EMPLD_PERCENT_FARM_FISH_AND_FORESTRY as I0331
, CENS_OCCUP_EMPLD_PERCENT_CONSTR_AND_EXTRACT as I0332
, CENS_OCCUP_EMPLD_PERCENT_INSTALL_MAINT_AND_REPAIR as I0333
, CENS_OCCUP_EMPLD_PERCENT_PRODUCTION as I0334
, CENS_OCCUP_EMPLD_PERCENT_TRANS_AND_MATERIAL_MOV_SUPV as I0335
, CENS_OCCUP_EMPLD_PERCENT_MOTOR_VEHICLE_OPS as I0336
, CENS_OCCUP_EMPLD_PERCENT_MATERIAL_MOVING_WORKERS as I0337
, CENS_RENT_RNTL_PERCENT_NO_CASH_RENT as I0338
, CENS_RENT_RNTL_PERCENT_CASH_RENT_0_99 as I0339
, CENS_RENT_RNTL_PERCENT_CASH_RENT_100_149 as I0340
, CENS_RENT_RNTL_PERCENT_CASH_RENT_150_199 as I0341
, CENS_RENT_RNTL_PERCENT_CASH_RENT_200_249 as I0342
, CENS_RENT_RNTL_PERCENT_CASH_RENT_250_299 as I0343
, CENS_RENT_RNTL_PERCENT_CASH_RENT_300_349 as I0344
, CENS_RENT_RNTL_PERCENT_CASH_RENT_350_399 as I0345
, CENS_RENT_RNTL_PERCENT_CASH_RENT_400_449 as I0346
, CENS_RENT_RNTL_PERCENT_CASH_RENT_450_499 as I0347
, CENS_RENT_RNTL_PERCENT_CASH_RENT_500_549 as I0348
, CENS_RENT_RNTL_PERCENT_CASH_RENT_550_599 as I0349
, CENS_RENT_RNTL_PERCENT_CASH_RENT_600_649 as I0350
, CENS_RENT_RNTL_PERCENT_CASH_RENT_650_699 as I0351
, CENS_RENT_RNTL_PERCENT_CASH_RENT_700_749 as I0352
, CENS_RENT_RNTL_PERCENT_CASH_RENT_750_799 as I0353
, CENS_RENT_RNTL_PERCENT_CASH_RENT_800_899 as I0354
, CENS_RENT_RNTL_PERCENT_CASH_RENT_900_1000 as I0355
, CENS_RENT_RNTL_PERCENT_CASH_RENT_1000_1249 as I0356
, CENS_RENT_RNTL_PERCENT_CASH_RENT_1250_1499 as I0357
, CENS_RENT_RNTL_PERCENT_CASH_RENT_1500_1999 as I0358
, CENS_RENT_RNTL_PERCENT_CASH_RENT_2000_OR_MORE as I0359
, CENS_RENT_RNTL_AGGREGATE_CONTRACT_RENT as I0360
, CENS_RENT_RNTL_MEDIAN_RENT as I0361
, CENS_TENANCY_HU_PERCENT_OCCUPIED as I0362
, CENS_TENANCY_HU_PERCENT_VACANT as I0363
, CENS_TENANCY_OCCHU_PERCENT_OWNER_OCCUPIED as I0364
, CENS_TENANCY_OCCHU_PERCENT_RENTER_OCCUPIED as I0365
, CENS_TENANCY_SFDUS_PERCENT_OWNER_OCCUPIED as I0366
, CENS_TYP_POP_PERCENT_IN_HOUSEHOLDS as I0367
, CENS_TYP_POP_PERCENT_IN_FAMILY_HH as I0368
, CENS_TYP_POP_PERCENT_HOH_IN_FAMILY_HH as I0369
, CENS_TYP_POP_PERCENT_MALE_HOH_IN_FAMILY_HH as I0370
, CENS_TYP_POP_PERCENT_FEMALE_HOH_IN_FAMILY_HH as I0371
, CENS_TYP_POP_PERCENT_SPOUSE_IN_FAMILY_HH as I0372
, CENS_TYP_POP_PERCENT_CHILD_IN_FAMILY_HH as I0373
, CENS_TYP_POP_PERCENT_NAT_ADOPTED_CHILD_IN_FAMILY_HH as I0374
, CENS_TYP_POP_PERCENT_STEPCHILD_IN_FAMILY_HH as I0375
, CENS_TYP_POP_PERCENT_GRANDCHILD_IN_FAMILY_HH as I0376
, CENS_TYP_POP_PERCENT_SIBLING_IN_FAMILY_HH as I0377
, CENS_TYP_POP_PERCENT_PARENT_IN_FAMILY_HH as I0378
, CENS_TYP_POP_PERCENT_OTHER_RELATIVE_IN_FAMILY_HH as I0379
, CENS_TYP_POP_PERCENT_NON_RELATIVE_IN_FAMILY_HH as I0380
, CENS_TYP_POP_PERCENT_IN_NON_FAMILY_HH as I0381
, CENS_TYP_POP_PERCENT_MALE_HOH_IN_NON_FAMILY_HH as I0382
, CENS_TYP_POP_PERCENT_MALE_LIVING_ALONE as I0383
, CENS_TYP_POP_PERCENT_MALE_HOH_IN_2_PLUS_NON_FAMILY_HH as I0384
, CENS_TYP_POP_PERCENT_FEMALE_HOH_IN_NON_FAMILY_HH as I0385
, CENS_TYP_POP_PERCENT_FEMALE_LIVING_ALONE as I0386
, CENS_TYP_POP_PERCENT_FEMALE_HOH_IN_2_PLUS_NON_FAMILY_HH as I0387
, CENS_TYP_POP_PERCENT_NONREL_IN_NON_FAMILY_HH as I0388
, CENS_TYP_HH_PERCENT_MARRIED_COUPLE_FAMILY as I0389
, CENS_URBAN_POP_PERCENT_URBAN as I0390
, CENS_URBAN_POP_PERCENT_URBAN_IN_URBAN_AREAS as I0391
, CENS_URBAN_POP_PERCENT_URBAN_IN_URBAN_CLUSTERS as I0392
, CENS_URBAN_POP_PERCENT_RURAL as I0393
, CENS_URBAN_POP_PERCENT_RURAL_ON_FARMS as I0394
, CENS_URBAN_POP_PERCENT_RURAL_NOT_ON_FARMS as I0395
, CENS_EDUC_ISPSA as I0396
, CENS_EDUC_ISPSA_DECILE as I0397
, CENS_INC_FAMILY_INC_STATE_INDEX as I0398
, CENS_INC_FAMILY_INC_STATE_DECILE as I0399
, CENS_INC_FAMILY_INC_CBSA_INDEX as I0400
, CENS_INC_FAMILY_INC_CBSA_DECILE as I0401
, CENS_HOMVAL_HOME_VALUE_STATE_INDEX as I0402
, CENS_MOVE_OCCHU_PERCENT_NEW_LISTINGS as I0404
from CENSUS
"""

df_census = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url_asi_tar) \
    .option("dbtable", f"({sql_query_census}) as subq") \
    .options(**jdbc_connection_properties) \
    .load() \
    .dropDuplicates(["geo_cd_2010_key"])

# Create DP9
df_mms_m_dp6.createOrReplaceTempView(f"{file_name}_DP6")
df_census.createOrReplaceTempView("CENSUS")

df_dp9 = spark.sql(f"""
    select a.CHID
          ,a.geo_cd_2010
          ,b.*
    from {file_name}_DP6 a
    left join CENSUS b on a.geo_cd_2010 = b.geo_cd_2010_key
""")

df_dp9 = df_dp9.withColumn("Census_Flg", F.when(F.col("geo_cd_2010_key").isNotNull(), 1).otherwise(0)) \
               .drop("geo_cd_2010_key")

print("Frequency count for Census_Flg in dp9:")
df_dp9.groupBy("Census_Flg").count().show()

df_mms_m_dp9 = df_dp9.dropDuplicates(["CHID"])

spark.sql("DROP VIEW IF EXISTS CENSUS")

# *** 2.J Append Attributes from AIQ HH Level Table **** ;
df_aiq_hh = spark.read.format("delta").load(f"{src_aiq_path}/{AIQ_Input}")

# Create DP10
df_aiq_hh.createOrReplaceTempView("AIQ_INPUT_TBL")

df_dp10 = spark.sql(f"""
select a.CHID
      , b.CHID_key
, b.AIQ_HHID
, b.AIQ_INDID
, b.AIQ_LOR as J0002
, b.AIQ_AMEX as J0003
, b.AIQ_PREMIUM_CC as J0004
, b.AIQ_RETAIL_CC as J0005
, b.AIQ_BANK_CC as J0006
, b.AIQ_TRAVEL_CC as J0007
, b.AIQ_PEOPLE_IN_HH as J0008
, b.AIQ_ADULTS_IN_HH as J0009
, b.AIQ_CHILDREN_IN_HH as J0010
, b.AIQ_GENDER as J0011
, b.AIQ_MARITAL as J0012
, b.AIQ_AGE as J0013
, b.AIQ_EDUCATION as J0014
, b.AIQ_HOMEOWNER_PROBABILITY as J0015
, b.AIQ_DWELLING as J0016
, b.AIQID as J0017
, b.Children_Aged_0_2 as J0018
, b.Children_M_Aged_0_2 as J0019
, b.Children_F_Aged_0_2 as J0020
, b.Children_U_Aged_0_2 as J0021
, b.Children_Aged_3_5 as J0022
, b.Children_M_Aged_3_5 as J0023
, b.Children_F_Aged_3_5 as J0024
, b.Children_U_Aged_3_5 as J0025
, b.Children_Aged_6_10 as J0026
, b.Children_M_Aged_6_10 as J0027
, b.Children_F_Aged_6_10 as J0028
, b.Children_U_Aged_6_10 as J0029
, b.Children_Aged_11_15 as J0030
, b.Children_M_Aged_11_15 as J0031
, b.Children_F_Aged_11_15 as J0032
, b.Children_U_Aged_11_15 as J0033
, b.Children_Aged_16_17 as J0034
, b.Children_M_Aged_16_17 as J0035
, b.Children_F_Aged_16_17 as J0036
, b.Children_U_Aged_16_17 as J0037
, b.Male_18_24 as J0038
, b.Female_18_24 as J0039
, b.Unknown_18_24 as J0040
, b.Male_25_34 as J0041
, b.Female_25_34 as J0042
, b.Unknown_25_34 as J0043
, b.Male_35_44 as J0044
, b.Female_35_44 as J0045
, b.Unknown_35_44 as J0046
, b.Male_45_54 as J0047
, b.Female_45_54 as J0048
, b.Unknown_45_54 as J0049
, b.Male_55_64 as J0050
, b.Female_55_64 as J0051
, b.Unknown_55_64 as J0052
, b.Male_65_74 as J0053
, b.Female_65_74 as J0054
, b.Unknown_65_74 as J0055
, b.Male_75P as J0056
, b.Female_75P as J0057
, b.Unknown_75P as J0058
, b.Discover_Gold_Premium as J0059
, b.Discover_Regular as J0060
, b.Mastercard_Gold_Premium as J0061
, b.Mastercard_Regular as J0062
, b.Visa_Gold_Premium as J0063
, b.Visa_Regular as J0064
, b.CC_Unknown_Type as J0065
, b.CC_User as J0066
, b.CC_New_Issue as J0067
, b.Invest_Active as J0068
, b.Invest_Personal as J0069
, b.Invest_Real_Estate as J0070
, b.Invest_Finance as J0071
, b.Invest_Foreign as J0072
, b.Presence_of_CC as J0073
, b.Investments as J0074
, b.Invest_Stocks_Securities as J0075
, b.Home_Purchase_Price as J0076
, b.Home_Purchase_Year as J0077
, b.Home_Purchase_Month as J0078
, b.Home_Purchase_Day as J0079
, b.Mortgage_Amount as J0080
, b.Refinance_Year as J0081
, b.Refinance_Month as J0082
, b.Refinance_Day as J0083
, b.Refinance_Amount as J0084
, b.Number_of_Sources as J0085
, b.Number_Lines_of_Credit as J0086
, b.Generations_in_HH as J0087
, b.Invest_Num_Res_Prop as J0088
, b.Home_Value_Score as J0089
, b.Most_Recent_2nd_Mtg_Amt as J0090
, b.Person_Birth_Year as J0091
, b.Person_Birth_Month as J0092
, b.Person_Birth_Day as J0093
, b.Loan_to_Value as J0094
, b.Home_Year_Built as J0095
, b.Secondary_Address_Ind as J0096
, b.Swimming_Pool_Ind as J0097
, b.Air_Conditioning_Code as J0098
, b.Home_Heat_Ind as J0099
, b.Home_Purchase_Price_Code as J0100
, b.Mortgage_Amount_Code as J0101
, b.Mortgage_Lender_Name as J0102
, b.Mortgage_Lender_Avail as J0103
, b.Mortgage_Rate_Type as J0104
, b.Mortgage_Loan_Type as J0105
, b.Transaction_Type as J0106
, b.Refinance_Amount_Code as J0107
, b.Refinance_Lender_Name as J0108
, b.Refinance_Lender_Avail as J0109
, b.Refinance_Rate_Type as J0110
, b.Refinance_Loan_Type as J0111
, b.Purch_2nd_Mtg_Amt as J0112
, b.Most_Recent_2nd_Mtg_Date as J0113
, b.Purch_Mtg_Date as J0114
, b.Most_Recent_2nd_Mtg_Loan_Type as J0115
, b.Purch_2nd_Mtg_Loan_Type as J0116
, b.Most_Recent_Lender_Code as J0117
, b.Most_Recent_2nd_Lender_Code as J0118
, b.Purch_Lender_Code as J0119
, b.Most_Recent_2nd_Lender as J0120
, b.Most_Recent_2nd_Mtg_Int_Type as J0121
, b.Most_Recent_Mtg_Int_Rate as J0122
, b.Most_Recent_2nd_Mtg_Int_Rate as J0123
, b.Presence_of_Children as J0124
, b.Person_Occupation as J0125
, b.Language_Code as J0126
, b.Ethnic_Group as J0127
, b.Religion_Code as J0128
, b.Hispanic_Country_Code as J0129
, b.Bus_Owner_Code as J0130
, b.CRA_Income_Code as J0131
, b.Time_Zone as J0132
, b.Inf_Household_Rank as J0133
, b.Credit_Range_New_Credit as J0134
, b.Sewer_Code as J0135
, b.Water_Code as J0136
, b.Assimilation_Code as J0137
, b.Email_Flag as J0138
, b.Ethnic_Code as J0139
, b.Property_Type as J0140
, b.Value_Hunter as J0141
, b.Opportunity_Seekers as J0142
, b.News_Financial as J0143
, b.Automotive_Buff as J0144
, b.Book_Reader as J0145
, b.Computer_Owner as J0146
, b.Cooking_Fan as J0147
, b.DIYers as J0148
, b.Exercise_Fan as J0149
, b.Home_Gardener as J0150
, b.Golf_Fan as J0151
, b.Home_Decorating_Fan as J0152
, b.Outdoor_Grouping as J0153
, b.Sports_Grouping as J0154
, b.Photography_Fan as J0155
, b.Traveling_Fan as J0156
, b.Pet_Owner as J0157
, b.Cat_Owner as J0158
, b.Dog_Owner as J0159
, b.Mail_Responder as J0160
, b.Sweepstakes_Fan as J0161
, b.Religious_Magazines as J0162
, b.Male_Merch_Buyer as J0163
, b.Female_Merch_Buyer as J0164
, b.Crafts_Hobbies_Merch_Buyer as J0165
, b.Gardening_Farm_Buyer as J0166
, b.Book_Buyer as J0167
, b.Collect_Special_Foods as J0168
, b.Religious_Contributor as J0169
, b.Political_Contributor as J0170
, b.Political_Charitable_Donation as J0171
, b.Political_Cons_Char_Donation as J0172
, b.Political_Lib_Char_Donation as J0173
, b.Health_Inst_Contributor as J0174
, b.Charitable_Contributor as J0175
, b.General_Contributor as J0176
, b.Donates_by_Mail as J0177
, b.Veteran_in_HH as J0178
, b.High_Tech_Leader as J0179
, b.Mail_Order_Buyer as J0180
, b.Online_Purchaser as J0181
, b.Apparel_Women as J0182
, b.Apparel_Women_Petite as J0183
, b.Apparel_Women_Plus as J0184
, b.Apparel_Women_Young as J0185
, b.Apparel_Men as J0186
, b.Apparel_Men_Big_Tall as J0187
, b.Apparel_Men_Young as J0188
, b.Apparel_Children as J0189
, b.Health_Beauty as J0190
, b.Beauty_Cosmetics as J0191
, b.Jewelry_Purchaser as J0192
, b.Luggage_Purchaser as J0193
, b.Animal_Welfare_Donation as J0194
, b.Arts_Cultural_Donation as J0195
, b.Childrens_Charitable_Donation as J0196
, b.Environmental_Donation as J0197
, b.International_Aid_Donation as J0198
, b.Veterans_Donation as J0199
, b.Other_Charitable_Donation as J0200
, b.Community_Charities as J0201
, b.Parenting_Category as J0202
, b.Single_Parent as J0203
, b.Apparel_Infants_Toddlers as J0204
, b.Children_Learning_Toys as J0205
, b.Children_Prod_Baby_Care as J0206
, b.Children_Prod_Back_School as J0207
, b.Children_Prod_General as J0208
, b.Young_Adult_in_HH as J0209
, b.Senior_Adult_in_HH as J0210
, b.Children_Interests as J0211
, b.Grandchildren_Category as J0212
, b.Christian_Families as J0213
, b.Equestrian_Fan as J0214
, b.Other_Pet_Owner as J0215
, b.Career_Improvement as J0216
, b.Working_Woman as J0217
, b.African_American_Professional as J0218
, b.Soho_Indicator as J0219
, b.Career_Oriented as J0220
, b.Books_Magazines as J0221
, b.Books_Music as J0222
, b.Books_Music_Audio as J0223
, b.Reading_General as J0224
, b.Reading_Science_Fiction as J0225
, b.Reading_Magazines as J0226
, b.Reading_Audio_Books as J0227
, b.Reading_Grouping as J0228
, b.History_Military as J0229
, b.Current_Affairs_Politics as J0230
, b.Religious_Inspirational as J0231
, b.Science_Space as J0232
, b.Magazine_Reader as J0233
, b.Education_OnLine as J0234
, b.Gaming_Fan as J0235
, b.Computing_Home_Office_General as J0236
, b.DVDs_Videos as J0237
, b.Elect_Computing_TV_Videos as J0238
, b.Elect_Computing_Home_Office as J0239
, b.High_End_Appliances as J0240
, b.Music_Home_Stereo as J0241
, b.Music_Player as J0242
, b.Music_Collector as J0243
, b.Music_Avid_Listener as J0244
, b.Movie_Collector as J0245
, b.TV_Cable as J0246
, b.Video_Games as J0247
, b.TV_Satellite_Dish as J0248
, b.Computer_Games as J0249
, b.Consumer_Electronics as J0250
, b.Movie_Music_Grouping as J0251
, b.Electronics_Computers_Grouping as J0252
, b.TeleCom as J0253
, b.Art_Antiques_Antiques as J0254
, b.Art_Antique_Art as J0255
, b.Theater_Performing_Arts as J0256
, b.Arts_Fan as J0257
, b.Musical_Instruments as J0258
, b.Collectibles_General as J0259
, b.Collectibles_Stamps as J0260
, b.Collectibles_Coins as J0261
, b.Collectibles_Arts as J0262
, b.Collectibles_Antiques as J0263
, b.Collector_Avid as J0264
, b.Collectibles_Antiques_Grouping as J0265
, b.Collectibles_Sports as J0266
, b.Military_Weaponry as J0267
, b.Lifestyles_Interests_Passions as J0268
, b.Auto_Work as J0269
, b.Sewing_Knitting_Needlework as J0270
, b.Woodworking_Fan as J0271
, b.Aviation_Fan as J0272
, b.House_Plants as J0273
, b.Crafts_Fan as J0274
, b.Home_Garden as J0275
, b.Gardening_Fan as J0276
, b.Home_Improvement_Grouping as J0277
, b.Photography_Video_Equip as J0278
, b.Home_Furnishings_Decorating as J0279
, b.Home_Improvement as J0280
, b.Food_Wines as J0281
, b.Cooking_General as J0282
, b.Food_Natural as J0283
, b.Cooking_Food_Grouping as J0284
, b.Board_Games_Puzzles as J0285
, b.Gaming_Casino as J0286
, b.Travel_Grouping as J0287
, b.Travel_Domestic as J0288
, b.Travel_International as J0289
, b.Travel_Cruise_Vacations as J0290
, b.Home_Living as J0291
, b.DIY_Living as J0292
, b.Sporty_Living as J0293
, b.Upscale_Living as J0294
, b.Cultural_Artistic_Living as J0295
, b.High_Brow as J0296
, b.Common_Living as J0297
, b.Professional_Living as J0298
, b.Broader_Living as J0299
, b.Exercise_Health_Grouping as J0300
, b.Exercise_Running_Jogging as J0301
, b.Exercise_Walking as J0302
, b.Exercise_Aerobic as J0303
, b.Spec_Sport_Auto_MC_Racing as J0304
, b.Spec_Sport_TV_Sports as J0305
, b.Spec_Sport_Football as J0306
, b.Spec_Sport_Baseball as J0307
, b.Spec_Sport_Basketball as J0308
, b.Spec_Sport_Hockey as J0309
, b.Spec_Sport_Soccer as J0310
, b.Tennis_Fan as J0311
, b.Snow_Skiing_Fan as J0312
, b.Motorcycling_Fan as J0313
, b.NASCAR_Fan as J0314
, b.Boating_Sailing_Fan as J0315
, b.Scuba_Diving_Fan as J0316
, b.Sports_Leisure as J0317
, b.Hunting_Fan as J0318
, b.Fishing_Fan as J0319
, b.Camping_Hiking_Fan as J0320
, b.Hunting_Shooting_Fan as J0321
, b.Health_Medical as J0322
, b.Dieting_Weight_Loss as J0323
, b.Self_Improvement as J0324
, b.Auto_Parts_Accessories as J0325
, b.SYMPHONY_CAT as J0326
, b.SYMPHONY as J0327
, b.HomevalueIQ as J0328
, b.HomeValueIQ_Confidence as J0329
, b.AIQ_Home_Value as J0330
, b.HomeValueIQ_Equity as J0331
, b.HomeValueIQ_Mortgage as J0332
, b.HomeValueIQ_EquityPct as J0333
, b.HomeValueIQ_LTV as J0334
, b.EthnicIQ as J0335
, b.ETHNICIQ_v2 as J0336
, b.ETHNICIQ_CONFIDENCE as J0337
, b.IncomeIQ_Plus as J0338
, b.IncomeIQ_Plus_RegB as J0339
, b.IncomeIQ_Plus_v2 as J0340
, b.IncomeIQ_Plus_v2_RegB as J0341
, b.INCOMEIQ_PLUS_v3 as J0342
, b.religioniq as J0343
, b.politicsiq as J0344
, b.Aspects_Plus as J0345
, b.Change_in_Home_Value_3Mo as J0346
, b.Change_in_Home_Value_6Mo as J0347
, b.Change_in_Home_Value_12Mo as J0348
, b.Change_in_Income_12Mo as J0349
, b.Change_in_Income_12Mo_RegB as J0350
, b.Delineate_Plus as J0351
, b.IncomeIQ_Plus_Ratio as J0352
, b.IncomeIQ_Plus_Ratio_RegB as J0353
, b.Home_to_Income_Ratio as J0354
, b.Home_to_Income_Ratio_RegB as J0355
, b.Affordability_Index as J0356
, b.Affordability_Index_scf as J0357
, b.InvestorIQ_Plus_Ratio as J0358
, b.InvestorIQ_Plus_Ratio_RegB as J0359
, b.InvestorIQ_Plus as J0360
, b.InvestorIQ_Plus_RegB as J0361
, b.InvestorIQ_Plus_v2 as J0362
, b.InvestorIQ_Plus_v2_RegB as J0363
, b.InvestorIQ_Plus_v3 as J0364
, b.InvestorIQ_Plus_v3_Checking as J0365
, b.InvestorIQ_Plus_v3_Securities as J0366
, b.InvestorIQ_Plus_v3_Savings as J0367
, b.InvestorIQ_Plus_v3_Bonds as J0368
, b.InvestorIQ_Plus_v3_Life as J0369
, b.InvestorIQ_Plus_v3_Annuity as J0370
, b.WealthIQ_Plus as J0371
, b.WealthIQ_Plus_RegB as J0372
, b.AIQ_Green_Plus as J0373
, b.IncomeIQ_Plus_DISP as J0374
, b.Spendex_Plus as J0375
, b.Spendex_DINEOUT as J0376
, b.Spendex_ALCOHOL as J0377
, b.Spendex_APPAREL as J0378
, b.Spendex_PERSONAL as J0379
, b.Spendex_READING as J0380
, b.Spendex_EDUCATION as J0381
, b.Spendex_TOBACCO as J0382
, b.Spendex_DONATION as J0383
, b.Spendex_FURNISH as J0384
, b.IncomeIQ_Plus_DISP_RegB as J0385
, b.Spendex_Plus_RegB as J0386
, b.Spendex_DINEOUT_RegB as J0387
, b.Spendex_ALCOHOL_RegB as J0388
, b.Spendex_APPAREL_RegB as J0389
, b.Spendex_ENTERTAIN_RegB as J0390
, b.Spendex_PERSONAL_RegB as J0391
, b.Spendex_READING_RegB as J0392
, b.Spendex_EDUCATION_RegB as J0393
, b.Spendex_TOBACCO_RegB as J0394
, b.Spendex_MISC_RegB as J0395
, b.Spendex_DONATION_RegB as J0396
, b.Spendex_FURNISH_RegB as J0397
, b.WealthIQ_Max_Plus as J0398
, b.WealthIQ_Plus_v2 as J0399
, b.WealthIQ_Plus_v2_RegB as J0400
, b.WealthIQ_Plus_v3 as J0401
, b.SocialIQ as J0402
, b.SocialIQ_V2 as J0403
, b.SocialIQ_Facebook as J0404
, b.Delineate_Social as J0405
, b.PoliticsIQ_Donor_Liberal as J0406
, b.PoliticsIQ_Donor_Cons as J0407
, b.SCR_DynaBase as J0408
, b.SCR_Dyn_Open as J0409
, b.SCR_Dyn_Bounce as J0410
, b.SCR_EMail_DynaBase as J0411
, b.AutoProp_FORD as J0412
, b.AutoProp_CHEVROLET as J0413
, b.AutoProp_TOYOTA as J0414
, b.AutoProp_DODGE as J0415
, b.AutoProp_HONDA as J0416
, b.AutoProp_NISSAN as J0417
, b.AutoProp_JEEP as J0418
, b.AutoProp_GMC as J0419
, b.AutoProp_BUICK as J0420
, b.AutoProp_HYUNDAI as J0421
, b.AutoProp_MAZDA as J0422
, b.AutoProp_CADILLAC as J0423
, b.AutoProp_VOLKSWAGEN as J0424
, b.AutoProp_BMW as J0425
, b.AutoProp_KIA as J0426
, b.AutoProp_LEXUS as J0427
, b.AutoProp_MERCEDES as J0428
, b.AutoProp_MITSUBISHI as J0429
, b.AutoProp_SUBARU as J0430
, b.AutoProp_LINCOLN as J0431
, b.AutoProp_ACURA as J0432
, b.AutoProp_VOLVO as J0433
, b.AutoProp_INFINITI as J0434
, b.AutoProp_AUDI as J0435
, b.Auto_InMkt as J0436
, b.WEALTH_PROFILES as J0437
, b.TravelProp_Cruise as J0438
, b.TravelProp_Domestic as J0439
, b.TravelProp_Intl as J0440
, b.Spendex_TRAVEL as J0441
, b.Spendex_TRAVEL_DOM as J0442
, b.Spendex_TRAVEL_INTL as J0443
, b.Spendex_TRAVEL_CRUISE as J0444
, b.Spendex_ENTERTAIN as J0445
, b.InMarket_Auto as J0446
, b.InMarket_LTC_INS as J0447
, b.InMarket_ONLINE_EDU as J0448
, b.InMarket_TERM_LIFE as J0449
, b.AIQ_ATP as J0450
, b.AIQ_ATP_DTI as J0451
, b.ChurnIQ_v2 as J0452
, b.SocialIQ_Twitter as J0453
, b.AIQ_Address_Indicator as J0454
, b.AIQ_BUSOWNER_SCALE as J0455
, b.AIQ_BUSINESS_OWNER as J0456
, b.SMARTPHONE_IPHONE as J0457
, b.SMARTPHONE_IPHONE_SCALE as J0458
, b.SMARTPHONE_ANDROID as J0459
, b.SMARTPHONE_ANDROID_SCALE as J0460
, b.SMARTPHONE_NONE as J0461
, b.SMARTPHONE_NONE_SCALE as J0462
, b.CHANNELIQ_BANNER as J0463
, b.CHANNELIQ_BANNER_SCALE as J0464
, b.CHANNELIQ_INTERTV as J0465
, b.CHANNELIQ_INTERTV_SCALE as J0466
, b.CHANNELIQ_SOURCES as J0467
, b.CHARITYIQ_HIGHDOLLAR as J0468
, b.SOCIALIQ_FACEBOOK_v2 as J0469
, b.SOCIALIQ_INSTAGRAM_v2 as J0470
, b.SOCIALIQ_LINKEDIN_v2 as J0471
, b.InvestorIQ_Plus_v4 as J0472
, b.InvestorIQ_Plus_v4_Checking as J0473
, b.InvestorIQ_Plus_v4_Savings as J0474
, b.InvestorIQ_Plus_v4_SBonds as J0475
, b.InvestorIQ_Plus_v4_Securities as J0476
, b.InvestorIQ_Plus_v4_Life as J0477
, b.InvestorIQ_Plus_v4_Annuity as J0478
, b.WealthIQ_Plus_v4 as J0479
, b.AIQ_RELIGIOUS as J0480
, b.LT_DEAL_SITE as J0481
, b.SOCIALIQ_DEAL_SITE as J0482
, b.AIQ_HOMEOWNER_PROB_v2 as J0483
, b.Ratio_Home_Value_12Mo_to_Income as J0484
, b.Ratio_Home_Val_12Mo_to_Inc_RegB as J0485
, b.IncomeIQ_Plus_DISP_V2 as J0486
, b.Spendex_Plus_v2 as J0487
, b.Spendex_Dineout_v2 as J0488
, b.Spendex_Alcohol_v2 as J0489
, b.Spendex_Cell_Phone_v2 as J0490
, b.Spendex_Furnish_v2 as J0491
, b.Spendex_Apparel_v2 as J0492
, b.Spendex_Personal_v2 as J0493
, b.Spendex_Reading_v2 as J0494
, b.Spendex_Education_v2 as J0495
, b.Spendex_Donation_v2 as J0496
, b.Spendex_Pers_Ins_v2 as J0497
, b.Spendex_TRAVEL_v2 as J0498
, b.Spendex_TRAVEL_DOM_v2 as J0499
, b.Spendex_TRAVEL_INTL_v2 as J0500
, b.Spendex_TRAVEL_CRUISE_v2 as J0501
, b.Spendex_ENTERTAIN_v2 as J0502
, b.JOBSIQ_BLUE_COLLAR as J0503
, b.JOBSIQ_CORP_LEADER as J0504
, b.JOBSIQ_PHYSICIANS as J0505
, b.JOBSIQ_EDUCATOR as J0506
, b.JOBSIQ_FINANCIAL as J0507
, b.JOBSIQ_HOMEMAKER as J0508
, b.JOBSIQ_INSURANCE as J0509
, b.JOBSIQ_LEGAL as J0510
, b.JOBSIQ_MIDDLE_MGMT as J0511
, b.JOBSIQ_OTHER_MEDICAL as J0512
, b.JOBSIQ_OTHER_W_COLLAR as J0513
, b.JOBSIQ_PROF_TECH as J0514
, b.JOBSIQ_PUBLIC as J0515
, b.JOBSIQ_REAL_ESTATE as J0516
, b.JOBSIQ_RETIRED as J0517
, b.JOBSIQ_SALES as J0518
, b.JOBSIQ_STUDENT as J0519
, b.JOBSIQ as J0520
, b.SOCIALIQ_TRIP_ADVISOR as J0521
, b.LT_GAMER_SCALE as J0522
, b.LT_GAMER as J0523
, b.LT_Exercise as J0524
, b.LT_Couponer as J0525
, b.Uber_Lyft_User as J0526
, b.Zipcar_User as J0527
, b.OS_AGREEABLE as J1001
, b.OS_Conscientious as J1002
, b.OS_COOKING as J1003
, b.OS_Donor_Children as J1004
, b.OS_Donor_Extrinsic as J1005
, b.OS_Donor_HC as J1006
, b.OS_Donor_Intrinsic as J1007
, b.OS_Donor_NonPlanned as J1008
, b.OS_EMOTION as J1009
, b.OS_EPFI as J1010
, b.OS_Extroversion as J1011
, b.OS_Fin_IMP as J1012
, b.OS_Fin_ImpControl as J1013
, b.OS_Fin_Motivation as J1014
, b.OS_Fin_Organization as J1015
, b.OS_Fin_Planning as J1016
, b.OS_FOODIE as J1017
, b.OS_Green as J1018
, b.OS_Impulsive as J1019
, b.OS_Innovator as J1020
, b.OS_Laggard as J1021
, b.OS_Materialism as J1022
, b.OS_Openness as J1023
, b.OS_Rel_Devotion as J1024
, b.OS_RiskTaking_Career as J1025
, b.OS_RiskTaking_Fin as J1026
, b.OS_RISKTAKING_HEALTH as J1027
, b.OS_RiskTaking_Rec as J1028
, b.OS_RiskTaking_Safe as J1029
, b.OS_RiskTaking_Social as J1030
, b.OS_Value_Seeker as J1031
, b.HW_ALCOHOL_v2 as J1032
, b.HW_BMI as J1033
, b.HW_Diet as J1034
, b.HW_Exercise as J1035
, b.HW_Job_Satis as J1036
, b.HW_JUNK_DIET as J1037
, b.HW_SLEEP_v2 as J1038
, b.HW_SMOKING as J1039
, b.HW_STRESS as J1040
, b.HW_YogaPilate as J1041
, b.InMarket_Amazon as J1042
, b.Inmarket_Apple as J1043
, b.Inmarket_Google as J1044
, b.InMarket_NonTradTV as J1045
, b.InMarket_OnlineDate as J1046
, b.InMarket_OnlineEDU_v2 as J1047
, b.InMarket_OnlineShop as J1048
, b.InMarket_OnlineStream as J1049
, b.InMarket_Sephora as J1050
, b.Inmarket_Smart_Speaker as J1051
, b.Inmarket_Target as J1052
, b.InMarket_Walmart as J1053
, b.Inmarket_Whole_Foods as J1054
, b.African_American_Prof_v2 as J1055
, b.AIQ_BUSINESS_OWNER_v2 as J1056
, b.AIQ_Employment as J1057
, b.AIQ_LGBTQ as J1058
, b.AIQ_NameVeracity as J1059
, b.BC_1stContact_Email as J1060
, b.BC_1stContact_InPerson as J1061
, b.BC_1stContact_Phone as J1062
, b.BC_BusinessOwner as J1063
, b.BC_BusinessOwner_Home as J1064
, b.BC_ContactOrient as J1065
, b.BC_Content_Webinar as J1066
, b.BC_Content_WhitePaper as J1067
, b.BC_ContentOrient as J1068
, b.BC_DecisionMaker as J1069
, b.BC_Exec_NonHomeBased_Ind as J1070
, b.BC_Influencer_Ind as J1071
, b.BC_Purchase_Price as J1072
, b.BC_Purchase_Quality as J1073
, b.BC_PurchaseOrient as J1074
, b.distanceiq_casualmale as J1076
, b.distanceiq_sephora as J1077
, b.distanceiq_target as J1078
, b.distanceiq_walmart as J1079
, b.distanceiq_wholefoods as J1080
, b.HISPANIC_PROFESSIONAL_v2 as J1081
, b.LT_Apparel_Women_Plus_v2 as J1082
, b.LT_Boating_Sailing_Fan_v2 as J1083
, b.LT_Career_Improvement_v2 as J1084
, b.LT_Cat_Owner_v2 as J1085
, b.LT_Crafts_Fan_v2 as J1086
, b.LT_Crafts_Hobbies_Buyer_v2 as J1087
, b.LT_Dog_Owner_v2 as J1088
, b.LT_Exercise_Aerobic_v2 as J1089
, b.LT_Exercise_Run_Jog_v2 as J1090
, b.LT_Gardening_Fan_v2 as J1091
, b.LT_Golf_Fan_v2 as J1092
, b.LT_Home_Garden_v2 as J1093
, b.LT_Hunting_Fan_v2 as J1094
, b.LT_Motorcycling_Fan_v2 as J1095
, b.LT_NASCAR_Fan_v2 as J1096
, b.LT_Other_Pet_Owner_v2 as J1097
, b.LT_Pet_Owner_v2 as J1098
, b.LT_Religious_Magazines_v2 as J1099
, b.LT_Senior_Adult_in_HH_v2 as J1100
, b.LT_Spec_Sport_Auto_MC_v2 as J1101
, b.LT_Travel_Domestic_v2 as J1102
, b.LT_Travel_International_v2 as J1103
, b.SCR_wellness as J1104
, b.SocialIQ_Houzz as J1105
, b.SocialIQ_Pinterest as J1106
, b.SocialIQ_Snapchat as J1107
, b.BC_DecisionMaker_Ind as J1108
, b.BC_Influencer as J1109
, b.InMarket_AirBnB as J1110
, b.InMarket_Microsoft as J1111
, b.InMarket_Pandora as J1112
, b.InMarket_Paypal as J1113
, b.InMarket_Spotify as J1114
, b.GRAND_PARENT as J1115
, b.LT_GRAND_PARENT as J1116
, b.AIQ_ATP_v2 as J1117
, b.AIQ_EDUCATION_v2 as J1118
, b.AIQ_Home_Age as J1119
, b.AIQ_LOR_v2 as J1120
, b.AIQ_MARITAL_V2 as J1121
, b.AIQ_Race_Jewish as J1122
, b.Apparel_Women_Plus_v2 as J1123
, b.Boating_Sailing_Fan_v2 as J1124
, b.Career_Improvement_v2 as J1125
, b.Cat_Owner_v2 as J1126
, b.Crafts_Fan_v2 as J1127
, b.Crafts_Hobbies_Buyer_v2 as J1128
, b.Dog_Owner_v2 as J1129
, b.Exercise_Aerobic_v2 as J1130
, b.Exercise_Running_Jogging_v2 as J1131
, b.Gardening_Fan_v2 as J1132
, b.Golf_Fan_v2 as J1133
, b.HW_Alcohol as J1134
, b.HW_Sleep as J1135
, b.Home_Garden_v2 as J1136
, b.Hunting_Fan_v2 as J1137
, b.Motorcycling_Fan_v2 as J1138
, b.NASCAR_Fan_v2 as J1139
, b.Other_Pet_Owner_v2 as J1140
, b.Pet_Owner_v2 as J1141
, b.Religious_Magazines_v2 as J1142
, b.Senior_Adult_in_HH_v2 as J1143
, b.Spec_Sport_Auto_MC_v2 as J1144
, b.Travel_Domestic_v2 as J1145
, b.Travel_International_v2 as J1146
from (select distinct CHID from {file_name} where CHID > 0 and CHID is not null) a
left join AIQ_INPUT_TBL b on a.CHID = cast(b.CHID_key as bigint)
""")

df_dp10 = df_dp10.withColumn("AIQ_HH_Flg", F.when(F.col("CHID_key").isNotNull(), 1).otherwise(0)) \
                 .drop("CHID_key")

print("Frequency count for AIQ_HH_Flg in dp10:")
df_dp10.groupBy("AIQ_HH_Flg").count().show()

df_mms_m_dp10 = df_dp10.dropDuplicates(["CHID"])

# *** 2.M Append New Attributes from AIQ 35plus non-member dataset **** ;
df_dp10a = df_dp10.select("chid", "aiq_hhid", "aiq_indid").dropDuplicates(["aiq_hhid", "aiq_indid"])

df_aiq_input2 = spark.read.format("delta").load(f"{btao_path}/{AIQ_Input2}")

df_dp10a.createOrReplaceTempView("dp10a")
df_aiq_input2.createOrReplaceTempView("aiq_input2")

df_dp10b = spark.sql("""
select a.chid
, b.AIQ_HHID
, b.AIQ_INDID
, b.Ventile_TRAVELSUPER_BUYER as M0002
, b.Ventile_TravelSuper_HT as M0003
, b.Ventile_Online_Booking_Prop as M0004
, b.Ventile_Online_Booking_HT_Prop as M0005
, b.Ventile_Hotel_Book_Prop as M0006
, b.Ventile_Hotel_Book_HighTran as M0007
, b.Ventile_CAR_RENTAL_PROP as M0008
, b.Ventile_EMAILCLICK_Last3Mo as M0009
, b.Ventile_EMAILOPEN_LAST3M as M0010
, b.Ventile_WebVisit_last3Mo as M0011
, b.Ventile_Travel_SEGMENT1 as M0012
, b.Ventile_Travel_SEGMENT2 as M0013
, b.Ventile_Travel_SEGMENT3 as M0014
, b.Ventile_Travel_SEGMENT4 as M0015
, b.Ventile_Travel_SEGMENT5 as M0016
, b.Ventile_Travel_SEGMENT6 as M0017
, b.Ventile_nmas_01 as M0018
, b.Ventile_nmas_02 as M0019
, b.Ventile_nmas_03 as M0020
, b.Ventile_nmas_04 as M0021
, b.Ventile_nmas_05 as M0022
, b.Ventile_nmas_06 as M0023
, b.Ventile_nmas_11 as M0024
, b.Ventile_nmas_40 as M0025
, b.Ventile_LifeStage_Seg1 as M0026
, b.Ventile_LifeStage_Seg2 as M0027
, b.Ventile_LifeStage_Seg3 as M0028
, b.Ventile_LifeStage_Seg4 as M0029
, b.Ventile_LifeStage_Seg5 as M0030
, b.Ventile_LifeStage_Seg6 as M0031
, b.Ventile_LifeStage_Seg7 as M0032
, b.Ventile_LifeStage_Seg8 as M0033
, b.Ventile_Auto_Prop as M0034
, b.Ventile_Auto_High_Trans as M0035
, b.Ventile_Home_PROP as M0036
, b.Ventile_nmas_12 as M0037
, b.Ventile_nmas_19 as M0038
, b.Ventile_nmas_34 as M0039
, b.Ventile_nmas_13 as M0040
, b.Ventile_Super_Buyer as M0041
, b.Ventile_Cruise_Prop as M0042
, b.Ventile_Home_High_Trans as M0043
, b.Cnt_Prospect_z4 as M0044
, b.Cnt_Active_z4 as M0045
, b.Ratio_Active_Prospect_z4 as M0046
, b.Cnt_Prospect_z5 as M0047
, b.Cnt_Active_z5 as M0048
, b.Ratio_Active_Prospect_z5 as M0049
, b.Cnt_Prospect_msa as M0050
, b.Cnt_Active_msa as M0051
, b.Ratio_Active_Prospect_msa as M0052
, b.Quartile_Ratio_z4 as M0053
, b.Quartile_Ratio_z5 as M0054
, b.Quartile_Ratio_msa as M0055
, b.Ventile_LifeIns_HighTran as M0056
, b.Ventile_LifeIns_Prop as M0057
, b.Ventile_MotorcycleIns_Prop as M0058
, b.Ventile_Pharmacy_HighTrans as M0059
, b.Ventile_nmas_20 as M0060
, b.Ventile_nmas_38 as M0061
, b.Ventile_nmas_21 as M0062
, b.Ventile_NewMember_Clone as M0063
, b.Ventile_HomeSecurity_Prop as M0064
, b.Ventile_Roadside_Prop as M0065
, b.Ventile_Roadside_HighTrans as M0066
, b.Ventile_nmas_23 as M0067
, b.Ventile_nmas_07 as M0068
, b.Ventile_nmas_08 as M0069
, b.Ventile_nmas_09 as M0070
, b.Ventile_nmas_10 as M0071
, b.Ventile_nmas_14 as M0072
, b.Ventile_nmas_15 as M0073
, b.Ventile_nmas_16 as M0074
, b.Ventile_nmas_17 as M0075
, b.Ventile_nmas_18 as M0076
, b.Ventile_nmas_22 as M0077
, b.Ventile_nmas_24 as M0078
, b.Ventile_nmas_25 as M0079
, b.Ventile_nmas_26 as M0080
, b.Ventile_nmas_27 as M0081
, b.Ventile_nmas_29 as M0082
, b.Ventile_nmas_30 as M0083
, b.Ventile_nmas_31 as M0084
, b.Ventile_nmas_32 as M0085
, b.Ventile_nmas_33 as M0086
, b.Ventile_nmas_35 as M0087
, b.Ventile_nmas_36 as M0088
, b.Ventile_nmas_37 as M0089
, b.Ventile_nmas_39 as M0090
, b.Ventile_Model1a as M0091
, b.Ventile_Model2a as M0092
, b.Ventile_Model3a as M0093
, b.Ventile_Model4a as M0094
, b.Ventile_Model5a as M0095
, b.Ventile_Model6a as M0096
, b.scr_aarp_caregiver_cloningT as M0097
, b.PRED_SEGMENT_NS as M0098
, b.Ventile_Model11a as M0099
, b.Ventile_Model12a as M0100
, b.Ventile_Model13a as M0101
, b.scr_aarp_caregiver_Married66p as M0102
, b.PRED_SEGMENT_S as M0103
, b.RegB_AccessBank_AtBank as J1147
, b.RegB_AccessBank_Mobile as J1148
, b.RegB_AccessBank_Online as J1149
, b.RegB_CC_Airline_IM as J1150
, b.RegB_CC_BalTrans_IM as J1151
, b.RegB_CC_LowInterest_IM as J1152
, b.RegB_FinTech_Digital as J1153
, b.RegB_FinTech_MWallet as J1154
, b.RegB_Ins_Auto_IM as J1155
, b.RegB_Ins_Health_IM as J1156
, b.RegB_Ins_Life_IM as J1157
, b.RegB_Ins_Employer_AH as J1158
, b.RegB_Ins_Market_AH as J1159
, b.RegB_Ins_Medicare_AH as J1160
, b.LT_Military_Veteran as J1161
, b.InMarket_Aldi as J1162
, b.InMarket_TraderJoes as J1163
, b.InMarket_SamsClub as J1164
, b.InMarket_Costco as J1165
, b.InMarket_Sprouts as J1166
, b.InMarket_Publix as J1167
, b.InMarket_Kroger as J1168
, b.InMarket_Safeway as J1169
, b.InMarket_VRBO as J1170
, b.InMarket_Venmo as J1171
, b.SocialIQ_Tumblr as J1172
, b.AIQ_Mail_Priority as J1173
, b.distanceiq_costco as J1174
, b.distanceiq_sprouts as J1175
, b.distanceiq_aldi as J1176
, b.distanceiq_kroger as J1177
, b.distanceiq_publix as J1178
, b.distanceiq_safeway as J1179
, b.distanceiq_sams as J1180
, b.distanceiq_traderjoe as J1181
, b.distanceiq_target as J1182
, b.Sephora_Shopper as J1183
, b.Walmart_Shopper as J1184
, b.Whole_Foods_Shopper as J1185
, b.Target_Shopper as J1186
, b.Military_Veteran as J1187
, b.Aldi_Shopper as J1188
, b.TraderJoes_Shopper as J1189
, b.SamsClub_Shopper as J1190
, b.Costco_Shopper as J1191
, b.Sprouts_Shopper as J1192
, b.Publix_Shopper as J1193
, b.Kroger_Shopper as J1194
, b.Safeway_Shopper as J1195
, b.COHABITATE as J1196
, b.RegB_FinTech_MInvest as J1197
, b.LT_Auto_Parts_Accessories_v2 as J1198
, b.Auto_Parts_Accessories_v2 as J1199
, b.LT_Auto_Work_v2 as J1200
, b.Auto_Work_v2 as J1201
, b.LT_DIYers_v2 as J1202
, b.DIYers_v2 as J1203
, b.LT_Home_Furnishings_Deco_v2 as J1204
, b.Home_Furnishings_Deco_v2 as J1205
, b.LT_Home_Improvement_v2 as J1206
, b.Home_Improvement_v2 as J1207
, b.LT_Woodworking_Fan_v2 as J1208
, b.Woodworking_Fan_v2 as J1209
, b.DIY_Living_v2 as J1210
, b.Home_Improvement_Grouping_v2 as J1211
, b.Home_Decorating_Fan_v2 as J1212
, b.InMarket_H_E_B as J1213
, b.Steam_User as J1214
, b.Twitch_Viewer as J1215
, b.YouTube_Creator as J1216
, b.GAMING_PLATFORM as J1217
, b.HW_Anxiety as J1218
, b.HW_Depression as J1219
, b.HW_Vaping as J1220
, b.Job_Seeker as J1221
, b.Job_Seeker_Active as J1222
, b.InMarket_Meijer as J1223
, b.RegB_Cryptocurrency as J1224
, b.RegB_Need_Mtg_Broker as J1225
, b.PoliticsIQ_Legalize_MJ as J1226
, b.HW_CBD_Usage as J1227
, b.HW_CBD_Usage_Smoking as J1228
, b.HW_CBD_Usage_Edibles as J1229
, b.HW_CBD_Reason_Pain as J1230
, b.HW_CBD_Reason_Stress as J1231
, b.Exercise_Fan_v2 as J1232
, b.LT_Exercise_Fan_v2 as J1233
, b.Exercise_Dance_v2 as J1234
, b.LT_Exercise_Dance_v2 as J1235
, b.Exercise_Int_Train_v2 as J1236
, b.LT_Exercise_Int_Train_v2 as J1237
, b.Exercise_Weight_Lift_v2 as J1238
, b.LT_Exercise_Weight_Lift_v2 as J1239
, b.Exercise_Yoga_v2 as J1240
, b.LT_Exercise_Yoga_v2 as J1241
, b.Hiking_Fan_v2 as J1242
, b.LT_Hiking_Fan_v2 as J1243
, b.HW_Meditate as J1244
, b.HW_Stress_v2 as J1245
, b.HW_Sleep_v3 as J1246
, b.HW_Primary_Care_Doctor as J1247
, b.HW_Primary_Care_Visits as J1248
, b.HW_ER_Visits as J1249
, b.HW_Med_Specialist_Visits as J1250
, b.HW_Urgent_Care_Visits as J1251
, b.HW_Med_Utilization as J1252
, b.HW_Online_RX as J1253
, b.HW_WebMD as J1254
, b.HW_Aromatherapy as J1255
, b.HW_Chiropractics as J1256
, b.HW_Herbal_Remedies as J1257
, b.HW_Homeopathic as J1258
, b.Premover_YN_v2 as J1259
, b.Premover_v2 as J1260
, b.Premover_Homeowner_v2 as J1261
, b.Premover_Renter_v2 as J1262
, b.Premover_New_Owner_v2 as J1263
, b.Cluster_50Plus as J1075
, b.RegB_AccessBank_AtBank_z4 as K1120
, b.RegB_AccessBank_Mobile_z4 as K1121
, b.RegB_AccessBank_Online_z4 as K1122
, b.RegB_CC_Airline_IM_z4 as K1123
, b.RegB_CC_BalTrans_IM_z4 as K1124
, b.RegB_CC_LowInterest_IM_z4 as K1125
, b.RegB_FinTech_Digital_z4 as K1126
, b.RegB_FinTech_MWallet_z4 as K1127
, b.RegB_Ins_Auto_IM_z4 as K1128
, b.RegB_Ins_Health_IM_z4 as K1129
, b.RegB_Ins_Life_IM_z4 as K1130
, b.RegB_Ins_Employer_AH_z4 as K1131
, b.RegB_Ins_Market_AH_z4 as K1132
, b.RegB_Ins_Medicare_AH_z4 as K1133
, b.LT_Military_Veteran_z4 as K1134
, b.InMarket_Aldi_z4 as K1135
, b.InMarket_TraderJoes_z4 as K1136
, b.InMarket_SamsClub_z4 as K1137
, b.InMarket_Costco_z4 as K1138
, b.InMarket_Sprouts_z4 as K1139
, b.InMarket_Publix_z4 as K1140
, b.InMarket_Kroger_z4 as K1141
, b.InMarket_Safeway_z4 as K1142
, b.InMarket_VRBO_z4 as K1143
, b.InMarket_Venmo_z4 as K1144
, b.SocialIQ_Tumblr_z4 as K1145
, b.Decile_1800Flowers_APR2016_NoMM as M1001
, b.Decile_ADT_APR2016_NoMM as M1002
, b.Decile_ALL_APR2016_NoMM as M1003
, b.Decile_Angie_APR2016_NoMM as M1004
, b.Decile_AVIS_v2_NoMM as M1005
, b.Decile_Catmaran_APR2016_NoMM as M1006
, b.Decile_CHASE_NoMM as M1007
, b.decile_consumercel_mar2016_nomm as M1008
, b.Decile_DENNY_NoMM as M1009
, b.Decile_EXPEDIA_NoMM as M1010
, b.Decile_FamilyDollar_NoMM as M1011
, b.Decile_freds_NOMM as M1012
, b.Decile_gcr_NoMM as M1013
, b.Decile_HARTFORD_AUTO_NoMM as M1014
, b.Decile_HARTFORD_HOME_NoMM as M1015
, b.Decile_Hilton_NoMM as M1016
, b.DECILE_HomeServe_NoMM as M1017
, b.decile_libertytravel_NoMM as M1018
, b.Decile_MedJet_NoMM as M1019
, b.Decile_NCL_NoMM as M1020
, b.Decile_NYL_Annuity_NoMM as M1021
, b.Decile_NYL_NoMM as M1022
, b.decile_ParkRideFly_apr2016_NoMM as M1023
, b.Decile_REGAL_NoMM as M1024
, b.Decile_SCHWAN_NoMM as M1025
, b.Decile_TANGER_NoMM as M1026
, b.Decile_TrustedID_NoMM as M1027
, b.Decile_UPS_NoMM as M1028
, b.decile_walgreens_apr2016_NoMM as M1029
, b.SCR_Spbyerdisc_OCT16_NoMM as M1030
, b.SCR_Spbyerfin_OCT16_NoMM as M1031
, b.scr_super_buyer_ins_NoMM as M1032
, b.scr_super_buyer_life_noMM as M1033
, b.scr_super_buyer_total_noMM as M1034
, b.DECILE_Spbyerdisc_OCT16_NoMM as M1037
, b.DECILE_Spbyerfin_OCT16_NoMM as M1038
, b.decile_super_buyer_ins_NoMM as M1039
, b.decile_super_buyer_life_noMM as M1040
, b.decile_super_buyer_total_noMM as M1041
, b.decile_aarp_caregiver_cloningT as M1035
, b.decile_aarp_caregiver_Married66p as M1036
from dp10a a
left join aiq_input2 b on a.AIQ_HHID = b.AIQ_HHID and a.AIQ_INDID = b.AIQ_INDID
""")

df_mms_m_dp10b = df_dp10b.dropDuplicates(["CHID"])

# *** 2.K Append Attributes from AIQ ZIP+4 Level Table **** ;
df_aiq_zip4 = spark.read.format("delta").load(f"{src_aiq_path}/{AIQ_Input_z4}")

# Create DP11
df_aiq_zip4.createOrReplaceTempView("AIQ_INPUT_Z4_TBL")

df_dp11 = spark.sql(f"""
select  a.CHID
       ,b.CHID_key
, b.VectorIQ as K0001
, b.VectorIQ_bkpt as K0002
, b.VectorIQ_bkpt_fc as K0003
, b.VectorIQ_bkpt_delta as K0004
, b.VectorIQ_bkpt_index as K0005
, b.VectorIQ_bdep as K0006
, b.VectorIQ_bdep_fc as K0007
, b.VectorIQ_bdep_delta as K0008
, b.VectorIQ_bdep_index as K0009
, b.VectorIQ_hinc as K0010
, b.VectorIQ_hinc_fc as K0011
, b.VectorIQ_hinc_delta as K0012
, b.VectorIQ_hinc_index as K0013
, b.VectorIQ_hsst as K0014
, b.VectorIQ_hsst_fc as K0015
, b.VectorIQ_hsst_delta as K0016
, b.VectorIQ_hsst_index as K0017
, b.VectorIQ_retl as K0018
, b.VectorIQ_retl_fc as K0019
, b.VectorIQ_retl_delta as K0020
, b.VectorIQ_retl_index as K0021
, b.VectorIQ_unem as K0022
, b.VectorIQ_unem_fc as K0023
, b.VectorIQ_unem_delta as K0024
, b.VectorIQ_unem_index as K0025
, b.VectorIQ_hprc as K0026
, b.VectorIQ_hprc_fc as K0027
, b.VectorIQ_hprc_delta as K0028
, b.VectorIQ_hprc_index as K0029
, b.AIQ_Income as K0030
, b.AIQ_wealth as K0031
, b.AIQ_Donor_Pct as K0032
, b.aiq_rel_pen_tot as K0033
, b.aiq_rel_pen_prot as K0034
, b.aiq_rel_pen_nonc as K0035
, b.aiq_rel_pen_cath as K0036
, b.aiq_rel_pen_jud as K0037
, b.aiq_rel_index as K0038
, b.aiq_rel_pct as K0039
, b.RelPct_Max_v2 as K0040
, b.RelIndex_Max_v2 as K0041
, b.RelPct_Black_v2 as K0042
, b.RelPct_Evang_v2 as K0043
, b.RelPct_Tot_v2 as K0044
, b.RelPct_NoUnk_v2 as K0045
, b.RelPct_Prot_v2 as K0046
, b.RelPct_Cath_v2 as K0047
, b.RelPct_Jud_v2 as K0048
, b.RelPct_NonC_v2 as K0049
, b.HomevalueIQ_TimeSeries as K0050
, b.ResponseIQ_Econ as K0051
, b.Resp_Comp_A as K0052
, b.Resp_Comp_B as K0053
, b.Resp_Comp_C as K0054
, b.Resp_Comp_D as K0055
, b.Resp_Comp_F as K0056
, b.AIQ_Sources as K0057
, b.WealthIQ as K0058
, b.WealthIQ_RegB as K0059
, b.WealthIQ2 as K0060
, b.WealthIQ2_RegB as K0061
, b.AIQ_Pol_Ind as K0062
, b.AIQ_Pol_Major as K0063
, b.AIQ_Politics as K0064
, b.InvestorIQ as K0065
, b.ChurnIQ as K0066
, b.WIQ_Dif as K0067
, b.WIQ_Dif_RegB as K0068
, b.IIQ_Dif as K0069
, b.Rel_Income_Index as K0070
, b.del_cat as K0071
, b.delineate as K0072
, b.AIQ_Rel_Prot as K0073
, b.AIQ_Rel_Prot_v2 as K0074
, b.AIQ_Rel_Cath as K0075
, b.AIQ_Rel_Cath_v2 as K0076
, b.AIQ_Rel_Jud as K0077
, b.AIQ_Rel_Jud_v2 as K0078
, b.AIQ_Rel_NonC_v2 as K0079
, b.AIQ_Religion as K0080
, b.AIQ_Religion_v2 as K0081
, b.AIQ_Culture as K0082
, b.Delineate_Choice as K0083
, b.AIQ_Green as K0084
, b.Aspects as K0085
, b.ResponseIQ3 as K0086
, b.ResponseIQ_v4 as K0087
, b.CharityIQ as K0088
, b.IncomeIQ as K0089
, b.IncomeIQ_Dol as K0090
, b.spendex as K0091
, b.ResponseIQ_Plus_v4 as K0092
, b.CharityIQ_Plus as K0093
, b.ResponseIQ_v5 as K0094
, b.ResponseIQ_Plus_v5 as K0095
, b.IncomeIQ_Disp as K0096
, b.DebtRatio as K0097
, b.SpendexRatio as K0098
, b.Spendex_Index as K0099
, b.InvestorIQ_max as K0100
, b.Premover as K0101
, b.WealthIQ_Max as K0102
, b.responseIQ_cng_6mo as K0103
, b.responseIQ_cng_pct_6mo as K0104
, b.responseIQ_cng_9mo as K0105
, b.responseIQ_cng_pct_9mo as K0106
, b.responseIQ_cng_3mo as K0107
, b.responseIQ_cng_pct_3mo as K0108
, b.responseIQ_cng_12mo as K0109
, b.responseIQ_cng_pct_12mo as K0110
, b.ResponseIQ_Fin_High as K0111
, b.ResponseIQ_Fin_Low as K0112
, b.ResponseIQ_Fin_v1 as K0113
, b.RecoveryIQ as K0114
, b.Invest_Active_z4 as K0115
, b.invest_finance_z4 as K0116
, b.invest_foreign_z4 as K0117
, b.Invest_num_res_prop_z4 as K0118
, b.invest_personal_z4 as K0119
, b.invest_real_estate_z4 as K0120
, b.invest_stocks_securities_z4 as K0121
, b.investments_z4 as K0122
, b.Value_Hunter_z4 as K0123
, b.Opportunity_Seekers_z4 as K0124
, b.News_Financial_z4 as K0125
, b.Automotive_Buff_z4 as K0126
, b.Book_Reader_z4 as K0127
, b.Computer_Owner_z4 as K0128
, b.Cooking_Fan_z4 as K0129
, b.DIYers_z4 as K0130
, b.Exercise_Fan_z4 as K0131
, b.Home_Gardener_z4 as K0132
, b.Golf_Fan_z4 as K0133
, b.Home_Decorating_Fan_z4 as K0134
, b.Outdoor_Grouping_z4 as K0135
, b.Sports_Grouping_z4 as K0136
, b.Photography_Fan_z4 as K0137
, b.Traveling_Fan_z4 as K0138
, b.Pet_Owner_z4 as K0139
, b.Cat_Owner_z4 as K0140
, b.Dog_Owner_z4 as K0141
, b.Mail_Responder_z4 as K0142
, b.Sweepstakes_Fan_z4 as K0143
, b.Religious_Magazines_z4 as K0144
, b.Male_Merch_Buyer_z4 as K0145
, b.Female_Merch_Buyer_z4 as K0146
, b.Crafts_Hobbies_Merch_Buyer_z4 as K0147
, b.Gardening_Farm_Buyer_z4 as K0148
, b.Book_Buyer_z4 as K0149
, b.Collect_Special_Foods_z4 as K0150
, b.Religious_Contributor_z4 as K0151
, b.Political_Contributor_z4 as K0152
, b.Political_Lib_Char_Donation_z4 as K0153
, b.Health_Inst_Contributor_z4 as K0154
, b.Charitable_Contributor_z4 as K0155
, b.General_Contributor_z4 as K0156
, b.Donates_by_Mail_z4 as K0157
, b.Veteran_in_HH_z4 as K0158
, b.High_Tech_Leader_z4 as K0159
, b.Mail_Order_Buyer_z4 as K0160
, b.Online_Purchaser_z4 as K0161
, b.Apparel_Women_z4 as K0162
, b.Apparel_Women_Petite_z4 as K0163
, b.Apparel_Women_Plus_z4 as K0164
, b.Apparel_Women_Young_z4 as K0165
, b.Apparel_Men_z4 as K0166
, b.Apparel_Men_Big_Tall_z4 as K0167
, b.Apparel_Men_Young_z4 as K0168
, b.Apparel_Children_z4 as K0169
, b.Health_Beauty_z4 as K0170
, b.Beauty_Cosmetics_z4 as K0171
, b.Jewelry_Purchaser_z4 as K0172
, b.Luggage_Purchaser_z4 as K0173
, b.Animal_Welfare_Donation_z4 as K0174
, b.Arts_Cultural_Donation_z4 as K0175
, b.Environmental_Donation_z4 as K0176
, b.International_Aid_Donation_z4 as K0177
, b.Veterans_Donation_z4 as K0178
, b.Other_Charitable_Donation_z4 as K0179
, b.Community_Charities_z4 as K0180
, b.Parenting_Category_z4 as K0181
, b.Single_Parent_z4 as K0182
, b.Apparel_Infants_Toddlers_z4 as K0183
, b.Children_Learning_Toys_z4 as K0184
, b.Children_Prod_Baby_Care_z4 as K0185
, b.Children_Prod_Back_School_z4 as K0186
, b.Children_Prod_General_z4 as K0187
, b.Young_Adult_in_HH_z4 as K0188
, b.Senior_Adult_in_HH_z4 as K0189
, b.Children_Interests_z4 as K0190
, b.Grandchildren_Category_z4 as K0191
, b.Christian_Families_z4 as K0192
, b.Equestrian_Fan_z4 as K0193
, b.Other_Pet_Owner_z4 as K0194
, b.Career_Improvement_z4 as K0195
, b.Working_Woman_z4 as K0196
, b.Soho_Indicator_z4 as K0197
, b.Career_Oriented_z4 as K0198
, b.Books_Magazines_z4 as K0199
, b.Books_Music_z4 as K0200
, b.Books_Music_Audio_z4 as K0201
, b.Reading_General_z4 as K0202
, b.Reading_Science_Fiction_z4 as K0203
, b.Reading_Magazines_z4 as K0204
, b.Reading_Audio_Books_z4 as K0205
, b.Reading_Grouping_z4 as K0206
, b.History_Military_z4 as K0207
, b.Current_Affairs_Politics_z4 as K0208
, b.Religious_Inspirational_z4 as K0209
, b.Science_Space_z4 as K0210
, b.Magazine_Reader_z4 as K0211
, b.Education_OnLine_z4 as K0212
, b.Gaming_Fan_z4 as K0213
, b.DVDs_Videos_z4 as K0214
, b.Elect_Computing_TV_Videos_z4 as K0215
, b.Elect_Computing_Home_Office_z4 as K0216
, b.High_End_Appliances_z4 as K0217
, b.Music_Home_Stereo_z4 as K0218
, b.Music_Player_z4 as K0219
, b.Music_Collector_z4 as K0220
, b.Music_Avid_Listener_z4 as K0221
, b.Movie_Collector_z4 as K0222
, b.TV_Cable_z4 as K0223
, b.Video_Games_z4 as K0224
, b.TV_Satellite_Dish_z4 as K0225
, b.Computer_Games_z4 as K0226
, b.Consumer_Electronics_z4 as K0227
, b.Movie_Music_Grouping_z4 as K0228
, b.Electronics_Comp_Group_z4 as K0229
, b.TeleCom_z4 as K0230
, b.Art_Antiques_Antiques_z4 as K0231
, b.Art_Antique_Art_z4 as K0232
, b.Theater_Performing_Arts_z4 as K0233
, b.Arts_Fan_z4 as K0234
, b.Musical_Instruments_z4 as K0235
, b.Collectibles_General_z4 as K0236
, b.Collectibles_Stamps_z4 as K0237
, b.Collectibles_Coins_z4 as K0238
, b.Collectibles_Arts_z4 as K0239
, b.Collectibles_Antiques_z4 as K0240
, b.Collector_Avid_z4 as K0241
, b.Collect_Antiques_Group_z4 as K0242
, b.Collectibles_Sports_z4 as K0243
, b.Military_Weaponry_z4 as K0244
, b.Auto_Work_z4 as K0245
, b.Sewing_Knitting_Needlework_z4 as K0246
, b.Woodworking_Fan_z4 as K0247
, b.Aviation_Fan_z4 as K0248
, b.House_Plants_z4 as K0249
, b.Crafts_Fan_z4 as K0250
, b.Home_Garden_z4 as K0251
, b.Gardening_Fan_z4 as K0252
, b.Home_Improvement_Grouping_z4 as K0253
, b.Photography_Video_Equip_z4 as K0254
, b.Home_Furnishings_Decorating_z4 as K0255
, b.Home_Improvement_z4 as K0256
, b.Food_Wines_z4 as K0257
, b.Cooking_General_z4 as K0258
, b.Food_Natural_z4 as K0259
, b.Cooking_Food_Grouping_z4 as K0260
, b.Board_Games_Puzzles_z4 as K0261
, b.Gaming_Casino_z4 as K0262
, b.Travel_Grouping_z4 as K0263
, b.Travel_Domestic_z4 as K0264
, b.Travel_International_z4 as K0265
, b.Travel_Cruise_Vacations_z4 as K0266
, b.Home_Living_z4 as K0267
, b.DIY_Living_z4 as K0268
, b.Sporty_Living_z4 as K0269
, b.Upscale_Living_z4 as K0270
, b.Cultural_Artistic_Living_z4 as K0271
, b.High_Brow_z4 as K0272
, b.Common_Living_z4 as K0273
, b.Professional_Living_z4 as K0274
, b.Broader_Living_z4 as K0275
, b.Exercise_Health_Grouping_z4 as K0276
, b.Exercise_Running_Jogging_z4 as K0277
, b.Exercise_Walking_z4 as K0278
, b.Exercise_Aerobic_z4 as K0279
, b.Spec_Sport_Auto_MC_Racing_z4 as K0280
, b.Spec_Sport_TV_Sports_z4 as K0281
, b.Spec_Sport_Football_z4 as K0282
, b.Spec_Sport_Baseball_z4 as K0283
, b.Spec_Sport_Basketball_z4 as K0284
, b.Spec_Sport_Hockey_z4 as K0285
, b.Spec_Sport_Soccer_z4 as K0286
, b.Tennis_Fan_z4 as K0287
, b.Snow_Skiing_Fan_z4 as K0288
, b.Motorcycling_Fan_z4 as K0289
, b.NASCAR_Fan_z4 as K0290
, b.Boating_Sailing_Fan_z4 as K0291
, b.Scuba_Diving_Fan_z4 as K0292
, b.Sports_Leisure_z4 as K0293
, b.Hunting_Fan_z4 as K0294
, b.Fishing_Fan_z4 as K0295
, b.Camping_Hiking_Fan_z4 as K0296
, b.Hunting_Shooting_Fan_z4 as K0297
, b.Health_Medical_z4 as K0298
, b.Dieting_Weight_Loss_z4 as K0299
, b.Self_Improvement_z4 as K0300
, b.Auto_Parts_Accessories_z4 as K0301
, b.Home_Age_z4 as K0302
, b.Age_z4 as K0303
, b.ResidenceTime_z4 as K0304
, b.MortgageAmount_z4 as K0305
, b.PersonsatResidence_z4 as K0306
, b.TypeofDwelling_z4 as K0307
, b.MarriageProbability_z4 as K0308
, b.GenderProbability_z4 as K0309
, b.AMEX_Reg_z4 as K0310
, b.AMEX_Prem_z4 as K0311
, b.Prem_CC_z4 as K0312
, b.Retail_CC_z4 as K0313
, b.Bank_CC_z4 as K0314
, b.Travel_CC_z4 as K0315
, b.NumberofAdults_z4 as K0316
, b.NumberofChildren_z4 as K0317
, b.HomeOwner_pct_z4 as K0318
, b.Renter_pct_z4 as K0319
, b.Education_Hs_z4 as K0320
, b.Education_Coll_z4 as K0321
, b.Education_Grad_z4 as K0322
, b.LoantoValue_z4 as K0323
, b.PoliticsIQ_Donor_Liberal_z4 as K0324
, b.PoliticsIQ_Donor_Cons_z4 as K0325
, b.IncomeIQ_plus_z4 as K0326
, b.Change_in_Home_Value_12mo_z4 as K0327
, b.Change_in_Home_Value_3mo_z4 as K0328
, b.Change_in_Home_Value_6mo_z4 as K0329
, b.IncomeIQ_plus_Disp_z4 as K0330
, b.IncomeIQ_plus_ratio_z4 as K0331
, b.Ratio_Home_Val_12mo_Inc_z4 as K0332
, b.WealthIQ_plus_z4 as K0333
, b.AIQ_Green_plus_z4 as K0334
, b.Home_to_Income_ratio_z4 as K0335
, b.InvestorIQ_plus_z4 as K0336
, b.InvestorIQ_plus_v2_z4 as K0337
, b.InvestorIQ_plus_ratio_z4 as K0338
, b.Spendex_plus_z4 as K0339
, b.Change_in_Income_12Mo_z4 as K0340
, b.Spendex_DineOut_z4 as K0341
, b.Spendex_Alcohol_z4 as K0342
, b.Spendex_Apparel_z4 as K0343
, b.Spendex_Entertain_z4 as K0344
, b.Spendex_Personal_z4 as K0345
, b.Spendex_Reading_z4 as K0346
, b.Spendex_Education_z4 as K0347
, b.Spendex_Tobacco_z4 as K0348
, b.Spendex_Donation_z4 as K0349
, b.Spendex_Furnish_z4 as K0350
, b.WealthIQ_plus_v2_z4 as K0351
, b.WealthIQ_max_plus_z4 as K0352
, b.HomeValueIQ_z4 as K0353
, b.AIQ_Home_Value_z4 as K0354
, b.SocialIQ_z4 as K0355
, b.SocialIQ_V2_z4 as K0356
, b.SCR_Dynabase_z4 as K0357
, b.SCR_Dyn_Open_z4 as K0358
, b.SCR_Dyn_Bounce_z4 as K0359
, b.SCR_Email_Dynabase_z4 as K0360
, b.EthnicIQ_z4 as K0361
, b.EthnicIQ_total_z4 as K0362
, b.ethniciq_v2_z4 as K0363
, b.Change_in_Income_12Mo_RegB_z4 as K0364
, b.Home_to_Income_Ratio_RegB_z4 as K0365
, b.IncomeIQ_Plus_DISP_RegB_z4 as K0366
, b.IncomeIQ_Plus_Ratio_RegB_z4 as K0367
, b.IncomeIQ_Plus_RegB_z4 as K0368
, b.InvestorIQ_Plus_Ratio_RegB_z4 as K0369
, b.InvestorIQ_Plus_RegB_z4 as K0370
, b.InvestorIQ_Plus_v2_RegB_z4 as K0371
, b.Ratio_HV_12Mo_to_Inc_RegB_z4 as K0372
, b.Spendex_ALCOHOL_RegB_z4 as K0373
, b.Spendex_APPAREL_RegB_z4 as K0374
, b.Spendex_DINEOUT_RegB_z4 as K0375
, b.Spendex_DONATION_RegB_z4 as K0376
, b.Spendex_EDUCATION_RegB_z4 as K0377
, b.Spendex_ENTERTAIN_RegB_z4 as K0378
, b.Spendex_FURNISH_RegB_z4 as K0379
, b.Spendex_MISC_RegB_z4 as K0380
, b.Spendex_PERSONAL_RegB_z4 as K0381
, b.Spendex_Plus_RegB_z4 as K0382
, b.Spendex_READING_RegB_z4 as K0383
, b.Spendex_TOBACCO_RegB_z4 as K0384
, b.Wealthiq_plus_RegB_z4 as K0385
, b.Wealthiq_plus_v2_RegB_z4 as K0386
, b.INVESTORIQ_PLUS_V3_z4 as K0387
, b.INVESTORIQ_PLUS_V3_CHECKING_z4 as K0388
, b.INVESTORIQ_PLUS_V3_SAVINGS_z4 as K0389
, b.INVESTORIQ_PLUS_V3_BONDS_z4 as K0390
, b.INVESTORIQ_PLUS_V3_LIFE_z4 as K0391
, b.INVESTORIQ_PLUS_V3_ANNUITY_z4 as K0392
, b.AutoProp_Ford_z4 as K0393
, b.AutoProp_Chevrolet_z4 as K0394
, b.AutoProp_Toyota_z4 as K0395
, b.AutoProp_Dodge_z4 as K0396
, b.AutoProp_Honda_z4 as K0397
, b.AutoProp_Nissan_z4 as K0398
, b.AutoProp_Jeep_z4 as K0399
, b.AutoProp_GMC_z4 as K0400
, b.AutoProp_Buick_z4 as K0401
, b.AutoProp_Hyundai_z4 as K0402
, b.AutoProp_Mazda_z4 as K0403
, b.AutoProp_Cadillac_z4 as K0404
, b.AutoProp_Volkswagen_z4 as K0405
, b.AutoProp_BMW_z4 as K0406
, b.AutoProp_Kia_z4 as K0407
, b.AutoProp_Lexus_z4 as K0408
, b.AutoProp_Mercedes_z4 as K0409
, b.AutoProp_Mitsubishi_z4 as K0410
, b.AutoProp_Subaru_z4 as K0411
, b.AutoProp_Lincoln_z4 as K0412
, b.AutoProp_Acura_z4 as K0413
, b.AutoProp_Volvo_z4 as K0414
, b.AutoProp_Infiniti_z4 as K0415
, b.AutoProp_Audi_z4 as K0416
, b.Auto_InMkt_z4 as K0417
, b.TravelProp_Cruise_z4 as K0418
, b.TravelProp_Domestic_z4 as K0419
, b.TravelProp_Intl_z4 as K0420
, b.Spendex_TRAVEL_z4 as K0421
, b.Spendex_TRAVEL_Dom_z4 as K0422
, b.Spendex_TRAVEL_Intl_z4 as K0423
, b.Spendex_TRAVEL_Cruise_z4 as K0424
, b.HomeValueIQ_Equity_z4 as K0425
, b.HomeValueIQ_Mortgage_z4 as K0426
, b.HomeValueIQ_EquityPct_z4 as K0427
, b.HomeValueIQ_LTV_z4 as K0428
, b.IncomeIQ_Plus_v2_z4 as K0429
, b.IncomeIQ_Plus_v2_RegB_z4 as K0430
, b.WealthIQ_Plus_v3_z4 as K0431
, b.InMarket_Auto_z4 as K0432
, b.InMarket_LTC_INS_z4 as K0433
, b.InMarket_ONLINE_EDU_z4 as K0434
, b.AIQ_ATP_z4 as K0435
, b.AIQ_ATP_DTI_z4 as K0436
, b.Socialiq_Twitter_z4 as K0437
, b.Socialiq_Facebook_z4 as K0438
, b.Churniq_v2_z4 as K0439
, b.InMarket_Term_Life_z4 as K0440
, b.Affordability_Index_z4 as K0441
, b.Affordability_Index_Scf_z4 as K0442
, b.AIQ_BusOwner_Scale_z4 as K0443
, b.AIQ_Address_Indicator_z4 as K0444
, b.smartphone_iphone_z4 as K0445
, b.smartphone_android_z4 as K0446
, b.smartphone_none_z4 as K0447
, b.channeliq_banner_z4 as K0448
, b.channeliq_interTV_z4 as K0449
, b.Channeliq_Sources_z4 as K0450
, b.Charityiq_HighDollar_z4 as K0451
, b.socialiq_facebook_v2_z4 as K0452
, b.socialiq_linkedin_v2_z4 as K0453
, b.socialiq_instagram_v2_z4 as K0454
, b.incomeiq_plus_v3_z4 as K0455
, b.Carrier_Route_Type_z4 as K0456
, b.NumberHouseholdsInZip4 as K0457
, b.Marriage_code_z4 as K0458
, b.Gender_code_z4 as K0459
, b.Homeequity_z4 as K0460
, b.NumberofAdultsinZip4 as K0461
, b.Homeequity_pct_z4 as K0462
, b.Dwellingtype_z4 as K0463
, b.Symphony_z4 as K0464
, b.Symphony_cat_z4 as K0465
, b.RiskIQ_Plus as K0466
, b.GC_TROP06 as K0467
, b.GC_TRADES as K0468
, b.GC_TROPEN as K0469
, b.GC_TROP12 as K0470
, b.GC_TROP24 as K0471
, b.GC_HSTSAT as K0472
, b.GC_HSAT24 as K0473
, b.GC_HST29X as K0474
, b.GC_HST39X as K0475
, b.GC_HST49X as K0476
, b.GC_TR4924 as K0477
, b.GC_TR49PR as K0478
, b.GC_TR224X as K0479
, b.GC_TOTRAT as K0480
, b.GC_SATRAT as K0481
, b.GC_AGEOTD as K0482
, b.GC_PUBREC as K0483
, b.GC_AUTRDS as K0484
, b.GC_BRTRDS as K0485
, b.GC_BROPEN as K0486
, b.GC_BRHSAT as K0487
, b.GC_BRBL50 as K0488
, b.GC_BRBAL0 as K0489
, b.GC_BRTBAL as K0490
, b.GC_BRHICR as K0491
, b.GC_BRUTIL as K0492
, b.GC_BROLDT as K0493
, b.GC_COLECT as K0494
, b.GC_CUTRDS as K0495
, b.GC_INQ006 as K0496
, b.GC_RETRDS as K0497
, b.GC_TOTBAL as K0498
, b.GC_TOTHIC as K0499
, b.GC_MTTRDS as K0500
, b.GC_MTTBAL as K0501
, b.GC_MTTHIC as K0502
, b.GC_MTOLDT as K0503
, b.GC_OPNINQRAT as K0504
, b.GC_RECENTRAT as K0505
, b.GC_RETRAT as K0506
, b.GC_AUTRAT as K0507
, b.GC_BRTRAT as K0508
, b.GC_SAT24RAT as K0509
, b.GC_TR39RAT as K0510
, b.GC_TR49RAT as K0511
, b.GC_PUBTRDRAT as K0512
, b.GC_COLTRDRAT as K0513
, b.GC_CUTRAT as K0514
, b.GC_BRBALTOTRD as K0515
, b.GC_BRHIGHRAT as K0516
, b.GC_BRSATRAT as K0517
, b.GC_TR49RECRAT as K0518
, b.GC_MTTRAT as K0519
, b.GC_MTUTIL as K0520
, b.GC_MTBALTOTRD as K0521
, b.Change_RiskIQ_6Mos as K0522
, b.AIQ_Religious_z4 as K0523
, b.LT_Deal_Site_z4 as K0524
, b.Socialiq_Deal_Site_z4 as K0525
, b.Investoriq_Plus_V4_z4 as K0526
, b.Investoriq_Plus_V4_Checking_z4 as K0527
, b.Investoriq_Plus_V4_Savings_z4 as K0528
, b.Investoriq_Plus_V4_Sbonds_z4 as K0529
, b.Investoriq_Plus_V4_Life_z4 as K0530
, b.Investoriq_Plus_V4_Annuity_z4 as K0531
, b.wealthiq_plus_v4_z4 as K0532
, b.Political_Cons_Char_Donation_z4 as K0533
, b.Political_Lib_Char_Donation_z4 as K0534
, b.Childrens_Charitable_Donation_z4 as K0535
, b.African_American_Professional_z4 as K0536
, b.Computing_Home_Office_General_z4 as K0537
, b.Lifestyles_Interests_Passions_z4 as K0538
, b.INVESTORIQ_PLUS_V3_SECURITIES_z4 as K0539
, b.Investoriq_Plus_V4_Securities_z4 as K0540
, b.JOBSIQ_BLUE_COLLAR_z4 as K0541
, b.JOBSIQ_CORP_LEADER_z4 as K0542
, b.JOBSIQ_PHYSICIANS_z4 as K0543
, b.JOBSIQ_EDUCATOR_z4 as K0544
, b.JOBSIQ_FINANCIAL_z4 as K0545
, b.JOBSIQ_HOMEMAKER_z4 as K0546
, b.JOBSIQ_INSURANCE_z4 as K0547
, b.JOBSIQ_LEGAL_z4 as K0548
, b.JOBSIQ_MIDDLE_MGMT_z4 as K0549
, b.JOBSIQ_OTHER_MEDICAL_z4 as K0550
, b.JOBSIQ_OTHER_W_COLLAR_z4 as K0551
, b.JOBSIQ_PROF_TECH_z4 as K0552
, b.JOBSIQ_PUBLIC_z4 as K0553
, b.JOBSIQ_REAL_ESTATE_z4 as K0554
, b.JOBSIQ_RETIRED_z4 as K0555
, b.JOBSIQ_SALES_z4 as K0556
, b.JOBSIQ_STUDENT_z4 as K0557
, b.Spendex_Plus_v2_z4 as K0558
, b.Spendex_DINEOUT_v2_z4 as K0559
, b.Spendex_ALCOHOL_v2_z4 as K0560
, b.Spendex_APPAREL_v2_z4 as K0561
, b.Spendex_ENTERTAIN_v2_z4 as K0562
, b.Spendex_PERSONAL_v2_z4 as K0563
, b.Spendex_READING_v2_z4 as K0564
, b.Spendex_EDUCATION_v2_z4 as K0565
, b.Spendex_CELL_PHONE_v2_z4 as K0566
, b.Spendex_DONATION_v2_z4 as K0567
, b.Spendex_FURNISH_v2_z4 as K0568
, b.Spendex_PERS_INS_v2_z4 as K0569
, b.Spendex_TRAVEL_v2_z4 as K0570
, b.Spendex_TRAVEL_DOM_v2_z4 as K0571
, b.Spendex_TRAVEL_INTL_v2_z4 as K0572
, b.Spendex_TRAVEL_CRUISE_v2_z4 as K0573
, b.SocialIQ_Trip_Advisor_z4 as K0574
, b.IncomeIQ_plus_DISP_v2_z4 as K0575
, b.LT_Gamer_scale_z4 as K0576
, b.LT_Exercise_z4 as K0577
, b.LT_Couponer_z4 as K0578
, b.Uber_Lyft_User_z4 as K0579
, b.Zipcar_User_z4 as K0580
, b.OS_AGREEABLE_z4 as K1001
, b.OS_Conscientious_z4 as K1002
, b.OS_COOKING_z4 as K1003
, b.OS_Donor_Children_z4 as K1004
, b.OS_Donor_Extrinsic_z4 as K1005
, b.OS_Donor_HC_z4 as K1006
, b.OS_Donor_Intrinsic_z4 as K1007
, b.OS_Donor_NonPlanned_z4 as K1008
, b.OS_EMOTION_z4 as K1009
, b.OS_EPFI_z4 as K1010
, b.OS_Extroversion_z4 as K1011
, b.OS_Fin_IMP_z4 as K1012
, b.OS_Fin_IMPControl_z4 as K1013
, b.OS_Fin_Motivation_z4 as K1014
, b.OS_Fin_Organization_z4 as K1015
, b.OS_Fin_Planning_z4 as K1016
, b.OS_FOODIE_z4 as K1017
, b.OS_Green_z4 as K1018
, b.OS_Impulsive_z4 as K1019
, b.OS_Innovator_z4 as K1020
, b.OS_Laggard_z4 as K1021
, b.OS_Materialism_z4 as K1022
, b.OS_Openness_z4 as K1023
, b.OS_Rel_Devotion_z4 as K1024
, b.OS_RiskTaking_Career_z4 as K1025
, b.OS_RiskTaking_Fin_z4 as K1026
, b.OS_RISKTAKING_HEALTH_z4 as K1027
, b.OS_RiskTaking_Rec_z4 as K1028
, b.OS_RiskTaking_Safe_z4 as K1029
, b.OS_RiskTaking_Social_z4 as K1030
, b.OS_Value_Seeker_z4 as K1031
, b.HW_ALCOHOL_v2_z4 as K1032
, b.HW_BMI_z4 as K1033
, b.HW_Diet_z4 as K1034
, b.HW_Job_Satis_z4 as K1035
, b.HW_Junk_Diet_z4 as K1036
, b.HW_SLEEP_v2_z4 as K1037
, b.HW_smoking_z4 as K1038
, b.HW_STRESS_z4 as K1039
, b.HW_YogaPilate_z4 as K1040
, b.InMarket_Amazon_z4 as K1041
, b.Inmarket_Apple_z4 as K1042
, b.Inmarket_Google_z4 as K1043
, b.InMarket_NonTradTV_z4 as K1044
, b.InMarket_OnlineDate_z4 as K1045
, b.InMarket_OnlineEDU_v2_z4 as K1046
, b.InMarket_OnlineShop_z4 as K1047
, b.InMarket_OnlineStream_z4 as K1048
, b.InMarket_Sephora_z4 as K1049
, b.Inmarket_Smart_Speaker_z4 as K1050
, b.Inmarket_Target_z4 as K1051
, b.InMarket_WalMart_z4 as K1052
, b.Inmarket_Whole_Foods_z4 as K1053
, b.African_American_Prof_v2_z4 as K1054
, b.AIQ_Business_Owner_v2_z4 as K1055
, b.AIQ_Employment_z4 as K1056
, b.AIQ_LGBTQ_z4 as K1057
, b.AIQ_NameVeracity_z4 as K1058
, b.BC_1stContact_Email_z4 as K1059
, b.BC_1stContact_InPerson_z4 as K1060
, b.BC_1stContact_Phone_z4 as K1061
, b.BC_Content_Webinar_z4 as K1062
, b.BC_Content_WhitePaper_z4 as K1063
, b.BC_DecisionMaker_z4 as K1064
, b.BC_Exec_NonHomeBased_Ind_z4 as K1065
, b.BC_Influencer_z4 as K1066
, b.BC_Purchase_Price_z4 as K1067
, b.BC_Purchase_Quality_z4 as K1068
, b.Hispanic_Professional_v2_z4 as K1069
, b.LT_Apparel_Women_Plus_v2_z4 as K1070
, b.LT_Boating_Sailing_Fan_v2_z4 as K1071
, b.LT_Career_Improvement_v2_z4 as K1072
, b.LT_Cat_Owner_v2_z4 as K1073
, b.LT_Crafts_Fan_v2_z4 as K1074
, b.LT_Crafts_Hobbies_Buyer_v2_z4 as K1075
, b.LT_Dog_Owner_v2_z4 as K1076
, b.LT_Exercise_Aerobic_v2_z4 as K1077
, b.LT_Exercise_Run_Jog_v2_z4 as K1078
, b.LT_Gardening_Fan_v2_z4 as K1079
, b.LT_Golf_Fan_v2_z4 as K1080
, b.LT_Home_Garden_v2_z4 as K1081
, b.LT_Hunting_Fan_v2_z4 as K1082
, b.LT_Motorcycling_Fan_v2_z4 as K1083
, b.LT_NASCAR_Fan_v2_z4 as K1084
, b.LT_Other_Pet_Owner_v2_z4 as K1085
, b.LT_Pet_Owner_v2_z4 as K1086
, b.LT_Religious_Magazines_v2_z4 as K1087
, b.LT_Senior_Adult_in_HH_v2_z4 as K1088
, b.LT_Spec_Sport_Auto_MC_v2_z4 as K1089
, b.LT_Travel_Domestic_v2_z4 as K1090
, b.LT_Travel_International_v2_z4 as K1091
, b.SCR_Wellness_z4 as K1092
, b.SocialIQ_Houzz_z4 as K1093
, b.SocialIQ_Pinterest_z4 as K1094
, b.SocialIQ_Snapchat_z4 as K1095
, b.AIQ_Race_Jewish_z4 as K1096
, b.BC_BusOwner_Add_z4 as K1097
, b.BC_BusOwner_HH_z4 as K1098
, b.BC_BusOwner_Home_Add_z4 as K1099
, b.BC_BusOwner_Home_HH_z4 as K1100
, b.BC_BusOwner_Home_Ind_z4 as K1101
, b.BC_BusOwner_Ind_z4 as K1102
, b.Change_RiskIQ_12Mos as K1103
, b.DivorceProb_v2_z4 as K1104
, b.Education_Coll_v2_z4 as K1105
, b.Education_Grad_v2_z4 as K1106
, b.Education_Hs_v2_z4 as K1107
, b.Education_LTHS_v2_z4 as K1108
, b.GeoCredit_Match_Flag as K1109
, b.InMarket_AirBNB_z4 as K1110
, b.InMarket_Microsoft_z4 as K1111
, b.InMarket_Pandora_z4 as K1112
, b.InMarket_PayPal_z4 as K1113
, b.InMarket_Spotify_z4 as K1114
, b.LT_Grand_Parent_z4 as K1115
, b.MarriageProb_v2_z4 as K1116
, b.Political_Charitable_Donation_z4 as K1117
, b.ResidenceTime_v2_z4 as K1118
, b.AIQ_ATP_V2_z4 as K1119
, b.GC_AGEAVG as N0001
, b.GC_AUOPEN as N0002
, b.GC_AUTOPNRAT as N0003
, b.GC_BRAVGA as N0004
, b.GC_BRBL75 as N0005
, b.GC_BROP06 as N0006
, b.GC_BROP12 as N0007
, b.GC_BROP24 as N0008
, b.GC_BROPNRAT as N0009
, b.GC_CUOPEN as N0010
, b.GC_ILOPEN as N0011
, b.GC_ILOPNRAT as N0012
, b.GC_ILTRAT as N0013
, b.GC_ILTRDS as N0014
, b.GC_ILTSAT as N0015
, b.GC_INQ012 as N0016
, b.GC_IQAVGA as N0017
, b.GC_OPNINQ12RAT as N0018
, b.GC_PFOPEN as N0019
, b.GC_PFOPNRAT as N0020
, b.GC_PFTRDS as N0021
, b.GC_PUBBKP as N0022
, b.GC_RECENTBRRAT as N0023
, b.GC_RECENTRVRAT as N0024
, b.GC_RVOP06 as N0025
, b.GC_RVOP12 as N0026
, b.GC_RVOP24 as N0027
, b.GC_RVOPEN as N0028
, b.GC_RVOPNRAT as N0029
, b.GC_RVTRAT as N0030
, b.GC_RVTRDS as N0031
, b.GC_RVTSAT as N0032
, b.GC_STBL06 as N0033
, b.GC_STMONP as N0034
, b.GC_STOP06 as N0035
, b.GC_STOPEN as N0036
, b.GC_STOPNRAT as N0037
, b.GC_STTRDS as N0038
, b.GC_STTSAT as N0039
, b.GC_T29PCT as N0040
, b.GC_T39PCT as N0041
, b.GC_T49PCT as N0042
, b.GC_T79PCT as N0043
, b.GC_TR324X as N0044
, b.GC_TRDBKP as N0045
, b.GC_UTTRDS as N0046
from (select distinct CHID from {file_name} where CHID > 0 and CHID is not null) a
left join AIQ_INPUT_Z4_TBL b on a.CHID = cast(b.CHID_key as bigint)
""")

df_dp11 = df_dp11.withColumn("AIQ_ZIP4_Flg", F.when(F.col("CHID_key").isNotNull(), 1).otherwise(0)) \
                 .drop("CHID_key")

print("Frequency count for AIQ_ZIP4_Flg in dp11:")
df_dp11.groupBy("AIQ_ZIP4_Flg").count().show()

df_mms_m_dp11 = df_dp11.dropDuplicates(["CHID"])

# *** 2.L Append Attributes from Aggregate GOB Table **** ;
df_gob_data_raw = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url_asi_tar) \
    .option("dbtable", f"gob_data_m{version}") \
    .options(**jdbc_connection_properties) \
    .load() \
    .withColumnRenamed("CHID_Key", "CHID")

# Filtering and replacing 0 with null
cols_to_nullify = [
    "tot_trans", "tot_dollar_amt", "tot_dollar_disc_amt",
    "tot_trans_6m", "tot_dollar_amt_6m", "tot_dollar_disc_amt_6m",
    "tot_trans_12m", "tot_dollar_amt_12m", "tot_dollar_disc_amt_12m",
    "tot_trans_24m", "tot_dollar_amt_24m", "tot_dollar_disc_amt_24m",
    "tot_trans_36m", "tot_dollar_amt_36m", "tot_dollar_disc_amt_36m"
]

df_gob_data = df_gob_data_raw
for col_name in cols_to_nullify:
    df_gob_data = df_gob_data.withColumn(col_name, F.when(F.col(col_name) == 0, None).otherwise(F.col(col_name)))

df_gob_data = df_gob_data.filter(
    (F.col("CHID").isNotNull()) &
    (F.col("CHID") != 0) &
    (~F.col("Provider").isin('United Healthcare', 'Delta Dental'))
)

# Join with main file
df_merge_all_1 = df_mms_m.select("CHID").distinct().join(df_gob_data, "CHID", "inner")

# Aggregation logic
agg_exprs = [
    # Flag36m
    F.max(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Auto Insurance', ' ')) & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_HAR_AT"),
    F.max(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Home Business Insurance', 'HomeOwners Insurance', 'Robust Toy Insurance')) & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_HAR_HM"),
    F.max(F.when((F.col("provider") == 'Allstate') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_ALL"),
    F.max(F.when((F.col("provider") == 'New York Life') & (F.col("product") != 'Immediate Fixed Annunities') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_NYL_LF"),
    F.max(F.when((F.col("provider") == 'New York Life') & (F.col("product") == 'Immediate Fixed Annunities') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_NYL_ANN"),
    F.max(F.when((F.col("provider") == 'Walgreens') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_WAL"),
    F.max(F.when((F.col("provider") == 'Chase') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_CHA"),
    F.max(F.when((F.col("provider") == 'Hilton') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_HIL"),
    F.max(F.when((F.col("provider") == 'Consumer Cellular') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_CEL"),
    F.max(F.when((F.col("provider") == "Denny's") & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_DEN"),
    F.max(F.when((F.col("provider") == 'Expedia') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_EXP"),
    F.max(F.when((F.col("provider") == 'Foremost') & (F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')) & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_FMS_MT"),
    F.max(F.when((F.col("provider") == 'Foremost') & (~F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')) & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_FMS_HM"),
    F.max(F.when((F.col("provider") == 'Tanger Outlets') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_TAN"),
    F.max(F.when((F.col("provider") == 'Regal Cinemas') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_REG"),
    F.max(F.when((F.col("provider") == 'The UPS Store') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_UPS"),
    F.max(F.when((F.col("provider") == 'Catamaran') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_CAT"),
    F.max(F.when((F.col("provider") == 'ADT Security Services Inc') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_ADT"),
    F.max(F.when((F.col("provider") == 'Budget') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_BUD"),
    F.max(F.when((F.col("provider") == 'Avis') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_AVI"),
    F.max(F.when((F.col("provider") == 'Norweign Cruise Line') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_NCL"),
    F.max(F.when((F.col("provider") == 'BORDERS') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_BDS"),
    F.max(F.when((F.col("provider") == 'AARP Membership Development') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_AMD"),
    F.max(F.when((F.col("provider") == 'Teleflora') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_TLF"),
    F.max(F.when((F.col("provider") == 'Grocery Coupon Center powered by Coupons.com') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_GCC"),
    F.max(F.when((F.col("provider") == 'ParkRideFly USA') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_PRF"),
    F.max(F.when((F.col("provider") == '1800Flowers') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_FLW"),
    F.max(F.when((F.col("provider") == 'Grand European Tours') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_GET"),
    F.max(F.when((F.col("provider") == "Schwan's Home Delivery") & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_SHD"),
    F.max(F.when((F.col("provider") == "Angie's List") & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_AGL"),
    F.max(F.when((F.col("provider") == 'Landrys') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_LDS"),
    F.max(F.when((F.col("provider") == 'Movies Unlimited') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_MVU"),
    F.max(F.when((F.col("provider") == 'Restaurant Discount Center') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_RDC"),
    F.max(F.when((F.col("provider") == 'Family Dollar') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_FMD"),
    F.max(F.when((F.col("provider") == "Sleepy's") & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_SLP"),
    F.max(F.when((F.col("provider") == 'Trusted ID') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_TID"),
    F.max(F.when((F.col("provider") == 'MedJet Assist') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_MJA"),
    F.max(F.when((F.col("provider") == "Gold's Gym") & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_GDG"),
    F.max(F.when((F.col("provider") == 'Collette Vacations') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_CLV"),
    F.max(F.when((F.col("provider") == 'HomeServe USA') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_HSU"),
    F.max(F.when((F.col("provider") == 'MGM Resorts') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_MGM"),
    F.max(F.when((F.col("provider") == 'Grand Canyon Railways') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_GCR"),
    F.max(F.when((F.col("provider") == 'Liberty Travel') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_LBT"),
    F.max(F.when((F.col("provider") == 'Vacations by Rail') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_VBR"),
    F.max(F.when((F.col("provider") == 'Euro Clearing House') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_ECH"),
    F.max(F.when((F.col("provider") == "Fred's") & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_FRD"),
    F.max(F.when((F.col("provider") == 'Salt Fork Lodge & Conf. Center') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_SFL"),
    F.max(F.when((F.col("provider") == 'Durham Sports') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_DHS"),
    F.max(F.when((F.col("provider") == 'Maumee Bay Lodge & Conf. Ctr') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_MBL"),
    F.max(F.when((F.col("provider") == 'Mohican Lodge & Conf. Center') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_MLC"),
    F.max(F.when((F.col("provider") == 'Furnace Creek Resort') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_FCR"),
    F.max(F.when((F.col("provider") == 'The Grand Hotel') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_TGH"),
    F.max(F.when((F.col("provider") == 'Leslie Sansone') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_LLS"),
    F.max(F.when((F.col("provider") == 'Kingsmill Resort') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_KMR"),
    F.max(F.when((F.col("provider") == 'Deer Creek Lodge & Conf. Ctr') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_DCL"),
    F.max(F.when((F.col("provider") == 'ACE') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_ACE"),
    F.max(F.when((F.col("provider") == 'Punderson Manor Lodge') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_PML"),
    F.max(F.when((F.col("provider") == 'G Adventures') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_GAD"),
    F.max(F.when((F.col("provider") == 'Smooth Fitness') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_SMF"),
    F.max(F.when((F.col("provider") == 'Motel 6') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_MT6"),
    F.max(F.when((F.col("provider") == 'Regina Cruises') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_RGC"),
    F.max(F.when((F.col("provider") == 'Group IST') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_GRI"),
    F.max(F.when((F.col("provider") == 'Journey Unlimited') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_JUL"),
    F.max(F.when((F.col("provider") == 'Smart Services Coupon') & (F.col("tot_trans_36m") >= 1), 1).otherwise(0)).alias("Flag36m_SSC"),

    # Flag_ever
    F.max(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Auto Insurance', ' ')), 1).otherwise(0)).alias("Flag_ever_HAR_AT"),
    F.max(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Home Business Insurance', 'HomeOwners Insurance', 'Robust Toy Insurance')), 1).otherwise(0)).alias("Flag_ever_HAR_HM"),
    F.max(F.when(F.col("provider") == 'Allstate', 1).otherwise(0)).alias("Flag_ever_ALL"),
    F.max(F.when((F.col("provider") == 'New York Life') & (F.col("product") != 'Immediate Fixed Annunities'), 1).otherwise(0)).alias("Flag_ever_NYL_LF"),
    F.max(F.when((F.col("provider") == 'New York Life') & (F.col("product") == 'Immediate Fixed Annunities'), 1).otherwise(0)).alias("Flag_ever_NYL_ANN"),
    F.max(F.when(F.col("provider") == 'Walgreens', 1).otherwise(0)).alias("Flag_ever_WAL"),
    F.max(F.when(F.col("provider") == 'Chase', 1).otherwise(0)).alias("Flag_ever_CHA"),
    F.max(F.when(F.col("provider") == 'Hilton', 1).otherwise(0)).alias("Flag_ever_HIL"),
    F.max(F.when(F.col("provider") == 'Consumer Cellular', 1).otherwise(0)).alias("Flag_ever_CEL"),
    F.max(F.when(F.col("provider") == "Denny's", 1).otherwise(0)).alias("Flag_ever_DEN"),
    F.max(F.when(F.col("provider") == 'Expedia', 1).otherwise(0)).alias("Flag_ever_EXP"),
    F.max(F.when((F.col("provider") == 'Foremost') & (F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), 1).otherwise(0)).alias("Flag_ever_FMS_MT"),
    F.max(F.when((F.col("provider") == 'Foremost') & (~F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), 1).otherwise(0)).alias("Flag_ever_FMS_HM"),
    F.max(F.when(F.col("provider") == 'Tanger Outlets', 1).otherwise(0)).alias("Flag_ever_TAN"),
    F.max(F.when(F.col("provider") == 'Regal Cinemas', 1).otherwise(0)).alias("Flag_ever_REG"),
    F.max(F.when(F.col("provider") == 'The UPS Store', 1).otherwise(0)).alias("Flag_ever_UPS"),
    F.max(F.when(F.col("provider") == 'Catamaran', 1).otherwise(0)).alias("Flag_ever_CAT"),
    F.max(F.when(F.col("provider") == 'ADT Security Services Inc', 1).otherwise(0)).alias("Flag_ever_ADT"),
    F.max(F.when(F.col("provider") == 'Budget', 1).otherwise(0)).alias("Flag_ever_BUD"),
    F.max(F.when(F.col("provider") == 'Avis', 1).otherwise(0)).alias("Flag_ever_AVI"),
    F.max(F.when(F.col("provider") == 'Norweign Cruise Line', 1).otherwise(0)).alias("Flag_ever_NCL"),
    F.max(F.when(F.col("provider") == 'BORDERS', 1).otherwise(0)).alias("Flag_ever_BDS"),
    F.max(F.when(F.col("provider") == 'AARP Membership Development', 1).otherwise(0)).alias("Flag_ever_AMD"),
    F.max(F.when(F.col("provider") == 'Teleflora', 1).otherwise(0)).alias("Flag_ever_TLF"),
    F.max(F.when(F.col("provider") == 'Grocery Coupon Center powered by Coupons.com', 1).otherwise(0)).alias("Flag_ever_GCC"),
    F.max(F.when(F.col("provider") == 'ParkRideFly USA', 1).otherwise(0)).alias("Flag_ever_PRF"),
    F.max(F.when(F.col("provider") == '1800Flowers', 1).otherwise(0)).alias("Flag_ever_FLW"),
    F.max(F.when(F.col("provider") == 'Grand European Tours', 1).otherwise(0)).alias("Flag_ever_GET"),
    F.max(F.when(F.col("provider") == "Schwan's Home Delivery", 1).otherwise(0)).alias("Flag_ever_SHD"),
    F.max(F.when(F.col("provider") == "Angie's List", 1).otherwise(0)).alias("Flag_ever_AGL"),
    F.max(F.when(F.col("provider") == 'Landrys', 1).otherwise(0)).alias("Flag_ever_LDS"),
    F.max(F.when(F.col("provider") == 'Movies Unlimited', 1).otherwise(0)).alias("Flag_ever_MVU"),
    F.max(F.when(F.col("provider") == 'Restaurant Discount Center', 1).otherwise(0)).alias("Flag_ever_RDC"),
    F.max(F.when(F.col("provider") == 'Family Dollar', 1).otherwise(0)).alias("Flag_ever_FMD"),
    F.max(F.when(F.col("provider") == "Sleepy's", 1).otherwise(0)).alias("Flag_ever_SLP"),
    F.max(F.when(F.col("provider") == 'Trusted ID', 1).otherwise(0)).alias("Flag_ever_TID"),
    F.max(F.when(F.col("provider") == 'MedJet Assist', 1).otherwise(0)).alias("Flag_ever_MJA"),
    F.max(F.when(F.col("provider") == "Gold's Gym", 1).otherwise(0)).alias("Flag_ever_GDG"),
    F.max(F.when(F.col("provider") == 'Collette Vacations', 1).otherwise(0)).alias("Flag_ever_CLV"),
    F.max(F.when(F.col("provider") == 'HomeServe USA', 1).otherwise(0)).alias("Flag_ever_HSU"),
    F.max(F.when(F.col("provider") == 'MGM Resorts', 1).otherwise(0)).alias("Flag_ever_MGM"),
    F.max(F.when(F.col("provider") == 'Grand Canyon Railways', 1).otherwise(0)).alias("Flag_ever_GCR"),
    F.max(F.when(F.col("provider")
== 'Liberty Travel', 1).otherwise(0)).alias("Flag_ever_LBT"),
    F.max(F.when(F.col("provider") == 'Vacations by Rail', 1).otherwise(0)).alias("Flag_ever_VBR"),
    F.max(F.when(F.col("provider") == 'Euro Clearing House', 1).otherwise(0)).alias("Flag_ever_ECH"),
    F.max(F.when(F.col("provider") == "Fred's", 1).otherwise(0)).alias("Flag_ever_FRD"),
    F.max(F.when(F.col("provider") == 'Salt Fork Lodge & Conf. Center', 1).otherwise(0)).alias("Flag_ever_SFL"),
    F.max(F.when(F.col("provider") == 'Durham Sports', 1).otherwise(0)).alias("Flag_ever_DHS"),
    F.max(F.when(F.col("provider") == 'Maumee Bay Lodge & Conf. Ctr', 1).otherwise(0)).alias("Flag_ever_MBL"),
    F.max(F.when(F.col("provider") == 'Mohican Lodge & Conf. Center', 1).otherwise(0)).alias("Flag_ever_MLC"),
    F.max(F.when(F.col("provider") == 'Furnace Creek Resort', 1).otherwise(0)).alias("Flag_ever_FCR"),
    F.max(F.when(F.col("provider") == 'The Grand Hotel', 1).otherwise(0)).alias("Flag_ever_TGH"),
    F.max(F.when(F.col("provider") == 'Leslie Sansone', 1).otherwise(0)).alias("Flag_ever_LLS"),
    F.max(F.when(F.col("provider") == 'Kingsmill Resort', 1).otherwise(0)).alias("Flag_ever_KMR"),
    F.max(F.when(F.col("provider") == 'Deer Creek Lodge & Conf. Ctr', 1).otherwise(0)).alias("Flag_ever_DCL"),
    F.max(F.when(F.col("provider") == 'ACE', 1).otherwise(0)).alias("Flag_ever_ACE"),
    F.max(F.when(F.col("provider") == 'Punderson Manor Lodge', 1).otherwise(0)).alias("Flag_ever_PML"),
    F.max(F.when(F.col("provider") == 'G Adventures', 1).otherwise(0)).alias("Flag_ever_GAD"),
    F.max(F.when(F.col("provider") == 'Smooth Fitness', 1).otherwise(0)).alias("Flag_ever_SMF"),
    F.max(F.when(F.col("provider") == 'Motel 6', 1).otherwise(0)).alias("Flag_ever_MT6"),
    F.max(F.when(F.col("provider") == 'Regina Cruises', 1).otherwise(0)).alias("Flag_ever_RGC"),
    F.max(F.when(F.col("provider") == 'Group IST', 1).otherwise(0)).alias("Flag_ever_GRI"),
    F.max(F.when(F.col("provider") == 'Journey Unlimited', 1).otherwise(0)).alias("Flag_ever_JUL"),
    F.max(F.when(F.col("provider") == 'Smart Services Coupon', 1).otherwise(0)).alias("Flag_ever_SSC"),

    # Transaction Aggregations
    F.sum("tot_trans_ever").alias("tot_trans_ever_TOT"),
    F.sum("tot_dollar_amt_ever").alias("tot_dollar_amt_ever_TOT"),
    F.sum("tot_dollar_disc_amt_ever").alias("tot_dollar_disc_amt_ever_TOT"),
    F.sum("tot_trans_6m").alias("tot_trans_6m_TOT"),
    F.sum("tot_dollar_amt_6m").alias("tot_dollar_amt_6m_TOT"),
    F.sum("tot_dollar_disc_amt_6m").alias("tot_dollar_disc_amt_6m_TOT"),
    F.sum("tot_trans_12m").alias("tot_trans_12m_TOT"),
    F.sum("tot_dollar_amt_12m").alias("tot_dollar_amt_12m_TOT"),
    F.sum("tot_dollar_disc_amt_12m").alias("tot_dollar_disc_amt_12m_TOT"),
    F.sum("tot_trans_24m").alias("tot_trans_24m_TOT"),
    F.sum("tot_dollar_amt_24m").alias("tot_dollar_amt_24m_TOT"),
    F.sum("tot_dollar_disc_amt_24m").alias("tot_dollar_disc_amt_24m_TOT"),
    F.sum("tot_trans_36m").alias("tot_trans_36m_TOT"),
    F.sum("tot_dollar_amt_36m").alias("tot_dollar_amt_36m_TOT"),
    F.sum("tot_dollar_disc_amt_36m").alias("tot_dollar_disc_amt_36m_TOT"),

    F.sum(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Auto Insurance', ' ')), F.col("tot_trans_12m"))).alias("tot_trans_12m_HAR_AT"),
    F.sum(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Auto Insurance', ' ')), F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_HAR_AT"),
    F.sum(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Auto Insurance', ' ')), F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_HAR_AT"),
    F.sum(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Auto Insurance', ' ')), F.col("tot_trans_36m"))).alias("tot_trans_36m_HAR_AT"),
    F.sum(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Auto Insurance', ' ')), F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_HAR_AT"),
    F.sum(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Auto Insurance', ' ')), F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_HAR_AT"),
    F.sum(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Auto Insurance', ' ')), F.col("tot_trans"))).alias("tot_trans_ever_HAR_AT"),
    F.sum(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Auto Insurance', ' ')), F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_HAR_AT"),
    F.sum(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Auto Insurance', ' ')), F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_HAR_AT"),
    
    F.sum(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Home Business Insurance', 'HomeOwners Insurance', 'Robust Toy Insurance')), F.col("tot_trans_12m"))).alias("tot_trans_12m_HAR_HM"),
    F.sum(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Home Business Insurance', 'HomeOwners Insurance', 'Robust Toy Insurance')), F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_HAR_HM"),
    F.sum(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Home Business Insurance', 'HomeOwners Insurance', 'Robust Toy Insurance')), F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_HAR_HM"),
    F.sum(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Home Business Insurance', 'HomeOwners Insurance', 'Robust Toy Insurance')), F.col("tot_trans_36m"))).alias("tot_trans_36m_HAR_HM"),
    F.sum(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Home Business Insurance', 'HomeOwners Insurance', 'Robust Toy Insurance')), F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_HAR_HM"),
    F.sum(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Home Business Insurance', 'HomeOwners Insurance', 'Robust Toy Insurance')), F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_HAR_HM"),
    F.sum(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Home Business Insurance', 'HomeOwners Insurance', 'Robust Toy Insurance')), F.col("tot_trans"))).alias("tot_trans_ever_HAR_HM"),
    F.sum(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Home Business Insurance', 'HomeOwners Insurance', 'Robust Toy Insurance')), F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_HAR_HM"),
    F.sum(F.when((F.col("provider") == 'The Hartford') & (F.col("product").isin('Home Business Insurance', 'HomeOwners Insurance', 'Robust Toy Insurance')), F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_HAR_HM"),

    F.sum(F.when(F.col("provider") == 'Allstate', F.col("tot_trans_12m"))).alias("tot_trans_12m_ALL"),
    F.sum(F.when(F.col("provider") == 'Allstate', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_ALL"),
    F.sum(F.when(F.col("provider") == 'Allstate', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_ALL"),
    F.sum(F.when(F.col("provider") == 'Allstate', F.col("tot_trans_36m"))).alias("tot_trans_36m_ALL"),
    F.sum(F.when(F.col("provider") == 'Allstate', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_ALL"),
    F.sum(F.when(F.col("provider") == 'Allstate', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_ALL"),
    F.sum(F.when(F.col("provider") == 'Allstate', F.col("tot_trans"))).alias("tot_trans_ever_ALL"),
    F.sum(F.when(F.col("provider") == 'Allstate', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_ALL"),
    F.sum(F.when(F.col("provider") == 'Allstate', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_ALL"),

    F.sum(F.when((F.col("provider") == 'New York Life') & (F.col("product") == 'Immediate Fixed Annunities'), F.col("tot_trans_12m"))).alias("tot_trans_12m_NYL_ANN"),
    F.sum(F.when((F.col("provider") == 'New York Life') & (F.col("product") == 'Immediate Fixed Annunities'), F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_NYL_ANN"),
    F.sum(F.when((F.col("provider") == 'New York Life') & (F.col("product") == 'Immediate Fixed Annunities'), F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_NYL_ANN"),
    F.sum(F.when((F.col("provider") == 'New York Life') & (F.col("product") == 'Immediate Fixed Annunities'), F.col("tot_trans_36m"))).alias("tot_trans_36m_NYL_ANN"),
    F.sum(F.when((F.col("provider") == 'New York Life') & (F.col("product") == 'Immediate Fixed Annunities'), F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_NYL_ANN"),
    F.sum(F.when((F.col("provider") == 'New York Life') & (F.col("product") == 'Immediate Fixed Annunities'), F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_NYL_ANN"),
    F.sum(F.when((F.col("provider") == 'New York Life') & (F.col("product") == 'Immediate Fixed Annunities'), F.col("tot_trans"))).alias("tot_trans_ever_NYL_ANN"),
    F.sum(F.when((F.col("provider") == 'New York Life') & (F.col("product") == 'Immediate Fixed Annunities'), F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_NYL_ANN"),
    F.sum(F.when((F.col("provider") == 'New York Life') & (F.col("product") == 'Immediate Fixed Annunities'), F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_NYL_ANN"),
    
    F.sum(F.when((F.col("provider") == 'New York Life') & (F.col("product") != 'Immediate Fixed Annunities'), F.col("tot_trans_12m"))).alias("tot_trans_12m_NYL_LF"),
    F.sum(F.when((F.col("provider") == 'New York Life') & (F.col("product") != 'Immediate Fixed Annunities'), F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_NYL_LF"),
    F.sum(F.when((F.col("provider") == 'New York Life') & (F.col("product") != 'Immediate Fixed Annunities'), F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_NYL_LF"),
    F.sum(F.when((F.col("provider") == 'New York Life') & (F.col("product") != 'Immediate Fixed Annunities'), F.col("tot_trans_36m"))).alias("tot_trans_36m_NYL_LF"),
    F.sum(F.when((F.col("provider") == 'New York Life') & (F.col("product") != 'Immediate Fixed Annunities'), F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_NYL_LF"),
    F.sum(F.when((F.col("provider") == 'New York Life') & (F.col("product") != 'Immediate Fixed Annunities'), F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_NYL_LF"),
    F.sum(F.when((F.col("provider") == 'New York Life') & (F.col("product") != 'Immediate Fixed Annunities'), F.col("tot_trans"))).alias("tot_trans_ever_NYL_LF"),
    F.sum(F.when((F.col("provider") == 'New York Life') & (F.col("product") != 'Immediate Fixed Annunities'), F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_NYL_LF"),
    F.sum(F.when((F.col("provider") == 'New York Life') & (F.col("product") != 'Immediate Fixed Annunities'), F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_NYL_LF"),
    
    F.sum(F.when(F.col("provider") == 'Walgreens', F.col("tot_trans_12m"))).alias("tot_trans_12m_WAL"),
    F.sum(F.when(F.col("provider") == 'Walgreens', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_WAL"),
    F.sum(F.when(F.col("provider") == 'Walgreens', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_WAL"),
    F.sum(F.when(F.col("provider") == 'Walgreens', F.col("tot_trans_36m"))).alias("tot_trans_36m_WAL"),
    F.sum(F.when(F.col("provider") == 'Walgreens', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_WAL"),
    F.sum(F.when(F.col("provider") == 'Walgreens', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_WAL"),
    F.sum(F.when(F.col("provider") == 'Walgreens', F.col("tot_trans"))).alias("tot_trans_ever_WAL"),
    F.sum(F.when(F.col("provider") == 'Walgreens', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_WAL"),
    F.sum(F.when(F.col("provider") == 'Walgreens', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_WAL"),
    
    F.sum(F.when(F.col("provider") == 'Chase', F.col("tot_trans_12m"))).alias("tot_trans_12m_CHA"),
    F.sum(F.when(F.col("provider") == 'Chase', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_CHA"),
    F.sum(F.when(F.col("provider") == 'Chase', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_CHA"),
    F.sum(F.when(F.col("provider") == 'Chase', F.col("tot_trans_36m"))).alias("tot_trans_36m_CHA"),
    F.sum(F.when(F.col("provider") == 'Chase', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_CHA"),
    F.sum(F.when(F.col("provider") == 'Chase', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_CHA"),
    F.sum(F.when(F.col("provider") == 'Chase', F.col("tot_trans"))).alias("tot_trans_ever_CHA"),
    F.sum(F.when(F.col("provider") == 'Chase', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_CHA"),
    F.sum(F.when(F.col("provider") == 'Chase', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_CHA"),
    
    F.sum(F.when(F.col("provider") == 'Hilton', F.col("tot_trans_12m"))).alias("tot_trans_12m_HIL"),
    F.sum(F.when(F.col("provider") == 'Hilton', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_HIL"),
    F.sum(F.when(F.col("provider") == 'Hilton', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_HIL"),
    F.sum(F.when(F.col("provider") == 'Hilton', F.col("tot_trans_36m"))).alias("tot_trans_36m_HIL"),
    F.sum(F.when(F.col("provider") == 'Hilton', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_HIL"),
    F.sum(F.when(F.col("provider") == 'Hilton', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_HIL"),
    F.sum(F.when(F.col("provider") == 'Hilton', F.col("tot_trans"))).alias("tot_trans_ever_HIL"),
    F.sum(F.when(F.col("provider") == 'Hilton', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_HIL"),
    F.sum(F.when(F.col("provider") == 'Hilton', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_HIL"),
    
    F.sum(F.when(F.col("provider") == 'Consumer Cellular', F.col("tot_trans_12m"))).alias("tot_trans_12m_CEL"),
    F.sum(F.when(F.col("provider") == 'Consumer Cellular', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_CEL"),
    F.sum(F.when(F.col("provider") == 'Consumer Cellular', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_CEL"),
    F.sum(F.when(F.col("provider") == 'Consumer Cellular', F.col("tot_trans_36m"))).alias("tot_trans_36m_CEL"),
    F.sum(F.when(F.col("provider") == 'Consumer Cellular', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_CEL"),
    F.sum(F.when(F.col("provider") == 'Consumer Cellular', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_CEL"),
    F.sum(F.when(F.col("provider") == 'Consumer Cellular', F.col("tot_trans"))).alias("tot_trans_ever_CEL"),
    F.sum(F.when(F.col("provider") == 'Consumer Cellular', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_CEL"),
    F.sum(F.when(F.col("provider") == 'Consumer Cellular', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_CEL"),
    
    F.sum(F.when(F.col("provider") == "Denny's", F.col("tot_trans_12m"))).alias("tot_trans_12m_DEN"),
    F.sum(F.when(F.col("provider") == "Denny's", F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_DEN"),
    F.sum(F.when(F.col("provider") == "Denny's", F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_DEN"),
    F.sum(F.when(F.col("provider") == "Denny's", F.col("tot_trans_36m"))).alias("tot_trans_36m_DEN"),
    F.sum(F.when(F.col("provider") == "Denny's", F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_DEN"),
    F.sum(F.when(F.col("provider") == "Denny's", F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_DEN"),
    F.sum(F.when(F.col("provider") == "Denny's", F.col("tot_trans"))).alias("tot_trans_ever_DEN"),
    F.sum(F.when(F.col("provider") == "Denny's", F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_DEN"),
    F.sum(F.when(F.col("provider") == "Denny's", F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_DEN"),
    
    F.sum(F.when(F.col("provider") == 'Expedia', F.col("tot_trans_12m"))).alias("tot_trans_12m_EXP"),
    F.sum(F.when(F.col("provider") == 'Expedia', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_EXP"),
    F.sum(F.when(F.col("provider") == 'Expedia', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_EXP"),
    F.sum(F.when(F.col("provider") == 'Expedia', F.col("tot_trans_36m"))).alias("tot_trans_36m_EXP"),
    F.sum(F.when(F.col("provider") == 'Expedia', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_EXP"),
    F.sum(F.when(F.col("provider") == 'Expedia', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_EXP"),
    F.sum(F.when(F.col("provider") == 'Expedia', F.col("tot_trans"))).alias("tot_trans_ever_EXP"),
    F.sum(F.when(F.col("provider") == 'Expedia', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_EXP"),
    F.sum(F.when(F.col("provider") == 'Expedia', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_EXP"),
    
    F.sum(F.when((F.col("provider") == 'Foremost') & (F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), F.col("tot_trans_12m"))).alias("tot_trans_12m_FMS_MT"),
    F.sum(F.when((F.col("provider") == 'Foremost') & (F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_FMS_MT"),
    F.sum(F.when((F.col("provider") == 'Foremost') & (F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_FMS_MT"),
    F.sum(F.when((F.col("provider") == 'Foremost') & (F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), F.col("tot_trans_36m"))).alias("tot_trans_36m_FMS_MT"),
    F.sum(F.when((F.col("provider") == 'Foremost') & (F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_FMS_MT"),
    F.sum(F.when((F.col("provider") == 'Foremost') & (F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_FMS_MT"),
    F.sum(F.when((F.col("provider") == 'Foremost') & (F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), F.col("tot_trans"))).alias("tot_trans_ever_FMS_MT"),
    F.sum(F.when((F.col("provider") == 'Foremost') & (F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_FMS_MT"),
    F.sum(F.when((F.col("provider") == 'Foremost') & (F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_FMS_MT"),

    F.sum(F.when((F.col("provider") == 'Foremost') & (~F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), F.col("tot_trans_12m"))).alias("tot_trans_12m_FMS_HM"),
    F.sum(F.when((F.col("provider") == 'Foremost') & (~F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_FMS_HM"),
    F.sum(F.when((F.col("provider") == 'Foremost') & (~F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_FMS_HM"),
    F.sum(F.when((F.col("provider") == 'Foremost') & (~F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), F.col("tot_trans_36m"))).alias("tot_trans_36m_FMS_HM"),
    F.sum(F.when((F.col("provider") == 'Foremost') & (~F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_FMS_HM"),
    F.sum(F.when((F.col("provider") == 'Foremost') & (~F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_FMS_HM"),
    F.sum(F.when((F.col("provider") == 'Foremost') & (~F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), F.col("tot_trans"))).alias("tot_trans_ever_FMS_HM"),
    F.sum(F.when((F.col("provider") == 'Foremost') & (~F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_FMS_HM"),
    F.sum(F.when((F.col("provider") == 'Foremost') & (~F.col("product").isin('Motorcycle Insurance', 'PRODUCT CODE 276')), F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_FMS_HM"),

    F.sum(F.when(F.col("provider") == 'Tanger Outlets', F.col("tot_trans_12m"))).alias("tot_trans_12m_TAN"),
    F.sum(F.when(F.col("provider") == 'Tanger Outlets', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_TAN"),
    F.sum(F.when(F.col("provider") == 'Tanger Outlets', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_TAN"),
    F.sum(F.when(F.col("provider") == 'Tanger Outlets', F.col("tot_trans_36m"))).alias("tot_trans_36m_TAN"),
    F.sum(F.when(F.col("provider") == 'Tanger Outlets', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_TAN"),
    F.sum(F.when(F.col("provider") == 'Tanger Outlets', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_TAN"),
    F.sum(F.when(F.col("provider") == 'Tanger Outlets', F.col("tot_trans"))).alias("tot_trans_ever_TAN"),
    F.sum(F.when(F.col("provider") == 'Tanger Outlets', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_TAN"),
    F.sum(F.when(F.col("provider") == 'Tanger Outlets', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_TAN"),
    
    F.sum(F.when(F.col("provider") == 'Regal Cinemas', F.col("tot_trans_12m"))).alias("tot_trans_12m_REG"),
    F.sum(F.when(F.col("provider") == 'Regal Cinemas', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_REG"),
    F.sum(F.when(F.col("provider") == 'Regal Cinemas', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_REG"),
    F.sum(F.when(F.col("provider") == 'Regal Cinemas', F.col("tot_trans_36m"))).alias("tot_trans_36m_REG"),
    F.sum(F.when(F.col("provider") == 'Regal Cinemas', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_REG"),
    F.sum(F.when(F.col("provider") == 'Regal Cinemas', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_REG"),
    F.sum(F.when(F.col("provider") == 'Regal Cinemas', F.col("tot_trans"))).alias("tot_trans_ever_REG"),
    F.sum(F.when(F.col("provider") == 'Regal Cinemas', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_REG"),
    F.sum(F.when(F.col("provider") == 'Regal Cinemas', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_REG"),
    
    F.sum(F.when(F.col("provider") == 'The UPS Store', F.col("tot_trans_12m"))).alias("tot_trans_12m_UPS"),
    F.sum(F.when(F.col("provider") == 'The UPS Store', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_UPS"),
    F.sum(F.when(F.col("provider") == 'The UPS Store', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_UPS"),
    F.sum(F.when(F.col("provider") == 'The UPS Store', F.col("tot_trans_36m"))).alias("tot_trans_36m_UPS"),
    F.sum(F.when(F.col("provider") == 'The UPS Store', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_UPS"),
    F.sum(F.when(F.col("provider") == 'The UPS Store', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_UPS"),
    F.sum(F.when(F.col("provider") == 'The UPS Store', F.col("tot_trans"))).alias("tot_trans_ever_UPS"),
    F.sum(F.when(F.col("provider") == 'The UPS Store', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_UPS"),
    F.sum(F.when(F.col("provider") == 'The UPS Store', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_UPS"),
    
    F.sum(F.when(F.col("provider") == 'Catamaran', F.col("tot_trans_12m"))).alias("tot_trans_12m_CAT"),
    F.sum(F.when(F.col("provider") == 'Catamaran', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_CAT"),
    F.sum(F.when(F.col("provider") == 'Catamaran', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_CAT"),
    F.sum(F.when(F.col("provider") == 'Catamaran', F.col("tot_trans_36m"))).alias("tot_trans_36m_CAT"),
    F.sum(F.when(F.col("provider") == 'Catamaran', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_CAT"),
    F.sum(F.when(F.col("provider") == 'Catamaran', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_CAT"),
    F.sum(F.when(F.col("provider") == 'Catamaran', F.col("tot_trans"))).alias("tot_trans_ever_CAT"),
    F.sum(F.when(F.col("provider") == 'Catamaran', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_CAT"),
    F.sum(F.when(F.col("provider") == 'Catamaran', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_CAT"),
    
    F.sum(F.when(F.col("provider") == 'ADT Security Services Inc', F.col("tot_trans_12m"))).alias("tot_trans_12m_ADT"),
    F.sum(F.when(F.col("provider") == 'ADT Security Services Inc', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_ADT"),
    F.sum(F.when(F.col("provider") == 'ADT Security Services Inc', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_ADT"),
    F.sum(F.when(F.col("provider") == 'ADT Security Services Inc', F.col("tot_trans_36m"))).alias("tot_trans_36m_ADT"),
    F.sum(F.when(F.col("provider") == 'ADT Security Services Inc', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_ADT"),
    F.sum(F.when(F.col("provider") == 'ADT Security Services Inc', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_ADT"),
    F.sum(F.when(F.col("provider") == 'ADT Security Services Inc', F.col("tot_trans"))).alias("tot_trans_ever_ADT"),
    F.sum(F.when(F.col("provider") == 'ADT Security Services Inc', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_ADT"),
    F.sum(F.when(F.col("provider") == 'ADT Security Services Inc', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_ADT"),
    
    F.sum(F.when(F.col("provider") == 'Budget', F.col("tot_trans_12m"))).alias("tot_trans_12m_BUD"),
    F.sum(F.when(F.col("provider") == 'Budget', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_BUD"),
    F.sum(F.when(F.col("provider") == 'Budget', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_BUD"),
    F.sum(F.when(F.col("provider") == 'Budget', F.col("tot_trans_36m"))).alias("tot_trans_36m_BUD"),
    F.sum(F.when(F.col("provider") == 'Budget', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_BUD"),
    F.sum(F.when(F.col("provider") == 'Budget', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_BUD"),
    F.sum(F.when(F.col("provider") == 'Budget', F.col("tot_trans"))).alias("tot_trans_ever_BUD"),
    F.sum(F.when(F.col("provider") == 'Budget', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_BUD"),
    F.sum(F.when(F.col("provider") == 'Budget', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_BUD"),
    
    F.sum(F.when(F.col("provider") == 'Avis', F.col("tot_trans_12m"))).alias("tot_trans_12m_AVI"),
    F.sum(F.when(F.col("provider") == 'Avis', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_AVI"),
    F.sum(F.when(F.col("provider") == 'Avis', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_AVI"),
    F.sum(F.when(F.col("provider") == 'Avis', F.col("tot_trans_36m"))).alias("tot_trans_36m_AVI"),
    F.sum(F.when(F.col("provider") == 'Avis', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_AVI"),
    F.sum(F.when(F.col("provider") == 'Avis', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_AVI"),
    F.sum(F.when(F.col("provider") == 'Avis', F.col("tot_trans"))).alias("tot_trans_ever_AVI"),
    F.sum(F.when(F.col("provider") == 'Avis', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_AVI"),
    F.sum(F.when(F.col("provider") == 'Avis', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_AVI"),

    # ... and so on for all providers and categories ...

    # Totals by DataSource
    F.sum(F.when(F.col("DataSource") == 'Lifestyle', F.col("tot_trans_6m"))).alias("tot_trans_6m_DS_LS"),
    F.sum(F.when(F.col("DataSource") == 'Lifestyle', F.col("tot_dollar_amt_6m"))).alias("tot_dollar_amt_6m_DS_LS"),
    F.sum(F.when(F.col("DataSource") == 'Lifestyle', F.col("tot_dollar_disc_amt_6m"))).alias("tot_dollar_disc_amt_6m_DS_LS"),
    F.sum(F.when(F.col("DataSource") == 'Lifestyle', F.col("tot_trans_12m"))).alias("tot_trans_12m_DS_LS"),
    F.sum(F.when(F.col("DataSource") == 'Lifestyle', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_DS_LS"),
    F.sum(F.when(F.col("DataSource") == 'Lifestyle', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_DS_LS"),
    F.sum(F.when(F.col("DataSource") == 'Lifestyle', F.col("tot_trans_24m"))).alias("tot_trans_24m_DS_LS"),
    F.sum(F.when(F.col("DataSource") == 'Lifestyle', F.col("tot_dollar_amt_24m"))).alias("tot_dollar_amt_24m_DS_LS"),
    F.sum(F.when(F.col("DataSource") == 'Lifestyle', F.col("tot_dollar_disc_amt_24m"))).alias("tot_dollar_disc_amt_24m_DS_LS"),
    F.sum(F.when(F.col("DataSource") == 'Lifestyle', F.col("tot_trans_36m"))).alias("tot_trans_36m_DS_LS"),
    F.sum(F.when(F.col("DataSource") == 'Lifestyle', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_DS_LS"),
    F.sum(F.when(F.col("DataSource") == 'Lifestyle', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_DS_LS"),
    F.sum(F.when(F.col("DataSource") == 'Lifestyle', F.col("tot_trans"))).alias("tot_trans_ever_DS_LS"),
    F.sum(F.when(F.col("DataSource") == 'Lifestyle', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_DS_LS"),
    F.sum(F.when(F.col("DataSource") == 'Lifestyle', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_DS_LS"),
    
    F.sum(F.when(F.col("DataSource") == 'Subscription', F.col("tot_trans_6m"))).alias("tot_trans_6m_DS_SUBS"),
    F.sum(F.when(F.col("DataSource") == 'Subscription', F.col("tot_dollar_amt_6m"))).alias("tot_dollar_amt_6m_DS_SUBS"),
    F.sum(F.when(F.col("DataSource") == 'Subscription', F.col("tot_dollar_disc_amt_6m"))).alias("tot_dollar_disc_amt_6m_DS_SUBS"),
    F.sum(F.when(F.col("DataSource") == 'Subscription', F.col("tot_trans_12m"))).alias("tot_trans_12m_DS_SUBS"),
    F.sum(F.when(F.col("DataSource") == 'Subscription', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_DS_SUBS"),
    F.sum(F.when(F.col("DataSource") == 'Subscription', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_DS_SUBS"),
    F.sum(F.when(F.col("DataSource") == 'Subscription', F.col("tot_trans_24m"))).alias("tot_trans_24m_DS_SUBS"),
    F.sum(F.when(F.col("DataSource") == 'Subscription', F.col("tot_dollar_amt_24m"))).alias("tot_dollar_amt_24m_DS_SUBS"),
    F.sum(F.when(F.col("DataSource") == 'Subscription', F.col("tot_dollar_disc_amt_24m"))).alias("tot_dollar_disc_amt_24m_DS_SUBS"),
    F.sum(F.when(F.col("DataSource") == 'Subscription', F.col("tot_trans_36m"))).alias("tot_trans_36m_DS_SUBS"),
    F.sum(F.when(F.col("DataSource") == 'Subscription', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_DS_SUBS"),
    F.sum(F.when(F.col("DataSource") == 'Subscription', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_DS_SUBS"),
    F.sum(F.when(F.col("DataSource") == 'Subscription', F.col("tot_trans"))).alias("tot_trans_ever_DS_SUBS"),
    F.sum(F.when(F.col("DataSource") == 'Subscription', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_DS_SUBS"),
    F.sum(F.when(F.col("DataSource") == 'Subscription', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_DS_SUBS"),

    # Totals by Order_Channel
    F.sum(F.when(F.col("Order_Channel").isin('W', 'WEB', 'Web'), F.col("tot_trans_6m"))).alias("tot_trans_6m_OC_WEB"),
    F.sum(F.when(F.col("Order_Channel").isin('W', 'WEB', 'Web'), F.col("tot_dollar_amt_6m"))).alias("tot_dollar_amt_6m_OC_WEB"),
    F.sum(F.when(F.col("Order_Channel").isin('W', 'WEB', 'Web'), F.col("tot_dollar_disc_amt_6m"))).alias("tot_dollar_disc_amt_6m_OC_WEB"),
    F.sum(F.when(F.col("Order_Channel").isin('W', 'WEB', 'Web'), F.col("tot_trans_12m"))).alias("tot_trans_12m_OC_WEB"),
    F.sum(F.when(F.col("Order_Channel").isin('W', 'WEB', 'Web'), F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_OC_WEB"),
    F.sum(F.when(F.col("Order_Channel").isin('W', 'WEB', 'Web'), F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_OC_WEB"),
    F.sum(F.when(F.col("Order_Channel").isin('W', 'WEB', 'Web'), F.col("tot_trans_24m"))).alias("tot_trans_24m_OC_WEB"),
    F.sum(F.when(F.col("Order_Channel").isin('W', 'WEB', 'Web'), F.col("tot_dollar_amt_24m"))).alias("tot_dollar_amt_24m_OC_WEB"),
    F.sum(F.when(F.col("Order_Channel").isin('W', 'WEB', 'Web'), F.col("tot_dollar_disc_amt_24m"))).alias("tot_dollar_disc_amt_24m_OC_WEB"),
    F.sum(F.when(F.col("Order_Channel").isin('W', 'WEB', 'Web'), F.col("tot_trans_36m"))).alias("tot_trans_36m_OC_WEB"),
    F.sum(F.when(F.col("Order_Channel").isin('W', 'WEB', 'Web'), F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_OC_WEB"),
    F.sum(F.when(F.col("Order_Channel").isin('W', 'WEB', 'Web'), F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_OC_WEB"),
    F.sum(F.when(F.col("Order_Channel").isin('W', 'WEB', 'Web'), F.col("tot_trans"))).alias("tot_trans_ever_OC_WEB"),
    F.sum(F.when(F.col("Order_Channel").isin('W', 'WEB', 'Web'), F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_OC_WEB"),
    F.sum(F.when(F.col("Order_Channel").isin('W', 'WEB', 'Web'), F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_OC_WEB"),
    
    F.sum(F.when(F.col("Order_Channel").isin('D', 'M'), F.col("tot_trans_6m"))).alias("tot_trans_6m_OC_DM"),
    F.sum(F.when(F.col("Order_Channel").isin('D', 'M'), F.col("tot_dollar_amt_6m"))).alias("tot_dollar_amt_6m_OC_DM"),
    F.sum(F.when(F.col("Order_Channel").isin('D', 'M'), F.col("tot_dollar_disc_amt_6m"))).alias("tot_dollar_disc_amt_6m_OC_DM"),
    F.sum(F.when(F.col("Order_Channel").isin('D', 'M'), F.col("tot_trans_12m"))).alias("tot_trans_12m_OC_DM"),
    F.sum(F.when(F.col("Order_Channel").isin('D', 'M'), F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_OC_DM"),
    F.sum(F.when(F.col("Order_Channel").isin('D', 'M'), F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_OC_DM"),
    F.sum(F.when(F.col("Order_Channel").isin('D', 'M'), F.col("tot_trans_24m"))).alias("tot_trans_24m_OC_DM"),
    F.sum(F.when(F.col("Order_Channel").isin('D', 'M'), F.col("tot_dollar_amt_24m"))).alias("tot_dollar_amt_24m_OC_DM"),
    F.sum(F.when(F.col("Order_Channel").isin('D', 'M'), F.col("tot_dollar_disc_amt_24m"))).alias("tot_dollar_disc_amt_24m_OC_DM"),
    F.sum(F.when(F.col("Order_Channel").isin('D', 'M'), F.col("tot_trans_36m"))).alias("tot_trans_36m_OC_DM"),
    F.sum(F.when(F.col("Order_Channel").isin('D', 'M'), F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_OC_DM"),
    F.sum(F.when(F.col("Order_Channel").isin('D', 'M'), F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_OC_DM"),
    F.sum(F.when(F.col("Order_Channel").isin('D', 'M'), F.col("tot_trans"))).alias("tot_trans_ever_OC_DM"),
    F.sum(F.when(F.col("Order_Channel").isin('D', 'M'), F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_OC_DM"),
    F.sum(F.when(F.col("Order_Channel").isin('D', 'M'), F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_OC_DM"),
    
    F.sum(F.when(F.col("Order_Channel").isin('P', 'PHO'), F.col("tot_trans_6m"))).alias("tot_trans_6m_OC_PHO"),
    F.sum(F.when(F.col("Order_Channel").isin('P', 'PHO'), F.col("tot_dollar_amt_6m"))).alias("tot_dollar_amt_6m_OC_PHO"),
    F.sum(F.when(F.col("Order_Channel").isin('P', 'PHO'), F.col("tot_dollar_disc_amt_6m"))).alias("tot_dollar_disc_amt_6m_OC_PHO"),
    F.sum(F.when(F.col("Order_Channel").isin('P', 'PHO'), F.col("tot_trans_12m"))).alias("tot_trans_12m_OC_PHO"),
    F.sum(F.when(F.col("Order_Channel").isin('P', 'PHO'), F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_OC_PHO"),
    F.sum(F.when(F.col("Order_Channel").isin('P', 'PHO'), F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_OC_PHO"),
    F.sum(F.when(F.col("Order_Channel").isin('P', 'PHO'), F.col("tot_trans_24m"))).alias("tot_trans_24m_OC_PHO"),
    F.sum(F.when(F.col("Order_Channel").isin('P', 'PHO'), F.col("tot_dollar_amt_24m"))).alias("tot_dollar_amt_24m_OC_PHO"),
    F.sum(F.when(F.col("Order_Channel").isin('P', 'PHO'), F.col("tot_dollar_disc_amt_24m"))).alias("tot_dollar_disc_amt_24m_OC_PHO"),
    F.sum(F.when(F.col("Order_Channel").isin('P', 'PHO'), F.col("tot_trans_36m"))).alias("tot_trans_36m_OC_PHO"),
    F.sum(F.when(F.col("Order_Channel").isin('P', 'PHO'), F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_OC_PHO"),
    F.sum(F.when(F.col("Order_Channel").isin('P', 'PHO'), F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_OC_PHO"),
    F.sum(F.when(F.col("Order_Channel").isin('P', 'PHO'), F.col("tot_trans"))).alias("tot_trans_ever_OC_PHO"),
    F.sum(F.when(F.col("Order_Channel").isin('P', 'PHO'), F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_OC_PHO"),
    F.sum(F.when(F.col("Order_Channel").isin('P', 'PHO'), F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_OC_PHO"),
    
    F.sum(F.when(F.col("Order_Channel") == 'POS', F.col("tot_trans_6m"))).alias("tot_trans_6m_OC_POS"),
    F.sum(F.when(F.col("Order_Channel") == 'POS', F.col("tot_dollar_amt_6m"))).alias("tot_dollar_amt_6m_OC_POS"),
    F.sum(F.when(F.col("Order_Channel") == 'POS', F.col("tot_dollar_disc_amt_6m"))).alias("tot_dollar_disc_amt_6m_OC_POS"),
    F.sum(F.when(F.col("Order_Channel") == 'POS', F.col("tot_trans_12m"))).alias("tot_trans_12m_OC_POS"),
    F.sum(F.when(F.col("Order_Channel") == 'POS', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_OC_POS"),
    F.sum(F.when(F.col("Order_Channel") == 'POS', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_OC_POS"),
    F.sum(F.when(F.col("Order_Channel") == 'POS', F.col("tot_trans_24m"))).alias("tot_trans_24m_OC_POS"),
    F.sum(F.when(F.col("Order_Channel") == 'POS', F.col("tot_dollar_amt_24m"))).alias("tot_dollar_amt_24m_OC_POS"),
    F.sum(F.when(F.col("Order_Channel") == 'POS', F.col("tot_dollar_disc_amt_24m"))).alias("tot_dollar_disc_amt_24m_OC_POS"),
    F.sum(F.when(F.col("Order_Channel") == 'POS', F.col("tot_trans_36m"))).alias("tot_trans_36m_OC_POS"),
    F.sum(F.when(F.col("Order_Channel") == 'POS', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_OC_POS"),
    F.sum(F.when(F.col("Order_Channel") == 'POS', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_OC_POS"),
    F.sum(F.when(F.col("Order_Channel") == 'POS', F.col("tot_trans"))).alias("tot_trans_ever_OC_POS"),
    F.sum(F.when(F.col("Order_Channel") == 'POS', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_OC_POS"),
    F.sum(F.when(F.col("Order_Channel") == 'POS', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_OC_POS"),

    # Totals by Category
    F.sum(F.when(F.col("Category") == 'Discounts', F.col("tot_trans_6m"))).alias("tot_trans_6m_C_DISC"),
    F.sum(F.when(F.col("Category") == 'Discounts', F.col("tot_dollar_amt_6m"))).alias("tot_dollar_amt_6m_C_DISC"),
    F.sum(F.when(F.col("Category") == 'Discounts', F.col("tot_dollar_disc_amt_6m"))).alias("tot_dollar_disc_amt_6m_C_DISC"),
    F.sum(F.when(F.col("Category") == 'Discounts', F.col("tot_trans_12m"))).alias("tot_trans_12m_C_DISC"),
    F.sum(F.when(F.col("Category") == 'Discounts', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_C_DISC"),
    F.sum(F.when(F.col("Category") == 'Discounts', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_C_DISC"),
    F.sum(F.when(F.col("Category") == 'Discounts', F.col("tot_trans_24m"))).alias("tot_trans_24m_C_DISC"),
    F.sum(F.when(F.col("Category") == 'Discounts', F.col("tot_dollar_amt_24m"))).alias("tot_dollar_amt_24m_C_DISC"),
    F.sum(F.when(F.col("Category") == 'Discounts', F.col("tot_dollar_disc_amt_24m"))).alias("tot_dollar_disc_amt_24m_C_DISC"),
    F.sum(F.when(F.col("Category") == 'Discounts', F.col("tot_trans_36m"))).alias("tot_trans_36m_C_DISC"),
    F.sum(F.when(F.col("Category") == 'Discounts', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_C_DISC"),
    F.sum(F.when(F.col("Category") == 'Discounts', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_C_DISC"),
    F.sum(F.when(F.col("Category") == 'Discounts', F.col("tot_trans"))).alias("tot_trans_ever_C_DISC"),
    F.sum(F.when(F.col("Category") == 'Discounts', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_C_DISC"),
    F.sum(F.when(F.col("Category") == 'Discounts', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_C_DISC"),
    
    F.sum(F.when(F.col("Category") == 'Finance', F.col("tot_trans_6m"))).alias("tot_trans_6m_C_FIN"),
    F.sum(F.when(F.col("Category") == 'Finance', F.col("tot_dollar_amt_6m"))).alias("tot_dollar_amt_6m_C_FIN"),
    F.sum(F.when(F.col("Category") == 'Finance', F.col("tot_dollar_disc_amt_6m"))).alias("tot_dollar_disc_amt_6m_C_FIN"),
    F.sum(F.when(F.col("Category") == 'Finance', F.col("tot_trans_12m"))).alias("tot_trans_12m_C_FIN"),
    F.sum(F.when(F.col("Category") == 'Finance', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_C_FIN"),
    F.sum(F.when(F.col("Category") == 'Finance', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_C_FIN"),
    F.sum(F.when(F.col("Category") == 'Finance', F.col("tot_trans_24m"))).alias("tot_trans_24m_C_FIN"),
    F.sum(F.when(F.col("Category") == 'Finance', F.col("tot_dollar_amt_24m"))).alias("tot_dollar_amt_24m_C_FIN"),
    F.sum(F.when(F.col("Category") == 'Finance', F.col("tot_dollar_disc_amt_24m"))).alias("tot_dollar_disc_amt_24m_C_FIN"),
    F.sum(F.when(F.col("Category") == 'Finance', F.col("tot_trans_36m"))).alias("tot_trans_36m_C_FIN"),
    F.sum(F.when(F.col("Category") == 'Finance', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_C_FIN"),
    F.sum(F.when(F.col("Category") == 'Finance', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_C_FIN"),
    F.sum(F.when(F.col("Category") == 'Finance', F.col("tot_trans"))).alias("tot_trans_ever_C_FIN"),
    F.sum(F.when(F.col("Category") == 'Finance', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_C_FIN"),
    F.sum(F.when(F.col("Category") == 'Finance', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_C_FIN"),
    
    F.sum(F.when(F.col("Category") == 'Health', F.col("tot_trans_6m"))).alias("tot_trans_6m_C_HLTH"),
    F.sum(F.when(F.col("Category") == 'Health', F.col("tot_dollar_amt_6m"))).alias("tot_dollar_amt_6m_C_HLTH"),
    F.sum(F.when(F.col("Category") == 'Health', F.col("tot_dollar_disc_amt_6m"))).alias("tot_dollar_disc_amt_6m_C_HLTH"),
    F.sum(F.when(F.col("Category") == 'Health', F.col("tot_trans_12m"))).alias("tot_trans_12m_C_HLTH"),
    F.sum(F.when(F.col("Category") == 'Health', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_C_HLTH"),
    F.sum(F.when(F.col("Category") == 'Health', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_C_HLTH"),
    F.sum(F.when(F.col("Category") == 'Health', F.col("tot_trans_24m"))).alias("tot_trans_24m_C_HLTH"),
    F.sum(F.when(F.col("Category") == 'Health', F.col("tot_dollar_amt_24m"))).alias("tot_dollar_amt_24m_C_HLTH"),
    F.sum(F.when(F.col("Category") == 'Health', F.col("tot_dollar_disc_amt_24m"))).alias("tot_dollar_disc_amt_24m_C_HLTH"),
    F.sum(F.when(F.col("Category") == 'Health', F.col("tot_trans_36m"))).alias("tot_trans_36m_C_HLTH"),
    F.sum(F.when(F.col("Category") == 'Health', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_C_HLTH"),
    F.sum(F.when(F.col("Category") == 'Health', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_C_HLTH"),
    F.sum(F.when(F.col("Category") == 'Health', F.col("tot_trans"))).alias("tot_trans_ever_C_HLTH"),
    F.sum(F.when(F.col("Category") == 'Health', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_C_HLTH"),
    F.sum(F.when(F.col("Category") == 'Health', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_C_HLTH"),
    
    F.sum(F.when(F.col("Category") == 'Travel', F.col("tot_trans_6m"))).alias("tot_trans_6m_C_TRVL"),
    F.sum(F.when(F.col("Category") == 'Travel', F.col("tot_dollar_amt_6m"))).alias("tot_dollar_amt_6m_C_TRVL"),
    F.sum(F.when(F.col("Category") == 'Travel', F.col("tot_dollar_disc_amt_6m"))).alias("tot_dollar_disc_amt_6m_C_TRVL"),
    F.sum(F.when(F.col("Category") == 'Travel', F.col("tot_trans_12m"))).alias("tot_trans_12m_C_TRVL"),
    F.sum(F.when(F.col("Category") == 'Travel', F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_C_TRVL"),
    F.sum(F.when(F.col("Category") == 'Travel', F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_C_TRVL"),
    F.sum(F.when(F.col("Category") == 'Travel', F.col("tot_trans_24m"))).alias("tot_trans_24m_C_TRVL"),
    F.sum(F.when(F.col("Category") == 'Travel', F.col("tot_dollar_amt_24m"))).alias("tot_dollar_amt_24m_C_TRVL"),
    F.sum(F.when(F.col("Category") == 'Travel', F.col("tot_dollar_disc_amt_24m"))).alias("tot_dollar_disc_amt_24m_C_TRVL"),
    F.sum(F.when(F.col("Category") == 'Travel', F.col("tot_trans_36m"))).alias("tot_trans_36m_C_TRVL"),
    F.sum(F.when(F.col("Category") == 'Travel', F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_C_TRVL"),
    F.sum(F.when(F.col("Category") == 'Travel', F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_C_TRVL"),
    F.sum(F.when(F.col("Category") == 'Travel', F.col("tot_trans"))).alias("tot_trans_ever_C_TRVL"),
    F.sum(F.when(F.col("Category") == 'Travel', F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_C_TRVL"),
    F.sum(F.when(F.col("Category") == 'Travel', F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_C_TRVL"),

    # Added Providers
    F.sum(F.when(F.col("provider") == "1800Flowers", F.col("tot_trans_12m"))).alias("tot_trans_12m_FLW"),
    F.sum(F.when(F.col("provider") == "1800Flowers", F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_FLW"),
    F.sum(F.when(F.col("provider") == "1800Flowers", F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_FLW"),
    F.sum(F.when(F.col("provider") == "1800Flowers", F.col("tot_trans_36m"))).alias("tot_trans_36m_FLW"),
    F.sum(F.when(F.col("provider") == "1800Flowers", F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_FLW"),
    F.sum(F.when(F.col("provider") == "1800Flowers", F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_FLW"),
    F.sum(F.when(F.col("provider") == "1800Flowers", F.col("tot_trans"))).alias("tot_trans_ever_FLW"),
    F.sum(F.when(F.col("provider") == "1800Flowers", F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_FLW"),
    F.sum(F.when(F.col("provider") == "1800Flowers", F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_FLW"),
    
    F.sum(F.when(F.col("provider") == "Schwan's Home Delivery", F.col("tot_trans_12m"))).alias("tot_trans_12m_SHD"),
    F.sum(F.when(F.col("provider") == "Schwan's Home Delivery", F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_SHD"),
    F.sum(F.when(F.col("provider") == "Schwan's Home Delivery", F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_SHD"),
    F.sum(F.when(F.col("provider") == "Schwan's Home Delivery", F.col("tot_trans_36m"))).alias("tot_trans_36m_SHD"),
    F.sum(F.when(F.col("provider") == "Schwan's Home Delivery", F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_SHD"),
    F.sum(F.when(F.col("provider") == "Schwan's Home Delivery", F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_SHD"),
    F.sum(F.when(F.col("provider") == "Schwan's Home Delivery", F.col("tot_trans"))).alias("tot_trans_ever_SHD"),
    F.sum(F.when(F.col("provider") == "Schwan's Home Delivery", F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_SHD"),
    F.sum(F.when(F.col("provider") == "Schwan's Home Delivery", F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_SHD"),
    
    F.sum(F.when(F.col("provider") == "HomeServe USA", F.col("tot_trans_12m"))).alias("tot_trans_12m_HSU"),
    F.sum(F.when(F.col("provider") == "HomeServe USA", F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_HSU"),
    F.sum(F.when(F.col("provider") == "HomeServe USA", F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_HSU"),
    F.sum(F.when(F.col("provider") == "HomeServe USA", F.col("tot_trans_36m"))).alias("tot_trans_36m_HSU"),
    F.sum(F.when(F.col("provider") == "HomeServe USA", F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_HSU"),
    F.sum(F.when(F.col("provider") == "HomeServe USA", F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_HSU"),
    F.sum(F.when(F.col("provider") == "HomeServe USA", F.col("tot_trans"))).alias("tot_trans_ever_HSU"),
    F.sum(F.when(F.col("provider") == "HomeServe USA", F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_HSU"),
    F.sum(F.when(F.col("provider") == "HomeServe USA", F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_HSU"),
    
    F.sum(F.when(F.col("provider") == "Trusted ID", F.col("tot_trans_12m"))).alias("tot_trans_12m_TID"),
    F.sum(F.when(F.col("provider") == "Trusted ID", F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_TID"),
    F.sum(F.when(F.col("provider") == "Trusted ID", F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_TID"),
    F.sum(F.when(F.col("provider") == "Trusted ID", F.col("tot_trans_36m"))).alias("tot_trans_36m_TID"),
    F.sum(F.when(F.col("provider") == "Trusted ID", F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_TID"),
    F.sum(F.when(F.col("provider") == "Trusted ID", F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_TID"),
    F.sum(F.when(F.col("provider") == "Trusted ID", F.col("tot_trans"))).alias("tot_trans_ever_TID"),
    F.sum(F.when(F.col("provider") == "Trusted ID", F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_TID"),
    F.sum(F.when(F.col("provider") == "Trusted ID", F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_TID"),
    
    F.sum(F.when(F.col("provider") == "MedJet Assist", F.col("tot_trans_12m"))).alias("tot_trans_12m_MJA"),
    F.sum(F.when(F.col("provider") == "MedJet Assist", F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_MJA"),
    F.sum(F.when(F.col("provider") == "MedJet Assist", F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_MJA"),
    F.sum(F.when(F.col("provider") == "MedJet Assist", F.col("tot_trans_36m"))).alias("tot_trans_36m_MJA"),
    F.sum(F.when(F.col("provider") == "MedJet Assist", F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_MJA"),
    F.sum(F.when(F.col("provider") == "MedJet Assist", F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_MJA"),
    F.sum(F.when(F.col("provider") == "MedJet Assist", F.col("tot_trans"))).alias("tot_trans_ever_MJA"),
    F.sum(F.when(F.col("provider") == "MedJet Assist", F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_MJA"),
    F.sum(F.when(F.col("provider") == "MedJet Assist", F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_MJA"),
    
    F.sum(F.when(F.col("provider") == "Collette Vacations", F.col("tot_trans_12m"))).alias("tot_trans_12m_CLV"),
    F.sum(F.when(F.col("provider") == "Collette Vacations", F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_CLV"),
    F.sum(F.when(F.col("provider") == "Collette Vacations", F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_CLV"),
    F.sum(F.when(F.col("provider") == "Collette Vacations", F.col("tot_trans_36m"))).alias("tot_trans_36m_CLV"),
    F.sum(F.when(F.col("provider") == "Collette Vacations", F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_CLV"),
    F.sum(F.when(F.col("provider") == "Collette Vacations", F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_CLV"),
    F.sum(F.when(F.col("provider") == "Collette Vacations", F.col("tot_trans"))).alias("tot_trans_ever_CLV"),
    F.sum(F.when(F.col("provider") == "Collette Vacations", F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_CLV"),
    F.sum(F.when(F.col("provider") == "Collette Vacations", F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_CLV"),
    
    F.sum(F.when(F.col("provider") == "Grand European Tours", F.col("tot_trans_12m"))).alias("tot_trans_12m_GET"),
    F.sum(F.when(F.col("provider") == "Grand European Tours", F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_GET"),
    F.sum(F.when(F.col("provider") == "Grand European Tours", F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_GET"),
    F.sum(F.when(F.col("provider") == "Grand European Tours", F.col("tot_trans_36m"))).alias("tot_trans_36m_GET"),
    F.sum(F.when(F.col("provider") == "Grand European Tours", F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_GET"),
    F.sum(F.when(F.col("provider") == "Grand European Tours", F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_GET"),
    F.sum(F.when(F.col("provider") == "Grand European Tours", F.col("tot_trans"))).alias("tot_trans_ever_GET"),
    F.sum(F.when(F.col("provider") == "Grand European Tours", F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_GET"),
    F.sum(F.when(F.col("provider") == "Grand European Tours", F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_GET"),
    
    F.sum(F.when(F.col("provider") == "Liberty Travel", F.col("tot_trans_12m"))).alias("tot_trans_12m_LBT"),
    F.sum(F.when(F.col("provider") == "Liberty Travel", F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_LBT"),
    F.sum(F.when(F.col("provider") == "Liberty Travel", F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_LBT"),
    F.sum(F.when(F.col("provider") == "Liberty Travel", F.col("tot_trans_36m"))).alias("tot_trans_36m_LBT"),
    F.sum(F.when(F.col("provider") == "Liberty Travel", F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_LBT"),
    F.sum(F.when(F.col("provider") == "Liberty Travel", F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_LBT"),
    F.sum(F.when(F.col("provider") == "Liberty Travel", F.col("tot_trans"))).alias("tot_trans_ever_LBT"),
    F.sum(F.when(F.col("provider") == "Liberty Travel", F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_LBT"),
    F.sum(F.when(F.col("provider") == "Liberty Travel", F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_LBT"),
    
    F.sum(F.when(F.col("provider") == "ParkRideFly USA", F.col("tot_trans_12m"))).alias("tot_trans_12m_PRF"),
    F.sum(F.when(F.col("provider") == "ParkRideFly USA", F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_PRF"),
    F.sum(F.when(F.col("provider") == "ParkRideFly USA", F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_PRF"),
    F.sum(F.when(F.col("provider") == "ParkRideFly USA", F.col("tot_trans_36m"))).alias("tot_trans_36m_PRF"),
    F.sum(F.when(F.col("provider") == "ParkRideFly USA", F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_PRF"),
    F.sum(F.when(F.col("provider") == "ParkRideFly USA", F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_PRF"),
    F.sum(F.when(F.col("provider") == "ParkRideFly USA", F.col("tot_trans"))).alias("tot_trans_ever_PRF"),
    F.sum(F.when(F.col("provider") == "ParkRideFly USA", F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_PRF"),
    F.sum(F.when(F.col("provider") == "ParkRideFly USA", F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_PRF"),
    
    F.sum(F.when(F.col("provider") == "Vacations by Rail", F.col("tot_trans_12m"))).alias("tot_trans_12m_VBR"),
    F.sum(F.when(F.col("provider") == "Vacations by Rail", F.col("tot_dollar_amt_12m"))).alias("tot_dollar_amt_12m_VBR"),
    F.sum(F.when(F.col("provider") == "Vacations by Rail", F.col("tot_dollar_disc_amt_12m"))).alias("tot_dollar_disc_amt_12m_VBR"),
    F.sum(F.when(F.col("provider") == "Vacations by Rail", F.col("tot_trans_36m"))).alias("tot_trans_36m_VBR"),
    F.sum(F.when(F.col("provider") == "Vacations by Rail", F.col("tot_dollar_amt_36m"))).alias("tot_dollar_amt_36m_VBR"),
    F.sum(F.when(F.col("provider") == "Vacations by Rail", F.col("tot_dollar_disc_amt_36m"))).alias("tot_dollar_disc_amt_36m_VBR"),
    F.sum(F.when(F.col("provider") == "Vacations by Rail", F.col("tot_trans"))).alias("tot_trans_ever_VBR"),
    F.sum(F.when(F.col("provider") == "Vacations by Rail", F.col("tot_dollar_amt"))).alias("tot_dollar_amt_ever_VBR"),
    F.sum(F.when(F.col("provider") == "Vacations by Rail", F.col("tot_dollar_disc_amt"))).alias("tot_dollar_disc_amt_ever_VBR"),
]

df_aggr_gob = df_merge_all_1.groupBy("CHID").agg(*agg_exprs)

# Convert 0s to nulls
for col_name in df_aggr_gob.columns:
    if col_name.startswith("Flag"):
        df_aggr_gob = df_aggr_gob.withColumn(col_name, F.when(F.col(col_name) == 0, None).otherwise(F.col(col_name)))
    elif col_name.startswith("tot_"):
        df_aggr_gob = df_aggr_gob.withColumn(col_name, F.when(F.col(col_name) == 0, None).otherwise(F.col(col_name)))

# Create aggr_gob_2
flag_36m_cols = [c for c in df_aggr_gob.columns if c.startswith('Flag36m_')]
flag_ever_cols = [c for c in df_aggr_gob.columns if c.startswith('Flag_ever_')]

df_aggr_gob_2 = df_aggr_gob.withColumn("number_of_products_36m", sum(F.coalesce(F.col(c), F.lit(0)) for c in flag_36m_cols)) \
                                .withColumn("number_of_products_ever", sum(F.coalesce(F.col(c), F.lit(0)) for c in flag_ever_cols))

cols_to_drop = [
    *flag_36m_cols, *flag_ever_cols,
    "tot_dollar_amt_12m_HAR_HM", "tot_dollar_disc_amt_12m_HAR_HM", "tot_dollar_amt_36m_HAR_HM", "tot_dollar_disc_amt_36m_HAR_HM", "tot_dollar_amt_ever_HAR_HM", "tot_dollar_disc_amt_ever_HAR_HM",
    "tot_dollar_amt_12m_HAR_AT", "tot_dollar_disc_amt_12m_HAR_AT", "tot_dollar_amt_36m_HAR_AT", "tot_dollar_disc_amt_36m_HAR_AT", "tot_dollar_amt_ever_HAR_AT", "tot_dollar_disc_amt_ever_HAR_AT",
    "tot_dollar_amt_12m_ALL", "tot_dollar_disc_amt_12m_ALL", "tot_dollar_amt_36m_ALL", "tot_dollar_disc_amt_36m_ALL", "tot_dollar_amt_ever_ALL", "tot_dollar_disc_amt_ever_ALL",
    "tot_dollar_amt_12m_NYL_LF", "tot_dollar_disc_amt_12m_NYL_LF", "tot_dollar_amt_36m_NYL_LF", "tot_dollar_disc_amt_36m_NYL_LF", "tot_dollar_amt_ever_NYL_LF", "tot_dollar_disc_amt_ever_NYL_LF",
    "tot_dollar_amt_12m_NYL_ANN", "tot_dollar_disc_amt_12m_NYL_ANN", "tot_dollar_amt_36m_NYL_ANN", "tot_dollar_disc_amt_36m_NYL_ANN", "tot_dollar_amt_ever_NYL_ANN", "tot_dollar_disc_amt_ever_NYL_ANN",
    "tot_dollar_disc_amt_12m_WAL",
    "tot_dollar_amt_12m_CHA", "tot_dollar_disc_amt_12m_CHA", "tot_dollar_amt_36m_CHA", "tot_dollar_disc_amt_36m_CHA", "tot_dollar_amt_ever_CHA", "tot_dollar_disc_amt_ever_CHA",
    "tot_dollar_amt_12m_CEL", "tot_dollar_disc_amt_12m_CEL", "tot_dollar_amt_36m_CEL", "tot_dollar_disc_amt_36m_CEL", "tot_dollar_amt_ever_CEL", "tot_dollar_disc_amt_ever_CEL",
    "tot_dollar_amt_12m_FMS_HM", "tot_dollar_disc_amt_12m_FMS_HM", "tot_dollar_amt_36m_FMS_HM", "tot_dollar_disc_amt_36m_FMS_HM", "tot_dollar_amt_ever_FMS_HM", "tot_dollar_disc_amt_ever_FMS_HM",
    "tot_dollar_amt_12m_FMS_MT", "tot_dollar_disc_amt_12m_FMS_MT", "tot_dollar_amt_36m_FMS_MT", "tot_dollar_disc_amt_36m_FMS_MT", "tot_dollar_amt_ever_FMS_MT", "tot_dollar_disc_amt_ever_FMS_MT",
    "tot_dollar_amt_12m_TAN", "tot_dollar_amt_36m_TAN", "tot_dollar_amt_ever_TAN",
    "tot_dollar_amt_12m_ADT", "tot_dollar_disc_amt_12m_ADT", "tot_dollar_amt_36m_ADT", "tot_dollar_disc_amt_36m_ADT", "tot_dollar_amt_ever_ADT", "tot_dollar_disc_amt_ever_ADT",
    "tot_dollar_amt_6m_DS_SUBS", "tot_dollar_disc_amt_6m_DS_SUBS", "tot_dollar_amt_12m_DS_SUBS", "tot_dollar_disc_amt_12m_DS_SUBS", "tot_dollar_amt_24m_DS_SUBS", "tot_dollar_disc_amt_24m_DS_SUBS", "tot_dollar_amt_36m_DS_SUBS", "tot_dollar_disc_amt_36m_DS_SUBS", "tot_dollar_amt_ever_DS_SUBS", "tot_dollar_disc_amt_ever_DS_SUBS",
    "tot_dollar_amt_6m_OC_DM", "tot_dollar_disc_amt_6m_OC_DM", "tot_dollar_amt_12m_OC_DM", "tot_dollar_disc_amt_12m_OC_DM", "tot_dollar_amt_24m_OC_DM", "tot_dollar_disc_amt_24m_OC_DM", "tot_dollar_amt_36m_OC_DM", "tot_dollar_disc_amt_36m_OC_DM", "tot_dollar_amt_ever_OC_DM", "tot_dollar_disc_amt_ever_OC_DM",
    "tot_dollar_amt_6m_C_FIN", "tot_dollar_disc_amt_6m_C_FIN", "tot_dollar_amt_12m_C_FIN", "tot_dollar_disc_amt_12m_C_FIN", "tot_dollar_amt_24m_C_FIN", "tot_dollar_disc_amt_24m_C_FIN", "tot_dollar_amt_36m_C_FIN", "tot_dollar_disc_amt_36m_C_FIN", "tot_dollar_amt_ever_C_FIN", "tot_dollar_disc_amt_ever_C_FIN"
]
df_aggr_gob_2 = df_aggr_gob_2.drop(*cols_to_drop)

# Create aggr_gob_3
def create_vars(df, vname, has_da_dda=True):
    df = df.withColumn(f"tot_trans_13to24m_{vname}", F.coalesce(F.col(f"tot_trans_24m_{vname}"), F.lit(0)) - F.coalesce(F.col(f"tot_trans_12m_{vname}"), F.lit(0)))
    df = df.withColumn(f"T2Y_tot_trans_13to24m_{vname}", F.coalesce(F.col(f"tot_trans_12m_{vname}"), F.lit(0)) - F.coalesce(F.col(f"tot_trans_13to24m_{vname}"), F.lit(0)))
    df = df.withColumn(f"T2YR_tot_trans_13to24m_{vname}", F.when((F.col(f"tot_trans_13to24m_{vname}") != 0) & F.col(f"tot_trans_13to24m_{vname}").isNotNull(), F.round(F.col(f"T2Y_tot_trans_13to24m_{vname}") / F.col(f"tot_trans_13to24m_{vname}"), 2)))
    df = df.withColumn(f"R_tot_trans_12m_{vname}", F.when(F.col("tot_trans_12m_TOT").isNotNull(), F.round(F.col(f"tot_trans_12m_{vname}") / F.col("tot_trans_12m_TOT"), 8)))
    df = df.withColumn(f"R_tot_trans_36m_{vname}", F.when(F.col("tot_trans_36m_TOT").isNotNull(), F.round(F.col(f"tot_trans_36m_{vname}") / F.col("tot_trans_36m_TOT"), 8)))

    if has_da_dda:
        df = df.withColumn(f"tot_da_13to24m_{vname}", F.coalesce(F.col(f"tot_dollar_amt_24m_{vname}"), F.lit(0)) - F.coalesce(F.col(f"tot_dollar_amt_12m_{vname}"), F.lit(0)))
        df = df.withColumn(f"tot_dda_13to24m_{vname}", F.coalesce(F.col(f"tot_dollar_disc_amt_24m_{vname}"), F.lit(0)) - F.coalesce(F.col(f"tot_dollar_disc_amt_12m_{vname}"), F.lit(0)))
        df = df.withColumn(f"T2Y_tot_da_13to24m_{vname}", F.coalesce(F.col(f"tot_dollar_amt_12m_{vname}"), F.lit(0)) - F.coalesce(F.col(f"tot_da_13to24m_{vname}"), F.lit(0)))
        df = df.withColumn(f"T2Y_tot_dda_13to24m_{vname}", F.coalesce(F.col(f"tot_dollar_disc_amt_12m_{vname}"), F.lit(0)) - F.coalesce(F.col(f"tot_dda_13to24m_{vname}"), F.lit(0)))
        df = df.withColumn(f"T2YR_tot_da_13to24m_{vname}", F.when((F.col(f"tot_da_13to24m_{vname}") != 0) & F.col(f"tot_da_13to24m_{vname}").isNotNull(), F.round(F.col(f"T2Y_tot_da_13to24m_{vname}") / F.col(f"tot_da_13to24m_{vname}"), 2)))
        df = df.withColumn(f"T2YR_tot_dda_13to24m_{vname}", F.when((F.col(f"tot_dda_13to24m_{vname}") != 0) & F.col(f"tot_dda_13to24m_{vname}").isNotNull(), F.round(F.col(f"T2Y_tot_dda_13to24m_{vname}") / F.col(f"tot_dda_13to24m_{vname}"), 2)))
        df = df.withColumn(f"R_tot_d_a_12m_{vname}", F.when(F.col("tot_dollar_amt_12m_TOT").isNotNull(), F.round(F.col(f"tot_dollar_amt_12m_{vname}") / F.col("tot_dollar_amt_12m_TOT"), 8)))
        df = df.withColumn(f"R_tot_d_a_36m_{vname}", F.when(F.col("tot_dollar_amt_36m_TOT").isNotNull(), F.round(F.col(f"tot_dollar_amt_36m_{vname}") / F.col("tot_dollar_amt_36m_TOT"), 8)))
        
    return df

df_aggr_gob_3 = df_aggr_gob_2
df_aggr_gob_3 = create_vars(df_aggr_gob_3, "DS_LS")
df_aggr_gob_3 = create_vars(df_aggr_gob_3, "DS_SUBS", has_da_dda=False)
df_aggr_gob_3 = create_vars(df_aggr_gob_3, "OC_WEB")
df_aggr_gob_3 = create_vars(df_aggr_gob_3, "OC_DM", has_da_dda=False)
df_aggr_gob_3 = create_vars(df_aggr_gob_3, "OC_PHO")
df_aggr_gob_3 = create_vars(df_aggr_gob_3, "OC_POS")
df_aggr_gob_3 = create_vars(df_aggr_gob_3, "C_DISC")
df_aggr_gob_3 = create_vars(df_aggr_gob_3, "C_FIN", has_da_dda=False)
df_aggr_gob_3 = create_vars(df_aggr_gob_3, "C_HLTH")
df_aggr_gob_3 = create_vars(df_aggr_gob_3, "C_TRVL")

# Create aggr_4
df_aggr_4 = df_aggr_gob.withColumn("Sum_Prod_Disc_201712_Ever", F.col("flag_ever_CEL") + F.col("flag_ever_ADT") + F.col("flag_ever_FLW") + F.col("flag_ever_DEN") + F.col("flag_ever_REG") + F.col("flag_ever_SHD") + F.col("flag_ever_TAN") + F.col("flag_ever_UPS") + F.col("flag_ever_WAL")) \
                        .withColumn("Sum_Prod_Disc_201712_36M", F.col("flag36m_CEL") + F.col("flag36m_ADT") + F.col("flag36m_FLW") + F.col("flag36m_DEN") + F.col("flag36m_REG") + F.col("flag36m_SHD") + F.col("flag36m_TAN") + F.col("flag36m_UPS") + F.col("flag36m_WAL")) \
                        .withColumn("Sum_Prod_Fin_201712_Ever", F.col("Flag_ever_FMS_HM") + F.col("Flag_ever_FMS_MT") + F.col("Flag_ever_HAR_HM") + F.col("Flag_ever_HAR_AT") + F.col("Flag_ever_NYL_LF") + F.col("flag_ever_HSU") + F.col("flag_ever_TID")) \
                        .withColumn("Sum_Prod_Fin_201712_36M", F.col("Flag36m_FMS_HM") + F.col("Flag36m_FMS_MT") + F.col("Flag36m_HAR_HM") + F.col("Flag36m_HAR_AT") + F.col("Flag36m_NYL_LF") + F.col("Flag36m_HSU") + F.col("flag36m_TID")) \
                        .withColumn("Sum_Prod_Trav_201712_Ever", F.col("flag_ever_ALL") + F.col("flag_ever_MJA") + F.col("flag_ever_EXP") + F.col("flag_ever_AVI") + F.col("flag_ever_BUD") + F.col("flag_ever_CLV") + F.col("flag_ever_GET") + F.col("flag_ever_HIL") + F.col("flag_ever_LBT") + F.col("flag_ever_PRF") + F.col("flag_ever_VBR")) \
                        .withColumn("Sum_Prod_Trav_201712_36M", F.col("flag36m_ALL") + F.col("flag36m_MJA") + F.col("flag36m_EXP") + F.col("flag36m_AVI") + F.col("flag36m_BUD") + F.col("flag36m_CLV") + F.col("flag36m_GET") + F.col("flag36m_HIL") + F.col("flag36m_LBT") + F.col("flag36m_PRF") + F.col("flag36m_VBR")) \
                        .select("chid", "Sum_Prod_Disc_201712_Ever", "Sum_Prod_Disc_201712_36M", "Sum_Prod_Fin_201712_Ever", "Sum_Prod_Fin_201712_36M", "Sum_Prod_Trav_201712_Ever", "Sum_Prod_Trav_201712_36M")

# Merge aggr_gob_3 and aggr_4
df_aggr_gob_3 = df_aggr_gob_3.join(df_aggr_4, "chid", "left")

# Create mms_m_dp12
df_mms_m_dp12 = df_aggr_gob_3.selectExpr(
    "CHID",
    "tot_trans_12m_HAR_HM as L0001",
    "tot_trans_36m_HAR_HM as L0002",
    "tot_trans_ever_HAR_HM as L0003",
    "tot_trans_12m_HAR_AT as L0004",
    "tot_trans_36m_HAR_AT as L0005",
    "tot_trans_ever_HAR_AT as L0006",
    "tot_trans_12m_ALL as L0007",
    "tot_trans_36m_ALL as L0008",
    "tot_trans_ever_ALL as L0009",
    "tot_trans_12m_NYL_LF as L0010",
    "tot_trans_36m_NYL_LF as L0011",
    "tot_trans_ever_NYL_LF as L0012",
    "tot_trans_12m_NYL_ANN as L0013",
    "tot_trans_36m_NYL_ANN as L0014",
    "tot_trans_ever_NYL_ANN as L0015",
    "tot_trans_12m_WAL as L0016",
    "tot_dollar_amt_12m_WAL as L0017",
    "tot_trans_36m_WAL as L0018",
    "tot_dollar_amt_36m_WAL as L0019",
    "tot_dollar_disc_amt_36m_WAL as L0020",
    "tot_trans_ever_WAL as L0021",
    "tot_dollar_amt_ever_WAL as L0022",
    "tot_dollar_disc_amt_ever_WAL as L0023",
    "tot_trans_12m_CHA as L0024",
    "tot_trans_36m_CHA as L0025",
    "tot_trans_ever_CHA as L0026",
    "tot_trans_12m_HIL as L0027",
    "tot_dollar_amt_12m_HIL as L0028",
    "tot_dollar_disc_amt_12m_HIL as L0029",
    "tot_trans_36m_HIL as L0030",
    "tot_dollar_amt_36m_HIL as L0031",
    "tot_dollar_disc_amt_36m_HIL as L0032",
    "tot_trans_ever_HIL as L0033",
    "tot_dollar_amt_ever_HIL as L0034",
    "tot_dollar_disc_amt_ever_HIL as L0035",
    "tot_trans_12m_CEL as L0036",
    "tot_trans_36m_CEL as L0037",
    "tot_trans_ever_CEL as L0038",
    "tot_trans_12m_DEN as L0039",
    "tot_dollar_amt_12m_DEN as L0040",
    "tot_dollar_disc_amt_12m_DEN as L0041",
    "tot_trans_36m_DEN as L0042",
    "tot_dollar_amt_36m_DEN as L0043",
    "tot_dollar_disc_amt_36m_DEN as L0044",
    "tot_trans_ever_DEN as L0045",
    "tot_dollar_amt_ever_DEN as L0046",
    "tot_dollar_disc_amt_ever_DEN as L0047",
    "tot_trans_12m_EXP as L0048",
    "tot_dollar_amt_12m_EXP as L0049",
    "tot_dollar_disc_amt_12m_EXP as L0050",
    "tot_trans_36m_EXP as L0051",
    "tot_dollar_amt_36m_EXP as L0052",
    "tot_dollar_disc_amt_36m_EXP as L0053",
    "tot_trans_ever_EXP as L0054",
    "tot_dollar_amt_ever_EXP as L0055",
    "tot_dollar_disc_amt_ever_EXP as L0056",
    "tot_trans_12m_FMS_HM as L0057",
    "tot_trans_36m_FMS_HM as L0058",
    "tot_trans_ever_FMS_HM as L0059",
    "tot_trans_12m_FMS_MT as L0060",
    "tot_trans_36m_FMS_MT as L0061",
    "tot_trans_ever_FMS_MT as L0062",
    "tot_trans_12m_TAN as L0063",
    "tot_dollar_disc_amt_12m_TAN as L0064",
    "tot_trans_36m_TAN as L0065",
    "tot_dollar_disc_amt_36m_TAN as L0066",
    "tot_trans_ever_TAN as L0067",
    "tot_dollar_disc_amt_ever_TAN as L0068",
    "tot_trans_12m_REG as L0069",
    "tot_dollar_amt_12m_REG as L0070",
    "tot_dollar_disc_amt_12m_REG as L0071",
    "tot_trans_36m_REG as L0072",
    "tot_dollar_amt_36m_REG as L0073",
    "tot_dollar_disc_amt_36m_REG as L0074",
    "tot_trans_ever_REG as L0075",
    "tot_dollar_amt_ever_REG as L0076",
    "tot_dollar_disc_amt_ever_REG as L0077",
    "tot_trans_12m_UPS as L0078",
    "tot_dollar_amt_12m_UPS as L0079",
    "tot_dollar_disc_amt_12m_UPS as L0080",
    "tot_trans_36m_UPS as L0081",
    "tot_dollar_amt_36m_UPS as L0082",
    "tot_dollar_disc_amt_36m_UPS as L0083",
    "tot_trans_ever_UPS as L0084",
    "tot_dollar_amt_ever_UPS as L0085",
    "tot_dollar_disc_amt_ever_UPS as L0086",
    "tot_trans_12m_CAT as L0087",
    "tot_dollar_amt_12m_CAT as L0088",
    "tot_dollar_disc_amt_12m_CAT as L0089",
    "tot_trans_36m_CAT as L0090",
    "tot_dollar_amt_36m_CAT as L0091",
    "tot_dollar_disc_amt_36m_CAT as L0092",
    "tot_trans_ever_CAT as L0093",
    "tot_dollar_amt_ever_CAT as L0094",
    "tot_dollar_disc_amt_ever_CAT as L0095",
    "tot_trans_12m_ADT as L0096",
    "tot_trans_36m_ADT as L0097",
    "tot_trans_ever_ADT as L0098",
    "tot_trans_12m_BUD as L0099",
    "tot_dollar_amt_12m_BUD as L0100",
    "tot_dollar_disc_amt_12m_BUD as L0101",
    "tot_trans_36m_BUD as L0102",
    "tot_dollar_amt_36m_BUD as L0103",
    "tot_dollar_disc_amt_36m_BUD as L0104",
    "tot_trans_ever_BUD as L0105",
    "tot_dollar_amt_ever_BUD as L0106",
    "tot_dollar_disc_amt_ever_BUD as L0107",
    "tot_trans_12m_AVI as L0108",
    "tot_dollar_amt_12m_AVI as L0109",
    "tot_dollar_disc_amt_12m_AVI as L0110",
    "tot_trans_36m_AVI as L0111",
    "tot_dollar_amt_36m_AVI as L0112",
    "tot_dollar_disc_amt_36m_AVI as L0113",
    "tot_trans_ever_AVI as L0114",
    "tot_dollar_amt_ever_AVI as L0115",
    "tot_dollar_disc_amt_ever_AVI as L0116",
    "tot_trans_6m_TOT as L0117",
    "tot_dollar_amt_6m_TOT as L0118",
    "tot_dollar_disc_amt_6m_TOT as L0119",
    "tot_trans_12m_TOT as L0120",
    "tot_dollar_amt_12m_TOT as L0121",
    "tot_dollar_disc_amt_12m_TOT as L0122",
    "tot_trans_24m_TOT as L0123",
    "tot_dollar_amt_24m_TOT as L0124",
    "tot_dollar_disc_amt_24m_TOT as L0125",
    "tot_trans_36m_TOT as L0126",
    "tot_dollar_amt_36m_TOT as L0127",
    "tot_dollar_disc_amt_36m_TOT as L0128",
    "tot_trans_ever_TOT as L0129",
    "tot_dollar_amt_ever_TOT as L0130",
    "tot_dollar_disc_amt_ever_TOT as L0131",
    "tot_trans_6m_DS_LS as L0132",
    "tot_dollar_amt_6m_DS_LS as L0133",
    "tot_dollar_disc_amt_6m_DS_LS as L0134",
    "tot_trans_12m_DS_LS as L0135",
    "tot_dollar_amt_12m_DS_LS as L0136",
    "tot_dollar_disc_amt_12m_DS_LS as L0137",
    "tot_trans_24m_DS_LS as L0138",
    "tot_dollar_amt_24m_DS_LS as L0139",
    "tot_dollar_disc_amt_24m_DS_LS as L0140",
    "tot_trans_36m_DS_LS as L0141",
    "tot_dollar_amt_36m_DS_LS as L0142",
    "tot_dollar_disc_amt_36m_DS_LS as L0143",
    "tot_trans_ever_DS_LS as L0144",
    "tot_dollar_amt_ever_DS_LS as L0145",
    "tot_dollar_disc_amt_ever_DS_LS as L0146",
    "tot_trans_6m_DS_SUBS as L0147",
    "tot_trans_12m_DS_SUBS as L0148",
    "tot_trans_24m_DS_SUBS as L0149",
    "tot_trans_36m_DS_SUBS as L0150",
    "tot_trans_ever_DS_SUBS as L0151",
    "tot_trans_6m_OC_WEB as L0152",
    "tot_dollar_amt_6m_OC_WEB as L0153",
    "tot_dollar_disc_amt_6m_OC_WEB as L0154",
    "tot_trans_12m_OC_WEB as L0155",
    "tot_dollar_amt_12m_OC_WEB as L0156",
    "tot_dollar_disc_amt_12m_OC_WEB as L0157",
    "tot_trans_24m_OC_WEB as L0158",
    "tot_dollar_amt_24m_OC_WEB as L0159",
    "tot_dollar_disc_amt_24m_OC_WEB as L0160",
    "tot_trans_36m_OC_WEB as L0161",
    "tot_dollar_amt_36m_OC_WEB as L0162",
    "tot_dollar_disc_amt_36m_OC_WEB as L0163",
    "tot_trans_ever_OC_WEB as L0164",
    "tot_dollar_amt_ever_OC_WEB as L0165",
    "tot_dollar_disc_amt_ever_OC_WEB as L0166",
    "tot_trans_6m_OC_DM as L0167",
    "tot_trans_12m_OC_DM as L0168",
    "tot_trans_24m_OC_DM as L0169",
    "tot_trans_36m_OC_DM as L0170",
    "tot_trans_ever_OC_DM as L0171",
    "tot_trans_6m_OC_PHO as L0172",
    "tot_dollar_amt_6m_OC_PHO as L0173",
    "tot_dollar_disc_amt_6m_OC_PHO as L0174",
    "tot_trans_12m_OC_PHO as L0175",
    "tot_dollar_amt_12m_OC_PHO as L0176",
    "tot_dollar_disc_amt_12m_OC_PHO as L0177",
    "tot_trans_24m_OC_PHO as L0178",
    "tot_dollar_amt_24m_OC_PHO as L0179",
    "tot_dollar_disc_amt_24m_OC_PHO as L0180",
    "tot_trans_36m_OC_PHO as L0181",
    "tot_dollar_amt_36m_OC_PHO as L0182",
    "tot_dollar_disc_amt_36m_OC_PHO as L0183",
    "tot_trans_ever_OC_PHO as L0184",
    "tot_dollar_amt_ever_OC_PHO as L0185",
    "tot_dollar_disc_amt_ever_OC_PHO as L0186",
    "tot_trans_6m_OC_POS as L0187",
    "tot_dollar_amt_6m_OC_POS as L0188",
    "tot_dollar_disc_amt_6m_OC_POS as L0189",
    "tot_trans_12m_OC_POS as L0190",
    "tot_dollar_amt_12m_OC_POS as L0191",
    "tot_dollar_disc_amt_12m_OC_POS as L0192",
    "tot_trans_24m_OC_POS as L0193",
    "tot_dollar_amt_24m_OC_POS as L0194",
    "tot_dollar_disc_amt_24m_OC_POS as L0195",
    "tot_trans_36m_OC_POS as L0196",
    "tot_dollar_amt_36m_OC_POS as L0197",
    "tot_dollar_disc_amt_36m_OC_POS as L0198",
    "tot_trans_ever_OC_POS as L0199",
    "tot_dollar_amt_ever_OC_POS as L0200",
    "tot_dollar_disc_amt_ever_OC_POS as L0201",
    "tot_trans_6m_C_DISC as L0202",
    "tot_dollar_amt_6m_C_DISC as L0203",
    "tot_dollar_disc_amt_6m_C_DISC as L0204",
    "tot_trans_12m_C_DISC as L0205",
    "tot_dollar_amt_12m_C_DISC as L0206",
    "tot_dollar_disc_amt_12m_C_DISC as L0207",
    "tot_trans_24m_C_DISC as L0208",
    "tot_dollar_amt_24m_C_DISC as L0209",
    "tot_dollar_disc_amt_24m_C_DISC as L0210",
    "tot_trans_36m_C_DISC as L0211",
    "tot_dollar_amt_36m_C_DISC as L0212",
    "tot_dollar_disc_amt_36m_C_DISC as L0213",
    "tot_trans_ever_C_DISC as L0214",
    "tot_dollar_amt_ever_C_DISC as L0215",
    "tot_dollar_disc_amt_ever_C_DISC as L0216",
    "tot_trans_6m_C_FIN as L0217",
    "tot_trans_12m_C_FIN as L0218",
    "tot_trans_24m_C_FIN as L0219",
    "tot_trans_36m_C_FIN as L0220",
    "tot_trans_ever_C_FIN as L0221",
    "tot_trans_6m_C_HLTH as L0222",
    "tot_dollar_amt_6m_C_HLTH as L0223",
    "tot_dollar_disc_amt_6m_C_HLTH as L0224",
    "tot_trans_12m_C_HLTH as L0225",
    "tot_dollar_amt_12m_C_HLTH as L0226",
    "tot_dollar_disc_amt_12m_C_HLTH as L0227",
    "tot_trans_24m_C_HLTH as L0228",
    "tot_dollar_amt_24m_C_HLTH as L0229",
    "tot_dollar_disc_amt_24m_C_HLTH as L0230",
    "tot_trans_36m_C_HLTH as L0231",
    "tot_dollar_amt_36m_C_HLTH as L0232",
    "tot_dollar_disc_amt_36m_C_HLTH as L0233",
    "tot_trans_ever_C_HLTH as L0234",
    "tot_dollar_amt_ever_C_HLTH as L0235",
    "tot_dollar_disc_amt_ever_C_HLTH as L0236",
    "tot_trans_6m_C_TRVL as L0237",
    "tot_dollar_amt_6m_C_TRVL as L0238",
    "tot_dollar_disc_amt_6m_C_TRVL as L0239",
    "tot_trans_12m_C_TRVL as L0240",
    "tot_dollar_amt_12m_C_TRVL as L0241",
    "tot_dollar_disc_amt_12m_C_TRVL as L0242",
    "tot_trans_24m_C_TRVL as L0243",
    "tot_dollar_amt_24m_C_TRVL as L0244",
    "tot_dollar_disc_amt_24m_C_TRVL as L0245",
    "tot_trans_36m_C_TRVL as L0246",
    "tot_dollar_amt_36m_C_TRVL as L0247",
    "tot_dollar_disc_amt_36m_C_TRVL as L0248",
    "tot_trans_ever_C_TRVL as L0249",
    "tot_dollar_amt_ever_C_TRVL as L0250",
    "tot_dollar_disc_amt_ever_C_TRVL as L0251",
    "number_of_products_36m as L0252",
    "number_of_products_ever as L0253",
    "tot_trans_13to24m_DS_LS as L0254",
    "tot_da_13to24m_DS_LS as L0255",
    "tot_dda_13to24m_DS_LS as L0256",
    "T2Y_tot_trans_13to24m_DS_LS as L0257",
    "T2Y_tot_da_13to24m_DS_LS as L0258",
    "T2Y_tot_dda_13to24m_DS_LS as L0259",
    "T2YR_tot_trans_13to24m_DS_LS as L0260",
    "T2YR_tot_da_13to24m_DS_LS as L0261",
    "T2YR_tot_dda_13to24m_DS_LS as L0262",
    "R_tot_trans_12m_DS_LS as L0263",
    "R_tot_d_a_12m_DS_LS as L0264",
    "R_tot_trans_36m_DS_LS as L0265",
    "R_tot_d_a_36m_DS_LS as L0266",
    "tot_trans_13to24m_DS_SUBS as L0267",
    "T2Y_tot_trans_13to24m_DS_SUBS as L0268",
    "T2YR_tot_trans_13to24m_DS_SUBS as L0269",
    "R_tot_trans_12m_DS_SUBS as L0270",
    "R_tot_trans_36m_DS_SUBS as L0271",
    "tot_trans_13to24m_OC_WEB as L0272",
    "tot_da_13to24m_OC_WEB as L0273",
    "tot_dda_13to24m_OC_WEB as L0274",
    "T2Y_tot_trans_13to24m_OC_WEB as L0275",
    "T2Y_tot_da_13to24m_OC_WEB as L0276",
    "T2Y_tot_dda_13to24m_OC_WEB as L0277",
    "T2YR_tot_trans_13to24m_OC_WEB as L0278",
    "T2YR_tot_da_13to24m_OC_WEB as L0279",
    "T2YR_tot_dda_13to24m_OC_WEB as L0280",
    "R_tot_trans_12m_OC_WEB as L0281",
    "R_tot_d_a_12m_OC_WEB as L0282",
    "R_tot_trans_36m_OC_WEB as L0283",
    "R_tot_d_a_36m_OC_WEB as L0284",
    "tot_trans_13to24m_OC_DM as L0285",
    "T2Y_tot_trans_13to24m_OC_DM as L0286",
    "T2YR_tot_trans_13to24m_OC_DM as L0287",
    "R_tot_trans_12m_OC_DM as L0288",
    "R_tot_trans_36m_OC_DM as L0289",
    "tot_trans_13to24m_OC_PHO as L0290",
    "tot_da_13to24m_OC_PHO as L0291",
    "tot_dda_13to24m_OC_PHO as L0292",
    "T2Y_tot_trans_13to24m_OC_PHO as L0293",
    "T2Y_tot_da_13to24m_OC_PHO as L0294",
    "T2Y_tot_dda_13to24m_OC_PHO as L0295",
    "T2YR_tot_trans_13to24m_OC_PHO as L0296",
    "T2YR_tot_da_13to24m_OC_PHO as L0297",
    "T2YR_tot_dda_13to24m_OC_PHO as L0298",
    "R_tot_trans_12m_OC_PHO as L0299",
    "R_tot_d_a_12m_OC_PHO as L0300",
    "R_tot_trans_36m_OC_PHO as L0301",
    "R_tot_d_a_36m_OC_PHO as L0302",
    "tot_trans_13to24m_OC_POS as L0303",
    "tot_da_13to24m_OC_POS as L0304",
    "tot_dda_13to24m_OC_POS as L0305",
    "T2Y_tot_trans_13to24m_OC_POS as L0306",
    "T2Y_tot_da_13to24m_OC_POS as L0307",
    "T2Y_tot_dda_13to24m_OC_POS as L0308",
    "T2YR_tot_trans_13to24m_OC_POS as L0309",
    "T2YR_tot_da_13to24m_OC_POS as L0310",
    "T2YR_tot_dda_13to24m_OC_POS as L0311",
    "R_tot_trans_12m_OC_POS as L0312",
    "R_tot_d_a_12m_OC_POS as L0313",
    "R_tot_trans_36m_OC_POS as L0314",
    "R_tot_d_a_36m_OC_POS as L0315",
    "tot_trans_13to24m_C_DISC as L0316",
    "tot_da_13to24m_C_DISC as L0317",
    "tot_dda_13to24m_C_DISC as L0318",
    "T2Y_tot_trans_13to24m_C_DISC as L0319",
    "T2Y_tot_da_13to24m_C_DISC as L0320",
    "T2Y_tot_dda_13to24m_C_DISC as L0321",
    "T2YR_tot_trans_13to24m_C_DISC as L0322",
    "T2YR_tot_da_13to24m_C_DISC as L0323",
    "T2YR_tot_dda_13to24m_C_DISC as L0324",
    "R_tot_trans_12m_C_DISC as L0325",
    "R_tot_d_a_12m_C_DISC as L0326",
    "R_tot_trans_36m_C_DISC as L0327",
    "R_tot_d_a_36m_C_DISC as L0328",
    "tot_trans_13to24m_C_FIN as L0329",
    "T2Y_tot_trans_13to24m_C_FIN as L0330",
    "T2YR_tot_trans_13to24m_C_FIN as L0331",
    "R_tot_trans_12m_C_FIN as L0332",
    "R_tot_trans_36m_C_FIN as L0333",
    "tot_trans_13to24m_C_HLTH as L0334",
    "tot_da_13to24m_C_HLTH as L0335",
    "tot_dda_13to24m_C_HLTH as L0336",
    "T2Y_tot_trans_13to24m_C_HLTH as L0337",
    "T2Y_tot_da_13to24m_C_HLTH as L0338",
    "T2Y_tot_dda_13to24m_C_HLTH as L0339",
    "T2YR_tot_trans_13to24m_C_HLTH as L0340",
    "T2YR_tot_da_13to24m_C_HLTH as L0341",
    "T2YR_tot_dda_13to24m_C_HLTH as L0342",
    "R_tot_trans_12m_C_HLTH as L0343",
    "R_tot_d_a_12m_C_HLTH as L0344",
    "R_tot_trans_36m_C_HLTH as L0345",
    "R_tot_d_a_36m_C_HLTH as L0346",
    "tot_trans_13to24m_C_TRVL as L0347",
    "tot_da_13to24m_C_TRVL as L0348",
    "tot_dda_13to24m_C_TRVL as L0349",
    "T2Y_tot_trans_13to24m_C_TRVL as L0350",
    "T2Y_tot_da_13to24m_C_TRVL as L0351",
    "T2Y_tot_dda_13to24m_C_TRVL as L0352",
    "T2YR_tot_trans_13to24m_C_TRVL as L0353",
    "T2YR_tot_da_13to24m_C_TRVL as L0354",
    "T2YR_tot_dda_13to24m_C_TRVL as L0355",
    "R_tot_trans_12m_C_TRVL as L0356",
    "R_tot_d_a_12m_C_TRVL as L0357",
    "R_tot_trans_36m_C_TRVL as L0358",
    "R_tot_d_a_36m_C_TRVL as L0359",
    "Sum_Prod_Disc_201712_Ever",
    "Sum_Prod_Disc_201712_36M",
    "Sum_Prod_Fin_201712_Ever",
    "Sum_Prod_Fin_201712_36M",
    "Sum_Prod_Trav_201712_Ever",
    "Sum_Prod_Trav_201712_36M"
)

# Final Merge
df_final_dp = df_mms_m.join(df_mms_m_dp1, "CHID", "left") \
    .join(df_mms_m_dp2, "CHID", "left") \
    .join(df_mms_m_dp3, "CHID", "left") \
    .join(df_mms_m_dp3b, "CHID", "left") \
    .join(df_mms_m_dp4, "CHID", "left") \
    .join(df_mms_m_dp5, "CHID", "left") \
    .join(df_mms_m_dp6, "CHID", "left") \
    .join(df_mms_m_dp7, "CHID", "left") \
    .join(df_mms_m_dp8, "CHID", "left") \
    .join(df_mms_m_dp9, "CHID", "left") \
    .join(df_mms_m_dp10, "CHID", "left") \
    .join(df_mms_m_dp10b, "CHID", "left") \
    .join(df_mms_m_dp11, "CHID", "left") \
    .join(df_mms_m_dp12, "CHID", "left")

df_final_dp.write.format("delta").mode("overwrite").save(f"{target_path}/{file_name}_DP")

# Model input variable lists
List_MBU_snapshot = [
    'mid_key', 'flag_mbu_click_12m_ever', 'flag_mbu_open_12m_ever', 'count_mbu_click_1m', 'count_mbu_click_2m', 'count_mbu_click_3m', 'count_mbu_click_6m', 'count_mbu_click_9m', 'count_mbu_click_12m', 'count_mbu_open_1m', 'count_mbu_open_2m', 'count_mbu_open_3m', 'count_mbu_open_6m', 'count_mbu_open_9m', 'count_mbu_open_12m', 'flag_mbu_open_6m_2', 'flag_mbu_click_12m_2'
]
List_FM_ORG_VAR = [
    'Flag_ORG_EXT_OBN_098_12', 'Flag_ORG_EXT_OBN_099_12', 'Flag_ORG_EXT_REF_008_12'
]
List_RX_Clone_201708 = [
    'B0007', 'E0033', 'G0001', 'I0397', 'J0012', 'J0014', 'J0413', 'K0360', 'K0426', 'K0429', 'K0501', 'A0122', 'A0134', 'B0011', 'B0017', 'B0082', 'C00012', 'C00016', 'C00049', 'E0030', 'E0038', 'G0006', 'J0352', 'J0404', 'D0022', 'J0091', 'L0307'
]
List_ABG_EM_201904 = [
    'A0088', 'C10023', 'C10024', 'E0039', 'G0018', 'H0307', 'J0077', 'J0139', 'J0449', 'K0041', 'K0465', 'K1059', 'K1070', 'L0125', 'L0243'
]
List_ABG_Clone_201904 = [
    'K0071', 'D0026', 'D0038', 'D0024', 'D0030', 'J0458', 'D0029', 'J0013', 'J0332', 'G0018', 'E0028', 'J0356'
]
List_CCI_EM_202002 = [
    'D0042', 'D0031', 'E0019', 'K0363', 'E0034', 'A0110', 'C00016', 'L0321', 'L0243', 'L0304', 'count_mbu_click_9m', 'count_mbu_open_1m'
]
List_FMMH_EM_202011 = [
    'C00035', 'E0026', 'E0039', 'I0301', 'J0089', 'J0109', 'K0499', 'STATE', 'FM_COUNTER_OPEN_PREV', 'FLAG_MBU_CLICK_12M_EVER', 'Flag_ORG_EXT_OBN_098_12', 'Flag_ORG_EXT_OBN_099_12', 'Flag_ORG_EXT_REF_008_12'
]
List_FMMC_EM_202011 = [
    'C00035', 'D0021', 'E0034', 'J0376', 'J0404', 'J0451', 'J1096', 'K0510', 'State', 'FM_flag_open_prev', 'flag_mbu_click_12m_ever', 'Flag_ORG_EXT_OBN_098_12', 'Flag_ORG_EXT_OBN_099_12', 'Flag_ORG_EXT_REF_008_12'
]
List_spsf_em_202003 = [
    'B0007', 'B0033', 'B0036', 'E0016', 'H0157', 'H0259', 'H0333', 'J0336', 'J1064', 'K0041', 'K0191', 'K0447', 'L0021', 'L0201', 'count_mbu_click_3m', 'count_mbu_open_1m', 'flag_mbu_click_12m_ever'
]
List_Hear_Clone_202004 = [
    'B0007', 'J0132', 'K0023', 'E0035', 'B0083', 'J1048', 'D0008', 'J0127', 'K0058', 'J1048', 'distancemiles', 'count_center', 'K0178', 'K0240', 'L0151', 'J0250'
]
List_HartFord_EM_201910 = [
    'J0326', 'J0520', 'J1118', 'C00048', 'C10002', 'D0026', 'D0048', 'E0034', 'E0038', 'J0479', 'J1080', 'K0017', 'K0050', 'HF_counter_open_prev', 'flag_mbu_click_12m_ever'
]
List_Coll_Resp_202002 = [
    'A0060', 'A0061', 'A0110', 'A0116', 'B0081', 'D0016', 'H0149', 'H0254', 'H0302', 'J0080', 'J0429', 'J1027', 'K0038', 'K0302', 'K0322'
]
List_Coll_Clone_202002 = [
    'A0057', 'B0081', 'D0014', 'E0029', 'E0040', 'J0469', 'J1101', 'K0187', 'K0473', 'K0480', 'K0493', 'L0185'
]
List_Coll_EM_201911 = [
    'J0520', 'B0083', 'D0004', 'D0011', 'J0013', 'J0354', 'J0500', 'J1003', 'J1033', 'K0062', 'K0354', 'K0493', 'K0504', 'K0521', 'coll_counter_open_prev', 'flag_mbu_click_12m_ever'
]
List_Exp_EM_201811 = [
    'A0108', 'E0038', 'J0333', 'J0405', 'K0041', 'K0126', 'K0234', 'K0506', 'count_mbu_click_3m'
]
List_Exp_Car_Clone_201811 = [
    'A0122', 'B0007', 'D0038', 'G0015', 'H0307', 'J0458', 'K0326', 'A0088', 'C00049', 'count_mbu_click_3m'
]
List_Exp_Hotel_Clone_201811 = [
    'B0007', 'D0038', 'G0014', 'G0015', 'G0016', 'L0122', 'L0129', 'L0165', 'count_mbu_click_3m'
]
List_Exp_Clone_201706 = [
    'A0080', 'A0092', 'A0101', 'B0018', 'B0031', 'B0073', 'B0090', 'C00034', 'D0029', 'D0038', 'E0038', 'G0015', 'G0018', 'H0200', 'H0222', 'J0054', 'J0181', 'L0252'
]
List_Exp_Val_201706 = [
    'C10021', 'G0014', 'G0015', 'G0018', 'I0213', 'J0387', 'J0401', 'J0422', 'J0448'
]
List_Dsct_Trvl_Web_Clone_201805 = [
    'A0133', 'B0007', 'B0073', 'count_mbu_click_3m', 'D0038', 'E0029', 'E0032', 'E0038', 'G0015', 'L0164'
]
List_Dsct_Shop_Clone_201805 = [
    'B0072', 'B0082', 'count_mbu_click_3m', 'D0021', 'D0038', 'E0030', 'E0038', 'G0004', 'H0329', 'J0405', 'L0257'
]
List_Dsct_Retail_Clone_201805 = [
    'B0007', 'B0073', 'B0082', 'count_mbu_click_3m', 'D0020', 'D0026', 'D0038', 'E0038', 'G0005', 'J0402', 'J0405'
]
List_Dsct_Ent_Clone_201805 = [
    'B0007', 'B0073', 'count_mbu_click_3m', 'D0024', 'D0029', 'D0038', 'E0038', 'E0039', 'G0015', 'J0435'
]
List_Dsct_EM_201803 = [
    'B0072', 'B0082', 'count_mbu_click_3m', 'D0021', 'D0038', 'E0030', 'E0038', 'G0004', 'H0329', 'J0405', 'L0257', 'J0445', 'K0180', 'L0144'
]
List_GET_Resp_201907 = [
    'J0289', 'B0007', 'D0021', 'D0042', 'E0030', 'H0170', 'J0080', 'J0520', 'K0442', 'K0497', 'L0253'
]
List_GET_Clone_201907 = [
    'A0025', 'A0103', 'A0124', 'D0014', 'D0021', 'E0028', 'G0015', 'J0013', 'L0161'
]
List_GET_EM_201912 = [
    'J0520', 'K0071', 'B0083', 'C00035', 'C10002', 'D0017', 'E0035', 'E0038', 'J0002', 'K0063', 'K0532', 'GET_counter_open_prev'
]
List_GET_Tree_201907 = [
    'G0018', 'A0124', 'J0289', 'L0250', 'A0104'
]
List_NYL_EM_201910 = [
    'J0326', 'B0007', 'B0016', 'B0017', 'B0083', 'D0042', 'D0045', 'E0039', 'FLAG_MBU_CLICK_12M_EVER', 'H0136', 'J0409', 'J0487', 'J1120'
]
List_GP_Old_EM_201805 = [
    'A0060', 'A0122', 'B0032', 'B0047', 'B0067', 'count_mbu_click_3m', 'GP_COUNTER_CLICK_PREV', 'E0030', 'E0037', 'E0039', 'GP_FLAG_OPEN_PREV', 'H0136', 'I0011', 'K0041'
]
List_GP_Young_EM_201805 = [
    'A0057', 'C00035', 'count_mbu_click_3m', 'E0039', 'J0002', 'J0127', 'K0041', 'K0357', 'K0401', 'K0496', 'gp_counter_click_prev', 'gp_counter_open_prev'
]
List_GP_Web_201806 = [
    'B0073', 'count_mbu_click_3m', 'D0018', 'D0038', 'E0029', 'E0031', 'E0038', 'J0402', 'J0435', 'L0199', 'L0257'
]
List_Marcus_EM_202009 = [
    'A0099', 'B0083', 'C00035', 'D0012', 'D0043', 'E0034', 'E0038', 'J0013', 'J1076', 'K0397', 'K0488', 'COUNT_MBU_CLICK_9M'
]
List_UHC_AEP_202009 = [
    'A0138', 'B0073', 'B0083', 'C00035', 'D0011', 'D0014', 'D0016', 'D0018', 'D0024', 'D0029', 'D0032', 'D0034', 'D0038', 'D0044', 'D0049', 'E0030', 'E0031', 'E0034', 'I0085', 'I0378', 'I0391', 'J0013', 'J0050', 'J0051', 'J0363', 'J0450', 'J0525', 'J1025', 'J1033', 'J1036', 'J1040', 'J1043', 'J1049', 'J1094', 'J1100', 'J1118', 'K0468', 'K0475', 'K0488', 'K0491', 'COUNT_MBU_CLICK_1M', 'COUNT_MBU_OPEN_12M'
]
List_NYL_LTC_Appt_202007 = [
    'E0033', 'b0007', 'B0017', 'B0081', 'K0071', 'M0005', 'D0003', 'D0006', 'D0029', 'J0178', 'J0210', 'J0336', 'J0342'
]
List_NYL_LTC_EM_202006 = [
    'D0006', 'D0046', 'B0083', 'B0031', 'D0014', 'D0045', 'B0007', 'D0050', 'H0240', 'A0061', 'LTC_Click_Flag', 'count_mbu_click_1m', 'flag_mbu_click_6m', 'state'
]
List_ATT_EM_202008 = [
    'D0020', 'H0307', 'I0391', 'J0342', 'J1237', 'K0040', 'B0007', 'count_mbu_click_12m', 'count_mbu_click_1m', 'count_mbu_click_9m'
]
List_ATT_Exit_202008 = [
    'B0007', 'C10002', 'J1084', 'D0046', 'J0411', 'K1050', 'D0030', 'D0038'
]
List_Exxon_EM_202008 = [
    'count_mbu_open_3m', 'D0038', 'D0043', 'E0037', 'E0039', 'H0186', 'H0208', 'J0156', 'J0454', 'B0007', 'J0342'
]
List_Cartus_EM_202007 = [
    'K0071', 'B0083', 'D0040', 'J0479', 'J0524', 'J1003', 'J1017', 'N0005', 'COUNT_MBU_CLICK_12M', 'COUNT_MBU_CLICK_2M', 'Cartus_FLAG_OPEN_PREV'
]
List_AS_EM_201701 = [
    'A0097', 'D0002', 'D0012', 'D0039', 'E0033', 'E0039', 'J0015', 'J0346', 'J0403', 'K0071', 'K0087', 'as_counter_click', 'as_flag_open'
]
List_Exxon_Clone_202010 = [
    'A0102', 'B0007', 'B0073', 'C10024', 'E0037', 'G0015', 'J0354', 'J0451', 'J0458', 'J1044'
]
List_Exxon_Prem_Clone_202010 = [
    'A0102', 'B0007', 'B0031', 'C10024', 'D0029', 'D0042', 'E0037', 'J0011', 'J0403', 'K0489'
]
List_RiskIQ_201708 = [
    'J0016', 'J0095', 'J0141', 'J0151', 'J0279', 'J0288', 'J0328', 'L0012'
]
List_AS_Road_Clone_202002 = [
    'A0077', 'B0055', 'E0003', 'E0013', 'E0022', 'E0023', 'F0014', 'J0284', 'K0001', 'K0466', 'L0009', 'L0171', 'L0218', 'L0249'
]
List_GET_NPL_CLONE_201608 = [
    'B0046', 'J0048', 'C00006', 'J0003', 'H0153', 'A0031', 'A0032', 'B0064', 'J0115', 'C00019', 'A0051', 'A0087', 'A0078', 'K0408', 'I0200', 'K0477', 'I0390', 'A0018', 'A0065', 'H0150', 'H0188', 'H0206', 'H0213', 'I0163', 'J0101', 'J0447', 'L0247', 'L0324'
]
List_NYL_DM_Eval_201912 = [
    'N0040', 'J1038', 'B0021', 'D0041', 'L0170', 'N0021', 'J0483', 'A0090', 'K0524', 'A0029', 'K1003', 'K0516', 'J0334', 'J1190', 'F0012', 'L0036', 'A0058', 'N0035', 'K1100', 'B0062', 'J0138', 'I0087', 'J0133', 'K0312', 'J1065', 'A0053', 'I0338', 'K0331', 'L0254', 'A0039', 'J1232', 'H0173'
]
List_Chase_Seg_Model_201806 = [
    'A0017', 'J0151', 'J0075', 'H0243', 'A0073', 'J0181', 'J0061', 'J0242', 'H0176', 'H0183', 'J0293', 'J0323', 'J0004', 'A0064', 'B0018'
]
List_FMMH_Clone_202010 = [
    'J0331', 'A0051', 'E0026', 'I0229', 'I0236', 'J0095', 'J0140', 'J0353', 'K0367', 'K0498', 'L0006', 'L0129', 'L0150', 'L0184'
]
List_FMMC_Clone_202010 = [
    'B0083', 'J0009', 'C00008', 'D0011', 'D0034', 'E0037', 'H0163', 'H0223', 'J0012', 'J0013', 'J0313', 'J0403', 'J0419', 'K0032', 'K0502', 'L0253'
]
List_Medjet_Clone_201802 = [
    'A0036', 'B0007', 'C10011', 'D0014', 'E0029', 'H0307', 'J0471', 'K0491', 'A0092'
]
List_Medjet_EM_201906 = [
    'Mdj_clicked', 'K0014', 'H0283', 'J0335', 'J0356', 'L0185', 'L0296', 'L0107', 'E0001', 'K0351'
]
List_AAA_Clone_201903 = [
    'K0041', 'K0043', 'K0046', 'K0097', 'K0157', 'K0204', 'K0372', 'K0442', 'K1030', 'K1089', 'K0204'
]
List_Allstate_Premium_202003 = [
    'B0064', 'B0007', 'B0048', 'E0027', 'J0011', 'J0336', 'J0520', 'J1119', 'J1120', 'K0071', 'K0476', 'K0519', 'N0043'
]
List_CRB_Exit_Clone_202009 = [
    'B0007', 'B0082', 'D0038', 'G0015', 'I0057', 'J0342', 'J1076', 'J1077', 'J1119', 'N0032'
]
List_GET_CPL_Resp_201708 = [
    'A0031', 'A0041', 'A0060', 'A0108', 'G0017', 'I0009', 'I0031', 'J0424', 'K0044', 'L0251', 'L0253'
]
List_GET_CPL_Clone_201708 = [
    'A0105', 'A0124', 'B0007', 'B0081', 'E0028', 'G0013', 'J0427', 'K0439', 'K0466', 'K0491', 'L0250'
]
List_AAA_MRI_Clone_202011 = [
    'J0483', 'K0465', 'A0124', 'B0007', 'D0013', 'E0029', 'K0089', 'K0321', 'K0498', 'J1120', 'K0044'
]
List_ATT_MRI_Clone_202011 = [
    'J0425', 'A0041', 'E0029', 'I0402', 'J0013', 'J0448', 'K0009', 'K0052'
]
List_Chase_DM_Resp_201806 = [
    'A0128', 'J0179', 'K0353', 'L0221', 'L0325'
]
List_AS_Combo_202012 = [
    'A0073', 'B0015', 'B0031', 'B0060', 'B0067', 'B0081', 'B0090', 'C00016', 'C00019', 'C00034', 'C00048', 'D0001', 'D0010', 'D0011', 'D0021', 'D0026', 'D0034', 'D0050', 'E0003', 'E0013', 'E0022', 'E0026', 'E0034', 'F0007', 'G0018', 'H0202', 'H0331', 'I0186', 'I0291', 'J0132', 'J0153', 'J0336', 'J0497', 'J1005', 'K0123', 'K0285', 'K0295', 'K0482', 'L0009', 'L0149', 'L0171', 'L0184', 'L0246', 'L0252', 'L0253', 'M0006'
]
List_Norton_ID_Clone_202012 = [
    'E0035', 'D0024', 'B0017', 'E0038', 'J0405', 'A0060', 'E0019', 'L0084', 'L0125'
]
List_EyeMed_Exit_202012 = [
    'B0073', 'B0083', 'C10024', 'D0011', 'E0034', 'J0013', 'J1053', 'J1120', 'K0502', 'N0025'
]
List_Wyndham_Exit_202101 = [
    'B0007', 'B0082', 'D0006', 'D0042', 'E0028', 'E0035', 'E0036', 'G0018', 'J1044', 'J1111'
]
List_Wyndham_Clone_202101 = [
    'A0092', 'D0001', 'D0014', 'D0038', 'I0106', 'J0015', 'J0382', 'J0447', 'K0101', 'K0227'
]
List_CRB_Clone_202101 = [
    'A0092', 'B0007', 'E0029', 'J0015', 'J0328', 'J0479', 'J1036', 'K0035'
]
List_Outback_Clone_202101 = [
    'A0104', 'I0049', 'I0366', 'I0252', 'J0326', 'J0416', 'J0440', 'K0544', 'K1051', 'K0473'
]
List_Outback_Exit_202101 = [
    'C10024', 'D0003', 'D0038', 'D0041', 'E0029', 'E0031', 'E0037', 'J0416', 'N0028'
]
List_Bonefish_Exit_202101 = [
    'B0007', 'B0073', 'C10024', 'D0014', 'E0035', 'E0039', 'J0483', 'J1032', 'J0520', 'J1118', 'K0066', 'K0071', 'K0468'
]
List_Bonefish_Clone_202101 = [
    'D0014', 'F0008', 'J0433', 'J0434', 'J1118', 'K0102', 'N0013', 'N0031'
]
List_Norton_ITS_EM_202102 = [
    'C10024', 'D0026', 'E0034', 'E0038', 'J0342', 'J0451', 'J1016', 'J1038', 'J1121'
]
List_Norton_Exit_202101 = [
    'D0014', 'B0007', 'D0011', 'E0028', 'E0034', 'J0433', 'J0441', 'J0451', 'J1016'
]
List_HIG_Auto_DigitalPref_202102 = [
    'B0073', 'E0015', 'E0034', 'D0030', 'J0132', 'C00016', 'B0082', 'K0050', 'L0218', 'B0055', 'I0303', 'A0002', 'B0032', 'K0135', 'B0072', 'J0002', 'A0093', 'I0354'
]
List_Valv_Clone_202102 = [
    'E0033', 'B0080', 'D0038', 'D0049', 'J0013', 'J0404', 'K0442', 'L0044', 'L0252'
]
List_BW_Clone_202102 = [
    'D0038', 'B0007', 'D0001', 'D0029', 'D0043', 'J0011', 'J0330', 'J0419', 'J1102', 'M0007'
]
List_BW_EM_202102 = [
    'C10024', 'C00035', 'C10023', 'D0026', 'D0043', 'E0034', 'E0036', 'J0342', 'J1233'
]
List_UHC_Exit_202105 = [
    'B0007', 'B0016', 'B0021', 'C10024', 'C00035', 'C00048', 'D0029', 'D0045', 'E0035', 'E0035', 'E0038', 'J1120', 'J0342'
]
List_UHC_MRI_202105 = [
    'A0104', 'B0016', 'B0031', 'F0008', 'J0472', 'J1049', 'A0102', 'B0007'
]
List_Pet_MRI_Clone_202105 = [
    'D0010', 'D0022', 'D0029', 'J0403', 'J0479', 'J0483', 'J1120', 'K0063', 'K0321', 'K0521'
]
List_Pet_Exit_Clone_202105 = [
    'J0520', 'B0016', 'B0021', 'B0083', 'C10001', 'D0042', 'E0035', 'E0038', 'G0004', 'G0018', 'J0011', 'J1098', 'J1120', 'J1121', 'N0007'
]
List_NYL_Digital_Clone_202104 = [
    'C00035', 'D0013', 'D0024', 'D0032', 'D0038', 'D0048', 'J0342', 'J1156', 'M0057'
]
List_Norton_HD_Clone_202107 = [
    'E0034', 'B0035', 'B0073', 'B0090', 'K0044', 'A0041', 'J0096', 'J0355', 'L0254', 'L0159'
]

part_a_vars = list(set(
    ['chid', 'mid_key', 'zip', 'dma_cd'] + 
    List_spsf_em_202003 + List_FMMC_EM_202011 + List_FMMH_EM_202011 + List_Hear_Clone_202004 + List_HartFord_EM_201910 + 
    List_Coll_Clone_202002 + List_Coll_Resp_202002 + List_Coll_EM_201911 + List_Exp_EM_201811 + 
    List_Exp_Car_Clone_201811 + List_Exp_Hotel_Clone_201811 + List_Exp_Clone_201706 + List_Exp_Val_201706 + 
    List_Dsct_Trvl_Web_Clone_201805 + List_Dsct_Ent_Clone_201805 + List_Dsct_Shop_Clone_201805 + 
    List_Dsct_Retail_Clone_201805 + List_Dsct_EM_201803 + List_GET_Resp_201907 + List_GET_Clone_201907 + 
    List_GET_EM_201912 + List_GET_Tree_201907 + List_NYL_EM_201910 + List_GP_Old_EM_201805 + 
    List_GP_Young_EM_201805 + List_GP_Web_201806 + List_CCI_EM_202002 + List_ABG_Clone_201904 + 
    List_ABG_EM_201904 + List_RX_Clone_201708 + List_Marcus_EM_202009 + List_UHC_AEP_202009 + 
    List_NYL_LTC_Appt_202007 + List_NYL_LTC_EM_202006 + List_ATT_Exit_202008 + List_ATT_EM_202008 + 
    List_Exxon_EM_202008 + List_Cartus_EM_202007 + List_AS_EM_201701 + List_Exxon_Clone_202010 + 
    List_Exxon_Prem_Clone_202010 + List_RiskIQ_201708 + List_AS_Road_Clone_202002 + List_GET_NPL_CLONE_201608 + 
    List_NYL_DM_Eval_201912 + List_Chase_Seg_Model_201806 + List_FMMH_Clone_202010 + List_FMMC_Clone_202010 + 
    List_Medjet_Clone_201802 + List_Medjet_EM_201906 + List_AAA_Clone_201903 + List_Allstate_Premium_202003 + 
    List_CRB_Exit_Clone_202009 + List_GET_CPL_Resp_201708 + List_GET_CPL_Clone_201708 + List_AAA_MRI_Clone_202011 + 
    List_ATT_MRI_Clone_202011 + List_Chase_DM_Resp_201806 + List_AS_Combo_202012 + List_Norton_ID_Clone_202012 + 
    List_EyeMed_Exit_202012 + List_Wyndham_Exit_202101 + List_Wyndham_Clone_202101 + List_Bonefish_Clone_202101 + 
    List_Bonefish_Exit_202101 + List_Outback_Exit_202101 + List_Outback_Clone_202101 + List_CRB_Clone_202101 + 
    List_Norton_Exit_202101 + List_Norton_ITS_EM_202102 + List_HIG_Auto_DigitalPref_202102 + 
    List_Valv_Clone_202102 + List_BW_Clone_202102 + List_BW_EM_202102 + List_UHC_Exit_202105 + 
    List_UHC_MRI_202105 + List_Pet_MRI_Clone_202105 + List_Pet_Exit_Clone_202105 + List_NYL_Digital_Clone_202104 + 
    List_Norton_HD_Clone_202107
))
df_prov_model_uni_a = df_final_dp.select([c for c in part_a_vars if c in df_final_dp.columns])
df_prov_model_uni_a.write.format("delta").mode("overwrite").saveAsTable(f"target.Prov_Model_uni_A_m{version}")


# Part B Model Variables
List_Wal_Clone_201906 = [
    'mid_key', 'zip', 'zip4', 'dma_cd', 'B0007', 'B0011', 'B0073', 'D0030', 'D0033', 'G0005', 'J0328', 'J0403', 'J0422', 'K0071', 'L0130', 'C00016', 'D0044', 'E0035', 'E0038', 'H0312', 'I0378', 'J0470', 'K0071', 'L0021'
]
List_Cartus_Clone_202005 = [
    'E0038', 'D0013', 'D0023', 'D0024', 'D0049', 'D0050', 'E0038', 'J0013', 'J1120', 'K0101', 'N0004'
]
List_Wal_EM_201906 = [
    'B0011', 'B0073', 'C00016', 'D0044', 'E0035', 'E0038', 'H0312', 'I0378', 'J0470', 'K0071', 'L0021'
]
List_Delta_EM_201906 = [
    'A0104', 'B0007', 'B0082', 'C00035', 'E0032', 'E0037', 'E0038', 'J0139', 'J0410', 'J0500', 'K0480'
]
List_CCI_Clone_201907 = [
    'B0067', 'B0082', 'A0093', 'B0018', 'B0017', 'J0340', 'B0031', 'B0029', 'L0253', 'L0172', 'D0011', 'H0192', 'A0036', 'J0141', 'L0316', 'B0073', 'A0087', 'H0168', 'L0037', 'I0352'
]
List_CCI_Resp_201907 = [
    'B0018', 'L0253', 'A0048', 'B0067', 'A0076', 'K0460', 'A0102', 'E0019', 'A0087', 'H0226', 'A0062', 'K0057', 'E0013', 'C00020', 'C00012', 'B0031', 'J0405', 'B0063', 'I0170', 'K0291', 'L0337'
]
List_NYL_LTC_Clone_202005 = [
    'A0056', 'B0072', 'K0442', 'H0187', 'H0168', 'B0017', 'J0169', 'K0440', 'B0067', 'A0061', 'B0055', 'H0314', 'B0082', 'E0041', 'I0189'
]
List_RX_HSA_Clone_202007 = [
    'D0032', 'B0035', 'D0024', 'D0038', 'D0044', 'E0037', 'E0039', 'J0013', 'J0460'
]
List_ExxMobil_Clone_202008 = [
    'B0007', 'C00034', 'D0037', 'D0048', 'E0038', 'J0011', 'J1014', 'J1038', 'J1112', 'J1117'
]
List_SPSF_Clone_202006 = [
    'A0115', 'B0073', 'C00034', 'D0003', 'E0038', 'H0139', 'H0235', 'K0040', 'E0034', 'G0018', 'B0064', 'E0001', 'J0414', 'D0011', 'K0315'
]
List_VBR_Clone_201710 = [
    'L0349', 'L0294', 'D0014', 'K0481', 'L0138', 'L0128', 'G0015', 'B0046', 'E0029', 'B0031', 'C00034', 'A0018', 'B0017', 'K0196', 'H0329', 'I0371', 'J0445', 'L0185'
]
List_FMMH_Clone_201905 = [
    'I0206', 'I0236', 'J0140', 'J0331', 'J1103', 'K0063', 'K0323', 'K0501', 'K0539', 'K0574', 'K0571', 'K0577', 'K1092'
]
List_FMMC_Clone_201905 = [
    'A0019', 'B0008', 'B0081', 'D0029', 'J0415', 'K0063', 'K0289', 'L0006', 'L0126'
]
List_Hilton_Clone_201904 = [
    'A0036', 'A0092', 'B0007', 'D0046', 'E0029', 'H0328', 'J0425', 'J1102', 'K0466', 'L0151', 'L0027', 'L0030', 'L0033'
]
List_Xanterra_Clone_201910 = [
    'B0007', 'B0031', 'B0083', 'D0011', 'D0014', 'J0336', 'J0357', 'J0483', 'K0042', 'K0302', 'K0498', 'K1030', 'N0003'
]
List_NYL_Clone_201905 = [
    'B0007', 'B0090', 'C00019', 'D0015', 'E0040', 'J0331', 'J0468', 'K0092', 'K0411', 'K0474'
]
List_ABG_Engager_201904 = [
    'L0099', 'L0108', 'L0102', 'L0111', 'L0105', 'L0114', 'L0117', 'L0120', 'L0123', 'L0126', 'L0129', 'D0024', 'D0026', 'D0029', 'D0030', 'D0038', 'E0028', 'G0018', 'J0013', 'J0332', 'J0356', 'J0458', 'K0071'
]
List_Tang_Outlet_Clone_202003 = [
    'B0007', 'A0104', 'D0029', 'D0042', 'D0048', 'G0015', 'J0357', 'J0446', 'K0046', 'K0097', 'K0472'
]
List_FL_1800_Clone_202003 = [
    'B0007', 'A0081', 'C10012', 'D0045', 'E0029', 'J1018', 'J1052', 'K1042', 'N0028'
]
List_CCI_Engager_201908 = [
    'L0035', 'L0036', 'L0037', 'L0038', 'L0117', 'L0120', 'L0123', 'L0126', 'L0129', 'A0092', 'B0007', 'B0083', 'C00049', 'D0022', 'J0364', 'J0451', 'J1043', 'K0502', 'L0221', 'K0502'
]
List_Dennys_Clone_201906 = [
    'L0039', 'L0042', 'L0045', 'L0117', 'L0120', 'L0123', 'L0126', 'L0129', 'B0007', 'C00034', 'D0029', 'I0118', 'I0151', 'I0189', 'I0290', 'I0402', 'J0336', 'J0342', 'J0426', 'K0041', 'K0046', 'K0048', 'K0495', 'N0016'
]
List_Hartford_Auto_Clone_201906 = [
    'B0082', 'C00019', 'D0022', 'E0027', 'J0485', 'K0041', 'K0043', 'K0045', 'K0050', 'K0352', 'K0579'
]
List_Hartford_Home_Clone_201906 = [
    'L0001', 'L0002', 'L0003', 'L0117', 'L0120', 'L0123', 'L0126', 'L0129', 'A0081', 'A0129', 'B0007', 'C00048', 'I0189', 'I0222', 'J0336', 'J0485', 'J1079', 'J1098', 'K0033', 'K0038', 'K0050', 'K0098', 'K0574'
]
List_HomeServ_Clone_202003 = [
    'B0016', 'A0032', 'A0033', 'A0036', 'A0082', 'A0096', 'B0016', 'D0005', 'D0006', 'D0041', 'E0002', 'J0354', 'J0379', 'J0379', 'L0253'
]
List_Chase_Clone_202003 = [
    'B0007', 'I0092', 'K0076', 'D0029', 'J0011', 'J0326', 'J0483', 'J1036', 'J1120', 'K0053', 'K0071', 'K0076', 'K0473', 'N0032'
]
List_Liberty_Trvl_Clone_202003 = [
    'B0007', 'I0092', 'K0076', 'D0029', 'J0011', 'J0326', 'J0483', 'J1036', 'J1120', 'K0053', 'K0071', 'K0076', 'K0473', 'N0032'
]
List_PRideFly_Clone_202003 = [
    'B0007', 'D0014', 'D0029', 'D0043', 'D0048', 'E0033', 'G0018', 'J0342', 'J1103'
]
List_Reg_Cinema_Clone_202003 = [
    'B0007', 'C00036', 'D0026', 'D0038', 'D0046', 'E0040', 'G0015', 'J0410', 'J0487', 'J1022', 'K0471', 'K0496'
]
List_Schwan_Clone_202003 = [
    'J1104', 'B0073', 'G0004', 'I0092', 'I0391', 'J0328', 'J0336', 'J1079', 'J1103', 'J1104'
]
List_UPS_Clone_202003 = [
    'D0042', 'D0043', 'D0046', 'E0029', 'G0018', 'I0057', 'I0066', 'J0002', 'J0012', 'J0013', 'J0328', 'J1117', 'L0131', 'L0135', 'L0252'
]
List_SupBuyer_Disc_202007 = [
    'Sum_Prod_Trav_201712_Ever', 'Sum_Prod_Trav_201712_36M', 'Sum_Prod_Disc_201712_36M', 'Sum_Prod_Disc_201712_Ever', 'L0120', 'L0123', 'L0126', 'L0129', 'B0007', 'D0029', 'D0038', 'D0048', 'I0189', 'J0141', 'J0328', 'J0421', 'J1120', 'L0129'
]
List_SupBuyer_Fin_202007 = [
    'B0090', 'D0004', 'D0038', 'D0045', 'E0040', 'J0013', 'J0331', 'J0356', 'J0426', 'J1044', 'J1118', 'K0334', 'K0492', 'K1052'
]
List_SupBuyer_Trvl_202007 = [
    'Sum_Prod_Trav_201712_Ever', 'Sum_Prod_Trav_201712_36M', 'L0120', 'L0123', 'L0126', 'L0129', 'B0007', 'D0029', 'G0018', 'J0008', 'J0328', 'J0336', 'J0342', 'J1017', 'J1032', 'J1037', 'J1118', 'K0071', 'K0468'
]
List_Delta63_Inq_201904 = [
    'B0007', 'State', 'B0007', 'D0043', 'C00019', 'D0001', 'D0043', 'E0040', 'H0201', 'H0323', 'J0013', 'K0308', 'K0425', 'K0512', 'L0151'
]
List_EyeMed63_Inq_201904 = [
    'B0007', 'A0133', 'B0015', 'B0081', 'D0001', 'D0025', 'E0002', 'I0238', 'I0262', 'I0271', 'J0427', 'K0047', 'K0494', 'K0510'
]
List_NYL63_Inq_201904 = [
    'B0007', 'A0069', 'B0058', 'B0072', 'B0072', 'C00019', 'E0002', 'E0033', 'H0168', 'H0201', 'J0013', 'J0094', 'J0127', 'J0127', 'J0451'
]
List_RX63_Inq_201904 = [
    'B0007', 'A0124', 'B0058', 'B0072', 'B0081', 'C00019', 'D0001', 'D0043', 'E0002', 'E0033', 'H0201', 'I0136', 'J0405', 'K0092', 'K0405', 'K0519', 'L0012'
]
List_Delta65_Inq_201904 = [
    'B0007', 'A0108', 'B0067', 'B0082', 'B0083', 'C00019', 'C10014', 'D0043', 'E0002', 'H0168', 'H0187', 'H0201', 'I0136', 'J0091', 'J0297', 'J0302', 'J0336', 'K0481'
]
List_EyeMed65_Inq_201904 = [
    'B0007', 'A0133', 'B0015', 'B0081', 'D0001', 'E0002', 'I0238', 'I0262', 'I0271', 'J0427', 'K0047', 'K0494', 'K0510'
]
List_NYL65_Inq_201904 = [
    'B0007', 'B0082', 'B0083', 'C00019', 'D0043', 'E0004', 'E0032', 'H0165', 'H0168', 'H0187', 'H0202', 'I0144', 'J0009', 'J0013', 'J0356', 'K0071'
]
List_RX65_Inq_201904 = [
    'B0007', 'A0088', 'A0122', 'B0052', 'B0072', 'B0082', 'B0083', 'D0043', 'E0002', 'E0032', 'E0038', 'H0187', 'H0201', 'J0323', 'K0358', 'K0363'
]
List_UHC65_INQ_201904 = [
    'B0007', 'C00019', 'D0001', 'D0043', 'E0033', 'E0039', 'I0401', 'J0011', 'J0405', 'K0435', 'L0124'
]
List_UHC65_SALE_201904 = [
    'B0007', 'A0104', 'B0081', 'D0011', 'D0025', 'E0030', 'E0039', 'J0476', 'K0041', 'K0048', 'K0483', 'L0151'
]
List_Delta65_Sale_201904 = [
    'B0007', 'A0060', 'A0108', 'A0116', 'I0003', 'I0174', 'J0132', 'J0373', 'J0378', 'J0386', 'J0405', 'J0405', 'K0001', 'K0005', 'K0034', 'K0056', 'K0501'
]
List_ATT_Clone_201602 = [
    'D0038', 'E0038', 'A0074', 'A0092', 'E0037', 'D0003', 'C00016', 'E0030', 'E0034', 'B0007'
]
List_Chase_NonEM_201606 = [
    'E0016', 'E0019', 'E0015', 'G0015', 'C10023', 'E0040', 'E0019', 'E0015', 'J0014', 'J0016', 'J0002', 'J0095', 'J0077', 'J0142', 'J0179', 'J0144', 'J0160', 'J0328', 'J0346'
]
List_Chase_EM_1st_201606 = [
    'C00034', 'J0179', 'E0016', 'E0019', 'E0015', 'G0015', 'J0142'
]
List_Chase_Risk_201606 = [
    'E0028', 'E0032', 'E0039', 'J0014', 'J0016', 'J0077', 'J0095', 'J0141', 'J0151', 'J0279', 'J0288', 'J0328', 'L0012'
]
List_Chase_Tree_201606 = [
    'C00034', 'G0015', 'E0029', 'J0142', 'J0095'
]
List_LiveWatch_Clone_201606 = [
    'C00012', 'K0047', 'C00006', 'A0080', 'D0027', 'J0453', 'K0500', 'A0078', 'J0378', 'B0073', 'J0003', 'J0008', 'K0064', 'A0031', 'L0151', 'H0274', 'B0032'
]
List_AS_BrandNewAARPMM_201806 = [
    'A0122', 'D0021', 'F0007'
]
List_AS_Former_201806 = [
    'B0007', 'B0067', 'B0081', 'E0003', 'E0022', 'E0036', 'K0466', 'K0474', 'TERM_YEAR'
]
List_AS_NewRenew_201806 = [
    'A0012', 'A0036', 'A0104', 'A0104', 'A0108', 'A0124', 'B0017', 'B0090', 'C00048', 'D0026', 'I0113', 'J0454', 'K0360', 'K0466'
]
List_AS_Engager_201806 = [
    'A0088', 'A0108', 'A0122', 'B0007', 'C00048', 'E0037', 'F0010', 'K0434', 'K0466', 'L0005'
]
List_AS_GoodCRD_201806 = [
    'B0082', 'D0029', 'D0033', 'E0001', 'E0026', 'H0268', 'I0071', 'I0379', 'J0095', 'J0405', 'J0453', 'K0045', 'K0097', 'K0317', 'L0220'
]
List_AS_Challenge_201806 = [
    'A0104', 'A0124', 'C00048', 'E0026', 'E0037', 'J0460', 'K0113', 'K0411', 'K0436', 'K0510', 'L0006'
]
List_AS_Conv_201903 = [
    'C00006', 'B0017', 'K0435', 'D0026', 'B0081', 'B0082', 'H0268', 'E0027', 'L0011', 'L0105'
]
List_Pet_resp_201605 = [
    'State', 'A0076', 'A0044', 'B0007', 'B0008', 'B0082', 'D0025', 'E0002', 'J0379', 'J0413', 'J0470', 'K0037'
]
List_Pet_dog_clone_201605 = [
    'State', 'A0025', 'H0205', 'A0023', 'A0011', 'A0124', 'B0011', 'D0021'
]
List_Pet_cat_clone_201605 = [
    'C10024', 'D0021', 'K0394', 'B0072', 'H0189', 'A0016', 'H0166', 'H0177', 'H0169', 'A0077'
]
List_Eyemed_Ins_201607 = [
    'State', 'A0122', 'B0083', 'B0081', 'B0066', 'B0015', 'B0031', 'C00034', 'C10012', 'D0018', 'D0023', 'F0010'
]
List_Eyemed_EM_201804 = [
    'B0007', 'B0082', 'C10023', 'C10024', 'C10024', 'C10024', 'G0005', 'I0144', 'I0144', 'J0405', 'K0401'
]
List_Delta_EM_201906 = [
    'A0104', 'B0007', 'B0082', 'C00035', 'E0032', 'E0037', 'E0038', 'J0139', 'J0410', 'J0500', 'K0480'
]
List_Delta_Engager_201906 = [
    'B0007', 'C00034', 'D0011', 'D0018', 'D0021', 'D0024', 'D0044', 'I0361', 'J0002', 'K0038', 'K0396'
]
List_Delta_MB_EM_201906 = [
    'C00035', 'D0029', 'E0035', 'E0038', 'J0002', 'J0408', 'J0420', 'K0040', 'K0092', 'Delta_COUNTER_OPEN_PREV'
]
List_AS_Segment_201806 = [
    'b0067', 'K0466', 'B0015'
]

part_b_vars = list(set(
    ['chid', 'mid_key'] + 
    List_Wal_Clone_201906 + List_Cartus_Clone_202005 + List_Wal_EM_201906 + List_Delta_EM_201906 + 
    List_CCI_Clone_201907 + List_CCI_Resp_201907 + List_NYL_LTC_Clone_202005 + List_RX_HSA_Clone_202007 + 
    List_ExxMobil_Clone_202008 + List_SPSF_Clone_202006 + List_VBR_Clone_201710 + List_FMMH_Clone_201905 + 
    List_FMMC_Clone_201905 + List_Hilton_Clone_201904 + List_Xanterra_Clone_201910 + List_NYL_Clone_201905 + 
    List_ABG_Engager_201904 + List_Tang_Outlet_Clone_202003 + List_FL_1800_Clone_202003 + 
    List_CCI_Engager_201908 + List_Dennys_Clone_201906 + List_Hartford_Auto_Clone_201906 + 
    List_Hartford_Home_Clone_201906 + List_HomeServ_Clone_202003 + List_Chase_Clone_202003 + 
    List_Liberty_Trvl_Clone_202003 + List_PRideFly_Clone_202003 + List_Reg_Cinema_Clone_202003 + 
    List_Schwan_Clone_202003 + List_UPS_Clone_202003 + List_SupBuyer_Disc_202007 + List_SupBuyer_Fin_202007 + 
    List_SupBuyer_Trvl_202007 + List_Delta63_Inq_201904 + List_EyeMed63_Inq_201904 + List_NYL63_Inq_201904 + 
    List_RX63_Inq_201904 + List_Delta65_Inq_201904 + List_EyeMed65_Inq_201904 + List_NYL65_Inq_201904 + 
    List_RX65_Inq_201904 + List_UHC65_INQ_201904 + List_UHC65_SALE_201904 + List_Delta65_Sale_201904 + 
    List_ATT_Clone_201602 + List_Chase_NonEM_201606 + List_Chase_EM_1st_201606 + List_Chase_Risk_201606 + 
    List_Chase_Tree_201606 + List_LiveWatch_Clone_201606 + List_AS_BrandNewAARPMM_201806 + List_AS_Former_201806 + 
    List_AS_NewRenew_201806 + List_AS_Engager_201806 + List_AS_GoodCRD_201806 + List_AS_Challenge_201806 + 
    List_AS_Conv_201903 + List_Pet_resp_201605 + List_Pet_dog_clone_201605 + List_Pet_cat_clone_201605 + 
    List_Eyemed_Ins_201607 + List_Eyemed_EM_201804 + List_Delta_EM_201906 + List_Delta_Engager_201906 + 
    List_Delta_MB_EM_201906 + List_AS_Segment_201806
))
df_prov_model_uni_b = df_final_dp.select([c for c in part_b_vars if c in df_final_dp.columns])
df_prov_model_uni_b.write.format("delta").mode("overwrite").saveAsTable(f"target.Prov_Model_uni_B_m{version}")
#End-DBShift