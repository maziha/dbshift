import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# Assume spark, runtype, muldate, conn, dsn, usern, passw, ref3, endofmon, bl, twobegdt, ONEBEGDT, enddt, mon, day, year2, ref, democurr, muldate2 are pre-defined variables
# For example:
# runtype = "Monthly"
# muldate = "2023-10-16"
# ...etc.

# Define schema for the fixed-width file based on the SAS INPUT statement
bonus_layout_schema = [
    ("KeyCode", 1, 12, "string"), ("MemAcctNum", 13, 22, "decimal(10,0)"),
    ("MerkleID", 23, 34, "string"), ("CampaignID", 35, 39, "string"),
    ("cntct_lifstyle_12mo_agg_hhd", 40, 42, "integer"), ("cntct_lifstyle_3mo_agg_hhd", 43, 45, "integer"),
    ("corp_member_Ind", 46, 46, "string"), ("NATitle", 60, 60, "string"),
    ("NAFname", 61, 61, "string"), ("NALname", 62, 62, "string"),
    ("NASuffix", 63, 63, "string"), ("NAAddr1", 64, 64, "string"),
    ("NAAddr2", 65, 65, "string"), ("NACity", 66, 66, "string"),
    ("State", 67, 68, "string"), ("Zip", 69, 73, "string"),
    ("ZipPlus4", 74, 77, "string"), ("CoaDate", 78, 85, "decimal(8,0)"),
    ("Age", 86, 88, "integer"), ("Gender", 89, 89, "string"),
    ("DateOfBirth", 90, 90, "string"), ("WorkStatus", 91, 91, "string"),
    ("MaritalStatus", 92, 92, "string"), ("NA5", 93, 93, "string"),
    ("NA6", 94, 94, "string"), ("NA7", 95, 95, "string"),
    ("NA8", 96, 96, "string"), ("SecGender", 97, 97, "string"),
    ("SecBirthDate", 98, 98, "string"), ("SecAge", 99, 101, "integer"),
    ("SegTypes", 102, 102, "string"), ("MemType", 103, 103, "string"),
    ("MemStatus", 104, 104, "string"), ("MemOriginDate", 105, 112, "decimal(8,0)"),
    ("MemOriginKey", 113, 121, "string"), ("MemXRenew", 123, 123, "integer"),
    ("MemPaidDate", 124, 129, "decimal(6,0)"), ("NA2", 132, 134, "string"),
    ("NA3", 135, 143, "string"), ("curr_order_create_dt", 144, 151, "string"),
    ("CoaSource", 152, 153, "string"), ("LastPromoDate", 154, 161, "string"),
    ("LastPromoKey", 162, 170, "string"), ("GeoAvgVal", 211, 220, "decimal(10,0)"),
    ("GeoCode", 227, 238, "string"), ("GeoMedVal", 252, 261, "decimal(10,0)"),
    ("GeoOccHouseUnit", 265, 267, "integer"), ("EthnicCode", 435, 436, "string"),
    ("ReligionCode", 437, 437, "string"), ("LanguageCode", 438, 439, "string"),
    ("OriginCode", 440, 441, "integer"), ("GroupEthnicCode", 442, 442, "string"),
    ("Life_Stage", 448, 448, "integer"), ("diversity_flag_agg_ind", 449, 449, "integer"),
    ("Diversity_Subgroup_Agg_Ind", 450, 452, "integer"), ("FedHouse", 479, 484, "string"),
    ("StateHouse", 491, 496, "string"), ("StateSenate", 497, 502, "string"),
    ("PartyCode", 503, 512, "string"), ("PartyMix", 513, 522, "string"),
    ("RepPartyCd", 523, 525, "string"), ("RegistrationDate", 526, 533, "string"),
    ("IS_Voter", 534, 534, "string"), ("VoterCount", 535, 544, "decimal(10,0)"),
    ("PartyAffiliation", 685, 687, "string"), ("EarliestRegistrationDate", 688, 697, "string"),
    ("VoterStatus", 698, 716, "string"), ("PrimElectn2012", 717, 726, "string"),
    ("GeneralElectn2012", 727, 736, "string"), ("SpecialElectn2012", 737, 746, "string"),
    ("sy_otsbn_polfund_2012a", 747, 752, "integer"), ("sy_otsbn_polfund_2012b", 753, 758, "integer"),
    ("advo_segment_cd", 770, 771, "string"), ("EmailableInd", 772, 772, "string"),
    ("NbrTimesSelEmailedInd", 773, 775, "string"), ("Globally_Opted_In", 776, 776, "string"),
    ("SUPERCLUSTER_CODE", 777, 778, "integer"), ("Aristotle_flag", 779, 779, "string"),
    ("geo_cd_2010", 780, 791, "string"),
    ("Age_HH_pct_with_HHer_55_64", 792, 796, "decimal(5,0)"), ("Age_HH_pct_with_HHer_65_74", 797, 801, "decimal(5,0)"),
    ("Age_HH_pct_with_HHer_75_84", 802, 806, "decimal(5,0)"), ("Age_HH_pct_with_HHer_85p", 807, 811, "decimal(5,0)"),
    ("Age_Pop_pct_60_64", 812, 816, "decimal(5,0)"), ("HH_pct_Spanish_Speaking", 817, 821, "decimal(5,0)"),
    ("HomVal_Home_Value_CBSA_Index", 822, 831, "decimal(10,0)"), ("Inc_HH_Median_HH_Income", 832, 841, "decimal(10,0)"),
    ("OCCHU_Median_Length_of_Residence", 842, 846, "decimal(5,0)"), ("OOHU_Median_Home_Value", 847, 856, "decimal(10,0)"),
    ("Pop_pct_Asian_Only_Hisp", 857, 861, "decimal(5,0)"), ("Pop_pct_Asian_Only_", 862, 866, "decimal(5,0)"),
    ("Pop_pct_Black_Only_Hisp", 867, 871, "decimal(5,0)"), ("Pop_pct_Black_Only_", 872, 876, "decimal(5,0)"),
    ("DRVS_Flag", 997, 997, "integer"), ("DRVS_Date", 999, 1006, "string"),
    ("ACEV_Flag", 1027, 1027, "string"), ("ACEV_Num", 1028, 1029, "string"),
    ("ACEV_Last_Date", 1030, 1037, "string"), ("ACEV_Last_Topic", 1038, 1040, "string"),
    ("Overall_Active_SP_Reltshps", 1229, 1231, "string"), ("Overall_Historic_SP_Reltshps", 1232, 1234, "string"),
    ("GE_Num_Active_Particpnts", 1235, 1237, "string"), ("GE_Num_InActive_Particpnts", 1238, 1240, "string"),
    ("GE_Orig_Partcp_Date", 1241, 1248, "string"), ("NYL_Num_Active_Particpnts", 1249, 1251, "string"),
    ("NYL_Num_InActive_Particpnts", 1252, 1254, "string"), ("NYL_Orig_Partcp_Date", 1255, 1262, "string"),
    ("Hartford_Num_Active_Particpnts", 1263, 1265, "string"), ("Hartford_Num_InActive_Particpnts", 1266, 1268, "string"),
    ("Hartford_Orig_Partcp_Date", 1269, 1276, "string"), ("Chase_Num_Active_Particpnts", 1277, 1279, "string"),
    ("Chase_Num_InActive_Particpnts", 1280, 1282, "string"), ("Chase_Orig_Partcp_Date", 1283, 1290, "string"),
    ("Foremost_Num_Active_Particpnts", 1291, 1293, "string"), ("Foremost_Num_InActive_Particpnts", 1294, 1296, "string"),
    ("Foremost_Orig_Partcp_Date", 1297, 1304, "string"), ("HistPartCt_Overall", 1311, 1313, "string"),
    ("CurrentPartCt_Overall", 1314, 1316, "string"), ("Past3MoTouchCt_Travel", 1317, 1319, "string"),
    ("Past12MoTouchCt_Travel", 1320, 1322, "string"), ("Past3MoTouchCt_Health", 1323, 1325, "string"),
    ("Past12MoTouchCt_Health", 1326, 1328, "string"), ("Past3MoTouchCt_Financial", 1329, 1331, "string"),
    ("Past12MoTouchCt_Financial", 1332, 1334, "string"), ("Past3MoTouchCt_Home", 1335, 1337, "string"),
    ("Past12MoTouchCt_Home", 1338, 1340, "string"), ("Past3MoTouchCt_Priv", 1341, 1343, "string"),
    ("Past12MoTouchCt_Priv", 1344, 1346, "string"), ("Past3MoTouchCt_AARP", 1347, 1349, "string"),
    ("Past12MoTouchCt_AARP", 1350, 1352, "string"), ("Past3MoTouchCt_Overall", 1353, 1355, "string"),
    ("Past12MoTouchCt_Overall", 1356, 1358, "string"), ("Fndn_TTD_Amt", 1359, 1370, "decimal(12,0)"),
    ("Fndn_TTD_Num", 1371, 1378, "decimal(8,0)"), ("Fndn_Last_Amt", 1379, 1386, "decimal(8,0)"),
    ("Fndn_Last_Dt", 1387, 1394, "decimal(8,0)"), ("Advo_TTD_Amt", 1395, 1406, "decimal(12,0)"),
    ("Advo_TTD_Num", 1407, 1414, "decimal(8,0)"), ("Advo_Last_Amt", 1415, 1422, "decimal(8,0)"),
    ("Advo_Last_Dt", 1423, 1430, "decimal(8,0)"), ("Advo_Last_Petition_Dt_Agg_Ind", 1431, 1438, "decimal(8,0)"),
    ("Advo_Last_Petition_Subj_Agg_Ind", 1439, 1448, "string"), ("advo_hpc_amt", 1449, 1456, "decimal(8,0)"),
    ("advo_hpc_dt", 1457, 1464, "decimal(8,0)"), ("advo_mrhpc_amt", 1465, 1472, "decimal(8,0)"),
    ("advo_mrhpc_dt", 1473, 1480, "decimal(8,0)"), ("fndn_hpc_amt", 1481, 1488, "decimal(8,0)"),
    ("fndn_hpc_dt", 1489, 1496, "decimal(8,0)"), ("fndn_mrhpc_amt", 1497, 1504, "decimal(8,0)"),
    ("fndn_mrhpc_dt", 1505, 1512, "decimal(8,0)"), ("partisanscore", 1513, 1520, "decimal(8,0)"),
    ("ideology", 1521, 1528, "decimal(8,0)"), ("vtm_active_vol_flag_act", 1529, 1529, "string"),
    ("vtm_dsp_active_vol", 1530, 1530, "string"), ("vtm_last_initiative_act", 1531, 1630, "string"),
    ("vtm_last_program_act", 1631, 1710, "string"), ("vtm_last_role_act", 1711, 1810, "string"),
    ("vtm_assignment_last_start_dt_act", 1811, 1818, "string"), ("vtm_num_last_12m_assignments_act", 1819, 1821, "integer"),
    ("vtm_num_assignments_act", 1822, 1824, "integer"), ("vtm_num_ytd_assignments_act", 1825, 1827, "integer"),
    ("vtm_vol_flag_act", 1828, 1828, "string")
]

df_raw_text = spark.read.text(bl)

df_bonus_layout = df_raw_text.select(
    *[F.trim(F.substring(F.col("value"), start, end - start + 1)).alias(name) 
      for name, start, end, dtype in bonus_layout_schema]
)

for name, start, end, dtype in bonus_layout_schema:
    if "string" not in dtype:
        df_bonus_layout = df_bonus_layout.withColumn(
            name,
            F.when(F.col(name) == "", None).otherwise(F.col(name)).cast(dtype)
        )

df_bonus_layout = df_bonus_layout.withColumn("Age_HH_pct_with_HHer_55_64", F.col("Age_HH_pct_with_HHer_55_64") * 10)
df_bonus_layout = df_bonus_layout.withColumn("Age_HH_pct_with_HHer_65_74", F.col("Age_HH_pct_with_HHer_65_74") * 10)
df_bonus_layout = df_bonus_layout.withColumn("HH_pct_Spanish_Speaking", F.col("HH_pct_Spanish_Speaking") * 10)
df_bonus_layout = df_bonus_layout.withColumn("Age_HH_pct_with_HHer_75_84", F.col("Age_HH_pct_with_HHer_75_84") * 10)
df_bonus_layout = df_bonus_layout.withColumn("Age_HH_pct_with_HHer_85p", F.col("Age_HH_pct_with_HHer_85p") * 10)
df_bonus_layout = df_bonus_layout.withColumn("Pop_pct_Asian_Only_Hisp", F.col("Pop_pct_Asian_Only_Hisp") * 10)
df_bonus_layout = df_bonus_layout.withColumn("Pop_pct_Asian_Only_", F.col("Pop_pct_Asian_Only_") * 10)
df_bonus_layout = df_bonus_layout.withColumn("Pop_pct_Black_Only_Hisp", F.col("Pop_pct_Black_Only_Hisp") * 10)
df_bonus_layout = df_bonus_layout.withColumn("Pop_pct_Black_Only_", F.col("Pop_pct_Black_Only_") * 10)
df_bonus_layout = df_bonus_layout.withColumn("OCCHU_Median_Length_of_Residence", F.col("OCCHU_Median_Length_of_Residence") * 100)

df_bonus_layout = df_bonus_layout.withColumn("Region",
    F.when(F.upper(F.col("STATE")).isin('AK', 'CO', 'HI', 'ID', 'MT', 'NV', 'NM', 'OR', 'UT', 'WY'), 'West Region')
     .when(F.upper(F.col("STATE")).isin('AR', 'IA', 'KS', 'MN', 'NE', 'ND', 'OK', 'SD', 'WI'), 'Central Region')
     .when(F.upper(F.col("STATE")).isin('AL', 'DC', 'KY', 'LA', 'MD', 'MS', 'SC', 'VA', 'WV'), 'South Region')
     .when(F.upper(F.col("STATE")).isin('CT', 'DE', 'ME', 'MA', 'NH', 'PR', 'RI', 'VT', 'VI'), 'East Coast Region')
     .when(F.upper(F.col("STATE")).isin('AZ', 'GA', 'IN', 'MI', 'MO', 'NJ', 'NC', 'TN', 'WA'), 'Large Region')
     .when(F.upper(F.col("STATE")).isin('CA', 'FL', 'IL', 'NY', 'OH', 'PA', 'TX'), 'Mega Region')
     .otherwise('Unknown')
)

df_bonus_layout = df_bonus_layout.withColumn("PrimElectn2012",
    F.when(F.col("PrimElectn2012") == 'absentee', 'A')
     .when(F.col("PrimElectn2012") == 'earlyVote', 'E')
     .when(F.col("PrimElectn2012") == 'mail', 'M')
     .when(F.col("PrimElectn2012").isin('polling', 'unknown'), 'Y')
     .otherwise(F.col("PrimElectn2012"))
)

df_bonus_layout = df_bonus_layout.withColumn("GeneralElectn2012",
    F.when(F.col("GeneralElectn2012") == 'absentee', 'A')
     .when(F.col("GeneralElectn2012") == 'earlyVote', 'E')
     .when(F.col("GeneralElectn2012") == 'mail', 'M')
     .when(F.col("GeneralElectn2012").isin('polling', 'unknown'), 'Y')
     .otherwise(F.col("GeneralElectn2012"))
)

df_bonus_layout = df_bonus_layout.withColumn("SpecialElectn2012",
    F.when(F.col("SpecialElectn2012") == 'absentee', 'A')
     .when(F.col("SpecialElectn2012") == 'earlyVote', 'E')
     .when(F.col("SpecialElectn2012") == 'mail', 'M')
     .when(F.col("SpecialElectn2012").isin('polling', 'unknown'), 'Y')
     .otherwise(F.col("SpecialElectn2012"))
)

if runtype == 'Monthly':
    sql_query_emu_lookup = f"select mid_key from {ref3}.vq_emu"
    df_emu_keys_from_db = spark.read.format("jdbc").option("url", conn).option("dbtable", sql_query_emu_lookup).option("user", usern).option("password", passw).load()
    
    df_bonus_layout_keys = df_bonus_layout.select(F.col("merkleid").cast("decimal(10,0)").alias("mid_key"))
    
    df_emu = df_emu_keys_from_db.join(df_bonus_layout_keys, on="mid_key", how="left_anti").orderBy("mid_key")
    df_emu.write.format("delta").mode("overwrite").saveAsTable("intermed.emu")

    sql_query_emu_mid_indiv = f"""
    select a.mid_key,chid_agg_ind, b.hid_key, advo_segment_cd, age_agg_ind as age, 
        diversity_fl_agg_ind as diversity_flag_agg_ind, drvs_fl_agg_ind as DRVS_Flag,
        Emailable_Agg_Ind, substring(gender_agg_ind,1) as Gender, lifestage_segment,
        marital_stat_agg_ind as maritalstatus, num_times_selected_email_agg_ind,
        IBX_RELIGIOUS_AFFILIATION_CODE_E_tech as ReligionCode,  Ibx_Ethnic_Code_E_Tech as EthnicCode,
        Ibx_Ethnic_Roll_Up_Code_E_Tech as GroupEthnicCode, 
        IBX_COUNTRY_OF_ORIGIN_CODE_E_TECh, 
        case when vtm_vol_flag_ind=1 then 'Y' else 'N' end as vtm_vol_flag_act, 
        employment_stat as workstatus, ideology_model as ideology, PARTISANSHIP_MODEL as partisanscore, 
        sy_otsbn_polfund_2012a,sy_otsbn_polfund_2012b,voted_in_2012_general_voting_method as GeneralElectn2012, 
        Party_Affiliation as PartyAffiliation, 
        VOTER_STATUS as VoterStatus, 
        advo_hpc_amt,
        Advo_Last_Amt,
        Advo_Last_Dt as advo_last_dt_i,
        Advo_TTD_Amt,
        Advo_TTD_Num, 
        Fndn_Last_Amt,
        Fndn_Last_Dt as fndn_last_dt_i,
        Fndn_TTD_Amt,
        fndn_ttd_num, 
        fndn_hpc_amt,
        advo_mrhpc_amt,
        case when vtm_active_vol_flag_ind=1 then 'Y' else 'N' end as vtm_active_vol_flag_act,
        case when vtm_dsp_active_vol_ind=1 then 'Y' else 'N' end as vtm_dsp_active_vol,
        vtm_num_assignments_ind as vtm_num_assignments_act, vtm_last_role_ind as vtm_last_role_act,
        addr_move_dt,fndn_mrhpc_dt,birth_dt_agg_ind,
        advo_last_petition_dt_agg_ind as advo_last_petition_dt_agg_ind_i
    from {ref3}.vq_emu a
    left join {ref3}.d_individual b on a.mid_key=b.mid_key
    left join {ref3}.d_catalist_voter_model c on a.mid_key=c.mid_key
    left join {ref3}.d_mm_individual d on a.mid_key=d.mid_key
    left join {ref3}.d_vtm_individual f on a.mid_key=f.mid_key
    """
    df_emu_mid_indiv_raw = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_emu_mid_indiv}) as subq").option("user", usern).option("password", passw).load()
    df_emu_mid_indiv = df_emu_mid_indiv_raw.withColumn("dateofbirth", F.when(F.col("birth_dt_agg_ind").isNotNull(), '1').otherwise('0')).drop("birth_dt_agg_ind")
    df_emu_mid_indiv.write.format("delta").mode("overwrite").saveAsTable("intermed.emu_mid_indiv")

    sql_query_emu_hid = f"""
    select a.mid_key,
        Num_Hist_Participation_Overall_Agg_hhd,
        Num_Contact_Aarp_12_Months_Agg_Hhd,
        Num_Contact_Financial_12_Months_agg_hhd,
        Num_Contact_Health_12_Months_Agg_hhd,
        Num_Contact_Overall_12_Months_Agg_hhd,
        num_contact_discounts_12_months_agg_hhd,
        num_contact_travel_12_months_agg_hhd,
        Num_Contact_Aarp_3_Months_Agg_Hhd,
        Num_Contact_Financial_3_Months_Agg_hhd,
        Num_Contact_Health_3_Months_Agg_hhd, 
        Num_Contact_Overall_3_Months_Agg_hhd,
        num_contact_discounts_3_months_agg_hhd,
        state,
        Zip,
        zip4 as ZipPlus4,
        NUM_CONTACT_LIFESTYLE_12_MONTHS_agg_hhd as cntct_lifstyle_12mo_agg_hhd,
        NUM_CONTACT_LIFESTYLE_3_MONTHS_Agg_hhd as cntct_lifstyle_3mo_agg_hhd, 
        NUM_CURR_PARTICIPATION_OVERALL_Agg_hhd,
        geo_cd as geocode,c.geo_cd_2010,
        representative_party_cd as RepPartyCd,
        case 
            when UPPER(STATE) in ('AK',  'CO', 'HI', 'ID',  'MT', 'NV', 'NM', 'OR','UT', 'WY') THEN 'West Region'
            when UPPER(STATE) in ('AR', 'IA', 'KS' ,'MN', 'NE', 'ND', 'OK', 'SD', 'WI')        THEN 'Central Region'
            when UPPER(STATE) in ('AL', 'DC', 'KY', 'LA', 'MD', 'MS', 'SC', 'VA', 'WV')        THEN 'South Region'
            when UPPER(STATE) in ('CT', 'DE', 'ME', 'MA', 'NH', 'PR', 'RI', 'VT', 'VI')       THEN 'East Coast Region'
            when UPPER(STATE) in ('AZ', 'GA', 'IN', 'MI', 'MO', 'NJ', 'NC', 'TN', 'WA')       THEN 'Large Region'
            when UPPER(STATE) in ('CA', 'FL', 'IL', 'NY', 'OH', 'PA', 'TX')                   THEN 'Mega Region'
            else 'Unknown' 
        end as region,
        cens_age_hh_percent_with_householder_age_55_64*10 as Age_HH_pct_with_HHer_55_64,
        cens_age_hh_percent_with_householder_age_65_74*10 as Age_HH_pct_with_HHer_65_74,
        cens_age_hh_percent_with_householder_age_75_84*10 as Age_HH_pct_with_HHer_75_84,
        cens_age_hh_percent_with_householder_age_85_plus*10 as Age_HH_pct_with_HHer_85p,
        CENS_ETHNIC_POP_PERCENT_ASIAN_ONly*10 as Pop_pct_Asian_Only_,
        cens_ethnic_pop_percent_asian_only_hisp*10 as Pop_pct_Asian_Only_Hisp,
        CENS_ETHNIC_POP_PERCENT_BLACK_Only_hisp*10 as Pop_pct_Black_Only_Hisp,
        CENS_HOMVAL_HOME_VALUE_CBSA_INDEx as HomVal_Home_Value_CBSA_Index,
        cens_inc_hh_median_household_income as Inc_HH_Median_HH_Income,
        CENS_MOVE_OCCHU_MEDIAN_LENGTH_OF_residence*100 as OCCHU_Median_Length_of_Residence,
        CENS_HOMVAL_OOHU_MEDIAN_HOME_VALue as OOHU_Median_Home_Value,
        cens_grpqtrs_pop_percent_college_dorms
    from {ref3}.vq_emu a
    left join {ref3}.f_joiner b on a.mid_key=b.mid_key
    left join {ref3}.d_household as c on b.hid_key=c.hid_key
    left join {ref3}.d_census_2010 as d on c.geo_cd_2010=d.geo_cd_2010
    """
    df_emu_hid = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_emu_hid}) as subq").option("user", usern).option("password", passw).load()
    df_emu_hid.write.format("delta").mode("overwrite").saveAsTable("intermed.emu_hid")

    sql_query_emu_account = f"""
    select a.mid_key,
        acev_num,
        num_active_part_chase_agg_act,
        num_hist_part_chase_agg_act,
        current_order_CREATE_dt_agg_act,
        num_active_part_foremost_agg_act,
        num_active_part_ge_agg_act,
        num_active_part_hartford_agg_act,
        num_active_part_nyl_agg_act,
        num_hist_part_nyl_agg_act,
        Kx_Create_Dt,
        orig_key_cd as memoriginkey,
        paid_through_dt,
        times_renewed_agg_act as memxrenew,
        term_agg_act,
        response_key_cd_agg_act as na3,
        active_sprel_overall_agg_act,
        hist_sprel_overall_agg_act,
        sec_age_agg_act as SecAge,
        c.account_stat,
        b.chid_key as memacctnum
    from {ref3}.vq_emu a
    left join {ref3}.f_joiner b on a.mid_key=b.mid_key
    left join {ref3}.d_account c on b.chid_key=c.chid_key
    where preferred_chid=1
    """
    df_emu_account_raw = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_emu_account}) as subq").option("user", usern).option("password", passw).load()
    df_emu_account = df_emu_account_raw.withColumn("mempaiddate", F.date_format(F.to_date(F.col("paid_through_dt")), "yyyyMM").cast("integer")).withColumnRenamed("acev_num", "acev_num_i")
    df_emu_account.write.format("delta").mode("overwrite").saveAsTable("intermed.emu_account")

    df_emu_complete = df_emu.join(df_emu_mid_indiv, on="mid_key", how="left") \
        .join(df_emu_hid, on="mid_key", how="left") \
        .join(df_emu_account, on="mid_key", how="left")
    
    df_emu_complete = df_emu_complete.withColumn("Life_Stage", F.col("lifestage_segment").cast("double")) \
        .withColumn("NbrTimesSelEmailedInd", F.col("num_times_selected_email_agg_ind").cast("string")) \
        .withColumn("OriginCode", F.col("IBX_COUNTRY_OF_ORIGIN_CODE_E_TEC").cast("double")) \
        .withColumn("Advo_Last_Dt", F.date_format(F.to_date(F.col("advo_last_dt_i")), "yyyyMMdd").cast("decimal(8,0)")) \
        .withColumn("Fndn_Last_Dt", F.date_format(F.to_date(F.col("fndn_last_dt_i")), "yyyyMMdd").cast("decimal(8,0)")) \
        .withColumn("advo_last_petition_dt_agg_ind", F.date_format(F.to_date(F.col("advo_last_petition_dt_agg_ind_i")), "yyyyMMdd").cast("decimal(8,0)")) \
        .withColumn("movedate", F.to_date(F.col("addr_move_dt"))) \
        .withColumn("coadate", F.date_format(F.to_date(F.col("addr_move_dt")), "yyyyMMdd").cast("decimal(8,0)")) \
        .withColumn("PartyMix", F.lit('          ')) \
        .withColumn("VoterCount", F.lit(None).cast("decimal(10,0)")) \
        .withColumn("HistPartCt_Overall", F.regexp_replace(F.col("Num_Hist_Participation_Overall_Agg_hhd").cast("string"), "\\s|\\.", "")) \
        .withColumn("Past12MoTouchCt_AARP", F.regexp_replace(F.col("Num_Contact_Aarp_12_Months_Agg_Hhd").cast("string"), "\\s|\\.", "")) \
        .withColumn("Past12MoTouchCt_Financial", F.regexp_replace(F.col("Num_Contact_Financial_12_Months_agg_hhd").cast("string"), "\\s|\\.", "")) \
        .withColumn("Past12MoTouchCt_Health", F.regexp_replace(F.col("Num_Contact_Health_12_Months_Agg_hhd").cast("string"), "\\s|\\.", "")) \
        .withColumn("Past12MoTouchCt_Overall", F.regexp_replace(F.col("Num_Contact_Overall_12_Months_Agg_hhd").cast("string"), "\\s|\\.", "")) \
        .withColumn("Past12MoTouchCt_Priv", F.regexp_replace(F.col("num_contact_discounts_12_months_agg_hhd").cast("string"), "\\s|\\.", "")) \
        .withColumn("Past12MoTouchCt_Travel", F.regexp_replace(F.col("num_contact_travel_12_months_agg_hhd").cast("string"), "\\s|\\.", "")) \
        .withColumn("Past3MoTouchCt_AARP", F.regexp_replace(F.col("Num_Contact_Aarp_3_Months_Agg_Hhd").cast("string"), "\\s|\\.", "")) \
        .withColumn("Past3MoTouchCt_Financial", F.regexp_replace(F.col("Num_Contact_Financial_3_Months_Agg_hhd").cast("string"), "\\s|\\.", "")) \
        .withColumn("Past3MoTouchCt_Health", F.regexp_replace(F.col("Num_Contact_Health_3_Months_Agg_hhd").cast("string"), "\\s|\\.", "")) \
        .withColumn("Past3MoTouchCt_Overall", F.regexp_replace(F.col("Num_Contact_Overall_3_Months_Agg_hhd").cast("string"), "\\s|\\.", "")) \
        .withColumn("Past3MoTouchCt_Priv", F.regexp_replace(F.col("num_contact_discounts_3_months_agg_hhd").cast("string"), "\\s|\\.", "")) \
        .withColumn("CurrentPartCt_Overall", F.regexp_replace(F.col("NUM_CURR_PARTICIPATION_OVERALL_Agg_hhd").cast("string"), "\\s|\\.", "")) \
        .withColumn("acev_num", F.when(F.col("acev_num_i") > 99, "*").otherwise(F.regexp_replace(F.col("acev_num_i").cast("string"), "\\s|\\.", ""))) \
        .withColumn("Chase_Num_Active_Particpnts", F.regexp_replace(F.col("num_active_part_chase_agg_act").cast("string"), "\\s|\\.", "")) \
        .withColumn("Chase_Num_InActive_Particpnts", F.regexp_replace(F.col("num_hist_part_chase_agg_act").cast("string"), "\\s|\\.", "")) \
        .withColumn("curr_order_create_dt", F.date_format(F.to_date(F.col("current_order_CREATE_dt_agg_act")), "yyyyMMdd")) \
        .withColumn("Foremost_Num_Active_Particpnts", F.regexp_replace(F.col("num_active_part_foremost_agg_act").cast("string"), "\\s|\\.", "")) \
        .withColumn("GE_Num_Active_Particpnts", F.regexp_replace(F.col("num_active_part_ge_agg_act").cast("string"), "\\s|\\.", "")) \
        .withColumn("Hartford_Num_Active_Particpnts", F.regexp_replace(F.col("num_active_part_hartford_agg_act").cast("string"), "\\s|\\.", "")) \
        .withColumn("NYL_Num_Active_Particpnts", F.regexp_replace(F.col("num_active_part_nyl_agg_act").cast("string"), "\\s|\\.", "")) \
        .withColumn("NYL_Num_InActive_Particpnts", F.regexp_replace(F.col("num_hist_part_nyl_agg_act").cast("string"), "\\s|\\.", "")) \
        .withColumn("memorigindate", F.date_format(F.to_date(F.col("Kx_Create_Dt")), "yyyyMMdd").cast("decimal(8,0)")) \
        .withColumn("na2", F.regexp_replace(F.col("term_agg_act").cast("string"), "\\s|\\.", "")) \
        .withColumn("Overall_Active_SP_Reltshps", F.regexp_replace(F.col("active_sprel_overall_agg_act").cast("string"), "\\s|\\.", "")) \
        .withColumn("Overall_Historic_SP_Reltshps", F.regexp_replace(F.col("hist_sprel_overall_agg_act").cast("string"), "\\s|\\.", "")) \
        .withColumn("memstatus", F.lit("E")) \
        .withColumn("acev_flag", F.when(F.col("acev_num_i") > 0, "Y").otherwise("N")) \
        .withColumn("globally_opted_in", F.lit("1")) \
        .withColumn("merkleid", F.lpad(F.regexp_replace(F.col("mid_key").cast("string"), "\\s|\\.", ""), 12, ' ')) \
        .drop("chid_agg_ind", "mid_key", "hid_key", "acev_num_i", "Advo_Last_Dt_i", "Fndn_Last_Dt_i", "advo_last_petition_dt_agg_ind_i")

    df_emu_complete.write.format("delta").mode("overwrite").saveAsTable("intermed.emu_complete")
    df_emu_complete = df_emu_complete.dropDuplicates(["merkleid"])

    # Align columns for union
    emu_cols = {c.lower() for c in df_emu_complete.columns}
    bonus_cols = {c.lower() for c in df_bonus_layout.columns}
    
    for col in emu_cols - bonus_cols:
        df_bonus_layout = df_bonus_layout.withColumn(col, F.lit(None).cast(df_emu_complete.schema[col].dataType))
    for col in bonus_cols - emu_cols:
        df_emu_complete = df_emu_complete.withColumn(col, F.lit(None).cast(df_bonus_layout.schema[col].dataType))
    
    df_bonus_layout = df_bonus_layout.select(sorted(df_bonus_layout.columns))
    df_emu_complete = df_emu_complete.select(sorted(df_emu_complete.columns))

    df_bonus_layout = df_bonus_layout.unionByName(df_emu_complete)

    %run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/Process_Demos_20210118"
    
    spark.sql("DROP TABLE IF EXISTS intermed.emu_complete")
    spark.sql("DROP TABLE IF EXISTS intermed.emu")
    spark.sql("DROP TABLE IF EXISTS intermed.emu_mid_indiv")
    spark.sql("DROP TABLE IF EXISTS intermed.emu_hid")
    spark.sql("DROP TABLE IF EXISTS intermed.emu_account")

    sql_query_household_monthly = f"""
    select house.geo_cd_2010,a.hid_key,representative_party_cd,
			CENS_AGE_HH_PERCENT_WITH_HOUSEHOLDER_AGE_15_24,
			CENS_AGE_POP_MEDIAN_AGE_OF_FEMALes,
			CENS_AGE_POP_MEDIAN_AGE_OF_MALES,
			CENS_AGE_POP_PERCENT_45_54,
			CENS_AGE_POP_PERCENT_55_59,
			CENS_AGE_POP_PERCENT_55_64,
			CENS_AGE_POP_PERCENT_60_64,
			cens_age_pop_percent_65_99_plus,
			CENS_BUILT_HU_PERCENT_BUILT_2000_TO_2004,
			CENS_BUILT_HU_PERCENT_BUILT_LT1940,
			CENS_CHILD_HH_PERCENT_FAM_WITH_PERSONS_LT18,
			CENS_CHILD_HH_PERCENT_FEMALE_HOH_FAM_WITH_PERSONS_LT18,
			CENS_CHILD_HH_PERCENT_WITHOUT_PERSONS_LT18,
			CENS_COMMUTE_COMMUTER_AVG_TRAV_TIME_TO_WORK,
			CENS_COMMUTE_COMMUTER_PERCENT_TRAV_TO_WORK_LT_30_MIN,
			CENS_COMMUTE_WRKRS_PERCENT_CARPOOLED_TO_WORK,
			CENS_COMMUTE_WRKRS_PERCENT_PUBLIC_TRANS_TO_WORK,
			CENS_COUNT_POPULATION,
			CENS_COUNT_RENTAL_UNITS,
			CENS_COUNT_WORKERS,
			CENS_DENSITY_PERSONS_PER_HH_FOR_POP_IN_HH,
			CENS_DENSITY_POPULATION_PER_SQUARE_MILE,
			CENS_EARN_HH_PERCENT_NO_EARNINGS,
			CENS_EARN_HH_PERCENT_NO_OTHER_TYPE_OF_INCOME,
			CENS_EARN_HH_PERCENT_NO_WAGE_SALARY_INCOME,
			CENS_EARN_HH_PERCENT_WITH_EARNINGS,
			CENS_EARN_HH_PERCENT_WITH_PUBLIC_ASSISTANCE_INCOME,
			CENS_EDUC_POP25_PLUS_MEDIAN_EDUCATION_ATTAINED,
			CENS_EDUC_POP25_PLUS_PERCENT_BACHELOR_DEGREE,
			CENS_EDUC_POP25_PLUS_PERCENT_PROF_DEGREE,
			CENS_EMPLOY_LABF_PERCENT_EMPLOYEd,
			CENS_EMPLOY_LABF_PERCENT_UNEMPLOyed,
			CENS_EMPLOY_POP18_PLUS_PERCENT_CIVILIAN_VETS,
			cens_ethnic_pop_percent_black_only,
			CENS_ETHNIC_POP_PERCENT_HI_NAT_OTH_PAC_ONLY,
			CENS_ETHNIC_POP_PERCENT_HISPANIC,
			cens_ethnic_pop_percent_non_hispanic,
			CENS_ETHNIC_POP_PERCENT_SOME_OTHER_RACE_ONLY,
			CENS_ETHNIC_POP_PERCENT_WHITE_ONly,
			CENS_GENDER_POP_PERCENT_FEMALE,
			CENS_HEAT_OCCHU_PERCENT_OIL_OR_KEROSENE_HEAT,
			CENS_HEAT_OCCHU_PERCENT_SOLAR_HEat, 
			CENS_HEAT_OCCHU_PERCENT_UTILITY_gas_heat,
			CENS_HHSIZE_HH_PERCENT_2_PERSONS,
			CENS_HOMVAL_HOME_VALUE_CBSA_INDEx,
			CENS_HOMVAL_OOHU_MEDIAN_HOME_VALue,
			CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_UNDER_10K,
			CENS_HUSTR_HU_PERCENT_2_UNITS,
			CENS_INC_FAMILY_INC_STATE_DECILE,
			cens_inc_hh_median_family_household_income,
			cens_inc_hh_median_household_income,
			CENS_INDUS_EMPLD_PERCENT_ACCOMODATION_AND_FOOD_SVCS,
			CENS_INDUS_EMPLD_PERCENT_EDUCATIONAL_SERVICES,
			CENS_INDUS_EMPLD_PERCENT_FINANCE_AND_INSURANCE,
			CENS_INDUS_EMPLD_PERCENT_MANUFACturing,
			CENS_LANG_HH_PERCENT_ENGLISH_SPEaking,
			CENS_LANG_HH_PERCENT_SPANISH_SPEaking,
			CENS_MARR_POP15_PLUS_PERCENT_SPOUSE_PRESENT,
			CENS_MARR_POP15_PLUS_PERCENT_WIDowed,
			CENS_MORTG_OOHU_PERCENT_NO_MORTGage,
			CENS_MOVE_OCCHU_PERCENT_NEW_LISTings,
			CENS_OCCUP_EMPLD_PERCENT_BUS_AND_FINANCIAL_OPS,
			CENS_OCCUP_EMPLD_PERCENT_HEALTH_DIAG_AND_TREAT_PRACS,
			CENS_OCCUP_EMPLD_PERCENT_SALES_AND_RELATED,
			CENS_OCCUP_EMPLD_PERCENT_TRANS_AND_MATERIAL_MOV_SUPV,
			CENS_RENT_RNTL_MEDIAN_RENT,
			CENS_STATE_CODE,
			CENS_TYP_POP_PERCENT_GRANDCHILD_IN_FAMILY_HH,
			CENS_TYP_POP_PERCENT_STEPCHILD_IN_FAMILY_HH,
			CENS_GRPQTRS_POP_PERCENT_COLLEGE_dorms,
			CENS_INDUS_EMPLD_PERCENT_HLTH_CARE_SOCIAL_ASSISTANCE,
			CENS_COUNT_FAMILY_HOUSEHOLDS,
			CENS_GRPQTRS_POP_PERCENT_MILITARY_QTRS,
			CENS_GRPQTRS_POP_PERCENT_NURSING_homes,
			CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_UNDER_10K, 
			CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_10_14K, 
			CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_15_19K,
			cens_hustr_hu_percent_1_unit_detached,
			cens_tenancy_occhu_percent_owner_OCCUPIED,
			CENS_OCCUP_EMPLD_PERCENT_FIRE_AND_PROT_SVCS_INCL_SUPV,
			CENS_OCCUP_EMPLD_PERCENT_LAW_ENFORCEMENT_INCL_SUPV,
			CENS_OCCUP_EMPLD_PERCENT_FOOD_PREP_AND_SERVING,
			CENS_OCCUP_EMPLD_PERCENT_BLDG_AND_GDS_CLEAN_AND_MTC,
			CENS_OCCUP_EMPLD_PERCENT_PERSONAL_CARE_SVCS,
			CENS_OCCUP_EMPLD_PERCENT_FARM_FISH_AND_FORESTRY,
			CENS_OCCUP_EMPLD_PERCENT_CONSTR_AND_EXTRACT,
			CENS_OCCUP_EMPLD_PERCENT_INSTALL_MAINT_AND_REPAIR,
			CENS_OCCUP_EMPLD_PERCENT_PRODUCTION,
			CENS_OCCUP_EMPLD_PERCENT_MOTOR_VEHICLE_OPS,
			CENS_OCCUP_EMPLD_PERCENT_MATERIAL_MOVING_WORKERS,
			CENS_OCCUP_EMPLD_PERCENT_MANAGEMENT,
			CENS_OCCUP_EMPLD_PERCENT_LEGAL,
			CENS_OCCUP_EMPLD_PERCENT_ARCHITECTURE_AND_ENGINEERING,
			CENS_OCCUP_EMPLD_PERCENT_COMPUTERS_AND_MATH,
			CENS_OCCUP_EMPLD_PERCENT_LIFE_PHYS_AND_SOC_SCIENCES,
			cens_ethnic_pop_percent_asian_only,
			CENS_CENSUS_TRACT,
			CENS_CENSUS_BLOCK_GROUP,
			CENS_AGE_POP_PERCENT_50_54,
			CENS_EDUC_POP25_PLUS_PERCENT_ASSOCIATE_DEGREE,
			CENS_INC_HH_MED_INC_HOUSEHOLDER_AGE_UNDER_25,
			CENS_INDUS_EMPLD_PERCENT_MINING,
			CENS_INDUS_EMPLD_PERCENT_INFORMAtion,
			CENS_MOVE_OCCHU_PERCENT_MOVED_IN_LT1970,
			CENS_MOVE_OCCHU_PERCENT_TURNOVER_LAST_5_YRS,
			CENS_EARN_HH_PERCENT_WITH_SELF_EMPLOYMENT_INCOME,
			CENS_COUNT_HOUSEHOLDS,
			CENS_AGE_POP_PERCENT_30_34,
			CENS_EARN_HH_PERCENT_NO_PUBLIC_ASSISTANCE_INCOME,
			CENS_HEAT_OCCHU_PERCENT_BOTTLE_OR_TANK_LP_GAS_HEAT,
			CENS_HEAT_OCCHU_PERCENT_OTHER_HEat,
			CENS_INDUS_EMPLD_PERCENT_WHOLESAle_trade,
			CENS_INDUS_EMPLD_PERCENT_TRANSPORT_AND_WAREHOUSING,
			CENS_MORTG_OOHU_PERCENT_TWO_MRTGS_AND_HOME_EQUITY_LN,
			CENS_OCCUP_EMPLD_PERCENT_HEALTHCARE_SUPP,
			CENS_URBAN_POP_PERCENT_URBAN_IN_URBAN_AREAS,
			CENS_ETHNIC_HH_PERCENT_HOH_HISPAnic,
			CENS_MARR_POP15_PLUS_PERCENT_NEVER_MARRIED,
			cens_age_pop_percent_35_39,
			cens_lang_hh_percent_span_speak_linguist_isol,
			cens_ethnic_pop_percent_am_ind_ak_nat_only,
			cens_rent_rntl_aggregate_contract_rent,
			cens_commute_wrkrs_percent_work_at_home,
			cens_inc_hh_median_non_family_household_income,
			cens_age_pop_percent_25_34,
			cens_commute_wrkrs_percent_drove_to_work_alone,
			cens_employ_labf_percent_in_armed_forces,
			cens_hustr_hu_percent_1_unit_attached,
			cens_indus_empld_percent_agric_forest_fish_and_hunt,
			cens_indus_empld_percent_construction,
			cens_tenancy_hu_percent_occupied,
			cens_typ_pop_percent_female_hoh_in_family_hh,
			cens_inc_family_inc_state_index,
			cens_employ_popfem16_plus_percent_in_labor_force,
			cens_earn_hh_percent_no_self_employment_income,
			cens_grpqtrs_pop_percent_oth_non_institution_grp_qtrs,
			num_curr_participation_financial_agg_hhd
	from (select coalesce(aa.hid_key,c.hid_key) as hid_key
	from (select b.hid_key,b.mid_key
	from {ref3}.d_account a
	left join
		{ref3}.f_joiner b
		on a.chid_key=b.chid_key
    where preferred_chid=1 and pri_sec=1 and (a.account_stat='0' or 
	(a.account_stat='5' and cast(Paid_Through_Dt as date) >= date'{twobegdt}' and
	cast(Paid_Through_Dt as date) <= date'{muldate}'))) aa
	full join 
	 (select emu.mid_key,j.chid_key,j.hid_key
			from {ref3}.vq_emu emu left join {ref3}.f_joiner j
			on emu.mid_key=j.mid_key
			where preferred_chid=1 or chid_key=1) c
	on aa.mid_key=c.mid_key) a
		left join  
	    {ref3}.d_household house
		on a.hid_key=house.hid_key
		left join 
		{ref3}.d_census_2010 cens
		on house.geo_cd_2010=cens.geo_cd_2010
    """
    df_household_monthly = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_household_monthly}) as subq").option("user", usern).option("password", passw).load()
    df_household_monthly.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("mulinter.household")

if runtype == 'Weekly':
    %run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/Process_Acxiom_Demographic_20201215"
    
    sql_query_orders = f"""
    select mid_key,response_key_cd,kx_create_dt, order_term,end_term_dt
    from {ref3}.f_account_order
    """
    df_orders_from_redshift = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_orders}) as subq").option("user", usern).option("password", passw).load()
    df_special_keycodes = spark.table("aarpdata.special_keycodes").select("sp_key_codes")
    
    df_orders_joined = df_orders_from_redshift.join(
        df_special_keycodes,
        F.substring(F.col("response_key_cd"), 1, 9) == F.col("sp_key_codes"),
        "left"
    )

    df_orders2 = df_orders_joined.withColumn("orders_all_i", F.lit(1)) \
        .withColumn("orders_12moterm_i", F.when(F.col("order_term") == 12, 1).otherwise(None)) \
        .withColumn("orders_36moterm_i", F.when(F.col("order_term") == 36, 1).otherwise(None)) \
        .withColumn("orders_60moterm_i", F.when(F.col("order_term") == 60, 1).otherwise(None)) \
        .withColumn("channel", F.substring(F.col("response_key_cd"), 1, 1)) \
        .withColumn("orders_acqmail_i", F.when((F.substring(F.col("response_key_cd"), 1, 1) == 'D') & F.col("sp_key_codes").isNull(), 1).otherwise(None)) \
        .withColumn("orders_renewals_i", F.when(F.substring(F.col("response_key_cd"), 1, 1).isin('D', 'Z') & F.col("sp_key_codes").isNull(), 1).otherwise(None)) \
        .withColumn("orders_altmedia_i", F.when((F.substring(F.col("response_key_cd"), 1, 1) == 'K') & F.col("sp_key_codes").isNull(), 1).otherwise(0)) \
        .withColumn("orders_online_i", F.when((F.substring(F.col("response_key_cd"), 1, 1) == 'U') & F.col("sp_key_codes").isNull(), 1).otherwise(0)) \
        .withColumn("orders_serviceprovider_i", F.when((F.substring(F.col("response_key_cd"), 1, 1) == 'F') | F.col("sp_key_codes").isNotNull(), 1).otherwise(0)) \
        .withColumn("orders_winback_i", F.when((F.substring(F.col("response_key_cd"), 1, 1) == 'W') & F.col("sp_key_codes").isNull(), 1).otherwise(0)) \
        .withColumn("orders_cl_i", F.when((F.substring(F.col("response_key_cd"), 1, 1) == 'N') & F.col("sp_key_codes").isNull(), 1).otherwise(0)) \
        .withColumn("orders_dm_i", F.when(F.substring(F.col("response_key_cd"), 1, 1).isin('D', 'R', 'Z') & F.col("sp_key_codes").isNull(), 1).otherwise(None)) \
        .withColumn("orders_sps_i", F.when(F.substring(F.col("response_key_cd"), 1, 1).isin('M', 'H') & F.col("sp_key_codes").isNull(), 1).otherwise(None)) \
        .withColumn("orders_am_i", F.when((F.substring(F.col("response_key_cd"), 1, 1) == 'K') & (F.to_date(F.col("kx_create_dt")) <= F.to_date(F.lit("2020-06-08"))) & F.col("sp_key_codes").isNull(), 1).otherwise(0))
    df_orders2.write.format("delta").mode("overwrite").saveAsTable("mulinter.orders2")

    df_f_account_order = spark.table("unica.f_account_order")
    df_a = df_f_account_order.filter((F.to_date(F.col("start_term_dt")) < F.current_date()) & (F.to_date(F.col("end_term_dt")) > F.current_date()))
    df_b = df_f_account_order.alias("b")
    df_order_curr_date = df_a.join(df_b, (F.col("a.mid_key") == F.col("b.mid_key")) & (F.col("a.order_num") == F.col("b.order_num") - 1), "left") \
        .select("a.*", F.col("b.order_num").alias("prev_order_num"), F.col("b.kx_create_dt").alias("new_kx_create_dt"))
    
    df_order_curr_date_sorted = df_order_curr_date.orderBy("mid_key", F.desc("order_num"))
    
    window_spec = Window.partitionBy("mid_key").orderBy(F.desc("order_num"))
    df_order_curr_date2 = df_order_curr_date_sorted.withColumn("row", F.row_number().over(window_spec)).filter(F.col("row") == 1).drop("row")
    
    df_allorderdata_agg = df_orders2.groupBy("mid_key").agg(
        F.sum("orders_all_i").alias("orders_all"),
        F.sum("orders_12moterm_i").alias("orders_12moterm"),
        F.sum("orders_36moterm_i").alias("orders_36moterm"),
        F.sum("orders_60moterm_i").alias("orders_60moterm"),
        F.sum("orders_acqmail_i").alias("orders_acqmail"),
        F.sum("orders_renewals_i").alias("orders_renewals"),
        F.sum("orders_altmedia_i").alias("orders_altmedia"),
        F.sum("orders_online_i").alias("orders_online"),
        F.sum("orders_serviceprovider_i").alias("orders_serviceprovider"),
        F.sum("orders_winback_i").alias("orders_winback"),
        F.sum("orders_cl_i").alias("orders_cl"),
        F.sum("orders_dm_i").alias("orders_dm"),
        F.sum("orders_sps_i").alias("orders_sps"),
        F.sum("orders_am_i").alias("orders_am")
    )
    
    df_allorderdata = df_allorderdata_agg.join(df_order_curr_date2.select("mid_key", "order_num", "order_type", "order_term", "new_kx_create_dt"), on="mid_key", how="left")
    df_allorderdata.write.format("delta").mode("overwrite").saveAsTable("mulinter.allorderdata")

    spark.sql("DROP TABLE IF EXISTS mulinter.orders")
    spark.sql("DROP TABLE IF EXISTS mulinter.orders2")
    spark.sql("DROP TABLE IF EXISTS mulinter.order_curr_date")
    spark.sql("DROP TABLE IF EXISTS mulinter.order_curr_date2")

    sql_query_life_engage_sum = f"""
    select mid_key, count(distinct svc_prov) as life_engage_svcprov_12mo_nps
	from {ref3}.f_lifestyle_engagement
	where cast(tran_dt as date) between (date'{ONEBEGDT}') and date'{enddt}'
	and cast(insight_update_dt as date) between (date'{ONEBEGDT}') and date'{enddt}'
	group by mid_key
    """
    df_lifestyle_engagement_sum = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_life_engage_sum}) as subq").option("user", usern).option("password", passw).load()
    df_lifestyle_engagement_sum.write.format("delta").mode("overwrite").saveAsTable("mulinter.lifestyle_engagement_sum")

    sql_query_life_engage_6mo = f"""
    select mid_key, count(distinct svc_prov) as life_engage_svcprov_3mo_6mo
	from {ref3}.f_lifestyle_engagement
	where cast(tran_dt as date) between (date'{enddt}' - 182) and (date'{enddt}' - 91)
	group by mid_key
    """
    df_lifestyle_engage_prov_sum_6mo = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_life_engage_6mo}) as subq").option("user", usern).option("password", passw).load()
    df_lifestyle_engage_prov_sum_6mo.write.format("delta").mode("overwrite").saveAsTable("mulinter.lifestyle_engage_prov_sum_6mo")

    sql_query_life_engage_1mo = f"""
    select mid_key, count(svc_prov) as life_engage_1mo
	from {ref3}.f_lifestyle_engagement
	where cast(tran_dt as date) between (date'{enddt}' - 30) and date'{enddt}'
		and cast(insight_update_dt as date) between (date'{enddt}' - 30) and date'{enddt}'
	group by mid_key
    """
    df_lifestyle_engagement_sum_1mo = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_life_engage_1mo}) as subq").option("user", usern).option("password", passw).load()
    df_lifestyle_engagement_sum_1mo.write.format("delta").mode("overwrite").saveAsTable("mulinter.lifestyle_engagement_sum_1mo")

    sql_query_life_engage_3mo = f"""
    select mid_key, count(distinct svc_prov) as life_engage_svcprov_3mo,
	     count(svc_prov) as life_engage_3mo
	from {ref3}.f_lifestyle_engagement
	where cast(tran_dt as date) between (date'{enddt}' - 91) and date'{enddt}'
	group by mid_key
    """
    df_lifestyle_engagement_sum_3mo = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_life_engage_3mo}) as subq").option("user", usern).option("password", passw).load()
    df_lifestyle_engagement_sum_3mo.write.format("delta").mode("overwrite").saveAsTable("mulinter.lifestyle_engagement_sum_3mo")
    
    sql_query_life_engage_6mo_count = f"""
    select mid_key, count(svc_prov) as life_engage_6mo
	from {ref3}.f_lifestyle_engagement
	where cast(tran_dt as date) between (date'{enddt}' - 182) and date'{enddt}'
	group by mid_key
    """
    df_lifestyle_engagement_sum_6mo = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_life_engage_6mo_count}) as subq").option("user", usern).option("password", passw).load()
    df_lifestyle_engagement_sum_6mo.write.format("delta").mode("overwrite").saveAsTable("mulinter.lifestyle_engagement_sum_6mo")

    sql_query_life_engage_12mo = f"""
    select mid_key, count(distinct svc_prov) as life_engage_svcprov_12mo,
     sum(tot_amt) as life_engage_sumamt_12mo,
	  count(svc_prov) as life_engage_12mo
	from {ref3}.f_lifestyle_engagement
	where cast(tran_dt as date) between (date'{enddt}' - 365) and date'{enddt}'
	group by mid_key
    """
    df_lifestyle_engagement_sum_12mo = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_life_engage_12mo}) as subq").option("user", usern).option("password", passw).load()
    df_lifestyle_engagement_sum_12mo.write.format("delta").mode("overwrite").saveAsTable("mulinter.lifestyle_engagement_sum_12mo")

    sql_query_activities_sum = f"""
    select mid_key, count(ss_key) as activities_3mo
	from {ref3}.f_activity
	where cast(activity_dt as date) between (date'{enddt}' - 90) and date'{enddt}'
	group by mid_key
    """
    df_activities_sum = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_activities_sum}) as subq").option("user", usern).option("password", passw).load()
    df_activities_sum.write.format("delta").mode("overwrite").saveAsTable("mulinter.activities_sum")

    df_gender_pred = spark.read.csv("/vg01/aarp_sas/ftp/incoming/Gender_Prediction.csv", header=True, inferSchema=True)
    
    sql_query_mid_key_indiv = f"""
    select distinct c.mid_key,
    cast(a.ibx_age_in_two_year_increments_2nd_individual_premier as char(2)) as ibx_age_in_two_year_increments_2nd_individual_premier,
    cast(a.ibx_children_age_ranges_present_in_household_premier as char(15)) as ibx_children_age_ranges_present_in_household_premier,
    cast(a.ibx_children_presence_of_household as char(1)) as ibx_children_presence_of_household,
    cast(a.IBX_CREDIT_CARD_IDT_PREMIER as char(8)) as IBX_CREDIT_CARD_IDT_PREMIER,
    cast(a.IBX_DONATION_CONTRIBUTION as char(1)) as IBX_DONATION_CONTRIBUTION,
    cast(a.IBX_GREEN_LIVING as char(1)) as IBX_GREEN_LIVING,
    cast(a.IBX_HEALTH_BEAUTY as char(1)) as IBX_HEALTH_BEAUTY,
    cast(a.IBX_HEALTH_HOMEOPATHIC as char(1)) as IBX_HEALTH_HOMEOPATHIC,
    cast(a.ibx_home_lot_square_footage_ranges as char(1)) as ibx_home_lot_square_footage_ranges,
    cast(a.IBX_HOME_MARKET_VALUE_DECILES as char(2)) as IBX_HOME_MARKET_VALUE_DECILES,
    cast(a.IBX_HOME_MARKET_VALUE_PREMIER as char(1)) as IBX_HOME_MARKET_VALUE_PREMIER,
    cast(a.IBX_HOME_OWNER_RENTER_PREMIER as char(1)) as IBX_HOME_OWNER_RENTER_PREMIER,
    cast(a.IBX_HOME_OWNER as char(1)) as IBX_HOME_OWNER,
    cast(a.IBX_HOME_YEAR_BUILT_ACTUAL as char(4)) as IBX_HOME_YEAR_BUILT_ACTUAL,
    cast(a.IBX_INFERRED_HOUSEHOLD_RANK as char(1)) as IBX_INFERRED_HOUSEHOLD_RANK,
    cast(a.ibx_investing_finance_grouping_premier as char(1)) as ibx_investing_finance_grouping_premier,
    cast(a.IBX_NETWORTH_PREMIER as char(1)) as IBX_NETWORTH_PREMIER,
    cast(a.IBX_NUM_OF_LINES_OF_CREDIT as char(1)) as IBX_NUM_OF_LINES_OF_CREDIT,
    cast(a.ibx_occupation_1st_individual_premier as char(1)) as ibx_occupation_1st_individual_premier,
    cast(a.IBX_OWNER_TYPE_DETAIL as char(1)) as IBX_OWNER_TYPE_DETAIL,
    cast(a.IBX_PERSONIC_CLUSTER as char(2)) as IBX_PERSONIC_CLUSTER,
    cast(a.IBX_HOME_ASSESSED_VALUE_RANGES as char(1)) as IBX_HOME_ASSESSED_VALUE_RANGES,
    cast(a.IBX_MOVIE_MUSIC_GROUPING as char(1)) as IBX_MOVIE_MUSIC_GROUPING,
    cast(a.ibx_race_cd_input_individual_premier as char(1)) as ibx_race_cd_input_individual_premier,
    cast(a.IBX_TOTAL_ONLINE_PURCHASES as char(3)) as IBX_TOTAL_ONLINE_PURCHASES,
    cast(a.IBX_PC_OWNER_PREMIER as char(1)) as IBX_PC_OWNER_PREMIER,
    cast(a.ibx_weeks_since_last_online_order as char(3)) as ibx_weeks_since_last_online_order,
    cast(a.ibx_retail_purchases_most_frequent_cat as char(2)) as ibx_retail_purchases_most_frequent_cat,
    cast(a.IBX_HOME_LOAN_AMOUNT_1_RANGES as char(1)) as IBX_HOME_LOAN_AMOUNT_1_RANGES,
    cast(a.IBX_HOUSEHOLD_INCOME as char(1)) as IBX_HOUSEHOLD_INCOME,
    cast(a.IBX_HEALTH_DIABETIC as char(1)) as IBX_HEALTH_DIABETIC,
    cast(a.ibx_current_affairs_politics_premier as char(1)) as ibx_current_affairs_politics_premier,
    cast(a.IBX_ONLINE_AVERAGE_AMT_PER_ORDER as char(6)) as IBX_ONLINE_AVERAGE_AMT_PER_ORDER,
    cast(a.ibx_education_input_individual_premier as char(1)) as ibx_education_input_individual_premier,
    cast(a.ibx_membership_clubs as char(1)) as ibx_membership_clubs,
    cast(a.IBX_PETS as char(1)) as IBX_PETS,
    cast(a.ibx_adults_number_of_household_premier as char(1)) as ibx_adults_number_of_household_premier,
    cast(a.ibx_political_party_input_individual as char(1)) as ibx_political_party_input_individual,
    cast(a.ibx_vehicle_known_owned_number_premier as char(1)) as ibx_vehicle_known_owned_number_premier,
    cast(a.IBX_RELIGIOUS_INSPIRATIONAL_PREMier as char(1)) as IBX_RELIGIOUS_INSPIRATIONAL_PREMier,
    cast(a.ibx_trends_for_telecom_internet_user as char(2)) as ibx_trends_for_telecom_internet_user,
    cast(a.ibx_home_property_type_details as char(1)) as ibx_home_property_type_details,
    cast(a.ibx_education_1st_individual as char(1)) as ibx_education_1st_individual,
    cast(a.ibx_trends_for_telecom_optional_calling_services as char(2)) as ibx_trends_for_telecom_optional_calling_services,
    cast(a.IBX_HEALTH_MEDICAL_SUPPLIES as char(1)) as IBX_HEALTH_MEDICAL_SUPPLIES,
    cast(a.ibx_health_nutraceuticals_vitamins as char(1)) as ibx_health_nutraceuticals_vitamins,
    cast(a.IBX_RECREATIONAL_VEHICLES_PREMIEr as char(1)) as IBX_RECREATIONAL_VEHICLES_PREMIEr,
    a.ibx_base_record_verification_dt,
    b.november_general_election_day_age,
    b.MAIL_READERSHIP_MODEL,
    b.EDUCATIONAL_ATTAINMENT_MODEL as EDUCATIONAL_ATTAINMENT_MODEL_i,
    b.GENERAL_ACTIVISM_MODEL as GENERAL_ACTIVIST_MODEL,
    b.GUN_OWNERSHIP_MODEL as GUN_OWNERSHIP_MODEL_i,
    b.HUNTER_MODEL,
    b.hunter_model as hunter_model_char,
    b.sy_otsbn_polfund,
    b.confidence,
    b.general_election_vote_propensity_model_2020,
    b.UNINSURED_MODEL,
    cast(b.ETHNICITY as char(50)) as ETHNICITY,
    c.AGE_AGG_IND,
    cast(c.GENDER_AGG_IND as char(1)) as gender_agg_ind_o,
    cast(c.ibx_education as char(1)) as ibx_education,
    cast(c.ftc_dnc_append_fl as char(1)) as ftc_dnc_append_fl,
    c.LIKELY_HISP_AGG,
    c.LIKELY_BLACK_AGG,
    cast(c.Lifestage_Segment as char(1)) as Lifestage_Segment,
    cast(a.ibx_age_input_individual_default_1st_individual_premier as char(3)) as ibx_age_input_individual_default_1st_individual_premier,
    cast(a.ibx_age_in_two_year_increments_1st_individual_premier as char(2)) as ibx_age_in_two_year_increments_1st_individual_premier,
    f.VTM_ASSIGNMENT_LAST_END_DT,
    g.serv_end_dt,
    cast(a.IBX_HEALTH_ORTHOPEDIC as char(1)) as IBX_HEALTH_ORTHOPEDIC,
    cast(a.IBX_INVESTORS_HIGHLY_LIKELY as char(1)) as IBX_INVESTORS_HIGHLY_LIKELY,
    cast(c.health_ind as char(1)) as health_ind,
    cast(a.ibx_health_cholesterol as char(1)) as ibx_health_cholesterol,
    cast(a.ibx_race_cd_1st_individual_premier as char(4)) as ibx_race_cd_1st_individual_premier,
    b.income_model,
    cast(a.IBX_INVESTORS_LIKELY as char(4)) as IBX_INVESTORS_LIKELY,
    a.ibx_health_organic,
    c.ibx_outdoors_dimension,
    c.ibx_veteran,
    a.ibx_electronics_company_grouping_premier,
    a.ibx_health_diet,
    b.spanish_speaker_model,
    b.likely_landline_connectivity_score,
    b.likely_cell_assignment_score,
    b.likely_landline_assignment_score,
    b.race_confidence_numeric
    from {ref3}.d_individual c
    left join {ref3}.d_acxiom_demographics_ind a on c.mid_key=a.mid_key
    left join {ref3}.d_catalist_voter_model b on c.mid_key=b.mid_key
    left join {ref3}.d_vtm_individual f on c.mid_key=f.mid_key
    left join {ref3}.f_vmis g on c.mid_key=g.mid_key
    """
    df_mid_key_indiv_raw = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_mid_key_indiv}) as subq").option("user", usern).option("password", passw).load()
    
    df_mid_key_indiv_joined = df_mid_key_indiv_raw.join(df_gender_pred, on="mid_key", how="left")

    df_mid_key_indiv_transformed = df_mid_key_indiv_joined \
        .withColumn("dvr", F.lit(None).cast("double")) \
        .withColumn("internet", F.lit(None).cast("double")) \
        .withColumn("radio", F.lit(None).cast("double")) \
        .withColumn("smartphone", F.lit(None).cast("double")) \
        .withColumn("tv", F.lit(None).cast("double")) \
        .withColumn("density_clusters", F.lit("")) \
        .withColumn("work_clusters", F.lit("")) \
        .withColumn("religious", F.lit(None).cast("double")) \
        .withColumn("cable", F.lit(None).cast("double")) \
        .withColumn("game_shows", F.lit(None).cast("double")) \
        .withColumn("kids_shows", F.lit(None).cast("double")) \
        .withColumn("educational_attainment_model", F.col("EDUCATIONAL_ATTAINMENT_MODEL_i") / 100) \
        .withColumn("gun_ownership_model", F.col("GUN_OWNERSHIP_MODEL_i") / 160) \
        .withColumn("gender_agg_ind", F.when((F.col("gender_agg_ind_o") == 'U') & (F.col("probability") > 0.8), F.col("gender_ind")).otherwise(F.col("gender_agg_ind_o"))) \
        .withColumn("HUNTER_MODEL", 
            F.when(F.col("HUNTER_MODEL") == 'Unlikely Hunter', 0.29)
             .when(F.col("HUNTER_MODEL") == 'Possibly Hunter', 0.72)
             .when(F.col("HUNTER_MODEL") == 'Likely Hunter', 0.93)
             .when(F.col("HUNTER_MODEL") == 'Hunter', 1.0)
             .otherwise(None)
        ) \
        .withColumn("race_confidence_numeric", F.col("race_confidence_numeric") * 100)
    
    df_mid_key_indiv = df_mid_key_indiv_transformed.drop("EDUCATIONAL_ATTAINMENT_MODEL_i", "GUN_OWNERSHIP_MODEL_i")
    df_mid_key_indiv.write.format("delta").mode("overwrite").saveAsTable("mulinter.mid_key_indiv")

    muldate_obj = datetime.strptime(muldate, "%Y-%m-%d") # Assuming YYYY-MM-DD format
    alpha = muldate_obj.day
    if 11 <= alpha < 18:
        df_d_individual = spark.table("unica.d_individual")
        df_gender_pred_out = df_d_individual.join(df_gender_pred, on="mid_key", how="left") \
            .select(
                F.col("a.mid_key"),
                F.substring(F.coalesce(F.col("b.gender_ind"), F.col("a.gender_agg_ind")), 1, 1).alias("gender_ind"),
                F.coalesce(F.col("b.probability"), F.lit(1)).alias("probability")
            )
        df_gender_pred_out.write.format("delta").mode("overwrite").saveAsTable("aarpdata.gender_pred")
        
        spark.sql("DROP TABLE IF EXISTS sandbox.GENDER_PRED_SX")
        df_gender_pred_for_sandbox = spark.table("aarpdata.gender_pred") \
            .withColumn("effective_date", F.lit(muldate))
        # This is a conceptual representation of writing to Redshift with bulk load options
        # Actual implementation depends on the specific Databricks Redshift connector configuration
        df_gender_pred_for_sandbox.write \
            .format("com.databricks.spark.redshift") \
            .option("url", "jdbc:redshift://...") \
            .option("dbtable", "sandbox.GENDER_PRED_SX") \
            .option("tempdir", "s3a://...") \
            .option("aws_iam_role", "arn:aws:iam::...") \
            .mode("overwrite") \
            .save()

    sql_query_mid_cid = f"""
    select mid_key, cid_key, hid_key,preferred_chid
	from {ref3}.f_joiner
    where preferred_chid <> 0
	order by mid_key
    """
    df_mid_cid = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_mid_cid}) as subq").option("user", usern).option("password", passw).load()
    df_mid_cid.write.format("delta").mode("overwrite").saveAsTable("mulinter.mid_cid")

    sql_query_hid_appends = f"""
    select 
     hid_key,
    cast(IBX_ADULT_AGE_55_64_AGG_HHD as Char(1)) as IBX_ADULT_AGE_55_64_AGG_HHD,
    cast(IBX_ADULT_AGE_65_74_AGG_HHD as Char(1)) as IBX_ADULT_AGE_65_74_AGG_HHD,
    cast(IBX_ADULT_AGE_75_P_AGG_HHD as Char(1)) as IBX_ADULT_AGE_75_P_AGG_HHD,
    cast(IBX_ADULTS_NUM_AGG_HHD as Char(1)) as IBX_ADULTS_NUM_AGG_HHD,
    cast(IBX_CHILD_AGE_00_05_AGG_HHD as Char(1)) as IBX_CHILD_AGE_00_05_AGG_HHD,
    cast(IBX_CHILD_AGE_06_10_AGG_HHD as Char(1)) as IBX_CHILD_AGE_06_10_AGG_HHD,
    cast(IBX_CHILD_NUM_AGG_HHD as Char(1)) as IBX_CHILD_NUM_AGG_HHD,
    cast(IBX_CHILD_PRESENCE_AGG_HHD as Char(1)) as IBX_CHILD_PRESENCE_AGG_HHD,
    cast(IBX_COMMUNITY_CHARITIES_AGG_HHD as Char(1)) as IBX_COMMUNITY_CHARITIES_AGG_HHD,
    cast(IBX_COMMUNITY_INVOLVEMENT_AID_AGG_HHD as Char(1)) as IBX_COMMUNITY_INVOLVEMENT_AID_AGG_HHD,
    cast(IBX_COMMUNITY_INVOLVEMENT_ANIMAL_AGG_HHD as Char(1)) as IBX_COMMUNITY_INVOLVEMENT_ANIMAL_AGG_HHD,
    cast(IBX_COMMUNITY_INVOLVEMENT_CHILDREN_AGG_HHD as Char(1)) as IBX_COMMUNITY_INVOLVEMENT_CHILDREN_AGG_HHD,
    cast(IBX_COMMUNITY_INVOLVEMENT_CULTURE_AGG_HHD as Char(1)) as IBX_COMMUNITY_INVOLVEMENT_CULTURE_AGG_HHD,
    cast(IBX_COMMUNITY_INVOLVEMENT_ENVIRONMENT_AGG_HHD as Char(1)) as IBX_COMMUNITY_INVOLVEMENT_ENVIRONMENT_AGG_HHD,
    cast(IBX_COMMUNITY_INVOLVEMENT_HEALTH_AGG_HHD as Char(1)) as IBX_COMMUNITY_INVOLVEMENT_HEALTH_AGG_HHD,
    cast(IBX_COMMUNITY_INVOLVEMENT_LIBERAL_AGG_HHD as Char(1)) as IBX_COMMUNITY_INVOLVEMENT_LIBERAL_AGG_HHD,
    cast(IBX_COMMUNITY_INVOLVEMENT_POLITICAL_AGG_HHD as Char(1)) as IBX_COMMUNITY_INVOLVEMENT_POLITICAL_AGG_HHD,
    cast(IBX_COMMUNITY_INVOLVEMENT_RELIGIOUS_AGG_HHD as Char(1)) as IBX_COMMUNITY_INVOLVEMENT_RELIGIOUS_AGG_HHD,
    cast(IBX_COMMUNITY_INVOLVEMENT_VETERAN_AGG_HHD as Char(1)) as IBX_COMMUNITY_INVOLVEMENT_VETERAN_AGG_HHD,
    cast(IBX_CREDIT_CARD_FREQ_AGG_HHD as Char(7)) as IBX_CREDIT_CARD_FREQ_AGG_HHD,
    cast(IBX_DWELLING_TYPE_AGG_HHD as Char(1)) as IBX_DWELLING_TYPE_AGG_HHD,
    cast(IBX_ELDERLY_PARENT_AGG_HHD as Char(1)) as IBX_ELDERLY_PARENT_AGG_HHD,
    cast(IBX_GRAND_CHILDREN_AGG_HHD as Char(1)) as IBX_GRAND_CHILDREN_AGG_HHD,
    cast(IBX_HOME_BUSINESS_AGG_HHD as Char(1)) as IBX_HOME_BUSINESS_AGG_HHD,
    cast(IBX_HOME_MARKET_VALUE_DECILES_AGG_HHD as Char(2)) as IBX_HOME_MARKET_VALUE_DECILES_AGG_HHD,
    cast(IBX_HOME_PURCHASED_AMT_RANGES_AGG_HHD as Char(1)) as IBX_HOME_PURCHASED_AMT_RANGES_AGG_HHD,
    cast(IBX_HOME_VALUE_RANGES_AGG_HHD as Char(1)) as IBX_HOME_VALUE_RANGES_AGG_HHD,
    cast(IBX_INCOME_ESTIMATED_NARROW_RANGES_AGG_HHD as Char(1)) as IBX_INCOME_ESTIMATED_NARROW_RANGES_AGG_HHD,
    cast(IBX_INVESTMENT_AGG_HHD as Char(1)) as IBX_INVESTMENT_AGG_HHD,
    cast(IBX_LENGTH_OF_RESIDENCE_AGG_HHD as Char(2)) as IBX_LENGTH_OF_RESIDENCE_AGG_HHD,
    cast(IBX_MAIL_BUYER_CAT_HEALTH_AGG_HHD as Char(1)) as IBX_MAIL_BUYER_CAT_HEALTH_AGG_HHD,
    cast(IBX_NETWORTH_PREMIER_AGG_HHD as Char(1)) as IBX_NETWORTH_PREMIER_AGG_HHD,
    cast(IBX_OCCUPATION_INPUT_AGG_HHD as Char(1)) as IBX_OCCUPATION_INPUT_AGG_HHD,
    cast(IBX_OUTDOORS_DIMENSION_AGG_HHD as Char(1)) as IBX_OUTDOORS_DIMENSION_AGG_HHD,
    cast(IBX_PETS_AGG_HHD as Char(1)) as IBX_PETS_AGG_HHD,
    cast(IBX_PRESENCE_OF_SENIOR_ADULT_AGG_HHD as Char(1)) as IBX_PRESENCE_OF_SENIOR_ADULT_AGG_HHD,
    cast(IBX_PROPERTY_TYPE_AGG_HHD as Char(1)) as IBX_PROPERTY_TYPE_AGG_HHD,
    cast(IBX_TELECOM_20PCT_LONG_DISTANCE_AGG_HHD as Char(2)) as IBX_TELECOM_20PCT_LONG_DISTANCE_AGG_HHD,
    cast(IBX_TELECOM_CALLING_SERVICES_AGG_HHD as Char(2)) as IBX_TELECOM_CALLING_SERVICES_AGG_HHD,
    cast(IBX_TELECOM_CELLULAR_AGG_HHD as Char(2)) as IBX_TELECOM_CELLULAR_AGG_HHD,
    cast(IBX_TELECOM_INTERNET_AGG_HHD as Char(2)) as IBX_TELECOM_INTERNET_AGG_HHD,
    cast(IBX_TRAVEL_CRUISE_AGG_HHD as Char(1)) as IBX_TRAVEL_CRUISE_AGG_HHD,
    cast(IBX_TRAVEL_TYPE_AGG_HHD as Char(1)) as IBX_TRAVEL_TYPE_AGG_HHD,
    cast(IBX_VEHICLE_DOMINANT_AGG_HHD as Char(1)) as IBX_VEHICLE_DOMINANT_AGG_HHD,
    cast(IBX_VEHICLE_OWNED_AGG_HHD as Char(1)) as IBX_VEHICLE_OWNED_AGG_HHD,
    cast(IBX_VEHICLE_TRUCK_MC_RV_AGG_HHD as Char(3)) as IBX_VEHICLE_TRUCK_MC_RV_AGG_HHD,
    cast(IBX_WORKING_WOMAN_AGG_HHD as Char(1)) as IBX_WORKING_WOMAN_AGG_HHD,
    MAX_INDV_INSIGHT_UPDATE_DT,
    cast(IBX_BUSINESS_OWNER_AGG_HHD as Char(1)) as IBX_BUSINESS_OWNER_AGG_HHD,
    cast(IBX_CREDIT_CARD_FREQ_24_P_AGG_HHD as Char(1)) as IBX_CREDIT_CARD_FREQ_24_P_AGG_HHD,
    cast(IBX_HEALTHY_BEHAVIOUR_AGG_HHD as Char(1)) as IBX_HEALTHY_BEHAVIOUR_AGG_HHD,
    cast(IBX_ADULT_AGE_45_54_AGG_HHD as Char(1)) as IBX_ADULT_AGE_45_54_AGG_HHD,
    cast(IBX_PRESENCE_OF_YOUNG_ADULT_AGG_HHD as Char(1)) as IBX_PRESENCE_OF_YOUNG_ADULT_AGG_HHD,
    cast(IBX_CURRENT_AFFAIRS_AGG_HHD as Char(1)) as IBX_CURRENT_AFFAIRS_AGG_HHD,
    cast(IBX_PC_USER_FL_AGG_HHD as Char(1)) as IBX_PC_USER_FL_AGG_HHD,
    cast(IBX_FINANCIAL_INVESTOR_AGG_HHD as char(1)) as IBX_FINANCIAL_INVESTOR_AGG_HHD,
    cast(ibx_home_equity_available_agg_hhd as char(1)) as ibx_home_equity_available_agg_hhd,
    cast(ibx_home_loan_interest_rt_agg_hhd as char(1)) as ibx_home_loan_interest_rt_agg_hhd,
    cast(IBX_HOME_GARDEN_AGG_HHD as char(1)) as IBX_HOME_GARDEN_AGG_HHD,
    cast(ibx_adult_age_25_34_agg_hhd as char(1)) as ibx_adult_age_25_34_agg_hhd,
    cast(IBX_TELECOM_LONG_DISTANCE_AGG_HHD as Char(2)) as IBX_TELECOM_LONG_DISTANCE_AGG_HHD,
    cast(ibx_child_age_11_15_agg_hhd as char(1)) as ibx_child_age_11_15_agg_hhd,
    cast(ibx_community_involvement_conservative_agg_hhd as char(1)) as ibx_community_involvement_conservative_agg_hhd,
    cast(IBX_ADULT_AGE_18_24_AGG_HHD as char(1)) as IBX_ADULT_AGE_18_24_AGG_HHD,
    cast(ibx_household_size_agg_hhd as char(4)) as ibx_household_size_agg_hhd,
    ibx_investors_highly_likely_agg_hhd,
    ibx_child_age_16_17_agg_hhd
	from {ref3}.d_acxiom_demographics_hhd demo
    """
    df_hid_key_appends = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_hid_appends}) as subq").option("user", usern).option("password", passw).load()
    df_hid_key_appends.write.format("delta").mode("overwrite").saveAsTable("mulinter.hid_key_appends")

    sql_query_household_weekly = f"""
    select house.geo_cd_2010,a.hid_key,representative_party_cd,
			CENS_AGE_HH_PERCENT_WITH_HOUSEHOLDER_AGE_15_24,
			CENS_AGE_POP_MEDIAN_AGE_OF_FEMALes,
			CENS_AGE_POP_MEDIAN_AGE_OF_MALES,
			CENS_AGE_POP_PERCENT_45_54,
			CENS_AGE_POP_PERCENT_55_59,
			CENS_AGE_POP_PERCENT_55_64,
			CENS_AGE_POP_PERCENT_60_64,
			cens_age_pop_percent_65_99_plus,
			CENS_BUILT_HU_PERCENT_BUILT_2000_TO_2004,
			CENS_BUILT_HU_PERCENT_BUILT_LT1940,
			CENS_CHILD_HH_PERCENT_FAM_WITH_PERSONS_LT18,
			CENS_CHILD_HH_PERCENT_FEMALE_HOH_FAM_WITH_PERSONS_LT18,
			CENS_CHILD_HH_PERCENT_WITHOUT_PERSONS_LT18,
			CENS_COMMUTE_COMMUTER_AVG_TRAV_TIME_TO_WORK,
			CENS_COMMUTE_COMMUTER_PERCENT_TRAV_TO_WORK_LT_30_MIN,
			CENS_COMMUTE_WRKRS_PERCENT_CARPOOLED_TO_WORK,
			CENS_COMMUTE_WRKRS_PERCENT_PUBLIC_TRANS_TO_WORK,
			CENS_COUNT_POPULATION,
			CENS_COUNT_RENTAL_UNITS,
			CENS_COUNT_WORKERS,
			CENS_DENSITY_PERSONS_PER_HH_FOR_POP_IN_HH,
			CENS_DENSITY_POPULATION_PER_SQUARE_MILE,
			CENS_EARN_HH_PERCENT_NO_EARNINGS,
			CENS_EARN_HH_PERCENT_NO_OTHER_TYPE_OF_INCOME,
			CENS_EARN_HH_PERCENT_NO_WAGE_SALARY_INCOME,
			CENS_EARN_HH_PERCENT_WITH_EARNINGS,
			CENS_EARN_HH_PERCENT_WITH_PUBLIC_ASSISTANCE_INCOME,
			CENS_EDUC_POP25_PLUS_MEDIAN_EDUCATION_ATTAINED,
			CENS_EDUC_POP25_PLUS_PERCENT_BACHELOR_DEGREE,
			CENS_EDUC_POP25_PLUS_PERCENT_PROF_DEGREE,
			CENS_EMPLOY_LABF_PERCENT_EMPLOYEd,
			CENS_EMPLOY_LABF_PERCENT_UNEMPLOyed,
			CENS_EMPLOY_POP18_PLUS_PERCENT_CIVILIAN_VETS,
			cens_ethnic_pop_percent_black_only,
			CENS_ETHNIC_POP_PERCENT_HI_NAT_OTH_PAC_ONLY,
			CENS_ETHNIC_POP_PERCENT_HISPANIC,
			cens_ethnic_pop_percent_non_hispanic,
			CENS_ETHNIC_POP_PERCENT_SOME_OTHER_RACE_ONLY,
			CENS_ETHNIC_POP_PERCENT_WHITE_ONly,
			CENS_GENDER_POP_PERCENT_FEMALE,
			CENS_HEAT_OCCHU_PERCENT_OIL_OR_KEROSENE_HEAT,
			CENS_HEAT_OCCHU_PERCENT_SOLAR_HEat, 
			CENS_HEAT_OCCHU_PERCENT_UTILITY_gas_heat,
			CENS_HHSIZE_HH_PERCENT_2_PERSONS,
			CENS_HOMVAL_HOME_VALUE_CBSA_INDEx,
			CENS_HOMVAL_OOHU_MEDIAN_HOME_VALue,
			CENS_HOMVAL_OOHU_PERCENT_HOME_VALUE_UNDER_10K,
			CENS_HUSTR_HU_PERCENT_2_UNITS,
			CENS_INC_FAMILY_INC_STATE_DECILE,
			cens_inc_hh_median_family_household_income,
			cens_inc_hh_median_household_income,
			CENS_INDUS_EMPLD_PERCENT_ACCOMODATION_AND_FOOD_SVCS,
			CENS_INDUS_EMPLD_PERCENT_EDUCATIONAL_SERVICES,
			CENS_INDUS_EMPLD_PERCENT_FINANCE_AND_INSURANCE,
			CENS_INDUS_EMPLD_PERCENT_MANUFACturing,
			CENS_LANG_HH_PERCENT_ENGLISH_SPEaking,
			CENS_LANG_HH_PERCENT_SPANISH_SPEaking,
			CENS_MARR_POP15_PLUS_PERCENT_SPOUSE_PRESENT,
			CENS_MARR_POP15_PLUS_PERCENT_WIDowed,
			CENS_MORTG_OOHU_PERCENT_NO_MORTGage,
			CENS_MOVE_OCCHU_PERCENT_NEW_LISTings,
			CENS_OCCUP_EMPLD_PERCENT_BUS_AND_FINANCIAL_OPS,
			CENS_OCCUP_EMPLD_PERCENT_HEALTH_DIAG_AND_TREAT_PRACS,
			CENS_OCCUP_EMPLD_PERCENT_SALES_AND_RELATED,
			CENS_OCCUP_EMPLD_PERCENT_TRANS_AND_MATERIAL_MOV_SUPV,
			CENS_RENT_RNTL_MEDIAN_RENT,
			CENS_STATE_CODE,
			CENS_TYP_POP_PERCENT_GRANDCHILD_IN_FAMILY_HH,
			CENS_TYP_POP_PERCENT_STEPCHILD_IN_FAMILY_HH,
			CENS_GRPQTRS_POP_PERCENT_COLLEGE_dorms,
			CENS_INDUS_EMPLD_PERCENT_HLTH_CARE_SOCIAL_ASSISTANCE,
			CENS_COUNT_FAMILY_HOUSEHOLDS,
			CENS_GRPQTRS_POP_PERCENT_MILITARY_QTRS,
			CENS_GRPQTRS_POP_PERCENT_NURSING_homes,
			CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_UNDER_10K, 
			CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_10_14K, 
			CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_15_19K,
			cens_hustr_hu_percent_1_unit_detached,
			cens_tenancy_occhu_percent_owner_OCCUPIED,
			CENS_OCCUP_EMPLD_PERCENT_FIRE_AND_PROT_SVCS_INCL_SUPV,
			CENS_OCCUP_EMPLD_PERCENT_LAW_ENFORCEMENT_INCL_SUPV,
			CENS_OCCUP_EMPLD_PERCENT_FOOD_PREP_AND_SERVING,
			CENS_OCCUP_EMPLD_PERCENT_BLDG_AND_GDS_CLEAN_AND_MTC,
			CENS_OCCUP_EMPLD_PERCENT_PERSONAL_CARE_SVCS,
			CENS_OCCUP_EMPLD_PERCENT_FARM_FISH_AND_FORESTRY,
			CENS_OCCUP_EMPLD_PERCENT_CONSTR_AND_EXTRACT,
			CENS_OCCUP_EMPLD_PERCENT_INSTALL_MAINT_AND_REPAIR,
			CENS_OCCUP_EMPLD_PERCENT_PRODUCTION,
			CENS_OCCUP_EMPLD_PERCENT_MOTOR_VEHICLE_OPS,
			CENS_OCCUP_EMPLD_PERCENT_MATERIAL_MOVING_WORKERS,
			CENS_OCCUP_EMPLD_PERCENT_MANAGEMENT,
			CENS_OCCUP_EMPLD_PERCENT_LEGAL,
			CENS_OCCUP_EMPLD_PERCENT_ARCHITECTURE_AND_ENGINEERING,
			CENS_OCCUP_EMPLD_PERCENT_COMPUTERS_AND_MATH,
			CENS_OCCUP_EMPLD_PERCENT_LIFE_PHYS_AND_SOC_SCIENCES,
			cens_ethnic_pop_percent_asian_only,
			CENS_CENSUS_TRACT,
			CENS_CENSUS_BLOCK_GROUP,
			CENS_AGE_POP_PERCENT_50_54,
			CENS_EDUC_POP25_PLUS_PERCENT_ASSOCIATE_DEGREE,
			CENS_INC_HH_MED_INC_HOUSEHOLDER_AGE_UNDER_25,
			CENS_INDUS_EMPLD_PERCENT_MINING,
			CENS_INDUS_EMPLD_PERCENT_INFORMAtion,
			CENS_MOVE_OCCHU_PERCENT_MOVED_IN_LT1970,
			CENS_MOVE_OCCHU_PERCENT_TURNOVER_LAST_5_YRS,
			CENS_EARN_HH_PERCENT_WITH_SELF_EMPLOYMENT_INCOME,
			CENS_COUNT_HOUSEHOLDS,
			CENS_AGE_POP_PERCENT_30_34,
			CENS_EARN_HH_PERCENT_NO_PUBLIC_ASSISTANCE_INCOME,
			CENS_HEAT_OCCHU_PERCENT_BOTTLE_OR_TANK_LP_GAS_HEAT,
			CENS_HEAT_OCCHU_PERCENT_OTHER_HEat,
			CENS_INDUS_EMPLD_PERCENT_WHOLESAle_trade,
			CENS_INDUS_EMPLD_PERCENT_TRANSPORT_AND_WAREHOUSING,
			CENS_MORTG_OOHU_PERCENT_TWO_MRTGS_AND_HOME_EQUITY_LN,
			CENS_OCCUP_EMPLD_PERCENT_HEALTHCARE_SUPP,
			CENS_URBAN_POP_PERCENT_URBAN_IN_URBAN_AREAS,
			CENS_ETHNIC_HH_PERCENT_HOH_HISPAnic,
			CENS_MARR_POP15_PLUS_PERCENT_NEVER_MARRIED,
			cens_age_pop_percent_35_39,
			cens_lang_hh_percent_span_speak_linguist_isol,
			cens_ethnic_pop_percent_am_ind_ak_nat_only,
			cens_rent_rntl_aggregate_contract_rent,
			cens_commute_wrkrs_percent_work_at_home,
			cens_inc_hh_median_non_family_household_income,
			cens_age_pop_percent_25_34,
			cens_commute_wrkrs_percent_drove_to_work_alone,
			cens_employ_labf_percent_in_armed_forces,
			cens_hustr_hu_percent_1_unit_attached,
			cens_indus_empld_percent_agric_forest_fish_and_hunt,
			cens_indus_empld_percent_construction,
			cens_tenancy_hu_percent_occupied,
			cens_typ_pop_percent_female_hoh_in_family_hh,
			cens_inc_family_inc_state_index,
			cens_employ_popfem16_plus_percent_in_labor_force,
			cens_earn_hh_percent_no_self_employment_income,
			cens_grpqtrs_pop_percent_oth_non_institution_grp_qtrs,
			num_curr_participation_financial_agg_hhd
		from (select coalesce(b.hid_key,c.hid_key) as hid_key
		from {ref3}.d_account a
		left join {ref3}.f_joiner b on a.chid_key=b.chid_key
		full join {ref3}.f_account_order c on a.chid_key=c.chid_key
 		where (preferred_chid=1 and pri_sec=1 and a.account_stat='0' and 
			cast(c.insight_create_Dt as date) >= date'{muldate2}' and
			cast(c.insight_create_Dt as date) <= date'{muldate}'
			and order_num=1)) a
		left join {ref3}.d_household house on a.hid_key=house.hid_key
		left join {ref3}.d_census_2010 cens on house.geo_cd_2010=cens.geo_cd_2010
    """
    df_household_weekly = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_household_weekly}) as subq").option("user", usern).option("password", passw).load()
    df_household_weekly.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("mulinter.household")

    df_f_service_participation = spark.table("unica.F_SERVICE_PARTICIPATION")
    df_sp_engagements = df_f_service_participation.filter(
        (F.to_date(F.col("service_effective_dt")) <= F.current_date()) &
        (F.months_between(F.current_date(), F.to_date(F.col("service_effective_dt"))) <= 12) &
        (F.col("sp_name").isNotNull()) &
        (F.col("active_engagement_flag") == 'A')
    ).groupBy("mid_key").agg(F.countDistinct("service_effective_dt").alias("num_sp"))
    df_sp_engagements.write.format("delta").mode("overwrite").saveAsTable("mulinter.SP_engagements")

df_monthly_engagements = spark.table("cran.monthly_engagements_final").filter(F.col("mid_key").isNotNull() & ~F.col("mid_key").isin(0,1))
df_mid_key_appends_2 = df_bonus_layout.join(
    df_monthly_engagements,
    df_bonus_layout["merkleid"].cast("decimal(10,0)") == df_monthly_engagements["mid_key"],
    "left"
).select(
    df_bonus_layout["*"],
    df_monthly_engagements["aarporg_i"], df_monthly_engagements["activist_i"], df_monthly_engagements["advocacy_donations_12mo"],
    df_monthly_engagements["advocacy_donations_ytd"], df_monthly_engagements["advocacy_donors_12mo_i"], df_monthly_engagements["advocacy_petitions_12mo"],
    df_monthly_engagements["advocacy_signers_12mo_i"], df_monthly_engagements["driver_class_12mo_i"], df_monthly_engagements["driver_class_12mo"],
    df_monthly_engagements["driver_online_12mo_i"], df_monthly_engagements["driver_online_12mo"], df_monthly_engagements["driver_safety_vol_12mo_i"],
    df_monthly_engagements["driver_safety_vol_12mo"], df_monthly_engagements["contact_leg_12mo_i"], df_monthly_engagements["contact_leg_12mo"],
    F.col("EMAILABLE_AGG_IND").cast("string").alias("EMAILABLE_AGG_IND"), df_monthly_engagements["foundation_donors_12mo_i"],
    df_monthly_engagements["foundation_donors_checkb_12mo_i"], df_monthly_engagements["individual_engagers_12mo"], df_monthly_engagements["national_activities_12mo"],
    df_monthly_engagements["national_activity_12mo_i"], df_monthly_engagements["newsletter_opens_cnt_12mo"], df_monthly_engagements["newsletter_opens_cnt_ytd"],
    df_monthly_engagements["num_months"], df_monthly_engagements["other_vol_12mo_i"], df_monthly_engagements["state_activities_12mo"],
    df_monthly_engagements["state_activity_12mo_i"], df_monthly_engagements["suppression"], df_monthly_engagements["teletown_12mo_i"],
    df_monthly_engagements["TERM_AGG_ACT"], df_monthly_engagements["VOTEPROP2016"], df_monthly_engagements["yeas_survey_12mo_i"],
    F.col("MEMBER_FL_AGG_IND").cast("string").alias("MEMBER_FL_AGG_IND"), df_monthly_engagements["community"], df_monthly_engagements["chapters_vol_12mo_i"],
    df_monthly_engagements["exp_corp_vol_12mo_i"], df_monthly_engagements["tax_aid_vol_12mo_i"], df_monthly_engagements["states_vol_12mo_i"],
    df_monthly_engagements["petition_sign_12mo_i"], df_monthly_engagements["petition_col_12mo_i"], df_monthly_engagements["leg_off_vis_12mo_i"],
    df_monthly_engagements["event_host_12mo_i"], df_monthly_engagements["outbound_call_12mo_i"], df_monthly_engagements["survey_resp_12mo_i"],
    df_monthly_engagements["story_sub_12mo_i"], df_monthly_engagements["moviesfg_12mo_i"], df_monthly_engagements["structured_12mo_i"],
    df_monthly_engagements["blockparty_12mo_i"], df_monthly_engagements["state_event_12mo_i"], df_monthly_engagements["popups_12mo_i"],
    df_monthly_engagements["foundation_donations_checkb_12mo"], df_monthly_engagements["moviesfg_12mo"], df_monthly_engagements["structured_12mo"],
    df_monthly_engagements["state_event_12mo"], df_monthly_engagements["survey_resp_12mo"], df_monthly_engagements["states_vol_12mo"],
    df_monthly_engagements["foundation_donations_12mo"], df_monthly_engagements["memorigin"], df_monthly_engagements["auto_renew_start_dt"],
    df_monthly_engagements["teletown_12mo"], df_monthly_engagements["activist"], df_monthly_engagements["petition_col_12mo"],
    df_monthly_engagements["petition_sign_12mo"], df_monthly_engagements["mem_type_agg_act"], df_monthly_engagements["tax_aid_vol_12mo"],
    df_monthly_engagements["popups_12mo"], df_monthly_engagements["foundation_donations_ytd"], df_monthly_engagements["SY_GENERALACTIVIST"],
    df_monthly_engagements["auto_renew_flag"], df_monthly_engagements["blockparty_12mo"]
).withColumn(
    "OriginCode", F.coalesce(F.col("OriginCode"), F.lit(0))
)

df_lifestyle_engagement_sum = spark.table("mulinter.lifestyle_engagement_sum")
df_activities_sum = spark.table("mulinter.activities_sum")
df_lifestyle_engagement_sum_1mo = spark.table("mulinter.lifestyle_engagement_sum_1mo")
df_lifestyle_engagement_sum_3mo = spark.table("mulinter.lifestyle_engagement_sum_3mo")
df_lifestyle_engagement_sum_6mo = spark.table("mulinter.lifestyle_engagement_sum_6mo")
df_lifestyle_engagement_sum_12mo = spark.table("mulinter.lifestyle_engagement_sum_12mo")
df_lifestyle_engage_prov_sum_6mo = spark.table("mulinter.lifestyle_engage_prov_sum_6mo")

df_mid_key_appends1a_2 = df_mid_key_appends_2 \
    .join(df_lifestyle_engagement_sum, df_mid_key_appends_2.merkleid.cast("decimal(10,0)") == df_lifestyle_engagement_sum.mid_key, "left") \
    .join(df_activities_sum, df_mid_key_appends_2.merkleid.cast("decimal(10,0)") == df_activities_sum.mid_key, "left") \
    .join(df_lifestyle_engagement_sum_1mo, df_mid_key_appends_2.merkleid.cast("decimal(10,0)") == df_lifestyle_engagement_sum_1mo.mid_key, "left") \
    .join(df_lifestyle_engagement_sum_3mo, df_mid_key_appends_2.merkleid.cast("decimal(10,0)") == df_lifestyle_engagement_sum_3mo.mid_key, "left") \
    .join(df_lifestyle_engagement_sum_6mo, df_mid_key_appends_2.merkleid.cast("decimal(10,0)") == df_lifestyle_engagement_sum_6mo.mid_key, "left") \
    .join(df_lifestyle_engagement_sum_12mo, df_mid_key_appends_2.merkleid.cast("decimal(10,0)") == df_lifestyle_engagement_sum_12mo.mid_key, "left") \
    .join(df_lifestyle_engage_prov_sum_6mo, df_mid_key_appends_2.merkleid.cast("decimal(10,0)") == df_lifestyle_engage_prov_sum_6mo.mid_key, "left")

df_advomodel_ctc_hist = spark.table(f"weiss.advomodel_ctc_hist_{year2}{mon}_weiss").filter(F.col("mid_key").isNotNull() & ~F.col("mid_key").isin(0,1))
df_vq_emu = spark.table(f"{ref}.vq_emu")

df_mid_key_appends2_2 = df_mid_key_appends1a_2.join(
    df_advomodel_ctc_hist.select("Mid_High", "mid_key", "totalmailings", "appealnewsmail", "LapsMail", "prospmail", "acknow", "advo_petition"),
    df_mid_key_appends1a_2.merkleid.cast("decimal(10,0)") == df_advomodel_ctc_hist.mid_key, "left"
).join(
    df_vq_emu.select("mid_key").alias("emu"),
    df_mid_key_appends1a_2.merkleid.cast("decimal(10,0)") == F.col("emu.mid_key"), "left"
).withColumn(
    "emu_indicator", F.when(F.col("emu.mid_key").isNotNull(), 'Y').otherwise('N')
)

df_wkly_demos = spark.table("aarpdata.wkly_demos").filter(F.col("merkleid").isNotNull() & ~F.col("merkleid").isin("", "0", "1"))
df_allorderdata = spark.table("mulinter.allorderdata")
df_mid_key_indiv = spark.table("mulinter.mid_key_indiv")
df_sp_engagements = spark.table("mulinter.SP_engagements")
df_democurr = spark.table(f"aarpdata.{democurr}")

df_mid_key_appends3_2 = df_mid_key_appends2_2 \
    .join(df_wkly_demos.alias("i"), "merkleid", "left") \
    .join(df_allorderdata.alias("k"), df_mid_key_appends2_2.merkleid.cast("decimal(10,0)") == F.col("k.mid_key"), "left") \
    .join(df_mid_key_indiv.alias("zz"), df_mid_key_appends2_2.merkleid.cast("decimal(10,0)") == F.col("zz.mid_key"), "left") \
    .join(df_sp_engagements.alias("sp"), df_mid_key_appends2_2.merkleid.cast("decimal(10,0)") == F.col("sp.mid_key"), "left") \
    .join(df_democurr.alias("demo"), df_mid_key_appends2_2.merkleid.cast("decimal(10,0)") == F.col("demo.mid_key"), "left") \
    .withColumn("hitech_merch", F.substring(F.col("i.mail_order_categories"), 1, 1)) \
    .withColumn("pc_prdct_buyer", F.substring(F.col("i.mail_order_categories"), 16, 1)) \
    .withColumn("Env_Humant_Educ", F.substring(F.col("i.COMMUNITY_INVOLVEMENT_CAUSES_SUP"), 2, 1)) \
    .withColumn("political", F.substring(F.col("i.COMMUNITY_INVOLVEMENT_CAUSES_SUP"), 4, 1)) \
    .withColumn("other_donors", F.substring(F.col("i.COMMUNITY_INVOLVEMENT_CAUSES_SUP"), 5, 1)) \
    .withColumn("Home_purch_yr", F.substring(F.col("i.home_purchase_date"), 1, 4).cast("integer")) \
    .withColumn("TRAVEL_FOREIGN_PREMIER", F.col("i.IBX_TRAVEL_FOREIGN_PREMIER")) \
    .withColumn("ibx_vehicle_dominant_lifestyle_p", F.col("i.vehicle_dominant_lifestyle"))
    
df_mid_cid = spark.table("mulinter.mid_cid")
df_all_donors = spark.table("weiss.all_donors").filter(F.col("cid_key").isNotNull())
df_cid_appends = df_mid_key_appends3_2 \
    .join(df_mid_cid.alias("b"), df_mid_key_appends3_2.merkleid.cast("decimal(10,0)") == F.col("b.mid_key"), "left") \
    .join(df_all_donors.alias("j"), F.col("b.cid_key") == F.col("j.cid_key"), "left") \
    .drop("b.mid_key")

df_cid_appends = df_cid_appends.dropDuplicates(["merkleid"])

df_geo_cid_hid = df_bonus_layout.select("memacctnum", "merkleid") \
    .join(df_cid_appends, on="merkleid", how="left")

df_household = spark.table("mulinter.household")
df_hid_key_appends = spark.table("mulinter.hid_key_appends")

df_geo_appends = df_geo_cid_hid.withColumnRenamed("reppartycd", "reppartycd_bl") \
    .join(df_household.alias("c"), on="hid_key", how="left") \
    .join(df_hid_key_appends.alias("b"), on="hid_key", how="left") \
    .withColumn("CENS_INC_HH_PERCENT_HOUSEHOLD_19", 
        F.coalesce(F.col("CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_UNDER_10K"), F.lit(0)) + 
        F.coalesce(F.col("CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_10_14K"), F.lit(0)) + 
        F.coalesce(F.col("CENS_INC_HH_PERCENT_HOUSEHOLD_INCOME_15_19K"), F.lit(0))) \
    .withColumn("cens_educ_pop25_plus_percent_col", 
        F.coalesce(F.col("cens_educ_pop25_plus_percent_bachelor_degree"), F.lit(0)) + 
        F.coalesce(F.col("cens_educ_pop25_plus_percent_prof_degree"), F.lit(0))) \
    .withColumn("cens_BLUECOLLAR", 
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_FIRE_AND_PROT_SVCS_INCL_SUPV"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_LAW_ENFORCEMENT_INCL_SUPV"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_FOOD_PREP_AND_SERVING"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_BLDG_AND_GDS_CLEAN_AND_MTC"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_PERSONAL_CARE_SVCS"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_FARM_FISH_AND_FORESTRY"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_CONSTR_AND_EXTRACT"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_INSTALL_MAINT_AND_REPAIR"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_PRODUCTION"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_TRANS_AND_MATERIAL_MOV_SUPV"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_MOTOR_VEHICLE_OPS"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_MATERIAL_MOVING_WORKERS"), F.lit(0))) \
    .withColumn("cens_MANAGEMENTPROFESSIONALS", 
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_MANAGEMENT"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_LEGAL"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_ARCHITECTURE_AND_ENGINEERING"), F.lit(0))) \
    .withColumn("cens_WHITECOLLAR", 
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_MANAGEMENT"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_LEGAL"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_ARCHITECTURE_AND_ENGINEERING"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_BUS_AND_FINANCIAL_OPS"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_COMPUTERS_AND_MATH"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_HEALTH_DIAG_AND_TREAT_PRACS"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_LIFE_PHYS_AND_SOC_SCIENCES"), F.lit(0)) +
        F.coalesce(F.col("CENS_OCCUP_EMPLD_PERCENT_SALES_AND_RELATED"), F.lit(0))) \
    .withColumn("mid_key", F.col("merkleid").cast("decimal(10,0)")) \
    .withColumn("reppartycd", F.coalesce(F.col("reppartycd_bl"), F.col("c.representative_party_cd")))

df_geo_appends = df_geo_appends.dropDuplicates(["memacctnum", "merkleid"])

char_cols_to_freq = [
    'ACEV_Flag', 'ACEV_Num', 'advo_segment_cd', 'audio_visual_composite', 'Chase_Num_Active_Particpnts', 
    'Chase_Num_InActive_Particpnts', 'CurrentPartCt_Overall', 'emailable_agg_ind', 'Env_Humant_Educ', 
    'EthnicCode', 'Foremost_Num_Active_Particpnts', 'Gender', 'gender_agg_ind', 'gender_input', 
    'GeneralElectn2012', 'GE_Num_Active_Particpnts', 'Globally_Opted_In', 'GroupEthnicCode', 
    'Hartford_Num_Active_Particpnts', 'HistPartCt_Overall', 'hitech_merch', 'home_purchase_date', 
    'household_size', 'mail_order_donor_categories', 'MaritalStatus', 'MemStatus', 'na2', 
    'NbrTimesSelEmailedInd', 'NYL_Num_Active_Particpnts', 'NYL_Num_InActive_Particpnts', 
    'other_donors', 'outdoors_dimension', 'Overall_Active_SP_Reltshps', 'Overall_Historic_SP_Reltshps', 
    'PartyAffiliation', 'PartyMix', 'pc_prdct_buyer', 'political', 'ReligionCode', 'RepPartyCd', 'State', 
    'travel_us_premier', 'VoterStatus', 'voter_party_input', 'vtm_active_vol_flag_act', 'vtm_vol_flag_act', 
    'WorkStatus', 'MEMBER_FL_AGG_IND', 'LapsMail', 'totalmailings', 'prospmail', 'acknow', 'advo_petition', 
    'community', 'exercise_health_group', 'region', 'ftc_dnc_append_fl', 'UNINSURED_MODEL', 
    'ETHNICITY', 'order_type', 'fishing', 'nascar', 'diy_living', 'environmental_issues', 
    'gaming_casino', 'hunting_shooting', 'investments_personal', 'reading_financial_newsletter_sub', 
    'reading_general', 'reading_religious_inspirational', 'science_space', 'smoking_tobacco', 
    'spectator_sports_auto_motorcycle', 'spectator_sports_basketball', 'spectator_sports_hockey', 
    'spectator_sports_tennis', 'strange_and_unusual', 'theater_performing_arts', 'em', 
    'advocacy_petition_signer', 'foundation_donor', 'dieting_weight_loss', 'exercise_aerobic', 
    'exercise_walking', 'home_pool_present', 'Tele_Townhall_Engagers', 'advocacy_grassroots_engager', 
    'attended_aarp_event', 'ibx_community_involvement_causes', 'num_entertainment_visits_past_3m', 'phn', 
    'motorcycling', 'auto_work', 'boating_sailing', 'broader_living', 'collectibles_coins', 
    'education_online', 'home_furnishings_decorating', 'music_collector', 'reading_best_sellers', 
    'spect_sports_motorcycle_racing', 'sweeps_contests', 'tv_guide_network', 'engaged_aarp_event', 
    'childrens_interests', 'spectator_sports_football'
]
# Filter out columns that don't exist in the DataFrame
char_cols_to_freq = [c for c in char_cols_to_freq if c in df_geo_appends.columns]
char_cols_to_freq.extend([c for c in df_geo_appends.columns if c.startswith('ibx:')])
char_cols_to_freq.extend([c for c in df_geo_appends.columns if c.startswith('past:')])
char_cols_to_freq.extend([c for c in df_geo_appends.columns if c.startswith('sy:')])
char_cols_to_freq.extend([c for c in df_geo_appends.columns if c.startswith('vehicle:')])
char_cols_to_freq = list(set(char_cols_to_freq))


all_freqs_list = []
total_count = df_geo_appends.count()

for col_name in char_cols_to_freq:
    freq_df = df_geo_appends.groupBy(col_name).count().withColumnRenamed("count", "_freq_")
    freq_df = freq_df.withColumn("col_name", F.lit(col_name))
    freq_df = freq_df.withColumnRenamed(col_name, "col_level")
    all_freqs_list.append(freq_df)

if all_freqs_list:
    final_freq_df = all_freqs_list[0]
    for i in range(1, len(all_freqs_list)):
        final_freq_df = final_freq_df.union(all_freqs_list[i])

    final_freq_df = final_freq_df.withColumn("percent", (F.col("_freq_") / total_count) * 100)
    final_freq_df = final_freq_df.select("col_name", "col_level", "_freq_", "percent").orderBy("col_name", "col_level")
    final_freq_df.write.format("delta").mode("overwrite").saveAsTable(f"scoring.Input_Char_Var_{runtype}_{muldate}")
    final_freq_df.show()

numeric_cols = [f.name for f in df_geo_appends.schema if isinstance(f.dataType, (DoubleType,))]
numeric_cols = [c for c in numeric_cols if c not in ['hid_key', 'mid_key', 'cid_key', 'memacctnum'] and not c.startswith('sy:')]
summary_df = df_geo_appends.select(numeric_cols).summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max")
summary_pd = summary_df.toPandas()
transposed_pd = summary_pd.set_index('summary').transpose().reset_index().rename(columns={'index': '_NAME_'})
df_input_num_var = spark.createDataFrame(transposed_pd)
df_input_num_var = df_input_num_var.withColumnRenamed("50%", "median")
df_input_num_var.write.format("delta").mode("overwrite").saveAsTable(f"scoring.Input_Num_Var_{runtype}_{muldate}")
df_input_num_var.show()

if runtype == 'Weekly':
    spark.sql("DROP TABLE IF EXISTS mulinter.orders")
    spark.sql("DROP TABLE IF EXISTS mulinter.orders2")
    spark.sql("DROP TABLE IF EXISTS mulinter.census")

if runtype == 'Monthly':
    spark.sql("DROP TABLE IF EXISTS mulinter.hid_key_appends")
    spark.sql("DROP TABLE IF EXISTS mulinter.mid_cid")
    spark.sql("DROP TABLE IF EXISTS mulinter.mid_key_indiv")
    spark.sql("DROP TABLE IF EXISTS mulinter.allorderdata")
    spark.sql("DROP TABLE IF EXISTS mulinter.lifestyle_engagement_sum")
    spark.sql("DROP TABLE IF EXISTS mulinter.lifestyle_engage_prov_sum_6mo")
    spark.sql("DROP TABLE IF EXISTS mulinter.lifestyle_engagement_sum_1mo")
    spark.sql("DROP TABLE IF EXISTS mulinter.lifestyle_engagement_sum_3mo")
    spark.sql("DROP TABLE IF EXISTS mulinter.lifestyle_engagement_sum_6mo")
    spark.sql("DROP TABLE IF EXISTS mulinter.lifestyle_engagement_sum_12mo")
    spark.sql("DROP TABLE IF EXISTS mulinter.order_curr_date")
    spark.sql("DROP TABLE IF EXISTS mulinter.order_curr_date2")
    spark.sql("DROP TABLE IF EXISTS mulinter.activities_sum")

spark.sql("DROP TABLE IF EXISTS intermed.mid_key_appends_2")
spark.sql("DROP TABLE IF EXISTS intermed.mid_key_appends1a_2")
spark.sql("DROP TABLE IF EXISTS intermed.mid_key_appends2_2")
spark.sql("DROP TABLE IF EXISTS intermed.mid_key_appends3_2")
spark.sql("DROP TABLE IF EXISTS intermed.geo_cid_hid")
spark.sql("DROP TABLE IF EXISTS intermed.cid_appends")
spark.sql("DROP TABLE IF EXISTS intermed.geo_appends_rpm_dedup")
spark.sql("DROP TABLE IF EXISTS intermed.household")
spark.sql("DROP TABLE IF EXISTS intermed.emu")
spark.sql("DROP TABLE IF EXISTS intermed.emu_complete")
spark.sql("DROP TABLE IF EXISTS intermed.emu_mid_indiv")
spark.sql("DROP TABLE IF EXISTS intermed.emu_hid")
spark.sql("DROP TABLE IF EXISTS intermed.emu_account")

#End-DBShift