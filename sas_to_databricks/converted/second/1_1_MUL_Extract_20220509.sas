import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from functools import reduce

# Assuming 'bl' and other SAS librefs are mapped to appropriate paths or connection properties
# For example:
# bl = "dbfs:/path/to/n_MUL_20190816.TXT"
# conn = "jdbc:..."
# dsn = "..."
# usern = "..."
# passw = "..."
# ref3 = "database_name"
# runtype = "Weekly" or "Monthly"
# muldate = "..."
# twobegdt = "..."
# enddt = "..."
# ONEBEGDT = "..."
# muldate2 = "..."
# mon = "..."
# day = "..."
# year2 = "..."
# democurr = "..."


df_raw_bonus_layout = spark.read.text(bl)

df_bonus_layout_parsed = df_raw_bonus_layout.select(
    F.substring(F.col("value"), 1, 12).alias("KeyCode"),
    F.substring(F.col("value"), 13, 10).alias("MemAcctNum"),
    F.substring(F.col("value"), 23, 12).alias("MerkleID"),
    F.substring(F.col("value"), 35, 5).alias("CampaignID"),
    F.substring(F.col("value"), 40, 3).alias("cntct_lifstyle_12mo_agg_hhd"),
    F.substring(F.col("value"), 43, 3).alias("cntct_lifstyle_3mo_agg_hhd"),
    F.substring(F.col("value"), 46, 1).alias("corp_member_Ind"),
    F.substring(F.col("value"), 60, 1).alias("NATitle"),
    F.substring(F.col("value"), 61, 1).alias("NAFname"),
    F.substring(F.col("value"), 62, 1).alias("NALname"),
    F.substring(F.col("value"), 63, 1).alias("NASuffix"),
    F.substring(F.col("value"), 64, 1).alias("NAAddr1"),
    F.substring(F.col("value"), 65, 1).alias("NAAddr2"),
    F.substring(F.col("value"), 66, 1).alias("NACity"),
    F.substring(F.col("value"), 67, 2).alias("State"),
    F.substring(F.col("value"), 69, 5).alias("Zip"),
    F.substring(F.col("value"), 74, 4).alias("ZipPlus4"),
    F.substring(F.col("value"), 78, 8).alias("CoaDate"),
    F.substring(F.col("value"), 86, 3).alias("Age"),
    F.substring(F.col("value"), 89, 1).alias("Gender"),
    F.substring(F.col("value"), 90, 1).alias("DateOfBirth"),
    F.substring(F.col("value"), 91, 1).alias("WorkStatus"),
    F.substring(F.col("value"), 92, 1).alias("MaritalStatus"),
    F.substring(F.col("value"), 93, 1).alias("NA5"),
    F.substring(F.col("value"), 94, 1).alias("NA6"),
    F.substring(F.col("value"), 95, 1).alias("NA7"),
    F.substring(F.col("value"), 96, 1).alias("NA8"),
    F.substring(F.col("value"), 97, 1).alias("SecGender"),
    F.substring(F.col("value"), 98, 1).alias("SecBirthDate"),
    F.substring(F.col("value"), 99, 3).alias("SecAge"),
    F.substring(F.col("value"), 102, 1).alias("SegTypes"),
    F.substring(F.col("value"), 103, 1).alias("MemType"),
    F.substring(F.col("value"), 104, 1).alias("MemStatus"),
    F.substring(F.col("value"), 105, 8).alias("MemOriginDate"),
    F.substring(F.col("value"), 113, 9).alias("MemOriginKey"),
    F.substring(F.col("value"), 123, 1).alias("MemXRenew"),
    F.substring(F.col("value"), 124, 6).alias("MemPaidDate"),
    F.substring(F.col("value"), 132, 3).alias("NA2"),
    F.substring(F.col("value"), 135, 9).alias("NA3"),
    F.substring(F.col("value"), 144, 8).alias("curr_order_create_dt"),
    F.substring(F.col("value"), 152, 2).alias("CoaSource"),
    F.substring(F.col("value"), 154, 8).alias("LastPromoDate"),
    F.substring(F.col("value"), 162, 9).alias("LastPromoKey"),
    F.substring(F.col("value"), 211, 10).alias("GeoAvgVal"),
    F.substring(F.col("value"), 227, 12).alias("GeoCode"),
    F.substring(F.col("value"), 252, 10).alias("GeoMedVal"),
    F.substring(F.col("value"), 265, 3).alias("GeoOccHouseUnit"),
    F.substring(F.col("value"), 435, 2).alias("EthnicCode"),
    F.substring(F.col("value"), 437, 1).alias("ReligionCode"),
    F.substring(F.col("value"), 438, 2).alias("LanguageCode"),
    F.substring(F.col("value"), 440, 2).alias("OriginCode"),
    F.substring(F.col("value"), 442, 1).alias("GroupEthnicCode"),
    F.substring(F.col("value"), 448, 1).alias("Life_Stage"),
    F.substring(F.col("value"), 449, 1).alias("diversity_flag_agg_ind"),
    F.substring(F.col("value"), 450, 3).alias("Diversity_Subgroup_Agg_Ind"),
    F.substring(F.col("value"), 479, 6).alias("FedHouse"),
    F.substring(F.col("value"), 491, 6).alias("StateHouse"),
    F.substring(F.col("value"), 497, 6).alias("StateSenate"),
    F.substring(F.col("value"), 503, 10).alias("PartyCode"),
    F.substring(F.col("value"), 513, 10).alias("PartyMix"),
    F.substring(F.col("value"), 523, 3).alias("RepPartyCd"),
    F.substring(F.col("value"), 526, 8).alias("RegistrationDate"),
    F.substring(F.col("value"), 534, 1).alias("IS_Voter"),
    F.substring(F.col("value"), 535, 10).alias("VoterCount"),
    F.substring(F.col("value"), 685, 3).alias("PartyAffiliation"),
    F.substring(F.col("value"), 688, 10).alias("EarliestRegistrationDate"),
    F.substring(F.col("value"), 698, 19).alias("VoterStatus"),
    F.substring(F.col("value"), 717, 10).alias("PrimElectn2012"),
    F.substring(F.col("value"), 727, 10).alias("GeneralElectn2012"),
    F.substring(F.col("value"), 737, 10).alias("SpecialElectn2012"),
    F.substring(F.col("value"), 747, 6).alias("sy_otsbn_polfund_2012a"),
    F.substring(F.col("value"), 753, 6).alias("sy_otsbn_polfund_2012b"),
    F.substring(F.col("value"), 770, 2).alias("advo_segment_cd"),
    F.substring(F.col("value"), 772, 1).alias("EmailableInd"),
    F.substring(F.col("value"), 773, 3).alias("NbrTimesSelEmailedInd"),
    F.substring(F.col("value"), 776, 1).alias("Globally_Opted_In"),
    F.substring(F.col("value"), 777, 2).alias("SUPERCLUSTER_CODE"),
    F.substring(F.col("value"), 779, 1).alias("Aristotle_flag"),
    F.substring(F.col("value"), 780, 12).alias("geo_cd_2010"),
    F.substring(F.col("value"), 792, 5).alias("Age_HH_pct_with_HHer_55_64"),
    F.substring(F.col("value"), 797, 5).alias("Age_HH_pct_with_HHer_65_74"),
    F.substring(F.col("value"), 802, 5).alias("Age_HH_pct_with_HHer_75_84"),
    F.substring(F.col("value"), 807, 5).alias("Age_HH_pct_with_HHer_85p"),
    F.substring(F.col("value"), 812, 5).alias("Age_Pop_pct_60_64"),
    F.substring(F.col("value"), 817, 5).alias("HH_pct_Spanish_Speaking"),
    F.substring(F.col("value"), 822, 10).alias("HomVal_Home_Value_CBSA_Index"),
    F.substring(F.col("value"), 832, 10).alias("Inc_HH_Median_HH_Income"),
    F.substring(F.col("value"), 842, 5).alias("OCCHU_Median_Length_of_Residence"),
    F.substring(F.col("value"), 847, 10).alias("OOHU_Median_Home_Value"),
    F.substring(F.col("value"), 857, 5).alias("Pop_pct_Asian_Only_Hisp"),
    F.substring(F.col("value"), 862, 5).alias("Pop_pct_Asian_Only_"),
    F.substring(F.col("value"), 867, 5).alias("Pop_pct_Black_Only_Hisp"),
    F.substring(F.col("value"), 872, 5).alias("Pop_pct_Black_Only_"),
    F.substring(F.col("value"), 997, 1).alias("DRVS_Flag"),
    F.substring(F.col("value"), 999, 8).alias("DRVS_Date"),
    F.substring(F.col("value"), 1027, 1).alias("ACEV_Flag"),
    F.substring(F.col("value"), 1028, 2).alias("ACEV_Num"),
    F.substring(F.col("value"), 1030, 8).alias("ACEV_Last_Date"),
    F.substring(F.col("value"), 1038, 3).alias("ACEV_Last_Topic"),
    F.substring(F.col("value"), 1229, 3).alias("Overall_Active_SP_Reltshps"),
    F.substring(F.col("value"), 1232, 3).alias("Overall_Historic_SP_Reltshps"),
    F.substring(F.col("value"), 1235, 3).alias("GE_Num_Active_Particpnts"),
    F.substring(F.col("value"), 1238, 3).alias("GE_Num_InActive_Particpnts"),
    F.substring(F.col("value"), 1241, 8).alias("GE_Orig_Partcp_Date"),
    F.substring(F.col("value"), 1249, 3).alias("NYL_Num_Active_Particpnts"),
    F.substring(F.col("value"), 1252, 3).alias("NYL_Num_InActive_Particpnts"),
    F.substring(F.col("value"), 1255, 8).alias("NYL_Orig_Partcp_Date"),
    F.substring(F.col("value"), 1263, 3).alias("Hartford_Num_Active_Particpnts"),
    F.substring(F.col("value"), 1266, 3).alias("Hartford_Num_InActive_Particpnts"),
    F.substring(F.col("value"), 1269, 8).alias("Hartford_Orig_Partcp_Date"),
    F.substring(F.col("value"), 1277, 3).alias("Chase_Num_Active_Particpnts"),
    F.substring(F.col("value"), 1280, 3).alias("Chase_Num_InActive_Particpnts"),
    F.substring(F.col("value"), 1283, 8).alias("Chase_Orig_Partcp_Date"),
    F.substring(F.col("value"), 1291, 3).alias("Foremost_Num_Active_Particpnts"),
    F.substring(F.col("value"), 1294, 3).alias("Foremost_Num_InActive_Particpnts"),
    F.substring(F.col("value"), 1297, 8).alias("Foremost_Orig_Partcp_Date"),
    F.substring(F.col("value"), 1311, 3).alias("HistPartCt_Overall"),
    F.substring(F.col("value"), 1314, 3).alias("CurrentPartCt_Overall"),
    F.substring(F.col("value"), 1317, 3).alias("Past3MoTouchCt_Travel"),
    F.substring(F.col("value"), 1320, 3).alias("Past12MoTouchCt_Travel"),
    F.substring(F.col("value"), 1323, 3).alias("Past3MoTouchCt_Health"),
    F.substring(F.col("value"), 1326, 3).alias("Past12MoTouchCt_Health"),
    F.substring(F.col("value"), 1329, 3).alias("Past3MoTouchCt_Financial"),
    F.substring(F.col("value"), 1332, 3).alias("Past12MoTouchCt_Financial"),
    F.substring(F.col("value"), 1335, 3).alias("Past3MoTouchCt_Home"),
    F.substring(F.col("value"), 1338, 3).alias("Past12MoTouchCt_Home"),
    F.substring(F.col("value"), 1341, 3).alias("Past3MoTouchCt_Priv"),
    F.substring(F.col("value"), 1344, 3).alias("Past12MoTouchCt_Priv"),
    F.substring(F.col("value"), 1347, 3).alias("Past3MoTouchCt_AARP"),
    F.substring(F.col("value"), 1350, 3).alias("Past12MoTouchCt_AARP"),
    F.substring(F.col("value"), 1353, 3).alias("Past3MoTouchCt_Overall"),
    F.substring(F.col("value"), 1356, 3).alias("Past12MoTouchCt_Overall"),
    F.substring(F.col("value"), 1359, 12).alias("Fndn_TTD_Amt"),
    F.substring(F.col("value"), 1371, 8).alias("Fndn_TTD_Num"),
    F.substring(F.col("value"), 1379, 8).alias("Fndn_Last_Amt"),
    F.substring(F.col("value"), 1387, 8).alias("Fndn_Last_Dt"),
    F.substring(F.col("value"), 1395, 12).alias("Advo_TTD_Amt"),
    F.substring(F.col("value"), 1407, 8).alias("Advo_TTD_Num"),
    F.substring(F.col("value"), 1415, 8).alias("Advo_Last_Amt"),
    F.substring(F.col("value"), 1423, 8).alias("Advo_Last_Dt"),
    F.substring(F.col("value"), 1431, 8).alias("Advo_Last_Petition_Dt_Agg_Ind"),
    F.substring(F.col("value"), 1439, 10).alias("Advo_Last_Petition_Subj_Agg_Ind"),
    F.substring(F.col("value"), 1449, 8).alias("advo_hpc_amt"),
    F.substring(F.col("value"), 1457, 8).alias("advo_hpc_dt"),
    F.substring(F.col("value"), 1465, 8).alias("advo_mrhpc_amt"),
    F.substring(F.col("value"), 1473, 8).alias("advo_mrhpc_dt"),
    F.substring(F.col("value"), 1481, 8).alias("fndn_hpc_amt"),
    F.substring(F.col("value"), 1489, 8).alias("fndn_hpc_dt"),
    F.substring(F.col("value"), 1497, 8).alias("fndn_mrhpc_amt"),
    F.substring(F.col("value"), 1505, 8).alias("fndn_mrhpc_dt"),
    F.substring(F.col("value"), 1513, 8).alias("partisanscore"),
    F.substring(F.col("value"), 1521, 8).alias("ideology"),
    F.substring(F.col("value"), 1529, 1).alias("vtm_active_vol_flag_act"),
    F.substring(F.col("value"), 1530, 1).alias("vtm_dsp_active_vol"),
    F.substring(F.col("value"), 1531, 100).alias("vtm_last_initiative_act"),
    F.substring(F.col("value"), 1631, 80).alias("vtm_last_program_act"),
    F.substring(F.col("value"), 1711, 100).alias("vtm_last_role_act"),
    F.substring(F.col("value"), 1811, 8).alias("vtm_assignment_last_start_dt_act"),
    F.substring(F.col("value"), 1819, 3).alias("vtm_num_last_12m_assignments_act"),
    F.substring(F.col("value"), 1822, 3).alias("vtm_num_assignments_act"),
    F.substring(F.col("value"), 1825, 3).alias("vtm_num_ytd_assignments_act"),
    F.substring(F.col("value"), 1828, 1).alias("vtm_vol_flag_act")
)

# Cast numeric columns
numeric_cols = [
    "cntct_lifstyle_12mo_agg_hhd", "cntct_lifstyle_3mo_agg_hhd", "CoaDate", "Age",
    "SecAge", "MemOriginDate", "MemXRenew", "MemPaidDate", "GeoAvgVal", "GeoMedVal",
    "GeoOccHouseUnit", "OriginCode", "Life_Stage", "diversity_flag_agg_ind",
    "Diversity_Subgroup_Agg_Ind", "VoterCount", "sy_otsbn_polfund_2012a", "sy_otsbn_polfund_2012b",
    "SUPERCLUSTER_CODE", "Age_HH_pct_with_HHer_55_64", "Age_HH_pct_with_HHer_65_74",
    "Age_HH_pct_with_HHer_75_84", "Age_HH_pct_with_HHer_85p", "Age_Pop_pct_60_64",
    "HH_pct_Spanish_Speaking", "HomVal_Home_Value_CBSA_Index", "Inc_HH_Median_HH_Income",
    "OCCHU_Median_Length_of_Residence", "OOHU_Median_Home_Value", "Pop_pct_Asian_Only_Hisp",
    "Pop_pct_Asian_Only_", "Pop_pct_Black_Only_Hisp", "Pop_pct_Black_Only_", "DRVS_Flag",
    "Fndn_TTD_Amt", "Fndn_TTD_Num", "Fndn_Last_Amt", "Fndn_Last_Dt", "Advo_TTD_Amt",
    "Advo_TTD_Num", "Advo_Last_Amt", "Advo_Last_Dt", "Advo_Last_Petition_Dt_Agg_Ind",
    "advo_hpc_amt", "advo_hpc_dt", "advo_mrhpc_amt", "advo_mrhpc_dt", "fndn_hpc_amt",
    "fndn_hpc_dt", "fndn_mrhpc_amt", "fndn_mrhpc_dt", "partisanscore", "ideology",
    "vtm_num_last_12m_assignments_act", "vtm_num_assignments_act", "vtm_num_ytd_assignments_act"
]

for col_name in numeric_cols:
    df_bonus_layout_parsed = df_bonus_layout_parsed.withColumn(
        col_name,
        F.when(F.trim(F.col(col_name)) == "", None).otherwise(F.trim(F.col(col_name))).cast(DecimalType(20, 5))
    )

df_bonus_layout_transformed = df_bonus_layout_parsed \
    .withColumn("Age_HH_pct_with_HHer_55_64", F.col("Age_HH_pct_with_HHer_55_64") * 10) \
    .withColumn("Age_HH_pct_with_HHer_65_74", F.col("Age_HH_pct_with_HHer_65_74") * 10) \
    .withColumn("HH_pct_Spanish_Speaking", F.col("HH_pct_Spanish_Speaking") * 10) \
    .withColumn("Age_HH_pct_with_HHer_75_84", F.col("Age_HH_pct_with_HHer_75_84") * 10) \
    .withColumn("Age_HH_pct_with_HHer_85p", F.col("Age_HH_pct_with_HHer_85p") * 10) \
    .withColumn("Pop_pct_Asian_Only_Hisp", F.col("Pop_pct_Asian_Only_Hisp") * 10) \
    .withColumn("Pop_pct_Asian_Only_", F.col("Pop_pct_Asian_Only_") * 10) \
    .withColumn("Pop_pct_Black_Only_Hisp", F.col("Pop_pct_Black_Only_Hisp") * 10) \
    .withColumn("Pop_pct_Black_Only_", F.col("Pop_pct_Black_Only_") * 10) \
    .withColumn("OCCHU_Median_Length_of_Residence", F.col("OCCHU_Median_Length_of_Residence") * 100)

df_bonus_layout_transformed = df_bonus_layout_transformed.withColumn("Region",
    F.when(F.upper(F.col("STATE")).isin('AK', 'CO', 'HI', 'ID', 'MT', 'NV', 'NM', 'OR', 'UT', 'WY'), 'West Region')
     .when(F.upper(F.col("STATE")).isin('AR', 'IA', 'KS', 'MN', 'NE', 'ND', 'OK', 'SD', 'WI'), 'Central Region')
     .when(F.upper(F.col("STATE")).isin('AL', 'DC', 'KY', 'LA', 'MD', 'MS', 'SC', 'VA', 'WV'), 'South Region')
     .when(F.upper(F.col("STATE")).isin('CT', 'DE', 'ME', 'MA', 'NH', 'PR', 'RI', 'VT', 'VI'), 'East Coast Region')
     .when(F.upper(F.col("STATE")).isin('AZ', 'GA', 'IN', 'MI', 'MO', 'NJ', 'NC', 'TN', 'WA'), 'Large Region')
     .when(F.upper(F.col("STATE")).isin('CA', 'FL', 'IL', 'NY', 'OH', 'PA', 'TX'), 'Mega Region')
     .otherwise('Unknown')
)

df_bonus_layout_transformed = df_bonus_layout_transformed.withColumn("PrimElectn2012",
    F.when(F.col("PrimElectn2012") == 'absentee', 'A')
     .when(F.col("PrimElectn2012") == 'earlyVote', 'E')
     .when(F.col("PrimElectn2012") == 'mail', 'M')
     .when(F.col("PrimElectn2012").isin('polling', 'unknown'), 'Y')
     .otherwise(F.col("PrimElectn2012"))
)

df_bonus_layout_transformed = df_bonus_layout_transformed.withColumn("GeneralElectn2012",
    F.when(F.col("GeneralElectn2012") == 'absentee', 'A')
     .when(F.col("GeneralElectn2012") == 'earlyVote', 'E')
     .when(F.col("GeneralElectn2012") == 'mail', 'M')
     .when(F.col("GeneralElectn2012").isin('polling', 'unknown'), 'Y')
     .otherwise(F.col("GeneralElectn2012"))
)

df_bonus_layout_transformed = df_bonus_layout_transformed.withColumn("SpecialElectn2012",
    F.when(F.col("SpecialElectn2012") == 'absentee', 'A')
     .when(F.col("SpecialElectn2012") == 'earlyVote', 'E')
     .when(F.col("SpecialElectn2012") == 'mail', 'M')
     .when(F.col("SpecialElectn2012").isin('polling', 'unknown'), 'Y')
     .otherwise(F.col("SpecialElectn2012"))
)

df_bonus_layout_transformed.write.format("delta").mode("overwrite").saveAsTable("intermed.BONUS_LAYOUT")
df_bonus_layout = spark.table("intermed.BONUS_LAYOUT")


def emu_macro():
    if runtype == 'Monthly':
        sql_query_emu_1 = f"select mid_key from {ref3}.vq_emu"
        df_vq_emu = spark.read.format("jdbc").option("url", conn).option("dbtable", sql_query_emu_1).option("user", usern).option("password", passw).load()

        df_bonus_layout_for_join = spark.table("intermed.bonus_layout").select(F.col("merkleid").cast("decimal(10,0)").alias("merkleid_num"))
        
        df_emu = df_vq_emu.join(
            df_bonus_layout_for_join,
            df_vq_emu.mid_key == df_bonus_layout_for_join.merkleid_num,
            "left_anti"
        ).select("mid_key").orderBy("mid_key")
        
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
left join
{ref3}.d_individual b
on a.mid_key=b.mid_key
left join
{ref3}.d_catalist_voter_model c
on a.mid_key=c.mid_key
left join
{ref3}.d_mm_individual d
on a.mid_key=d.mid_key
left join
{ref3}.d_vtm_individual f
on a.mid_key=f.mid_key
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
 			when UPPER(STATE) in ('CT', 'DE', 'ME', 'MA', 'NH', 'PR', 'RI', 'VT', 'VI')		THEN 'East Coast Region'
 			when UPPER(STATE) in ('AZ', 'GA', 'IN', 'MI', 'MO', 'NJ', 'NC', 'TN', 'WA')		THEN 'Large Region'
 			when UPPER(STATE) in ('CA', 'FL', 'IL', 'NY', 'OH', 'PA', 'TX')					THEN 'Mega Region'
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
	left join
		{ref3}.f_joiner b
		on a.mid_key=b.mid_key
		left join 
		{ref3}.d_household as c
		on b.hid_key=c.hid_key
		left join
		{ref3}.d_census_2010 as d
		on c.geo_cd_2010=d.geo_cd_2010
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
	left join
		{ref3}.f_joiner b
		on a.mid_key=b.mid_key
	left join 
 {ref3}.d_account c
on b.chid_key=c.chid_key
where preferred_chid=1
        """
        df_emu_account_raw = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_emu_account}) as subq").option("user", usern).option("password", passw).load()
        df_emu_account = df_emu_account_raw \
            .withColumn("mempaiddate", F.date_format(F.to_date(F.col("paid_through_dt")), "yyyyMM").cast("integer")) \
            .withColumn("acev_num_i", F.col("acev_num")) \
            .drop("acev_num")
        df_emu_account.write.format("delta").mode("overwrite").saveAsTable("intermed.emu_account")

        df_emu = spark.table("intermed.emu")
        df_emu_mid_indiv = spark.table("intermed.emu_mid_indiv")
        df_emu_hid = spark.table("intermed.emu_hid")
        df_emu_account = spark.table("intermed.emu_account")

        df_emu.createOrReplaceTempView("emu")
        df_emu_mid_indiv.createOrReplaceTempView("emu_mid_indiv")
        df_emu_hid.createOrReplaceTempView("emu_hid")
        df_emu_account.createOrReplaceTempView("emu_account")

        df_emu_complete = spark.sql("""
        SELECT *,
            CAST(lifestage_segment AS DOUBLE) AS Life_Stage,
            CAST(num_times_selected_email_agg_ind AS STRING) AS NbrTimesSelEmailedInd,
            CAST(IBX_COUNTRY_OF_ORIGIN_CODE_E_TEC AS DOUBLE) AS OriginCode,
            CAST(DATE_FORMAT(TO_DATE(CAST(Advo_Last_Dt_i AS STRING)), 'yyyyMMdd') AS DOUBLE) AS Advo_Last_Dt,
            CAST(DATE_FORMAT(TO_DATE(CAST(Fndn_Last_Dt_i AS STRING)), 'yyyyMMdd') AS DOUBLE) AS Fndn_Last_Dt,
            CAST(DATE_FORMAT(TO_DATE(CAST(advo_last_petition_dt_agg_ind_i AS STRING)), 'yyyyMMdd') AS DOUBLE) AS advo_last_petition_dt_agg_ind,
            TO_DATE(CAST(addr_move_dt AS STRING)) as movedate,
            CAST(DATE_FORMAT(TO_DATE(CAST(addr_move_dt AS STRING)), 'yyyyMMdd') AS DOUBLE) as coadate,
            '          ' as PartyMix, 
            CAST(NULL AS DOUBLE) as VoterCount, 
            REGEXP_REPLACE(CAST(Num_Hist_Participation_Overall_A AS STRING), ' |\\\\.', '') as HistPartCt_Overall,
            REGEXP_REPLACE(CAST(Num_Contact_Aarp_12_Months_Agg_H AS STRING), ' |\\\\.', '') as Past12MoTouchCt_AARP,
            REGEXP_REPLACE(CAST(Num_Contact_Financial_12_Months_ AS STRING), ' |\\\\.', '') as Past12MoTouchCt_Financial,
            REGEXP_REPLACE(CAST(Num_Contact_Health_12_Months_Agg AS STRING), ' |\\\\.', '') as Past12MoTouchCt_Health,
            REGEXP_REPLACE(CAST(Num_Contact_Overall_12_Months_Ag AS STRING), ' |\\\\.', '') as Past12MoTouchCt_Overall,
            REGEXP_REPLACE(CAST(num_contact_discounts_12_months_ AS STRING), ' |\\\\.', '') as Past12MoTouchCt_Priv,
            REGEXP_REPLACE(CAST(num_contact_travel_12_months_agg AS STRING), ' |\\\\.', '') as Past12MoTouchCt_Travel,
            REGEXP_REPLACE(CAST(Num_Contact_Aarp_3_Months_Agg_Hh AS STRING), ' |\\\\.', '') as Past3MoTouchCt_AARP,
            REGEXP_REPLACE(CAST(Num_Contact_Financial_3_Months_A AS STRING), ' |\\\\.', '') as Past3MoTouchCt_Financial,
            REGEXP_REPLACE(CAST(Num_Contact_Health_3_Months_Agg_ AS STRING), ' |\\\\.', '') as Past3MoTouchCt_Health, 
            REGEXP_REPLACE(CAST(Num_Contact_Overall_3_Months_Agg AS STRING), ' |\\\\.', '') as Past3MoTouchCt_Overall,
            REGEXP_REPLACE(CAST(num_contact_discounts_3_months_a AS STRING), ' |\\\\.', '') as Past3MoTouchCt_Priv,
            REGEXP_REPLACE(CAST(NUM_CURR_PARTICIPATION_OVERALL_A AS STRING), ' |\\\\.', '') as CurrentPartCt_Overall,
            CASE WHEN acev_num_i > 99 THEN '*' ELSE REGEXP_REPLACE(CAST(acev_num_i AS STRING), ' |\\\\.', '') END AS acev_num,
            REGEXP_REPLACE(CAST(num_active_part_chase_agg_act AS STRING), ' |\\\\.', '') AS Chase_Num_Active_Particpnts,
            REGEXP_REPLACE(CAST(num_hist_part_chase_agg_act AS STRING), ' |\\\\.', '') AS Chase_Num_InActive_Particpnts,
            DATE_FORMAT(TO_DATE(CAST(current_order_CREATE_dt_agg_act AS STRING)), 'yyyyMMdd') AS curr_order_create_dt,
            REGEXP_REPLACE(CAST(num_active_part_foremost_agg_act AS STRING), ' |\\\\.', '') AS Foremost_Num_Active_Particpnts,
            REGEXP_REPLACE(CAST(num_active_part_ge_agg_act AS STRING), ' |\\\\.', '') AS GE_Num_Active_Particpnts,
            REGEXP_REPLACE(CAST(num_active_part_hartford_agg_act AS STRING), ' |\\\\.', '') AS Hartford_Num_Active_Particpnts,
            REGEXP_REPLACE(CAST(num_active_part_nyl_agg_act AS STRING), ' |\\\\.', '') AS NYL_Num_Active_Particpnts,
            REGEXP_REPLACE(CAST(num_hist_part_nyl_agg_act AS STRING), ' |\\\\.', '') AS NYL_Num_InActive_Particpnts,
            CAST(DATE_FORMAT(TO_DATE(CAST(Kx_Create_Dt AS STRING)), 'yyyyMMdd') AS DOUBLE) as memorigindate,
            REGEXP_REPLACE(CAST(term_agg_act AS STRING), ' |\\\\.', '') AS na2,
            REGEXP_REPLACE(CAST(active_sprel_overall_agg_act AS STRING), ' |\\\\.', '') AS Overall_Active_SP_Reltshps,
            REGEXP_REPLACE(CAST(hist_sprel_overall_agg_act AS STRING), ' |\\\\.', '') AS Overall_Historic_SP_Reltshps,
            'E' as memstatus,
            CASE WHEN acev_num_i > 0 THEN 'Y' ELSE 'N' END AS acev_flag,
            '1' as globally_opted_in,
            CAST(b.mid_key AS STRING) as merkleid
        FROM
            emu a
        LEFT JOIN 
            emu_mid_indiv b ON a.mid_key = b.mid_key
        LEFT JOIN 
            emu_hid c ON a.mid_key = c.mid_key
        LEFT JOIN 
            emu_account d ON a.mid_key = d.mid_key
        """)
        
        df_emu_complete = df_emu_complete.drop("chid_agg_ind", "mid_key", "hid_key", "acev_num_i", "Advo_Last_Dt_i", "Fndn_Last_Dt_i", "advo_last_petition_dt_agg_ind_i")
        
        df_emu_complete = df_emu_complete.dropDuplicates(["merkleid"])

        df_bonus_layout = spark.table("intermed.bonus_layout")
        
        # To match force option, align schemas
        cols_bonus = {c.name: c.dataType for c in df_bonus_layout.schema}
        cols_emu = {c.name: c.dataType for c in df_emu_complete.schema}
        
        for col_name in set(cols_bonus.keys()) - set(cols_emu.keys()):
            df_emu_complete = df_emu_complete.withColumn(col_name, F.lit(None).cast(cols_bonus[col_name]))
            
        for col_name in set(cols_emu.keys()) - set(cols_bonus.keys()):
            df_bonus_layout = df_bonus_layout.withColumn(col_name, F.lit(None).cast(cols_emu[col_name]))

        df_bonus_layout_appended = df_bonus_layout.unionByName(df_emu_complete.select(df_bonus_layout.columns))
        df_bonus_layout_appended.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.bonus_layout")

        %run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/Process_Demos_20210118"

        if 'sysrc' not in locals() or sysrc == 0:
            spark.sql("DROP TABLE IF EXISTS intermed.emu_complete")
            spark.sql("DROP TABLE IF EXISTS intermed.emu")
            spark.sql("DROP TABLE IF EXISTS intermed.emu_mid_indiv")
            spark.sql("DROP TABLE IF EXISTS intermed.emu_hid")
            spark.sql("DROP TABLE IF EXISTS intermed.emu_account")
        
        spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")

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
	(a.account_stat='5' and cast(Paid_Through_Dt as date) >='{twobegdt}' and
	cast(Paid_Through_Dt as date) <='{muldate}'))) aa
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
        df_household = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_household_monthly}) as subq").option("user", usern).option("password", passw).load()
        df_household.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("mulinter.household")

    if runtype == 'Weekly':
        %run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/Process_Acxiom_Demographic_20201215"
        
        sql_query_orders2 = f"""
        select mid_key,response_key_cd,kx_create_dt, order_term,end_term_dt
		from {ref3}.f_account_order
        """
        df_orders_remote = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_orders2}) as subq").option("user", usern).option("password", passw).load()
        
        df_special_keycodes = spark.table("aarpdata.special_keycodes").select("sp_key_codes")
        
        df_orders2 = df_orders_remote.join(df_special_keycodes, F.substring(df_orders_remote.response_key_cd, 1, 9) == df_special_keycodes.sp_key_codes, "left")
        
        df_orders2 = df_orders2.withColumn("response_key_cd", F.col("response_key_cd").cast("string")) \
            .withColumn("orders_all_i", F.lit(1)) \
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
            .withColumn("orders_am_i", F.when((F.substring(F.col("response_key_cd"), 1, 1) == 'K') & (F.to_date(F.col("kx_create_dt")) <= F.to_date(F.lit("2020-06-08"), "yyyy-MM-dd")) & F.col("sp_key_codes").isNull(), 1).otherwise(0))

        df_orders2.write.format("delta").mode("overwrite").saveAsTable("mulinter.orders2")
        
        sql_query_order_curr_date = f"""
        select a.*,b.order_num as prev_order_num, b.kx_create_dt as new_kx_create_dt
	from 
	(select mid_key,kx_create_dt,order_num,order_type,order_term
	from unica.f_account_order
	where date(start_term_dt)<current_date and date(end_term_dt)>current_date) a
	left join 
	unica.f_account_order b
	on a.mid_key=b.mid_key and a.order_num=b.order_num-1
        """
        df_order_curr_date = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_order_curr_date}) as subq").option("user", usern).option("password", passw).load()
        
        window_spec = Window.partitionBy("mid_key").orderBy(F.desc("order_num"))
        df_order_curr_date2 = df_order_curr_date.withColumn("row_num", F.row_number().over(window_spec)) \
            .filter(F.col("row_num") == 1).drop("row_num")
        df_order_curr_date2.write.format("delta").mode("overwrite").saveAsTable("mulinter.order_curr_date2")

        df_orders2 = spark.table("mulinter.orders2")
        df_orders2.createOrReplaceTempView("orders2")

        df_allorderdata_agg = spark.sql("""
        select mid_key, sum(orders_all_i) as orders_all, sum(orders_12moterm_i) as orders_12moterm, 
            sum(orders_36moterm_i) as orders_36moterm,sum(orders_60moterm_i) as orders_60moterm,
            sum(orders_acqmail_i) as orders_acqmail, sum(orders_renewals_i) as orders_renewals,
            sum(orders_altmedia_i) as orders_altmedia, sum(orders_online_i) as orders_online,
            sum(orders_serviceprovider_i) as orders_serviceprovider,
            sum(orders_winback_i) as orders_winback,sum(orders_cl_i) as orders_cl,sum(orders_dm_i) as orders_dm,
            sum(orders_sps_i) as orders_sps,sum(orders_am_i) as orders_am	
        from orders2
        group by mid_key
        """)
        
        df_order_curr_date2 = spark.table("mulinter.order_curr_date2")
        
        df_allorderdata = df_allorderdata_agg.join(
            df_order_curr_date2.select("mid_key", "order_num", "order_type", "order_term", "new_kx_create_dt"),
            "mid_key",
            "left"
        )
        df_allorderdata.write.format("delta").mode("overwrite").saveAsTable("mulinter.allorderdata")

        spark.sql("DROP TABLE IF EXISTS mulinter.orders")
        spark.sql("DROP TABLE IF EXISTS mulinter.orders2")
        spark.sql("DROP TABLE IF EXISTS mulinter.order_curr_date")
        spark.sql("DROP TABLE IF EXISTS mulinter.order_curr_date2")
        
        sql_query_lifestyle_engagement_sum = f"""
        select mid_key, count(distinct svc_prov) as life_engage_svcprov_12mo_nps
	from {ref3}.f_lifestyle_engagement
	where cast(tran_dt as date) between ('{ONEBEGDT}') and ('{enddt}')
	and cast(insight_update_dt as date) between ('{ONEBEGDT}') and ('{enddt}')
	group by mid_key
        """
        df_lifestyle_engagement_sum = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_lifestyle_engagement_sum}) as subq").option("user", usern).option("password", passw).load()
        df_lifestyle_engagement_sum.write.format("delta").mode("overwrite").saveAsTable("mulinter.lifestyle_engagement_sum")

        sql_query_lifestyle_engage_prov_sum_6mo = f"""
        select mid_key, count(distinct svc_prov) as life_engage_svcprov_3mo_6mo
	from {ref3}.f_lifestyle_engagement
	where cast(tran_dt as date) between (date('{enddt}')-182) and (date('{enddt}')-91)
	group by mid_key
        """
        df_lifestyle_engage_prov_sum_6mo = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_lifestyle_engage_prov_sum_6mo}) as subq").option("user", usern).option("password", passw).load()
        df_lifestyle_engage_prov_sum_6mo.write.format("delta").mode("overwrite").saveAsTable("mulinter.lifestyle_engage_prov_sum_6mo")

        sql_query_lifestyle_engagement_sum_1mo = f"""
        select mid_key, count(svc_prov) as life_engage_1mo
	from {ref3}.f_lifestyle_engagement
	where cast(tran_dt as date) between (date('{enddt}')-30) and ('{enddt}')
		and cast(insight_update_dt as date) between (date('{enddt}')-30) and ('{enddt}')
	group by mid_key
        """
        df_lifestyle_engagement_sum_1mo = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_lifestyle_engagement_sum_1mo}) as subq").option("user", usern).option("password", passw).load()
        df_lifestyle_engagement_sum_1mo.write.format("delta").mode("overwrite").saveAsTable("mulinter.lifestyle_engagement_sum_1mo")

        sql_query_lifestyle_engagement_sum_3mo = f"""
        select mid_key, count(distinct svc_prov) as life_engage_svcprov_3mo,
		     count(svc_prov) as life_engage_3mo
	from {ref3}.f_lifestyle_engagement
	where cast(tran_dt as date) between (date('{enddt}')-91) and ('{enddt}')
	group by mid_key
        """
        df_lifestyle_engagement_sum_3mo = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_lifestyle_engagement_sum_3mo}) as subq").option("user", usern).option("password", passw).load()
        df_lifestyle_engagement_sum_3mo.write.format("delta").mode("overwrite").saveAsTable("mulinter.lifestyle_engagement_sum_3mo")
        
        sql_query_lifestyle_engagement_sum_6mo = f"""
        select mid_key, count(svc_prov) as life_engage_6mo
	from {ref3}.f_lifestyle_engagement
	where cast(tran_dt as date) between (date('{enddt}')-182) and ('{enddt}')
	group by mid_key
        """
        df_lifestyle_engagement_sum_6mo = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_lifestyle_engagement_sum_6mo}) as subq").option("user", usern).option("password", passw).load()
        df_lifestyle_engagement_sum_6mo.write.format("delta").mode("overwrite").saveAsTable("mulinter.lifestyle_engagement_sum_6mo")
        
        sql_query_lifestyle_engagement_sum_12mo = f"""
        select mid_key, count(distinct svc_prov) as life_engage_svcprov_12mo,
     sum(tot_amt) as life_engage_sumamt_12mo,
	  count(svc_prov) as life_engage_12mo
	from {ref3}.f_lifestyle_engagement
	where cast(tran_dt as date) between (date('{enddt}')-365) and ('{enddt}')
	group by mid_key
        """
        df_lifestyle_engagement_sum_12mo = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_lifestyle_engagement_sum_12mo}) as subq").option("user", usern).option("password", passw).load()
        df_lifestyle_engagement_sum_12mo.write.format("delta").mode("overwrite").saveAsTable("mulinter.lifestyle_engagement_sum_12mo")

        sql_query_activities_sum = f"""
        select mid_key, count(ss_key) as activities_3mo
	from {ref3}.f_activity
	where cast(activity_dt as date) between (date('{enddt}')-90) and ('{enddt}')
	group by mid_key
        """
        df_activities_sum = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_activities_sum}) as subq").option("user", usern).option("password", passw).load()
        df_activities_sum.write.format("delta").mode("overwrite").saveAsTable("mulinter.activities_sum")

        df_gender_pred = spark.read.csv("/vg01/aarp_sas/ftp/incoming/Gender_Prediction.csv", header=True, inferSchema=True)
        df_gender_pred.write.format("delta").mode("overwrite").saveAsTable("gender_pred")
        
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
case
	when b.HUNTER_MODEL='Unlikely Hunter' then .29
	when b.HUNTER_MODEL='Possibly Hunter' then .72
	when b.HUNTER_MODEL='Likely Hunter' then .93
	when b.HUNTER_MODEL='Hunter' then 1
	else NULL
end as HUNTER_MODEL,
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
b.race_confidence_numeric*100 as race_confidence_numeric
	from {ref3}.d_individual c
	left join
	{ref3}.d_acxiom_demographics_ind a
	on c.mid_key=a.mid_key
	left join {ref3}.d_catalist_voter_model b
	on c.mid_key=b.mid_key
	left join
	{ref3}.d_vtm_individual f
	on c.mid_key=f.mid_key
	left join
	{ref3}.f_vmis g
	on c.mid_key=g.mid_key
        """
        df_mid_key_indiv_remote = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_mid_key_indiv}) as subq").option("user", usern).option("password", passw).load()
        
        df_gender_pred = spark.table("gender_pred")
        
        df_mid_key_indiv_joined = df_mid_key_indiv_remote.join(df_gender_pred, "mid_key", "left")

        df_mid_key_indiv = df_mid_key_indiv_joined \
            .withColumn("VTM_ASSIGNMENT_LAST_END_DT", F.col("VTM_ASSIGNMENT_LAST_END_DT")) \
            .withColumn("dvr", F.lit(None).cast("double")) \
            .withColumn("internet", F.lit(None).cast("double")) \
            .withColumn("radio", F.lit(None).cast("double")) \
            .withColumn("smartphone", F.lit(None).cast("double")) \
            .withColumn("tv", F.lit(None).cast("double")) \
            .withColumn("density_clusters", F.lit('').cast("string")) \
            .withColumn("work_clusters", F.lit('').cast("string")) \
            .withColumn("religious", F.lit(None).cast("double")) \
            .withColumn("cable", F.lit(None).cast("double")) \
            .withColumn("game_shows", F.lit(None).cast("double")) \
            .withColumn("kids_shows", F.lit(None).cast("double")) \
            .withColumn("educational_attainment_model", F.col("educational_attainment_model_i") / 100) \
            .withColumn("gun_ownership_model", F.col("gun_ownership_model_i") / 160) \
            .withColumn("gender_agg_ind", 
                F.when((F.col("gender_agg_ind_o") == 'U') & (F.col("probability") > 0.8), F.col("gender_ind"))
                 .otherwise(F.col("gender_agg_ind_o"))
            ).drop("educational_attainment_model_i")

        df_mid_key_indiv.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("mulinter.mid_key_indiv")
        
        def output_gender_macro():
            from datetime import datetime
            alpha = int(datetime.strptime(muldate, '%Y%m%d').strftime('%d'))
            print(alpha)
            if 11 <= alpha < 18:
                df_individual_remote = spark.read.format("jdbc").option("url", conn).option("dbtable", "unica.d_individual").option("user", usern).option("password", passw).load()
                df_gender_pred = spark.table("gender_pred")
                
                df_gender_pred_output = df_individual_remote.join(df_gender_pred, "mid_key", "left") \
                    .select(
                        "mid_key",
                        F.substring(F.coalesce(F.col("gender_ind"), F.col("gender_agg_ind")), 1, 1).alias("gender_ind"),
                        F.coalesce(F.col("probability"), F.lit(1)).alias("probability")
                    )
                df_gender_pred_output.write.format("delta").mode("overwrite").saveAsTable("aarpdata.gender_pred")

                df_gender_pred_to_sandbox = spark.table("aarpdata.gender_pred").withColumn("effective_date", F.lit(muldate))
                
                # The bl_ options suggest a direct write to S3, which is not a standard Spark feature for tables.
                # The equivalent in Databricks is to write to a location.
                # Assuming sandbox is a database pointing to an S3 location.
                spark.sql("DROP TABLE IF EXISTS sandbox.GENDER_PRED_SX")
                df_gender_pred_to_sandbox.write.format("delta").mode("overwrite").saveAsTable("sandbox.GENDER_PRED_SX")

        output_gender_macro()
        
        sql_query_mid_cid = f"""
        select mid_key, cid_key, hid_key,preferred_chid
	from {ref3}.f_joiner
	order by mid_key
        """
        df_mid_cid = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_mid_cid}) as subq").option("user", usern).option("password", passw).load()
        df_mid_cid.filter(F.col("preferred_chid") != 0).write.format("delta").mode("overwrite").saveAsTable("mulinter.mid_cid")
        
        sql_query_hid_key_appends = f"""
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
	from
	{ref3}.d_acxiom_demographics_hhd demo
        """
        df_hid_key_appends = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_hid_key_appends}) as subq").option("user", usern).option("password", passw).load()
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
		left join
		{ref3}.f_joiner b
		on a.chid_key=b.chid_key
		full join
		{ref3}.f_account_order c
		on a.chid_key=c.chid_key
 		where (preferred_chid=1 and pri_sec=1 and a.account_stat='0' and 
			cast(c.insight_create_Dt as date) >='{muldate2}' and
			cast(c.insight_create_Dt as date) <='{muldate}'
			and order_num=1)) a
		left join  
	    {ref3}.d_household house
		on a.hid_key=house.hid_key
		left join 
		{ref3}.d_census_2010 cens
		on house.geo_cd_2010=cens.geo_cd_2010
        """
        df_household = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_household_weekly}) as subq").option("user", usern).option("password", passw).load()
        df_household.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("mulinter.household")
        
        sql_query_sp_engagements = f"""
        select distinct mid_key,count(distinct service_effective_dt) as num_sp
	from (select distinct mid_key, sp_mail_id, current_date,sp_service_type,service_effective_dt
	from unica.F_SERVICE_PARTICIPATION 
	where 
	     date(service_effective_dt)<=current_date  and months_between(current_date, 
			date(service_effective_dt))<=12 and  sp_name is not null 
			and active_engagement_flag='A')
	group by mid_key
        """
        df_sp_engagements = spark.read.format("jdbc").option("url", conn).option("dbtable", f"({sql_query_sp_engagements}) as subq").option("user", usern).option("password", passw).load()
        df_sp_engagements.write.format("delta").mode("overwrite").saveAsTable("mulinter.SP_engagements")


emu_macro()

df_bonus_layout = spark.table("intermed.bonus_layout")
df_monthly_engagements_final = spark.table("cran.monthly_engagements_final").filter(F.col("mid_key").isNotNull() & ~F.col("mid_key").isin(0, 1))

df_mid_key_appends_2 = df_bonus_layout.join(
    df_monthly_engagements_final,
    F.when(F.trim(df_bonus_layout.merkleid) == "", None).otherwise(F.trim(df_bonus_layout.merkleid)).cast("decimal(10,0)") == df_monthly_engagements_final.mid_key,
    "left"
)

df_mid_key_appends_2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.mid_key_appends_2")

df_mid_key_appends_2 = spark.table("intermed.mid_key_appends_2")
df_lifestyle_engagement_sum = spark.table("mulinter.lifestyle_engagement_sum")
df_activities_sum = spark.table("mulinter.activities_sum")
df_lifestyle_engagement_sum_1mo = spark.table("mulinter.lifestyle_engagement_sum_1mo")
df_lifestyle_engagement_sum_3mo = spark.table("mulinter.lifestyle_engagement_sum_3mo")
df_lifestyle_engagement_sum_6mo = spark.table("mulinter.lifestyle_engagement_sum_6mo")
df_lifestyle_engagement_sum_12mo = spark.table("mulinter.lifestyle_engagement_sum_12mo")
df_lifestyle_engage_prov_sum_6mo = spark.table("mulinter.lifestyle_engage_prov_sum_6mo")

df_mid_key_appends1a_2 = df_mid_key_appends_2 \
    .join(df_lifestyle_engagement_sum, F.when(F.trim(df_mid_key_appends_2.merkleid) == "", None).otherwise(F.trim(df_mid_key_appends_2.merkleid)).cast("decimal(10,0)") == df_lifestyle_engagement_sum.mid_key, "left") \
    .join(df_activities_sum, F.when(F.trim(df_mid_key_appends_2.merkleid) == "", None).otherwise(F.trim(df_mid_key_appends_2.merkleid)).cast("decimal(10,0)") == df_activities_sum.mid_key, "left") \
    .join(df_lifestyle_engagement_sum_1mo, F.when(F.trim(df_mid_key_appends_2.merkleid) == "", None).otherwise(F.trim(df_mid_key_appends_2.merkleid)).cast("decimal(10,0)") == df_lifestyle_engagement_sum_1mo.mid_key, "left") \
    .join(df_lifestyle_engagement_sum_3mo, F.when(F.trim(df_mid_key_appends_2.merkleid) == "", None).otherwise(F.trim(df_mid_key_appends_2.merkleid)).cast("decimal(10,0)") == df_lifestyle_engagement_sum_3mo.mid_key, "left") \
    .join(df_lifestyle_engagement_sum_6mo, F.when(F.trim(df_mid_key_appends_2.merkleid) == "", None).otherwise(F.trim(df_mid_key_appends_2.merkleid)).cast("decimal(10,0)") == df_lifestyle_engagement_sum_6mo.mid_key, "left") \
    .join(df_lifestyle_engagement_sum_12mo, F.when(F.trim(df_mid_key_appends_2.merkleid) == "", None).otherwise(F.trim(df_mid_key_appends_2.merkleid)).cast("decimal(10,0)") == df_lifestyle_engagement_sum_12mo.mid_key, "left") \
    .join(df_lifestyle_engage_prov_sum_6mo, F.when(F.trim(df_mid_key_appends_2.merkleid) == "", None).otherwise(F.trim(df_mid_key_appends_2.merkleid)).cast("decimal(10,0)") == df_lifestyle_engage_prov_sum_6mo.mid_key, "left")

df_mid_key_appends1a_2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.mid_key_appends1a_2")

df_mid_key_appends1a_2 = spark.table("intermed.mid_key_appends1a_2")
df_advomodel_ctc_hist = spark.table(f"weiss.advomodel_ctc_hist_{year2}{mon}_weiss").filter(F.col("mid_key").isNotNull() & ~F.col("mid_key").isin(0, 1))
df_vq_emu_ref = spark.table(f"{ref3}.vq_emu")

df_mid_key_appends2_2 = df_mid_key_appends1a_2.alias("a").join(
    df_advomodel_ctc_hist.alias("h"),
    F.when(F.trim(F.col("a.merkleid")) == "", None).otherwise(F.trim(F.col("a.merkleid"))).cast("decimal(10,0)") == F.col("h.mid_key"),
    "left"
).join(
    df_vq_emu_ref.alias("emu"),
    F.when(F.trim(F.col("a.merkleid")) == "", None).otherwise(F.trim(F.col("a.merkleid"))).cast("decimal(10,0)") == F.col("emu.mid_key"),
    "left"
).withColumn("emu_indicator", F.when(F.col("emu.mid_key").isNotNull(), 'Y').otherwise('N'))

df_mid_key_appends2_2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.mid_key_appends2_2")

df_mid_key_appends2_2 = spark.table("intermed.mid_key_appends2_2")
df_wkly_demos = spark.table("aarpdata.wkly_demos").filter(F.col("merkleid").isNotNull() & ~F.col("merkleid").isin("", "0", "1"))
df_allorderdata = spark.table("mulinter.allorderdata")
df_mid_key_indiv = spark.table("mulinter.mid_key_indiv")
df_sp_engagements = spark.table("mulinter.SP_engagements")
df_democurr = spark.table(f"aarpdata.{democurr}")

df_mid_key_appends3_2 = df_mid_key_appends2_2.alias("a") \
    .join(df_wkly_demos.alias("i"), F.col("a.merkleid") == F.col("i.merkleid"), "left") \
    .join(df_allorderdata.alias("k"), F.when(F.trim(F.col("a.merkleid")) == "", None).otherwise(F.trim(F.col("a.merkleid"))).cast("decimal(10,0)") == F.col("k.mid_key"), "left") \
    .join(df_mid_key_indiv.alias("zz"), F.when(F.trim(F.col("a.merkleid")) == "", None).otherwise(F.trim(F.col("a.merkleid"))).cast("decimal(10,0)") == F.col("zz.mid_key"), "left") \
    .join(df_sp_engagements.alias("sp"), F.when(F.trim(F.col("a.merkleid")) == "", None).otherwise(F.trim(F.col("a.merkleid"))).cast("decimal(10,0)") == F.col("sp.mid_key"), "left") \
    .join(df_democurr.alias("demo"), F.when(F.trim(F.col("a.merkleid")) == "", None).otherwise(F.trim(F.col("a.merkleid"))).cast("decimal(10,0)") == F.col("demo.mid_key"), "left") \
    .withColumn("hitech_merch", F.substring(F.col("i.mail_order_categories"), 1, 1)) \
    .withColumn("pc_prdct_buyer", F.substring(F.col("i.mail_order_categories"), 16, 1)) \
    .withColumn("Env_Humant_Educ", F.substring(F.col("i.COMMUNITY_INVOLVEMENT_CAUSES_SUP"), 2, 1)) \
    .withColumn("political", F.substring(F.col("i.COMMUNITY_INVOLVEMENT_CAUSES_SUP"), 4, 1)) \
    .withColumn("other_donors", F.substring(F.col("i.COMMUNITY_INVOLVEMENT_CAUSES_SUP"), 5, 1)) \
    .withColumn("Home_purch_yr", F.substring(F.col("demo.home_purchase_date"), 1, 4).cast("integer")) \
    .withColumn("ibx_vehicle_dominant_lifestyle_p", F.col("i.vehicle_dominant_lifestyle"))

df_mid_key_appends3_2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.mid_key_appends3_2")

df_mid_key_appends3_2 = spark.table("intermed.mid_key_appends3_2")
df_mid_cid = spark.table("mulinter.mid_cid")
df_all_donors = spark.table("weiss.all_donors").filter(F.col("cid_key").isNotNull())

df_cid_appends = df_mid_key_appends3_2.drop("mid_key").alias("a") \
    .join(df_mid_cid.alias("b"), F.when(F.trim(F.col("a.merkleid")) == "", None).otherwise(F.trim(F.col("a.merkleid"))).cast("decimal(10,0)") == F.col("b.mid_key"), "left") \
    .join(df_all_donors.alias("j"), F.col("b.cid_key") == F.col("j.cid_key"), "left")

df_cid_appends.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.cid_appends")

df_cid_appends = spark.table("intermed.cid_appends").dropDuplicates(["merkleid"])
df_cid_appends.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.cid_appends")

df_bonus_layout = spark.table("intermed.bonus_layout").select("memacctnum", "merkleid")
df_cid_appends = spark.table("intermed.cid_appends")

df_geo_cid_hid = df_bonus_layout.alias("a").join(
    df_cid_appends.alias("cid"),
    F.col("a.merkleid") == F.col("cid.merkleid"),
    "left"
)
df_geo_cid_hid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_cid_hid")

df_geo_cid_hid = spark.table("intermed.geo_cid_hid")
df_household = spark.table("mulinter.household")
df_hid_key_appends = spark.table("mulinter.hid_key_appends")

df_geo_appends = df_geo_cid_hid.withColumnRenamed("reppartycd", "reppartycd_bl").alias("a") \
    .join(df_household.alias("c"), F.col("a.hid_key") == F.col("c.hid_key"), "left") \
    .join(df_hid_key_appends.alias("b"), F.col("a.hid_key") == F.col("b.hid_key"), "left") \
    .withColumn("CENS_INC_HH_PERCENT_HOUSEHOLD_19", F.col("CENS_INC_HH_PERCENT_HOUSEHOLD_IN") + F.col("CENS_INC_HH_PERCENT_HOUSEHOLD_I0") + F.col("CENS_INC_HH_PERCENT_HOUSEHOLD_I1")) \
    .withColumn("cens_educ_pop25_plus_percent_col", F.col("cens_educ_pop25_plus_percent_bac") + F.col("cens_educ_pop25_plus_percent_pro")) \
    .withColumn("cens_BLUECOLLAR", F.col("CENS_OCCUP_EMPLD_PERCENT_FIRE_AN") + F.col("CENS_OCCUP_EMPLD_PERCENT_LAW_ENF") + F.col("CENS_OCCUP_EMPLD_PERCENT_FOOD_PR") + F.col("CENS_OCCUP_EMPLD_PERCENT_BLDG_AN") + F.col("CENS_OCCUP_EMPLD_PERCENT_PERSONA") + F.col("CENS_OCCUP_EMPLD_PERCENT_FARM_FI") + F.col("CENS_OCCUP_EMPLD_PERCENT_CONSTR_") + F.col("CENS_OCCUP_EMPLD_PERCENT_INSTALL") + F.col("CENS_OCCUP_EMPLD_PERCENT_PRODUCT") + F.col("CENS_OCCUP_EMPLD_PERCENT_TRANS_A") + F.col("CENS_OCCUP_EMPLD_PERCENT_MOTOR_V") + F.col("CENS_OCCUP_EMPLD_PERCENT_MATERIA")) \
    .withColumn("cens_MANAGEMENTPROFESSIONALS", F.col("CENS_OCCUP_EMPLD_PERCENT_MANAGEM") + F.col("CENS_OCCUP_EMPLD_PERCENT_LEGAL") + F.col("CENS_OCCUP_EMPLD_PERCENT_ARCHITE")) \
    .withColumn("cens_WHITECOLLAR", F.col("CENS_OCCUP_EMPLD_PERCENT_MANAGEM") + F.col("CENS_OCCUP_EMPLD_PERCENT_LEGAL") + F.col("CENS_OCCUP_EMPLD_PERCENT_ARCHITE") + F.col("CENS_OCCUP_EMPLD_PERCENT_BUS_AND") + F.col("CENS_OCCUP_EMPLD_PERCENT_COMPUTE") + F.col("CENS_OCCUP_EMPLD_PERCENT_HEALTH_") + F.col("CENS_OCCUP_EMPLD_PERCENT_LIFE_PH") + F.col("CENS_OCCUP_EMPLD_PERCENT_SALES_A")) \
    .withColumn("mid_key", F.when(F.trim(F.col("a.merkleid")) == "", None).otherwise(F.trim(F.col("a.merkleid"))).cast("decimal(10,0)")) \
    .withColumn("reppartycd", F.coalesce(F.col("reppartycd_bl"), F.col("representative_party_cd")))

df_geo_appends = df_geo_appends.dropDuplicates(["memacctnum", "merkleid"])
df_geo_appends.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends")

freq_cols = [
    "ibx_age_in_two_year_increments_2nd_individual_premier", "ibx_children_age_ranges_present_in_household_premier", "ibx_children_presence_of_household",
    "IBX_CREDIT_CARD_IDT_PREMIER", "IBX_DONATION_CONTRIBUTION", "IBX_GREEN_LIVING", "IBX_HEALTH_BEAUTY", "IBX_HEALTH_HOMEOPATHIC",
    "ibx_home_lot_square_footage_ranges", "IBX_HOME_MARKET_VALUE_DECILES", "IBX_HOME_MARKET_VALUE_PREMIER", "IBX_HOME_OWNER_RENTER_PREMIER",
    "IBX_HOME_OWNER", "IBX_HOME_YEAR_BUILT_ACTUAL", "IBX_INFERRED_HOUSEHOLD_RANK", "ibx_investing_finance_grouping_premier",
    "IBX_NETWORTH_PREMIER", "IBX_NUM_OF_LINES_OF_CREDIT", "ibx_occupation_1st_individual_premier", "IBX_OWNER_TYPE_DETAIL",
    "IBX_PERSONIC_CLUSTER", "IBX_HOME_ASSESSED_VALUE_RANGES", "IBX_MOVIE_MUSIC_GROUPING", "ibx_race_cd_input_individual_premier",
    "IBX_TOTAL_ONLINE_PURCHASES", "IBX_PC_OWNER_PREMIER", "ibx_weeks_since_last_online_order", "ibx_retail_purchases_most_frequent_cat",
    "IBX_HOME_LOAN_AMOUNT_1_RANGES", "IBX_HOUSEHOLD_INCOME", "ibx_vehicle_dominant_lifestyle_p", "IBX_HEALTH_DIABETIC",
    "ibx_current_affairs_politics_premier", "ibx_education_input_individual_premier", "ibx_membership_clubs", "IBX_PETS",
    "ibx_adults_number_of_household_premier", "ibx_political_party_input_individual", "ibx_vehicle_known_owned_number_premier", "IBX_RELIGIOUS_INSPIRATIONAL_PREMier",
    "IBX_TRAVEL_FOREIGN_PREMIER", "ibx_trends_for_telecom_internet_user", "ibx_home_lender_type_1", "ibx_home_property_type_details",
    "ibx_education_1st_individual", "ibx_trends_for_telecom_optional_calling_services", "IBX_HEALTH_MEDICAL_SUPPLIES", "ibx_health_nutraceuticals_vitamins",
    "IBX_RECREATIONAL_VEHICLES_PREMIEr", "ibx_vacation_travel_internationa", "Past12MoTouchCt_AARP", "Past12MoTouchCt_Financial",
    "Past12MoTouchCt_Health", "Past12MoTouchCt_Home", "Past12MoTouchCt_Overall", "Past12MoTouchCt_Priv", "Past12MoTouchCt_Travel",
    "Past3MoTouchCt_AARP", "Past3MoTouchCt_Financial", "Past3MoTouchCt_Health", "Past3MoTouchCt_Home", "Past3MoTouchCt_Overall",
    "Past3MoTouchCt_Priv", "Past3MoTouchCt_Travel", "sy_otsbn_polfund_2012a", "sy_otsbn_polfund_2012b",
    "vehicle_dominant_lifestyle", "vehicle_known_owned_number", "ACEV_Flag", "ACEV_Num", "advo_segment_cd",
    "audio_visual_composite", "Chase_Num_Active_Particpnts", "Chase_Num_InActive_Particpnts",
    "CurrentPartCt_Overall", "emailable_agg_ind", "Env_Humant_Educ", "EthnicCode", "Foremost_Num_Active_Particpnts",
    "Gender", "gender_agg_ind", "gender_input", "GeneralElectn2012", "GE_Num_Active_Particpnts",
    "Globally_Opted_In", "GroupEthnicCode", "Hartford_Num_Active_Particpnts",
    "HistPartCt_Overall", "hitech_merch", "home_purchase_date", "household_size",
    "mail_order_donor_categories", "MaritalStatus", "MemStatus", "na2", "NbrTimesSelEmailedInd",
    "NYL_Num_Active_Particpnts", "NYL_Num_InActive_Particpnts", "other_donors", "outdoors_dimension",
    "Overall_Active_SP_Reltshps", "Overall_Historic_SP_Reltshps", "PartyAffiliation", "PartyMix",
    "pc_prdct_buyer", "political", "ReligionCode", "RepPartyCd", "State", "travel_us_premier",
    "VoterStatus", "voter_party_input", "vtm_active_vol_flag_act", "vtm_vol_flag_act", "WorkStatus",
    "MEMBER_FL_AGG_IND", "LapsMail", "totalmailings", "prospmail", "acknow", "advo_petition", "community",
    "exercise_health_group", "region", "ftc_dnc_append_fl", "UNINSURED_MODEL", "ETHNICITY",
    "order_type", "fishing", "nascar", "diy_living", "environmental_issues", "gaming_casino", "hunting_shooting",
    "investments_personal", "reading_financial_newsletter_sub", "reading_general", "reading_religious_inspirational",
    "science_space", "smoking_tobacco", "spectator_sports_auto_motorcycle", "spectator_sports_basketball",
    "spectator_sports_hockey", "spectator_sports_tennis", "strange_and_unusual", "theater_performing_arts",
    "em", "advocacy_petition_signer", "foundation_donor", "dieting_weight_loss", "exercise_aerobic",
    "exercise_walking", "home_pool_present", "Tele_Townhall_Engagers", "advocacy_grassroots_engager",
    "attended_aarp_event", "ibx_community_involvement_causes", "num_entertainment_visits_past_3m",
    "phn", "motorcycling", "auto_work", "boating_sailing", "broader_living", "collectibles_coins",
    "education_online", "home_furnishings_decorating", "music_collector", "reading_best_sellers",
    "spect_sports_motorcycle_racing", "sweeps_contests", "tv_guide_network", "engaged_aarp_event",
    "childrens_interests", "spectator_sports_football"
]

all_freqs = []
total_count = df_geo_appends.count()

for col_name in freq_cols:
    if col_name in df_geo_appends.columns:
        freq_df = df_geo_appends.groupBy(col_name).count() \
            .withColumn("table", F.lit(col_name)) \
            .withColumn("percent", (F.col("count") / total_count) * 100) \
            .select(
                F.col("table").alias("col_name"),
                F.col(col_name).alias("col_level"),
                F.col("count").alias("_freq_"),
                "percent"
            )
        all_freqs.append(freq_df)

if all_freqs:
    df_input_characters_pct = reduce(DataFrame.union, all_freqs)
    df_input_characters_pct = df_input_characters_pct.orderBy("col_name", "col_level")
    df_input_characters_pct.write.format("delta").mode("overwrite").saveAsTable(f"scoring.Input_Char_Var_{runtype}_{muldate}")
    df_input_characters_pct.show()

numeric_cols_for_means = [c for c, t in df_geo_appends.dtypes if t in ('int', 'double', 'float', 'decimal', 'long', 'short') and c not in ('hid_key', 'mid_key', 'cid_key', 'memacctnum')]
df_summary = df_geo_appends.select(numeric_cols_for_means).summary("count", "mean", "stddev", "min", "25%", "75%", "max")
df_summary_transposed = df_summary.withColumn("id", F.monotonically_increasing_id())
stack_expr = "stack(8, 'NMiss', `count`, 'Mean', `mean`, 'Std', `stddev`, 'Min', `min`, 'P25', `25%`, 'P75', `75%`, 'Max', `max`) as (statistic, value)"
# this is a mock transpose since proc transpose behavior is complex. This should be adjusted based on exact output needed.
df_summary.show()
df_summary.write.format("delta").mode("overwrite").saveAsTable(f"scoring.Input_Num_Var_{runtype}_{muldate}")

def cleanup_macro(dsn):
    if spark.catalog.tableExists(dsn):
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
        spark.sql("DROP TABLE IF EXISTS intermed.emu_hid")
        spark.sql("DROP TABLE IF EXISTS intermed.emu_mid_indiv")
        spark.sql("DROP TABLE IF EXISTS intermed.emu_account")
    else:
        print(f"Data set {dsn} does not exist")

cleanup_macro("intermed.geo_appends")
#End-DBShift