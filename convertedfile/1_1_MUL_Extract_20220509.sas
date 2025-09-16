import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType
from functools import reduce

# twoyears_native = spark.sql("SELECT date_sub(to_date('{muldate}'), 732)").collect()[0][0]

# if endofmon == "EoMay17":
#     engagename = "monthly_engagements_final_may17"
# else:
#     engagename = "monthly_engagements_final"
# print(engagename)

# Define the schema for the fixed-width file based on the INPUT statement
# This assumes the input file is located at the path specified by the 'bl' fileref
# The path needs to be configured in the Databricks environment.
# e.g., bl_path = "dbfs:/path/to/n_MUL_20190816.TXT"
bl_path = "/vg01/aarp_sas/ftp/incoming/mdsm/n_MUL_20190816.TXT" # Placeholder path

df_raw_bonus_layout = spark.read.text(bl_path)

df_intermed_bonus_layout = df_raw_bonus_layout.select(
    F.substring(F.col("value"), 1, 12).alias("KeyCode"),
    F.substring(F.col("value"), 13, 10).cast(IntegerType()).alias("MemAcctNum"),
    F.substring(F.col("value"), 23, 12).alias("MerkleID"),
    F.substring(F.col("value"), 35, 5).alias("CampaignID"),
    F.substring(F.col("value"), 40, 3).cast(IntegerType()).alias("cntct_lifstyle_12mo_agg_hhd"),
    F.substring(F.col("value"), 43, 3).cast(IntegerType()).alias("cntct_lifstyle_3mo_agg_hhd"),
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
    F.substring(F.col("value"), 78, 8).cast(IntegerType()).alias("CoaDate"),
    F.substring(F.col("value"), 86, 3).cast(IntegerType()).alias("Age"),
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
    F.substring(F.col("value"), 99, 3).cast(IntegerType()).alias("SecAge"),
    F.substring(F.col("value"), 102, 1).alias("SegTypes"),
    F.substring(F.col("value"), 103, 1).alias("MemType"),
    F.substring(F.col("value"), 104, 1).alias("MemStatus"),
    F.substring(F.col("value"), 105, 8).cast(IntegerType()).alias("MemOriginDate"),
    F.substring(F.col("value"), 113, 9).alias("MemOriginKey"),
    F.substring(F.col("value"), 123, 1).cast(IntegerType()).alias("MemXRenew"),
    F.substring(F.col("value"), 124, 6).cast(IntegerType()).alias("MemPaidDate"),
    F.substring(F.col("value"), 132, 3).alias("NA2"),
    F.substring(F.col("value"), 135, 9).alias("NA3"),
    F.substring(F.col("value"), 144, 8).alias("curr_order_create_dt"),
    F.substring(F.col("value"), 152, 2).alias("CoaSource"),
    F.substring(F.col("value"), 154, 8).alias("LastPromoDate"),
    F.substring(F.col("value"), 162, 9).alias("LastPromoKey"),
    F.substring(F.col("value"), 211, 10).cast(DoubleType()).alias("GeoAvgVal"),
    F.substring(F.col("value"), 227, 12).alias("GeoCode"),
    F.substring(F.col("value"), 252, 10).cast(DoubleType()).alias("GeoMedVal"),
    F.substring(F.col("value"), 265, 3).cast(IntegerType()).alias("GeoOccHouseUnit"),
    F.substring(F.col("value"), 435, 2).alias("EthnicCode"),
    F.substring(F.col("value"), 437, 1).alias("ReligionCode"),
    F.substring(F.col("value"), 438, 2).alias("LanguageCode"),
    F.substring(F.col("value"), 440, 2).cast(IntegerType()).alias("OriginCode"),
    F.substring(F.col("value"), 442, 1).alias("GroupEthnicCode"),
    F.substring(F.col("value"), 448, 1).cast(IntegerType()).alias("Life_Stage"),
    F.substring(F.col("value"), 449, 1).cast(IntegerType()).alias("diversity_flag_agg_ind"),
    F.substring(F.col("value"), 450, 3).cast(IntegerType()).alias("Diversity_Subgroup_Agg_Ind"),
    F.substring(F.col("value"), 479, 6).alias("FedHouse"),
    F.substring(F.col("value"), 491, 6).alias("StateHouse"),
    F.substring(F.col("value"), 497, 6).alias("StateSenate"),
    F.substring(F.col("value"), 503, 10).alias("PartyCode"),
    F.substring(F.col("value"), 513, 10).alias("PartyMix"),
    F.substring(F.col("value"), 523, 3).alias("RepPartyCd"),
    F.substring(F.col("value"), 526, 8).alias("RegistrationDate"),
    F.substring(F.col("value"), 534, 1).alias("IS_Voter"),
    F.substring(F.col("value"), 535, 10).cast(IntegerType()).alias("VoterCount"),
    F.substring(F.col("value"), 685, 3).alias("PartyAffiliation"),
    F.substring(F.col("value"), 688, 10).alias("EarliestRegistrationDate"),
    F.substring(F.col("value"), 698, 19).alias("VoterStatus"),
    F.substring(F.col("value"), 717, 10).alias("PrimElectn2012"),
    F.substring(F.col("value"), 727, 10).alias("GeneralElectn2012"),
    F.substring(F.col("value"), 737, 10).alias("SpecialElectn2012"),
    F.substring(F.col("value"), 747, 6).cast(IntegerType()).alias("sy_otsbn_polfund_2012a"),
    F.substring(F.col("value"), 753, 6).cast(IntegerType()).alias("sy_otsbn_polfund_2012b"),
    F.substring(F.col("value"), 770, 2).alias("advo_segment_cd"),
    F.substring(F.col("value"), 772, 1).alias("EmailableInd"),
    F.substring(F.col("value"), 773, 3).alias("NbrTimesSelEmailedInd"),
    F.substring(F.col("value"), 776, 1).alias("Globally_Opted_In"),
    F.substring(F.col("value"), 777, 2).cast(IntegerType()).alias("SUPERCLUSTER_CODE"),
    F.substring(F.col("value"), 779, 1).alias("Aristotle_flag"),
    F.substring(F.col("value"), 780, 12).alias("geo_cd_2010"),
    F.substring(F.col("value"), 792, 5).cast(IntegerType()).alias("Age_HH_pct_with_HHer_55_64"),
    F.substring(F.col("value"), 797, 5).cast(IntegerType()).alias("Age_HH_pct_with_HHer_65_74"),
    F.substring(F.col("value"), 802, 5).cast(IntegerType()).alias("Age_HH_pct_with_HHer_75_84"),
    F.substring(F.col("value"), 807, 5).cast(IntegerType()).alias("Age_HH_pct_with_HHer_85p"),
    F.substring(F.col("value"), 812, 5).cast(IntegerType()).alias("Age_Pop_pct_60_64"),
    F.substring(F.col("value"), 817, 5).cast(IntegerType()).alias("HH_pct_Spanish_Speaking"),
    F.substring(F.col("value"), 822, 10).cast(DoubleType()).alias("HomVal_Home_Value_CBSA_Index"),
    F.substring(F.col("value"), 832, 10).cast(DoubleType()).alias("Inc_HH_Median_HH_Income"),
    F.substring(F.col("value"), 842, 5).cast(IntegerType()).alias("OCCHU_Median_Length_of_Residence"),
    F.substring(F.col("value"), 847, 10).cast(DoubleType()).alias("OOHU_Median_Home_Value"),
    F.substring(F.col("value"), 857, 5).cast(IntegerType()).alias("Pop_pct_Asian_Only_Hisp"),
    F.substring(F.col("value"), 862, 5).cast(IntegerType()).alias("Pop_pct_Asian_Only_"),
    F.substring(F.col("value"), 867, 5).cast(IntegerType()).alias("Pop_pct_Black_Only_Hisp"),
    F.substring(F.col("value"), 872, 5).cast(IntegerType()).alias("Pop_pct_Black_Only_"),
    F.substring(F.col("value"), 997, 1).cast(IntegerType()).alias("DRVS_Flag"),
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
    F.substring(F.col("value"), 1359, 12).cast(DoubleType()).alias("Fndn_TTD_Amt"),
    F.substring(F.col("value"), 1371, 8).cast(IntegerType()).alias("Fndn_TTD_Num"),
    F.substring(F.col("value"), 1379, 8).cast(IntegerType()).alias("Fndn_Last_Amt"),
    F.substring(F.col("value"), 1387, 8).cast(IntegerType()).alias("Fndn_Last_Dt"),
    F.substring(F.col("value"), 1395, 12).cast(DoubleType()).alias("Advo_TTD_Amt"),
    F.substring(F.col("value"), 1407, 8).cast(IntegerType()).alias("Advo_TTD_Num"),
    F.substring(F.col("value"), 1415, 8).cast(IntegerType()).alias("Advo_Last_Amt"),
    F.substring(F.col("value"), 1423, 8).cast(IntegerType()).alias("Advo_Last_Dt"),
    F.substring(F.col("value"), 1431, 8).cast(IntegerType()).alias("Advo_Last_Petition_Dt_Agg_Ind"),
    F.substring(F.col("value"), 1439, 10).alias("Advo_Last_Petition_Subj_Agg_Ind"),
    F.substring(F.col("value"), 1449, 8).cast(IntegerType()).alias("advo_hpc_amt"),
    F.substring(F.col("value"), 1457, 8).cast(IntegerType()).alias("advo_hpc_dt"),
    F.substring(F.col("value"), 1465, 8).cast(IntegerType()).alias("advo_mrhpc_amt"),
    F.substring(F.col("value"), 1473, 8).cast(IntegerType()).alias("advo_mrhpc_dt"),
    F.substring(F.col("value"), 1481, 8).cast(IntegerType()).alias("fndn_hpc_amt"),
    F.substring(F.col("value"), 1489, 8).cast(IntegerType()).alias("fndn_hpc_dt"),
    F.substring(F.col("value"), 1497, 8).cast(IntegerType()).alias("fndn_mrhpc_amt"),
    F.substring(F.col("value"), 1505, 8).cast(IntegerType()).alias("fndn_mrhpc_dt"),
    F.substring(F.col("value"), 1513, 8).cast(DoubleType()).alias("partisanscore"),
    F.substring(F.col("value"), 1521, 8).cast(DoubleType()).alias("ideology"),
    F.substring(F.col("value"), 1529, 1).alias("vtm_active_vol_flag_act"),
    F.substring(F.col("value"), 1530, 1).alias("vtm_dsp_active_vol"),
    F.substring(F.col("value"), 1531, 100).alias("vtm_last_initiative_act"),
    F.substring(F.col("value"), 1631, 80).alias("vtm_last_program_act"),
    F.substring(F.col("value"), 1711, 100).alias("vtm_last_role_act"),
    F.substring(F.col("value"), 1811, 8).alias("vtm_assignment_last_start_dt_act"),
    F.substring(F.col("value"), 1819, 3).cast(IntegerType()).alias("vtm_num_last_12m_assignments_act"),
    F.substring(F.col("value"), 1822, 3).cast(IntegerType()).alias("vtm_num_assignments_act"),
    F.substring(F.col("value"), 1825, 3).cast(IntegerType()).alias("vtm_num_ytd_assignments_act"),
    F.substring(F.col("value"), 1828, 1).alias("vtm_vol_flag_act")
)

df_intermed_bonus_layout = df_intermed_bonus_layout.withColumn("Age_HH_pct_with_HHer_55_64", F.col("Age_HH_pct_with_HHer_55_64") * 10)
df_intermed_bonus_layout = df_intermed_bonus_layout.withColumn("Age_HH_pct_with_HHer_65_74", F.col("Age_HH_pct_with_HHer_65_74") * 10)
df_intermed_bonus_layout = df_intermed_bonus_layout.withColumn("HH_pct_Spanish_Speaking", F.col("HH_pct_Spanish_Speaking") * 10)
df_intermed_bonus_layout = df_intermed_bonus_layout.withColumn("Age_HH_pct_with_HHer_75_84", F.col("Age_HH_pct_with_HHer_75_84") * 10)
df_intermed_bonus_layout = df_intermed_bonus_layout.withColumn("Age_HH_pct_with_HHer_85p", F.col("Age_HH_pct_with_HHer_85p") * 10)
df_intermed_bonus_layout = df_intermed_bonus_layout.withColumn("Pop_pct_Asian_Only_Hisp", F.col("Pop_pct_Asian_Only_Hisp") * 10)
df_intermed_bonus_layout = df_intermed_bonus_layout.withColumn("Pop_pct_Asian_Only_", F.col("Pop_pct_Asian_Only_") * 10)
df_intermed_bonus_layout = df_intermed_bonus_layout.withColumn("Pop_pct_Black_Only_Hisp", F.col("Pop_pct_Black_Only_Hisp") * 10)
df_intermed_bonus_layout = df_intermed_bonus_layout.withColumn("Pop_pct_Black_Only_", F.col("Pop_pct_Black_Only_") * 10)
df_intermed_bonus_layout = df_intermed_bonus_layout.withColumn("OCCHU_Median_Length_of_Residence", F.col("OCCHU_Median_Length_of_Residence") * 100)

df_intermed_bonus_layout = df_intermed_bonus_layout.withColumn("Region", 
    F.when(F.upper(F.col("STATE")).isin('AK', 'CO', 'HI', 'ID', 'MT', 'NV', 'NM', 'OR', 'UT', 'WY'), 'West Region')
     .when(F.upper(F.col("STATE")).isin('AR', 'IA', 'KS', 'MN', 'NE', 'ND', 'OK', 'SD', 'WI'), 'Central Region')
     .when(F.upper(F.col("STATE")).isin('AL', 'DC', 'KY', 'LA', 'MD', 'MS', 'SC', 'VA', 'WV'), 'South Region')
     .when(F.upper(F.col("STATE")).isin('CT', 'DE', 'ME', 'MA', 'NH', 'PR', 'RI', 'VT', 'VI'), 'East Coast Region')
     .when(F.upper(F.col("STATE")).isin('AZ', 'GA', 'IN', 'MI', 'MO', 'NJ', 'NC', 'TN', 'WA'), 'Large Region')
     .when(F.upper(F.col("STATE")).isin('CA', 'FL', 'IL', 'NY', 'OH', 'PA', 'TX'), 'Mega Region')
     .otherwise('Unknown')
)

df_intermed_bonus_layout = df_intermed_bonus_layout.withColumn("PrimElectn2012",
    F.when(F.col("PrimElectn2012") == 'absentee', 'A')
     .when(F.col("PrimElectn2012") == 'earlyVote', 'E')
     .when(F.col("PrimElectn2012") == 'mail', 'M')
     .when(F.col("PrimElectn2012").isin('polling', 'unknown'), 'Y')
     .otherwise(F.col("PrimElectn2012"))
)

df_intermed_bonus_layout = df_intermed_bonus_layout.withColumn("GeneralElectn2012",
    F.when(F.col("GeneralElectn2012") == 'absentee', 'A')
     .when(F.col("GeneralElectn2012") == 'earlyVote', 'E')
     .when(F.col("GeneralElectn2012") == 'mail', 'M')
     .when(F.col("GeneralElectn2012").isin('polling', 'unknown'), 'Y')
     .otherwise(F.col("GeneralElectn2012"))
)

df_intermed_bonus_layout = df_intermed_bonus_layout.withColumn("SpecialElectn2012",
    F.when(F.col("SpecialElectn2012") == 'absentee', 'A')
     .when(F.col("SpecialElectn2012") == 'earlyVote', 'E')
     .when(F.col("SpecialElectn2012") == 'mail', 'M')
     .when(F.col("SpecialElectn2012").isin('polling', 'unknown'), 'Y')
     .otherwise(F.col("SpecialElectn2012"))
)

df_intermed_bonus_layout.write.format("delta").mode("overwrite").saveAsTable("intermed.BONUS_LAYOUT")

# df_summary_view = df_intermed_bonus_layout.select("FNDN_TTD_AMT", "FNDN_TTD_NUM").summary()
# df_summary_view.show()

# df_intermed_bonus_layout = df_intermed_bonus_layout.dropDuplicates(["MemAcctNum"])

def emu(conn, dsn, usern, passw, ref3, runtype, twobegdt, muldate, ONEBEGDT, enddt, mon, day, year2, democurr, bl_bucket, bl_key, bl_secret):
    if runtype == 'Monthly':
        df_intermed_bonus_layout = spark.table("intermed.bonus_layout")
        
        sql_query_emu = f"""
        select mid_key
	from {ref3}.vq_emu
        """
        df_emu_from_db = spark.read.format("jdbc").option("url", f"jdbc:{conn}").option("dsn", dsn).option("user", usern).option("password", passw).option("dbtable", f"({sql_query_emu}) as subq").load()
        
        df_intermed_emu = df_emu_from_db.alias("b").join(
            df_intermed_bonus_layout.alias("a"),
            F.col("a.merkleid").cast('int') == F.col("b.mid_key"),
            "left"
        ).where(F.col("a.merkleid").isNull()).select("b.mid_key").orderBy("b.mid_key")
        
        df_intermed_emu.write.format("delta").mode("overwrite").saveAsTable("intermed.emu")
        
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
		fndn_hpc_amt,	/* added 20190610 for fndn_em_proseng */
		advo_mrhpc_amt, /* added 20201014 for major_gifts  */
		case when vtm_active_vol_flag_ind=1 then 'Y' else 'N' end as vtm_active_vol_flag_act,
		case when vtm_dsp_active_vol_ind=1 then 'Y' else 'N' end as vtm_dsp_active_vol,
		vtm_num_assignments_ind as vtm_num_assignments_act, vtm_last_role_ind as vtm_last_role_act,
		addr_move_dt,fndn_mrhpc_dt,birth_dt_agg_ind,
		/* added 20210521  */
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
        df_emu_mid_indiv_from_db = spark.read.format("jdbc").option("url", f"jdbc:{conn}").option("dsn", dsn).option("user", usern).option("password", passw).option("dbtable", f"({sql_query_emu_mid_indiv}) as subq").load()
        
        df_intermed_emu_mid_indiv = df_emu_mid_indiv_from_db.withColumn("dateofbirth", F.when(F.col("birth_dt_agg_ind").isNotNull(), '1').otherwise('0')).drop("birth_dt_agg_ind")
        df_intermed_emu_mid_indiv.write.format("delta").mode("overwrite").saveAsTable("intermed.emu_mid_indiv")
        
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
			when UPper(STATE) in ('AK',  'CO', 'HI', 'ID',  'MT', 'NV', 'NM', 'OR','UT', 'WY') THEN 'West Region'
 			when UPper(STATE) in ('AR', 'IA', 'KS' ,'MN', 'NE', 'ND', 'OK', 'SD', 'WI')        THEN 'Central Region'
 			when UPper(STATE) in ('AL', 'DC', 'KY', 'LA', 'MD', 'MS', 'SC', 'VA', 'WV')        THEN 'South Region'
 			when UPper(STATE) in ('CT', 'DE', 'ME', 'MA', 'NH', 'PR', 'RI', 'VT', 'VI')		THEN 'East Coast Region'
 			when UPper(STATE) in ('AZ', 'GA', 'IN', 'MI', 'MO', 'NJ', 'NC', 'TN', 'WA')		THEN 'Large Region'
 			when UPper(STATE) in ('CA', 'FL', 'IL', 'NY', 'OH', 'PA', 'TX')					THEN 'Mega Region'
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
        df_intermed_emu_hid = spark.read.format("jdbc").option("url", f"jdbc:{conn}").option("dsn", dsn).option("user", usern).option("password", passw).option("dbtable", f"({sql_query_emu_hid}) as subq").load()
        df_intermed_emu_hid.write.format("delta").mode("overwrite").saveAsTable("intermed.emu_hid")
        
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
        df_emu_account_from_db = spark.read.format("jdbc").option("url", f"jdbc:{conn}").option("dsn", dsn).option("user", usern).option("password", passw).option("dbtable", f"({sql_query_emu_account}) as subq").load()
        
        df_intermed_emu_account = df_emu_account_from_db.withColumn("mempaiddate", F.date_format(F.to_date(F.col("paid_through_dt")), "yyMMdd").cast("int")).withColumn("acev_num_i", F.col("acev_num")).drop("acev_num")
        df_intermed_emu_account.write.format("delta").mode("overwrite").saveAsTable("intermed.emu_account")
        
        df_emu = spark.table("intermed.emu")
        df_emu_mid_indiv = spark.table("intermed.emu_mid_indiv")
        df_emu_hid = spark.table("intermed.emu_hid")
        df_emu_account = spark.table("intermed.emu_account")

        df_emu.createOrReplaceTempView("emu")
        df_emu_mid_indiv.createOrReplaceTempView("emu_mid_indiv")
        df_emu_hid.createOrReplaceTempView("emu_hid")
        df_emu_account.createOrReplaceTempView("emu_account")
        
        df_intermed_emu_complete = spark.sql("""
            SELECT *,
                CAST(lifestage_segment AS INT) as Life_Stage,
                CAST(num_times_selected_email_agg_ind AS STRING) as NbrTimesSelEmailedInd,
                CAST(IBX_COUNTRY_OF_ORIGIN_CODE_E_TEC AS INT) as OriginCode,
                CAST(DATE_FORMAT(TO_DATE(CAST(Advo_Last_Dt_i AS STRING)), 'yyyyMMdd') AS INT) as Advo_Last_Dt,
                CAST(DATE_FORMAT(TO_DATE(CAST(Fndn_Last_Dt_i AS STRING)), 'yyyyMMdd') AS INT) as Fndn_Last_Dt,
                CAST(DATE_FORMAT(TO_DATE(CAST(advo_last_petition_dt_agg_ind_i AS STRING)), 'yyyyMMdd') AS INT) as advo_last_petition_dt_agg_ind,
                TO_DATE(CAST(addr_move_dt AS STRING)) as movedate,
                CAST(DATE_FORMAT(TO_DATE(CAST(addr_move_dt AS STRING)), 'yyyyMMdd') AS INT) as coadate,
                '          ' as PartyMix, 
                CAST(NULL AS INT) as VoterCount, 
                REGEXP_REPLACE(CAST(Num_Hist_Participation_Overall_A AS STRING), '[ .]', '') as HistPartCt_Overall,
                REGEXP_REPLACE(CAST(Num_Contact_Aarp_12_Months_Agg_H AS STRING), '[ .]', '') as Past12MoTouchCt_AARP,
                REGEXP_REPLACE(CAST(Num_Contact_Financial_12_Months_ AS STRING), '[ .]', '') as Past12MoTouchCt_Financial,
                REGEXP_REPLACE(CAST(Num_Contact_Health_12_Months_Agg AS STRING), '[ .]', '') as Past12MoTouchCt_Health,
                REGEXP_REPLACE(CAST(Num_Contact_Overall_12_Months_Ag AS STRING), '[ .]', '') as Past12MoTouchCt_Overall,
                REGEXP_REPLACE(CAST(num_contact_discounts_12_months_ AS STRING), '[ .]', '') as Past12MoTouchCt_Priv,
                REGEXP_REPLACE(CAST(num_contact_travel_12_months_agg AS STRING), '[ .]', '') as Past12MoTouchCt_Travel,
                REGEXP_REPLACE(CAST(Num_Contact_Aarp_3_Months_Agg_Hh AS STRING), '[ .]', '') as Past3MoTouchCt_AARP,
                REGEXP_REPLACE(CAST(Num_Contact_Financial_3_Months_A AS STRING), '[ .]', '') as Past3MoTouchCt_Financial,
                REGEXP_REPLACE(CAST(Num_Contact_Health_3_Months_Agg_ AS STRING), '[ .]', '') as Past3MoTouchCt_Health, 
                REGEXP_REPLACE(CAST(Num_Contact_Overall_3_Months_Agg AS STRING), '[ .]', '') as Past3MoTouchCt_Overall,
                REGEXP_REPLACE(CAST(num_contact_discounts_3_months_a AS STRING), '[ .]', '') as Past3MoTouchCt_Priv,
                REGEXP_REPLACE(CAST(NUM_CURR_PARTICIPATION_OVERALL_A AS STRING), '[ .]', '') as CurrentPartCt_Overall,
                CASE WHEN acev_num_i > 99 THEN '*' ELSE REGEXP_REPLACE(CAST(acev_num_i AS STRING), '[ .]', '') END as acev_num,
                REGEXP_REPLACE(CAST(num_active_part_chase_agg_act AS STRING), '[ .]', '') as Chase_Num_Active_Particpnts,
                REGEXP_REPLACE(CAST(num_hist_part_chase_agg_act AS STRING), '[ .]', '') as Chase_Num_InActive_Particpnts,
                DATE_FORMAT(TO_DATE(CAST(current_order_CREATE_dt_agg_act AS STRING)), 'yyyyMMdd') as curr_order_create_dt,
                REGEXP_REPLACE(CAST(num_active_part_foremost_agg_act AS STRING), '[ .]', '') as Foremost_Num_Active_Particpnts,
                REGEXP_REPLACE(CAST(num_active_part_ge_agg_act AS STRING), '[ .]', '') as GE_Num_Active_Particpnts,
                REGEXP_REPLACE(CAST(num_active_part_hartford_agg_act AS STRING), '[ .]', '') as Hartford_Num_Active_Particpnts,
                REGEXP_REPLACE(CAST(num_active_part_nyl_agg_act AS STRING), '[ .]', '') as NYL_Num_Active_Particpnts,
                REGEXP_REPLACE(CAST(num_hist_part_nyl_agg_act AS STRING), '[ .]', '') as NYL_Num_InActive_Particpnts,
                CAST(DATE_FORMAT(TO_DATE(CAST(Kx_Create_Dt AS STRING)), 'yyyyMMdd') AS INT) as memorigindate,
                REGEXP_REPLACE(CAST(term_agg_act AS STRING), '[ .]', '') as na2,
                REGEXP_REPLACE(CAST(active_sprel_overall_agg_act AS STRING), '[ .]', '') as Overall_Active_SP_Reltshps,
                REGEXP_REPLACE(CAST(hist_sprel_overall_agg_act AS STRING), '[ .]', '') as Overall_Historic_SP_Reltshps,
                'E' as memstatus,
                CASE WHEN acev_num_i > 0 THEN 'Y' ELSE 'N' END as acev_flag,
                '1' as globally_opted_in,
                RPAD(REGEXP_REPLACE(CAST(a.mid_key AS STRING), '[ .]', ''), 12, ' ') as merkleid
            FROM emu a
            LEFT JOIN emu_mid_indiv b ON a.mid_key=b.mid_key
            LEFT JOIN emu_hid c ON a.mid_key=c.mid_key
            LEFT JOIN emu_account d ON a.mid_key=d.mid_key
        """).drop("chid_agg_ind", "mid_key", "hid_key", "acev_num_i", "Advo_Last_Dt_i", "Fndn_Last_Dt_i", "advo_last_petition_dt_agg_ind_i")
        df_intermed_emu_complete.write.format("delta").mode("overwrite").saveAsTable("intermed.emu_complete")
        
        df_intermed_emu_complete = spark.table("intermed.emu_complete").dropDuplicates(['merkleid'])
        
        df_intermed_bonus_layout = spark.table("intermed.bonus_layout")
        df_intermed_emu_complete = spark.table("intermed.emu_complete")
        df_intermed_bonus_layout = df_intermed_bonus_layout.unionByName(df_intermed_emu_complete, allowMissingColumns=True)
        
        %run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/Process_Demos_20210118"
        
        if spark.sql("SELECT 1 FROM (SHOW TABLES IN intermed) WHERE tableName = 'emu_complete'").count() > 0:
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
        df_mulinter_household = spark.read.format("jdbc").option("url", f"jdbc:{conn}").option("dsn", dsn).option("user", usern).option("password", passw).option("dbtable", f"({sql_query_household_monthly}) as subq").load()
        df_mulinter_household.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("mulinter.household")

    if runtype == 'Weekly':
        %run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/Process_Acxiom_Demographic_20201215"
        
        sql_query_orders2 = f"""
        select mid_key,response_key_cd,kx_create_dt, order_term,end_term_dt
		from {ref3}.f_account_order
        """
        df_orders_from_redshift = spark.read.format("jdbc").option("url", f"jdbc:{conn}").option("driver", "com.amazon.redshift.jdbc.Driver").option("dbtable", f"({sql_query_orders2}) as subq").load()
        
        df_special_keycodes = spark.table("aarpdata.special_keycodes").select(F.col("sp_key_codes"))
        
        df_mulinter_orders2 = df_orders_from_redshift.alias("ord").join(
            df_special_keycodes.alias("c"),
            F.substring(F.col("ord.response_key_cd"), 1, 9) == F.col("c.sp_key_codes"),
            "left"
        ).select(
            F.col("ord.mid_key"),
            F.col("ord.response_key_cd").alias("response_key_cd"),
            F.col("ord.kx_create_dt"),
            F.col("ord.order_term"),
            F.col("ord.end_term_dt"),
            F.lit(1).alias("orders_all_i"),
            F.when(F.col("ord.order_term") == 12, 1).otherwise(F.lit(None)).alias("orders_12moterm_i"),
            F.when(F.col("ord.order_term") == 36, 1).otherwise(F.lit(None)).alias("orders_36moterm_i"),
            F.when(F.col("ord.order_term") == 60, 1).otherwise(F.lit(None)).alias("orders_60moterm_i"),
            F.substring(F.col("ord.response_key_cd"), 1, 1).alias("channel"),
            F.when((F.substring(F.col("ord.response_key_cd"), 1, 1) == 'D') & F.col("c.sp_key_codes").isNull(), 1).otherwise(F.lit(None)).alias("orders_acqmail_i"),
            F.when(F.substring(F.col("ord.response_key_cd"), 1, 1).isin('D', 'Z') & F.col("c.sp_key_codes").isNull(), 1).otherwise(F.lit(None)).alias("orders_renewals_i"),
            F.when((F.substring(F.col("ord.response_key_cd"), 1, 1) == 'K') & F.col("c.sp_key_codes").isNull(), 1).otherwise(0).alias("orders_altmedia_i"),
            F.when((F.substring(F.col("ord.response_key_cd"), 1, 1) == 'U') & F.col("c.sp_key_codes").isNull(), 1).otherwise(0).alias("orders_online_i"),
            F.when((F.substring(F.col("ord.response_key_cd"), 1, 1) == 'F') | F.col("c.sp_key_codes").isNotNull(), 1).otherwise(0).alias("orders_serviceprovider_i"),
            F.when((F.substring(F.col("ord.response_key_cd"), 1, 1) == 'W') & F.col("c.sp_key_codes").isNull(), 1).otherwise(0).alias("orders_winback_i"),
            F.when((F.substring(F.col("ord.response_key_cd"), 1, 1) == 'N') & F.col("c.sp_key_codes").isNull(), 1).otherwise(0).alias("orders_cl_i"),
            F.when(F.substring(F.col("ord.response_key_cd"), 1, 1).isin('D', 'R', 'Z') & F.col("c.sp_key_codes").isNull(), 1).otherwise(F.lit(None)).alias("orders_dm_i"),
            F.when(F.substring(F.col("ord.response_key_cd"), 1, 1).isin('M', 'H') & F.col("c.sp_key_codes").isNull(), 1).otherwise(F.lit(None)).alias("orders_sps_i"),
            F.when((F.substring(F.col("ord.response_key_cd"), 1, 1) == 'K') & (F.to_date(F.col("ord.kx_create_dt")) <= F.to_date(F.lit('2020-06-08'))) & F.col("c.sp_key_codes").isNull(), 1).otherwise(0).alias("orders_am_i")
        )
        df_mulinter_orders2.write.format("delta").mode("overwrite").saveAsTable("mulinter.orders2")

        df_f_account_order = spark.table("unica.f_account_order")
        
        df_a = df_f_account_order.filter((F.to_date(F.col("start_term_dt")) < F.current_date()) & (F.to_date(F.col("end_term_dt")) > F.current_date())) \
            .select("mid_key", "kx_create_dt", "order_num", "order_type", "order_term")
        
        df_b = df_f_account_order.select("mid_key", "order_num", "kx_create_dt")
        
        df_mulinter_order_curr_date = df_a.alias("a").join(
            df_b.alias("b"),
            (F.col("a.mid_key") == F.col("b.mid_key")) & (F.col("a.order_num") == F.col("b.order_num") - 1),
            "left"
        ).select(
            F.col("a.*"),
            F.col("b.order_num").alias("prev_order_num"),
            F.col("b.kx_create_dt").alias("new_kx_create_dt")
        )
        df_mulinter_order_curr_date.write.format("delta").mode("overwrite").saveAsTable("mulinter.order_curr_date")
        
        df_mulinter_order_curr_date = spark.table("mulinter.order_curr_date").orderBy("mid_key", F.desc("order_num"))
        
        windowSpec = Window.partitionBy("mid_key").orderBy(F.desc("order_num"))
        df_mulinter_order_curr_date2 = df_mulinter_order_curr_date.withColumn("row", F.row_number().over(windowSpec)).filter(F.col("row") == 1).drop("row")
        df_mulinter_order_curr_date2.write.format("delta").mode("overwrite").saveAsTable("mulinter.order_curr_date2")
        
        df_mulinter_orders2 = spark.table("mulinter.orders2")
        df_mulinter_order_curr_date2 = spark.table("mulinter.order_curr_date2")
        
        df_mulinter_orders2.createOrReplaceTempView("orders2")
        df_mulinter_order_curr_date2.createOrReplaceTempView("order_curr_date2")
        
        df_mulinter_allorderdata = spark.sql("""
            SELECT a.*, b.order_num, b.order_type, b.order_term, b.new_kx_create_dt
            FROM (
                SELECT 
                    mid_key, 
                    SUM(orders_all_i) as orders_all, 
                    SUM(orders_12moterm_i) as orders_12moterm, 
                    SUM(orders_36moterm_i) as orders_36moterm,
                    SUM(orders_60moterm_i) as orders_60moterm,
                    SUM(orders_acqmail_i) as orders_acqmail, 
                    SUM(orders_renewals_i) as orders_renewals,
                    SUM(orders_altmedia_i) as orders_altmedia, 
                    SUM(orders_online_i) as orders_online,
                    SUM(orders_serviceprovider_i) as orders_serviceprovider,
                    SUM(orders_winback_i) as orders_winback,
                    SUM(orders_cl_i) as orders_cl,
                    SUM(orders_dm_i) as orders_dm,
                    SUM(orders_sps_i) as orders_sps,
                    SUM(orders_am_i) as orders_am	
                FROM orders2
                GROUP BY mid_key
            ) a
            LEFT JOIN order_curr_date2 b
            ON a.mid_key = b.mid_key
        """)
        df_mulinter_allorderdata.write.format("delta").mode("overwrite").saveAsTable("mulinter.allorderdata")
        
        spark.sql("DROP TABLE IF EXISTS mulinter.orders")
        spark.sql("DROP TABLE IF EXISTS mulinter.orders2")
        spark.sql("DROP TABLE IF EXISTS mulinter.order_curr_date")
        spark.sql("DROP TABLE IF EXISTS mulinter.order_curr_date2")
        
        sql_query_lifestyle_sum = f"""
        select mid_key, count(distinct svc_prov) as life_engage_svcprov_12mo_nps
	from {ref3}.f_lifestyle_engagement
	where cast(tran_dt as date) between ('{ONEBEGDT}') and '{enddt}'
	and cast(insight_update_dt as date) between ('{ONEBEGDT}') and '{enddt}'
	group by mid_key
        """
        df_mulinter_lifestyle_engagement_sum = spark.read.format("jdbc").option("url", f"jdbc:{conn}").option("driver", "com.amazon.redshift.jdbc.Driver").option("dbtable", f"({sql_query_lifestyle_sum}) as subq").load()
        df_mulinter_lifestyle_engagement_sum.write.format("delta").mode("overwrite").saveAsTable("mulinter.lifestyle_engagement_sum")
        
        sql_query_lifestyle_prov_sum_6mo = f"""
        select mid_key, count(distinct svc_prov) as life_engage_svcprov_3mo_6mo
	from {ref3}.f_lifestyle_engagement
	where cast(tran_dt as date) between (date('{enddt}') - 182) and (date('{enddt}') - 91)
	group by mid_key
        """
        df_mulinter_lifestyle_engage_prov_sum_6mo = spark.read.format("jdbc").option("url", f"jdbc:{conn}").option("driver", "com.amazon.redshift.jdbc.Driver").option("dbtable", f"({sql_query_lifestyle_prov_sum_6mo}) as subq").load()
        df_mulinter_lifestyle_engage_prov_sum_6mo.write.format("delta").mode("overwrite").saveAsTable("mulinter.lifestyle_engage_prov_sum_6mo")
        
        sql_query_lifestyle_sum_1mo = f"""
        select mid_key, count(svc_prov) as life_engage_1mo
	from {ref3}.f_lifestyle_engagement
	where cast(tran_dt as date) between (date('{enddt}') - 30) and date('{enddt}')
		and cast(insight_update_dt as date) between (date('{enddt}') - 30) and date('{enddt}')
	group by mid_key
        """
        df_mulinter_lifestyle_engagement_sum_1mo = spark.read.format("jdbc").option("url", f"jdbc:{conn}").option("driver", "com.amazon.redshift.jdbc.Driver").option("dbtable", f"({sql_query_lifestyle_sum_1mo}) as subq").load()
        df_mulinter_lifestyle_engagement_sum_1mo.write.format("delta").mode("overwrite").saveAsTable("mulinter.lifestyle_engagement_sum_1mo")
        
        sql_query_lifestyle_sum_3mo = f"""
        select mid_key, count(distinct svc_prov) as life_engage_svcprov_3mo,
	     count(svc_prov) as life_engage_3mo
	from {ref3}.f_lifestyle_engagement
	where cast(tran_dt as date) between (date('{enddt}') - 91) and date('{enddt}')
	group by mid_key
        """
        df_mulinter_lifestyle_engagement_sum_3mo = spark.read.format("jdbc").option("url", f"jdbc:{conn}").option("driver", "com.amazon.redshift.jdbc.Driver").option("dbtable", f"({sql_query_lifestyle_sum_3mo}) as subq").load()
        df_mulinter_lifestyle_engagement_sum_3mo.write.format("delta").mode("overwrite").saveAsTable("mulinter.lifestyle_engagement_sum_3mo")

        sql_query_lifestyle_sum_6mo = f"""
        select mid_key, count(svc_prov) as life_engage_6mo
	from {ref3}.f_lifestyle_engagement
	where cast(tran_dt as date) between (date('{enddt}') - 182) and date('{enddt}')
	group by mid_key
        """
        df_mulinter_lifestyle_engagement_sum_6mo = spark.read.format("jdbc").option("url", f"jdbc:{conn}").option("driver", "com.amazon.redshift.jdbc.Driver").option("dbtable", f"({sql_query_lifestyle_sum_6mo}) as subq").load()
        df_mulinter_lifestyle_engagement_sum_6mo.write.format("delta").mode("overwrite").saveAsTable("mulinter.lifestyle_engagement_sum_6mo")
        
        sql_query_lifestyle_sum_12mo = f"""
        select mid_key, count(distinct svc_prov) as life_engage_svcprov_12mo,
     sum(tot_amt) as life_engage_sumamt_12mo,
	  count(svc_prov) as life_engage_12mo
	from {ref3}.f_lifestyle_engagement
	where cast(tran_dt as date) between (date('{enddt}') - 365) and date('{enddt}')
	group by mid_key
        """
        df_mulinter_lifestyle_engagement_sum_12mo = spark.read.format("jdbc").option("url", f"jdbc:{conn}").option("driver", "com.amazon.redshift.jdbc.Driver").option("dbtable", f"({sql_query_lifestyle_sum_12mo}) as subq").load()
        df_mulinter_lifestyle_engagement_sum_12mo.write.format("delta").mode("overwrite").saveAsTable("mulinter.lifestyle_engagement_sum_12mo")
        
        sql_query_activities_sum = f"""
        select mid_key, count(ss_key) as activities_3mo
	from {ref3}.f_activity
	where cast(activity_dt as date) between (date('{enddt}') - 90) and date('{enddt}')
	group by mid_key
        """
        df_mulinter_activities_sum = spark.read.format("jdbc").option("url", f"jdbc:{conn}").option("driver", "com.amazon.redshift.jdbc.Driver").option("dbtable", f"({sql_query_activities_sum}) as subq").load()
        df_mulinter_activities_sum.write.format("delta").mode("overwrite").saveAsTable("mulinter.activities_sum")
        
        df_gender_pred = spark.read.option("header", "true").csv("/vg01/aarp_sas/ftp/incoming/Gender_Prediction.csv")
        df_gender_pred.write.format("delta").mode("overwrite").saveAsTable("gender_pred")
        
        sql_query_mid_key_indiv = f"""
        select distinct c.mid_key,
cast(a.ibx_age_in_two_year_increments_2nd_individual_premier as char(2)),
cast(a.ibx_children_age_ranges_present_in_household_premier as char(15)),
cast(a.ibx_children_presence_of_household as char(1)),
cast(a.IBX_CREDIT_CARD_IDT_PREMIER as char(8)),
cast(a.IBX_DONATION_CONTRIBUTION as char(1)),
cast(a.IBX_GREEN_LIVING as char(1)),
cast(a.IBX_HEALTH_BEAUTY as char(1)),
cast(a.IBX_HEALTH_HOMEOPATHIC as char(1)),
cast(a.ibx_home_lot_square_footage_ranges as char(1)),
cast(a.IBX_HOME_MARKET_VALUE_DECILES as char(2)),
cast(a.IBX_HOME_MARKET_VALUE_PREMIER as char(1)),
cast(a.IBX_HOME_OWNER_RENTER_PREMIER as char(1)),
cast(a.IBX_HOME_OWNER as char(1)),
cast(a.IBX_HOME_YEAR_BUILT_ACTUAL as char(4)),
cast(a.IBX_INFERRED_HOUSEHOLD_RANK as char(1)),
cast(a.ibx_investing_finance_grouping_premier as char(1)),
cast(a.IBX_NETWORTH_PREMIER as char(1)),
cast(a.IBX_NUM_OF_LINES_OF_CREDIT as char(1)),
cast(a.ibx_occupation_1st_individual_premier as char(1)),
cast(a.IBX_OWNER_TYPE_DETAIL as char(1)),
cast(a.IBX_PERSONIC_CLUSTER as char(2)),
cast(a.IBX_HOME_ASSESSED_VALUE_RANGES as char(1)),
cast(a.IBX_MOVIE_MUSIC_GROUPING as char(1)),
cast(a.ibx_race_cd_input_individual_premier as char(1)),
cast(a.IBX_TOTAL_ONLINE_PURCHASES as char(3)),
cast(a.IBX_PC_OWNER_PREMIER as char(1)),
cast(a.ibx_weeks_since_last_online_order as char(3)),
cast(a.ibx_retail_purchases_most_frequent_cat as char(2)),
cast(a.IBX_HOME_LOAN_AMOUNT_1_RANGES as char(1)),
cast(a.IBX_HOUSEHOLD_INCOME as char(1)),
cast(a.IBX_HEALTH_DIABETIC as char(1)),
cast(a.ibx_current_affairs_politics_premier as char(1)),
cast(a.IBX_ONLINE_AVERAGE_AMT_PER_ORDER as char(6)),
cast(a.ibx_education_input_individual_premier as char(1)),
cast(a.ibx_membership_clubs as char(1)),
cast(a.IBX_PETS as char(1)),
cast(a.ibx_adults_number_of_household_premier as char(1)),
cast(a.ibx_political_party_input_individual as char(1)),
cast(a.ibx_vehicle_known_owned_number_premier as char(1)),
cast(a.IBX_RELIGIOUS_INSPIRATIONAL_PREMier as char(1)),
cast(a.ibx_trends_for_telecom_internet_user as char(2)),
cast(a.ibx_home_property_type_details as char(1)),
cast(a.ibx_education_1st_individual as char(1)),
cast(a.ibx_trends_for_telecom_optional_calling_services as char(2)),
cast(a.IBX_HEALTH_MEDICAL_SUPPLIES as char(1)),
cast(a.ibx_health_nutraceuticals_vitamins as char(1)),
cast(a.IBX_RECREATIONAL_VEHICLES_PREMIEr as char(1)),
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
cast(b.ETHNICITY as char(50)),
c.AGE_AGG_IND,
cast(c.GENDER_AGG_IND as char(1)) as gender_agg_ind_o,
cast(c.ibx_education as char(1)),
cast(c.ftc_dnc_append_fl as char(1)),
c.LIKELY_HISP_AGG,
c.LIKELY_BLACK_AGG,
cast(c.Lifestage_Segment as char(1)),
cast(a.ibx_age_input_individual_default_1st_individual_premier as char(3)),
cast(a.ibx_age_in_two_year_increments_1st_individual_premier as char(2)),
f.VTM_ASSIGNMENT_LAST_END_DT,
g.serv_end_dt,
cast(a.IBX_HEALTH_ORTHOPEDIC as char(1)),
cast(a.IBX_INVESTORS_HIGHLY_LIKELY as char(1)),
cast(c.health_ind as char(1)),
cast(a.ibx_health_cholesterol as char(1)),
cast(a.ibx_race_cd_1st_individual_premier as char(4)),
b.income_model,
cast(a.IBX_INVESTORS_LIKELY as char(4)),
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
        df_mid_key_indiv_from_db = spark.read.format("jdbc").option("url", f"jdbc:{conn}").option("driver", "com.amazon.redshift.jdbc.Driver").option("dbtable", f"({sql_query_mid_key_indiv}) as subq").load()
        
        df_gender_pred = spark.table("gender_pred")
        
        df_joined_indiv = df_mid_key_indiv_from_db.alias("aa").join(
            df_gender_pred.alias("b"),
            F.col("aa.mid_key") == F.col("b.mid_key"),
            "left"
        ).select("aa.*", "b.gender_ind", "b.probability")
        
        df_mulinter_mid_key_indiv = df_joined_indiv.withColumn("VTM_ASSIGNMENT_LAST_END_DT_col", F.col("VTM_ASSIGNMENT_LAST_END_DT")) \
            .withColumn("dvr", F.lit(None).cast('double')) \
            .withColumn("internet", F.lit(None).cast('double')) \
            .withColumn("radio", F.lit(None).cast('double')) \
            .withColumn("smartphone", F.lit(None).cast('double')) \
            .withColumn("tv", F.lit(None).cast('double')) \
            .withColumn("density_clusters", F.lit('').cast('string')) \
            .withColumn("work_clusters", F.lit('').cast('string')) \
            .withColumn("religious", F.lit(None).cast('double')) \
            .withColumn("cable", F.lit(None).cast('double')) \
            .withColumn("game_shows", F.lit(None).cast('double')) \
            .withColumn("kids_shows", F.lit(None).cast('double')) \
            .withColumn("educational_attainment_model", (F.col("EDUCATIONAL_ATTAINMENT_MODEL_i") / 100).cast('double')) \
            .withColumn("gun_ownership_model", (F.col("gun_ownership_model_i") / 160).cast('double')) \
            .withColumn("gender_agg_ind", F.when((F.col("gender_agg_ind_o") == 'U') & (F.col("probability") > 0.8), F.col("gender_ind")).otherwise(F.col("gender_agg_ind_o"))) \
            .drop("vtm_end_dt_i", "educational_attainment_model_i")
        
        df_mulinter_mid_key_indiv.write.format("delta").mode("overwrite").saveAsTable("mulinter.mid_key_indiv")
        
        from datetime import datetime
        
        def output_gender(muldate, bl_bucket, bl_key, bl_secret):
            alpha = datetime.strptime(muldate, '%Y-%m-%d').day
            print(alpha)
            if 11 <= alpha < 18:
                df_d_individual = spark.table("unica.d_individual")
                df_gender_pred = spark.table("gender_pred")
                
                df_aarpdata_gender_pred = df_d_individual.alias("a").join(
                    df_gender_pred.alias("b"),
                    F.col("a.mid_key") == F.col("b.mid_key"),
                    "left"
                ).select(
                    F.col("a.mid_key"),
                    F.substring(F.coalesce(F.col("b.gender_ind"), F.col("a.gender_agg_ind")), 1, 1).alias("gender_ind"),
                    F.coalesce(F.col("b.probability"), F.lit(1)).alias("probability")
                )
                df_aarpdata_gender_pred.write.format("delta").mode("overwrite").saveAsTable("aarpdata.gender_pred")
                
                spark.sql("DROP TABLE IF EXISTS sandbox.GENDER_PRED_SX")
                
                # The bl_... options are specific to SAS/ACCESS to Redshift bulk loading.
                # In Databricks, we write to a location and then can use COPY INTO in Redshift if needed.
                # Here we save as a Delta table, which is the Databricks standard.
                df_to_write = df_aarpdata_gender_pred.withColumn("effective_date", F.lit(muldate))
                df_to_write.write.format("delta").mode("overwrite").saveAsTable("sandbox.GENDER_PRED_SX")

        output_gender(muldate, bl_bucket, bl_key, bl_secret)
        
        spark.sql("CREATE INDEX mid_key ON mulinter.mid_key_indiv")
        
        sql_query_mid_cid = f"""
        select mid_key, cid_key, hid_key,preferred_chid
	from {ref3}.f_joiner
	order by mid_key
        """
        df_mulinter_mid_cid = spark.read.format("jdbc").option("url", f"jdbc:{conn}").option("dsn", dsn).option("user", usern).option("password", passw).option("dbtable", f"({sql_query_mid_cid}) as subq").load()
        df_mulinter_mid_cid = df_mulinter_mid_cid.filter(F.col("preferred_chid") != 0)
        df_mulinter_mid_cid.write.format("delta").mode("overwrite").saveAsTable("mulinter.mid_cid")
        
        sql_query_hid_key_appends = f"""
        select 
 hid_key,
cast(IBX_ADULT_AGE_55_64_AGG_HHD as Char(1)),
cast(IBX_ADULT_AGE_65_74_AGG_HHD as Char(1)),
cast(IBX_ADULT_AGE_75_P_AGG_HHD as Char(1)),
cast(IBX_ADULTS_NUM_AGG_HHD as Char(1)),
cast(IBX_CHILD_AGE_00_05_AGG_HHD as Char(1)),
cast(IBX_CHILD_AGE_06_10_AGG_HHD as Char(1)),
cast(IBX_CHILD_NUM_AGG_HHD as Char(1)),
cast(IBX_CHILD_PRESENCE_AGG_HHD as Char(1)),
cast(IBX_COMMUNITY_CHARITIES_AGG_HHD as Char(1)),
cast(IBX_COMMUNITY_INVOLVEMENT_AID_AGG_HHD as Char(1)),
cast(IBX_COMMUNITY_INVOLVEMENT_ANIMAL_AGG_HHD as Char(1)),
cast(IBX_COMMUNITY_INVOLVEMENT_CHILDREN_AGG_HHD as Char(1)),
cast(IBX_COMMUNITY_INVOLVEMENT_CULTURE_AGG_HHD as Char(1)),
cast(IBX_COMMUNITY_INVOLVEMENT_ENVIRONMENT_AGG_HHD as Char(1)),
cast(IBX_COMMUNITY_INVOLVEMENT_HEALTH_AGG_HHD as Char(1)),
cast(IBX_COMMUNITY_INVOLVEMENT_LIBERAL_AGG_HHD as Char(1)),
cast(IBX_COMMUNITY_INVOLVEMENT_POLITICAL_AGG_HHD as Char(1)),
cast(IBX_COMMUNITY_INVOLVEMENT_RELIGIOUS_AGG_HHD as Char(1)),
cast(IBX_COMMUNITY_INVOLVEMENT_VETERAN_AGG_HHD as Char(1)),
cast(IBX_CREDIT_CARD_FREQ_AGG_HHD as Char(7)),
cast(IBX_DWELLING_TYPE_AGG_HHD as Char(1)),
cast(IBX_ELDERLY_PARENT_AGG_HHD as Char(1)),
cast(IBX_GRAND_CHILDREN_AGG_HHD as Char(1)),
cast(IBX_HOME_BUSINESS_AGG_HHD as Char(1)),
cast(IBX_HOME_MARKET_VALUE_DECILES_AGG_HHD as Char(2)),
cast(IBX_HOME_PURCHASED_AMT_RANGES_AGG_HHD as Char(1)),
cast(IBX_HOME_VALUE_RANGES_AGG_HHD as Char(1)),
cast(IBX_INCOME_ESTIMATED_NARROW_RANGES_AGG_HHD as Char(1)),
cast(IBX_INVESTMENT_AGG_HHD as Char(1)),
cast(IBX_LENGTH_OF_RESIDENCE_AGG_HHD as Char(2)),
cast(IBX_MAIL_BUYER_CAT_HEALTH_AGG_HHD as Char(1)),
cast(IBX_NETWORTH_PREMIER_AGG_HHD as Char(1)),
cast(IBX_OCCUPATION_INPUT_AGG_HHD as Char(1)),
cast(IBX_OUTDOORS_DIMENSION_AGG_HHD as Char(1)),
cast(IBX_PETS_AGG_HHD as Char(1)),
cast(IBX_PRESENCE_OF_SENIOR_ADULT_AGG_HHD as Char(1)),
cast(IBX_PROPERTY_TYPE_AGG_HHD as Char(1)),
cast(IBX_TELECOM_20PCT_LONG_DISTANCE_AGG_HHD as Char(2)),
cast(IBX_TELECOM_CALLING_SERVICES_AGG_HHD as Char(2)),
cast(IBX_TELECOM_CELLULAR_AGG_HHD as Char(2)),
cast(IBX_TELECOM_INTERNET_AGG_HHD as Char(2)),
cast(IBX_TRAVEL_CRUISE_AGG_HHD as Char(1)),
cast(IBX_TRAVEL_TYPE_AGG_HHD as Char(1)),
cast(IBX_VEHICLE_DOMINANT_AGG_HHD as Char(1)),
cast(IBX_VEHICLE_OWNED_AGG_HHD as Char(1)),
cast(IBX_VEHICLE_TRUCK_MC_RV_AGG_HHD as Char(3)),
cast(IBX_WORKING_WOMAN_AGG_HHD as Char(1)),
MAX_INDV_INSIGHT_UPDATE_DT,
cast(IBX_BUSINESS_OWNER_AGG_HHD as Char(1)),
cast(IBX_CREDIT_CARD_FREQ_24_P_AGG_HHD as Char(1)),
cast(IBX_HEALTHY_BEHAVIOUR_AGG_HHD as Char(1)),
cast(IBX_ADULT_AGE_45_54_AGG_HHD as Char(1)),
cast(IBX_PRESENCE_OF_YOUNG_ADULT_AGG_HHD as Char(1)),
cast(IBX_CURRENT_AFFAIRS_AGG_HHD as Char(1)),
cast(IBX_PC_USER_FL_AGG_HHD as Char(1)),
cast(IBX_FINANCIAL_INVESTOR_AGG_HHD as char(1)),
cast(ibx_home_equity_available_agg_hhd as char(1)),
cast(ibx_home_loan_interest_rt_agg_hhd as char(1)),
cast(IBX_HOME_GARDEN_AGG_HHD as char(1)),
cast(ibx_adult_age_25_34_agg_hhd as char(1)),
cast(IBX_TELECOM_LONG_DISTANCE_AGG_HHD as Char(2)),
cast(ibx_child_age_11_15_agg_hhd as char(1)),
cast(ibx_community_involvement_conservative_agg_hhd as char(1)),
cast(IBX_ADULT_AGE_18_24_AGG_HHD as char(1)),
cast(ibx_household_size_agg_hhd as char(4)),
ibx_investors_highly_likely_agg_hhd,
ibx_child_age_16_17_agg_hhd
	from
	{ref3}.d_acxiom_demographics_hhd demo
        """
        df_mulinter_hid_key_appends = spark.read.format("jdbc").option("url", f"jdbc:{conn}").option("dsn", dsn).option("user", usern).option("password", passw).option("dbtable", f"({sql_query_hid_key_appends}) as subq").load()
        df_mulinter_hid_key_appends.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("mulinter.hid_key_appends")
        
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
        df_mulinter_household_weekly = spark.read.format("jdbc").option("url", f"jdbc:{conn}").option("dsn", dsn).option("user", usern).option("password", passw).option("dbtable", f"({sql_query_household_weekly}) as subq").load()
        df_mulinter_household_weekly.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("mulinter.household")
        
        df_f_service_participation = spark.table("unica.F_SERVICE_PARTICIPATION")
        df_mulinter_sp_engagements = df_f_service_participation.filter(
            (F.to_date(F.col("service_effective_dt")) <= F.current_date()) &
            (F.months_between(F.current_date(), F.to_date(F.col("service_effective_dt"))) <= 12) &
            (F.col("sp_name").isNotNull()) &
            (F.col("active_engagement_flag") == 'A')
        ).groupBy("mid_key").agg(
            F.countDistinct("service_effective_dt").alias("num_sp")
        )
        df_mulinter_sp_engagements.write.format("delta").mode("overwrite").saveAsTable("mulinter.SP_engagements")
        
    return

# Call the emu function with appropriate parameters
# emu(conn, dsn, usern, passw, ref3, runtype, twobegdt, muldate, ONEBEGDT, enddt, mon, day, year2, democurr, bl_bucket, bl_key, bl_secret)

df_intermed_bonus_layout = spark.table("intermed.bonus_layout")
df_monthly_engagements_final = spark.table("cran.monthly_engagements_final")
df_intermed_bonus_layout.createOrReplaceTempView("bonus_layout")
df_monthly_engagements_final.createOrReplaceTempView("monthly_engagements_final")

df_intermed_mid_key_appends_2 = spark.sql("""
SELECT
    a.acev_flag,
    a.acev_num,
    a.advo_hpc_amt,
    a.Advo_Last_Amt,
    a.Advo_Last_Dt,
    a.advo_segment_cd,
    a.Advo_TTD_Amt,
    a.Advo_TTD_Num, 
    a.Age_HH_pct_with_HHer_55_64,
    a.Age_HH_pct_with_HHer_65_74, 
    a.Age_HH_pct_with_HHer_75_84,
    a.Age_HH_pct_with_HHer_85p,
    a.Chase_Num_Active_Particpnts,
    a.Chase_Num_InActive_Particpnts,
    a.cntct_lifstyle_12mo_agg_hhd,
    a.cntct_lifstyle_3mo_agg_hhd,
    a.curr_order_create_dt,
    a.CurrentPartCt_Overall,
    a.diversity_flag_agg_ind,
    a.DRVS_Flag,
    a.EthnicCode,
    a.Fndn_Last_Amt,
    a.Fndn_Last_Dt,  
    a.Fndn_TTD_Amt,
    a.fndn_ttd_num,
    a.Foremost_Num_Active_Particpnts,
    a.GE_Num_Active_Particpnts,
    a.Gender,
    a.GeneralElectn2012,
    a.geo_cd_2010,
    a.geocode,
    a.globally_opted_in,
    a.GroupEthnicCode,
    a.Hartford_Num_Active_Particpnts,
    a.HistPartCt_Overall,
    a.HomVal_Home_Value_CBSA_Index,
    a.ideology,
    a.Inc_HH_Median_HH_Income,
    a.Life_Stage,
    a.maritalstatus,
    a.memacctnum,
    a.memorigindate,
    a.memoriginkey,
    a.mempaiddate,
    a.memstatus,
    a.memxrenew,
    a.merkleid,
    a.na2,
    a.na3,
    a.NbrTimesSelEmailedInd,
    a.NYL_Num_Active_Particpnts,
    a.NYL_Num_InActive_Particpnts,
    a.OCCHU_Median_Length_of_Residence,
    a.OOHU_Median_Home_Value,
    CASE WHEN a.OriginCode IS NULL THEN 0 ELSE a.origincode END as OriginCode,
    a.Overall_Active_SP_Reltshps,
    a.Overall_Historic_SP_Reltshps ,
    a.partisanscore,
    a.PartyAffiliation,
    a.PartyMix,
    a.Past12MoTouchCt_AARP,
    a.Past12MoTouchCt_Financial,
    a.Past12MoTouchCt_Health,
    a.Past12MoTouchCt_Overall,
    a.Past12MoTouchCt_Priv,
    a.Past12MoTouchCt_Travel,
    a.Past3MoTouchCt_AARP,
    a.Past3MoTouchCt_Financial,
    a.Past3MoTouchCt_Health,
    a.Past3MoTouchCt_Overall,
    a.Past3MoTouchCt_Priv,
    a.Pop_pct_Asian_Only_,
    a.Pop_pct_Asian_Only_Hisp,
    a.Pop_pct_Black_Only_Hisp,
    a.ReligionCode,
    a.RepPartyCd,
    a.SecAge,
    a.state,
    a.sy_otsbn_polfund_2012a,
    a.sy_otsbn_polfund_2012b,
    a.VoterCount,
    a.VoterStatus,
    a.vtm_active_vol_flag_act,
    a.vtm_dsp_active_vol,
    a.vtm_last_role_act,
    a.vtm_num_assignments_act,
    a.vtm_vol_flag_act,
    a.workstatus,
    a.Zip,
    a.ZipPlus4,
    a.Advo_Last_Petition_Dt_Agg_Ind,
    a.region,
    a.fndn_mrhpc_dt,
    a.advo_hpc_dt,
    a.fndn_hpc_amt,
    b.aarporg_i,
    b.activist_i,
    b.advocacy_donations_12mo,
    b.advocacy_donations_ytd,
    b.advocacy_donors_12mo_i,
    b.advocacy_petitions_12mo,
    b.advocacy_signers_12mo_i,
    b.driver_class_12mo_i,
    b.driver_class_12mo,
    b.driver_online_12mo_i,
    b.driver_online_12mo,
    b.driver_safety_vol_12mo_i,
    b.driver_safety_vol_12mo,
    b.contact_leg_12mo_i,
    b.contact_leg_12mo,
    CAST(b.EMAILABLE_AGG_IND AS STRING),
    b.foundation_donors_12mo_i,
    b.foundation_donors_checkb_12mo_i,
    b.individual_engagers_12mo,
    b.national_activities_12mo,
    b.national_activity_12mo_i,
    b.newsletter_opens_cnt_12mo,
    b.newsletter_opens_cnt_ytd,
    b.num_months,
    b.other_vol_12mo_i,
    b.state_activities_12mo,
    b.state_activity_12mo_i,
    b.suppression,
    b.teletown_12mo_i,
    b.TERM_AGG_ACT,
    b.VOTEPROP2016,
    b.yeas_survey_12mo_i,
    CAST(b.MEMBER_FL_AGG_IND AS STRING),
    b.community, 
    b.chapters_vol_12mo_i,
    b.exp_corp_vol_12mo_i,
    b.tax_aid_vol_12mo_i,
    b.states_vol_12mo_i,
    b.petition_sign_12mo_i,
    b.petition_col_12mo_i,
    b.leg_off_vis_12mo_i,
    b.event_host_12mo_i,
    b.outbound_call_12mo_i,
    b.survey_resp_12mo_i,
    b.story_sub_12mo_i,
    b.moviesfg_12mo_i,
    b.structured_12mo_i,
    b.blockparty_12mo_i,
    b.state_event_12mo_i,
    b.popups_12mo_i,
    b.foundation_donations_checkb_12mo,
    b.moviesfg_12mo,
    b.structured_12mo,
    b.state_event_12mo,
    b.survey_resp_12mo,
    b.states_vol_12mo,
    b.foundation_donations_12mo,
    b.memorigin,
    b.auto_renew_start_dt,
    b.teletown_12mo,
    b.activist,
    b.petition_col_12mo,
    b.petition_sign_12mo,
    b.mem_type_agg_act,
    b.tax_aid_vol_12mo,
    b.popups_12mo,
    b.foundation_donations_ytd,
    a.advo_mrhpc_amt,
    b.SY_GENERALACTIVIST,
    b.auto_renew_flag,
    b.blockparty_12mo
FROM bonus_layout a
LEFT JOIN monthly_engagements_final b ON CAST(a.merkleid AS INT) = b.mid_key
WHERE b.mid_key IS NOT NULL AND b.mid_key NOT IN (0, 1)
""")
df_intermed_mid_key_appends_2.write.format("delta").mode("overwrite").saveAsTable("intermed.mid_key_appends_2")

df_intermed_mid_key_appends_2.createOrReplaceTempView("mid_key_appends_2")
spark.table("mulinter.lifestyle_engagement_sum").createOrReplaceTempView("lifestyle_engagement_sum")
spark.table("mulinter.activities_sum").createOrReplaceTempView("activities_sum")
spark.table("mulinter.lifestyle_engagement_sum_1mo").createOrReplaceTempView("lifestyle_engagement_sum_1mo")
spark.table("mulinter.lifestyle_engagement_sum_3mo").createOrReplaceTempView("lifestyle_engagement_sum_3mo")
spark.table("mulinter.lifestyle_engagement_sum_6mo").createOrReplaceTempView("lifestyle_engagement_sum_6mo")
spark.table("mulinter.lifestyle_engagement_sum_12mo").createOrReplaceTempView("lifestyle_engagement_sum_12mo")
spark.table("mulinter.lifestyle_engage_prov_sum_6mo").createOrReplaceTempView("lifestyle_engage_prov_sum_6mo")

df_intermed_mid_key_appends1a_2 = spark.sql("""
SELECT *
FROM mid_key_appends_2 a
LEFT JOIN lifestyle_engagement_sum e ON CAST(a.merkleid AS INT) = e.mid_key
LEFT JOIN activities_sum f ON CAST(a.merkleid AS INT) = f.mid_key
LEFT JOIN lifestyle_engagement_sum_1mo g ON CAST(a.merkleid AS INT) = g.mid_key
LEFT JOIN lifestyle_engagement_sum_3mo h ON CAST(a.merkleid AS INT) = h.mid_key
LEFT JOIN lifestyle_engagement_sum_6mo i ON CAST(a.merkleid AS INT) = i.mid_key
LEFT JOIN lifestyle_engagement_sum_12mo j ON CAST(a.merkleid AS INT) = j.mid_key
LEFT JOIN lifestyle_engage_prov_sum_6mo k ON CAST(a.merkleid AS INT) = k.mid_key
""")
df_intermed_mid_key_appends1a_2.write.format("delta").mode("overwrite").saveAsTable("intermed.mid_key_appends1a_2")

df_intermed_mid_key_appends1a_2.createOrReplaceTempView("mid_key_appends1a_2")
spark.table(f"weiss.advomodel_ctc_hist_{year2}{mon}_weiss").createOrReplaceTempView("advomodel_ctc_hist")
spark.table(f"{ref}.vq_emu").createOrReplaceTempView("vq_emu")

df_intermed_mid_key_appends2_2 = spark.sql(f"""
SELECT a.*,
    CASE WHEN emu.mid_key IS NOT NULL THEN 'Y' ELSE 'N' END as emu_indicator
FROM mid_key_appends1a_2 as a
LEFT JOIN advomodel_ctc_hist as h
ON CAST(a.merkleid AS INT) = h.mid_key
WHERE h.mid_key IS NOT NULL AND h.mid_key NOT IN (0, 1)
LEFT JOIN vq_emu emu
ON CAST(a.merkleid AS INT) = emu.mid_key
""")
df_intermed_mid_key_appends2_2.write.format("delta").mode("overwrite").saveAsTable("intermed.mid_key_appends2_2")

df_intermed_mid_key_appends2_2.createOrReplaceTempView("mid_key_appends2_2")
spark.table("aarpdata.wkly_demos").createOrReplaceTempView("wkly_demos")
spark.table("mulinter.allorderdata").createOrReplaceTempView("allorderdata")
spark.table("mulinter.mid_key_indiv").createOrReplaceTempView("mid_key_indiv")
spark.table("mulinter.SP_engagements").createOrReplaceTempView("SP_engagements")
spark.table(f"aarpdata.{democurr}").createOrReplaceTempView("democurr")

df_intermed_mid_key_appends3_2 = spark.sql("""
SELECT
    a.*,
    k.*,
    zz.*,
    i.MOVIE_MUSIC_GROUPING,
    i.gender_input,
    i.home_purchase_date,
    i.household_size,
    i.mail_order_categories as mail_order_buyer_categories,
    i.COMMUNITY_INVOLVEMENT_CAUSES_SUP as mail_order_donor_categories,
    i.networth_premier,
    i.outdoors_dimension,
    i.travel_us_premier,
    i.vehicle_known_owned_number,
    i.voter_party_input,
    i.exercise_health_group,
    i.IBX_NUM_OF_SOURCES_PREMIER,
    i.IBX_TRAVEL_FOREIGN_PREMIER,
    i.IBX_TRAVEL_FOREIGN_PREMIER as TRAVEL_FOREIGN_PREMIER,
    i.ibx_vacation_travel_internationa,
    i.audio_visual_composite,
    i.ibx_home_lender_type_1,
    i.vehicle_dominant_lifestyle,
    i.vehicle_dominant_lifestyle as ibx_vehicle_dominant_lifestyle_p,
    SUBSTRING(i.mail_order_categories, 1, 1) as hitech_merch,
    SUBSTRING(i.mail_order_categories, 16, 1) as pc_prdct_buyer,
    SUBSTRING(i.COMMUNITY_INVOLVEMENT_CAUSES_SUP, 2, 1) as Env_Humant_Educ,
    SUBSTRING(i.COMMUNITY_INVOLVEMENT_CAUSES_SUP, 4, 1) as political,
    SUBSTRING(i.COMMUNITY_INVOLVEMENT_CAUSES_SUP, 5, 1) as other_donors,
    CAST(SUBSTRING(demo.home_purchase_date, 1, 4) AS INT) as Home_purch_yr,
    sp.num_sp,
    fishing,
    nascar,
    diy_living,
    environmental_issues,
    gaming_casino,
    hunting_shooting,
    investments_personal,
    Reading__Newsletter_Subscribers as reading_financial_newsletter_sub,
    reading_general,
    reading_religious_inspirational,
    science_space,
    smoking_tobacco,
    Spect_Sports_Motorcycle_Racing as spectator_sports_auto_motorcycle,
    spectator_sports_basketball,
    spectator_sports_hockey,
    spectator_sports_tennis,
    strange_and_unusual,
    theater_performing_arts,
    ibx_income_estimated_narrow_hhd,
    num_curr_participation_technolog,
    num_curr_participation_discounts,
    num_giving_back_visits_past_3mon,
    em,
    advocacy_petition_signer,
    foundation_donor,
    Dieting_WeightLoss as dieting_weight_loss,
    exercise_aerobic,
    exercise_walking,
    ibx_health_medical_supplies_orth,
    home_pool_present,
    IBX_MOTORCYCLE_AGG_HHD,
    num_health_visits_past_3months,
    ibx_health_vitamins_nutrition,
    demo.dm,
    demo.advocacy_donor,
    demo.Career,
    demo.Equestrian,
    demo.Tennis,
    demo.collectibles_antiques,
    demo.collectibles_arts,
    demo.consumer_electronics,
    demo.ibx_community_charities_premier,
    demo.music_home_stereo,
    demo.reading_grouping,
    demo.IBX_NUM_LINES_OF_CREDIT_AGG_HHD,
    demo.Tele_Townhall_Engagers,
    demo.advocacy_grassroots_engager,
    demo.attended_aarp_event,
    demo.ibx_community_involvement_causes,
    demo.ibx_grandchildren_premier,
    demo.IBX_DWELLING_TYPE,
    demo.IBX_HOME_PURCHASE_DT_PREMIER_AGG,
    demo.num_entertainment_visits_past_3m,
    demo.phn,
    demo.motorcycling,
    demo.auto_work,
    demo.boating_sailing,
    demo.broader_living,
    demo.collectibles_coins,
    demo.education_online,
    demo.home_furnishings_decorating,
    demo.music_collector,
    demo.reading_best_sellers,
    demo.spect_sports_motorcycle_racing,
    demo.sweeps_contests,
    demo.tv_guide_network,
    demo.engaged_aarp_event,
    demo.childrens_interests,
    demo.spectator_sports_football
FROM intermed.mid_key_appends2_2 as a
LEFT JOIN wkly_demos as i ON a.merkleid = i.merkleid
LEFT JOIN allorderdata as k ON CAST(a.merkleid AS INT) = k.mid_key
LEFT JOIN mid_key_indiv as zz ON CAST(a.merkleid AS INT) = zz.mid_key
LEFT JOIN SP_engagements as sp ON CAST(a.merkleid AS INT) = sp.mid_key
LEFT JOIN democurr demo ON CAST(a.merkleid AS INT) = demo.mid_key
WHERE i.merkleid IS NOT NULL AND i.merkleid NOT IN ("", "0", "1")
""")
df_intermed_mid_key_appends3_2.write.format("delta").mode("overwrite").saveAsTable("intermed.mid_key_appends3_2")

df_intermed_mid_key_appends3_2.createOrReplaceTempView("mid_key_appends3_2")
spark.table("mulinter.mid_cid").createOrReplaceTempView("mid_cid")
spark.table("weiss.all_donors").createOrReplaceTempView("all_donors")

df_intermed_cid_appends = spark.sql("""
SELECT *
FROM mid_key_appends3_2 a
LEFT JOIN mid_cid b ON CAST(a.merkleid AS INT) = b.mid_key
LEFT JOIN all_donors j ON b.cid_key = j.cid_key
WHERE j.cid_key IS NOT NULL
""").drop("mid_key")
df_intermed_cid_appends.write.format("delta").mode("overwrite").saveAsTable("intermed.cid_appends")

df_intermed_cid_appends = spark.table("intermed.cid_appends").dropDuplicates(["merkleid"])
df_intermed_cid_appends.write.format("delta").mode("overwrite").saveAsTable("intermed.cid_appends")

spark.table("intermed.bonus_layout").createOrReplaceTempView("bonus_layout")
spark.table("intermed.cid_appends").createOrReplaceTempView("cid_appends")

df_intermed_geo_cid_hid = spark.sql("""
SELECT a.memacctnum, a.merkleid, cid.*
FROM bonus_layout(keep=memacctnum, merkleid) a
LEFT JOIN cid_appends cid ON a.merkleid = cid.merkleid
""")
df_intermed_geo_cid_hid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_cid_hid")

spark.table("intermed.geo_cid_hid").createOrReplaceTempView("geo_cid_hid")
spark.table("mulinter.household").createOrReplaceTempView("household")
spark.table("mulinter.hid_key_appends").createOrReplaceTempView("hid_key_appends")

df_intermed_geo_appends = spark.sql("""
SELECT *,
    (CENS_INC_HH_PERCENT_HOUSEHOLD_IN + CENS_INC_HH_PERCENT_HOUSEHOLD_I0 + CENS_INC_HH_PERCENT_HOUSEHOLD_I1) as CENS_INC_HH_PERCENT_HOUSEHOLD_19,
    (cens_educ_pop25_plus_percent_bac + cens_educ_pop25_plus_percent_pro) as cens_educ_pop25_plus_percent_col,	
    (CENS_OCCUP_EMPLD_PERCENT_FIRE_AN + CENS_OCCUP_EMPLD_PERCENT_LAW_ENF + CENS_OCCUP_EMPLD_PERCENT_FOOD_PR +
     CENS_OCCUP_EMPLD_PERCENT_BLDG_AN + CENS_OCCUP_EMPLD_PERCENT_PERSONA + CENS_OCCUP_EMPLD_PERCENT_FARM_FI +
     CENS_OCCUP_EMPLD_PERCENT_CONSTR_ + CENS_OCCUP_EMPLD_PERCENT_INSTALL + CENS_OCCUP_EMPLD_PERCENT_PRODUCT +
     CENS_OCCUP_EMPLD_PERCENT_TRANS_A + CENS_OCCUP_EMPLD_PERCENT_MOTOR_V + CENS_OCCUP_EMPLD_PERCENT_MATERIA) as cens_BLUECOLLAR,
    (CENS_OCCUP_EMPLD_PERCENT_MANAGEM + CENS_OCCUP_EMPLD_PERCENT_LEGAL + CENS_OCCUP_EMPLD_PERCENT_ARCHITE) as cens_MANAGEMENTPROFESSIONALS,
    (CENS_OCCUP_EMPLD_PERCENT_MANAGEM + CENS_OCCUP_EMPLD_PERCENT_LEGAL + CENS_OCCUP_EMPLD_PERCENT_ARCHITE +
     CENS_OCCUP_EMPLD_PERCENT_BUS_AND + CENS_OCCUP_EMPLD_PERCENT_COMPUTE + CENS_OCCUP_EMPLD_PERCENT_HEALTH_ +
     CENS_OCCUP_EMPLD_PERCENT_LIFE_PH + CENS_OCCUP_EMPLD_PERCENT_SALES_A) as cens_WHITECOLLAR,
    CAST(a.merkleid AS INT) as mid_key,
    COALESCE(a.reppartycd_bl, c.representative_party_cd) as reppartycd
FROM geo_cid_hid a
LEFT JOIN household c ON a.hid_key = c.hid_key
LEFT JOIN hid_key_appends b ON a.hid_key = b.hid_key
""").drop("mid_key", "reppartycd")

df_intermed_geo_appends.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.geo_appends")

df_intermed_geo_appends = spark.table("intermed.geo_appends")
df_intermed_geo_appends.write.format("delta").mode("overwrite").saveAsTable("intermed.geo_appends")

df_intermed_geo_appends = spark.table("intermed.geo_appends").dropDuplicates(["memacctnum", "merkleid"])
spark.sql("CREATE INDEX mid_key ON intermed.geo_appends")

# ODS functionality for PDF reporting is translated to creating and saving summary tables.
df_geo_appends = spark.table("intermed.geo_appends")
cols_to_freq = [
    # list of columns from the SAS PROC FREQ step
]
# The full list of columns from SAS PROC FREQ
cols_to_freq = [col for col in df_geo_appends.columns if col.lower().startswith('ibx')] + \
               [col for col in df_geo_appends.columns if col.lower().startswith('past')] + \
               [col for col in df_geo_appends.columns if col.lower().startswith('sy')] + \
               [col for col in df_geo_appends.columns if col.lower().startswith('vehicle')] + \
               ['ACEV_Flag', 'ACEV_Num', 'advo_segment_cd', 'audio_visual_composite', 'Chase_Num_Active_Particpnts', 
                'Chase_Num_InActive_Particpnts', 'CurrentPartCt_Overall', 'emailable_agg_ind', 'Env_Humant_Educ', 
                'EthnicCode', 'Foremost_Num_Active_Particpnts', 'Gender', 'gender_agg_ind', 'gender_input', 
                'GeneralElectn2012', 'GE_Num_Active_Particpnts', 'Globally_Opted_In', 'GroupEthnicCode', 
                'Hartford_Num_Active_Particpnts', 'HistPartCt_Overall', 'hitech_merch', 'home_purchase_date', 
                'household_size', 'mail_order_donor_categories', 'MaritalStatus', 'MemStatus', 'na2', 
                'NbrTimesSelEmailedInd', 'NYL_Num_Active_Particpnts', 'NYL_Num_InActive_Particpnts', 'other_donors', 
                'outdoors_dimension', 'Overall_Active_SP_Reltshps', 'Overall_Historic_SP_Reltshps', 'PartyAffiliation', 
                'PartyMix', 'pc_prdct_buyer', 'political', 'ReligionCode', 'RepPartyCd', 'State', 'travel_us_premier', 
                'VoterStatus', 'voter_party_input', 'vtm_active_vol_flag_act', 'vtm_vol_flag_act', 'WorkStatus', 
                'MEMBER_FL_AGG_IND', 'LapsMail', 'totalmailings', 'prospmail', 'acknow', 'advo_petition', 'community', 
                'exercise_health_group', 'region', 'ftc_dnc_append_fl', 'UNINSURED_MODEL', 'ETHNICITY', 'order_type', 
                'fishing', 'nascar', 'diy_living', 'environmental_issues', 'gaming_casino', 'hunting_shooting', 
                'investments_personal', 'reading_financial_newsletter_sub', 'reading_general', 'reading_religious_inspirational', 
                'science_space', 'smoking_tobacco', 'spectator_sports_auto_motorcycle', 'spectator_sports_basketball', 
                'spectator_sports_hockey', 'spectator_sports_tennis', 'strange_and_unusual', 'theater_performing_arts', 'em', 
                'advocacy_petition_signer', 'foundation_donor', 'dieting_weight_loss', 'exercise_aerobic', 'exercise_walking', 
                'home_pool_present', 'Tele_Townhall_Engagers', 'advocacy_grassroots_engager', 'attended_aarp_event', 
                'ibx_community_involvement_causes', 'num_entertainment_visits_past_3m', 'phn', 'motorcycling', 'auto_work', 
                'boating_sailing', 'broader_living', 'collectibles_coins', 'education_online', 'home_furnishings_decorating', 
                'music_collector', 'reading_best_sellers', 'spect_sports_motorcycle_racing', 'sweeps_contests', 
                'tv_guide_network', 'engaged_aarp_event', 'childrens_interests', 'spectator_sports_football']

all_freqs = []
total_count = df_geo_appends.count()
for col_name in cols_to_freq:
    if col_name in df_geo_appends.columns:
        freq_df = df_geo_appends.groupBy(col_name).count()
        freq_df = freq_df.withColumn("col_name", F.lit(col_name)) \
            .withColumn("percent", (F.col("count") / total_count) * 100) \
            .select(F.col("col_name"), F.col(col_name).alias("col_level"), F.col("count").alias("_freq_"), "percent")
        all_freqs.append(freq_df)

if all_freqs:
    df_input_characters_pct = reduce(lambda df1, df2: df1.unionByName(df2), all_freqs)
    df_input_characters_pct = df_input_characters_pct.orderBy("col_name", "col_level")
    df_input_characters_pct.write.format("delta").mode("overwrite").saveAsTable(f"scoring.Input_Char_Var_{runtype}_{muldate}")

df_means_input = df_geo_appends.drop('hid_key', 'mid_key', 'cid_key', 'memacctnum')
numeric_cols = [f.name for f in df_means_input.schema.fields if isinstance(f.dataType, (IntegerType, DoubleType)) and not f.name.lower().startswith('sy:')]

agg_exprs = []
for col_name in numeric_cols:
    agg_exprs.append(F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias(f"{col_name}_NMiss"))
    agg_exprs.append(F.mean(col_name).alias(f"{col_name}_Mean"))
    agg_exprs.append(F.expr(f'percentile_approx({col_name}, 0.5)').alias(f"{col_name}_Median"))
    agg_exprs.append(F.stddev(col_name).alias(f"{col_name}_StdDev"))
    agg_exprs.append(F.min(col_name).alias(f"{col_name}_Min"))
    agg_exprs.append(F.expr(f'percentile_approx({col_name}, 0.25)').alias(f"{col_name}_P25"))
    agg_exprs.append(F.expr(f'percentile_approx({col_name}, 0.75)').alias(f"{col_name}_P75"))
    agg_exprs.append(F.max(col_name).alias(f"{col_name}_Max"))

df_input_means = df_means_input.agg(*agg_exprs)

# Transposing the result
unpivot_expr = "stack(1, " + ", ".join([f"'{col}', `{col}`" for col in df_input_means.columns]) + ") as (Metric, Value)"
df_transposed = df_input_means.selectExpr(unpivot_expr)

df_transposed = df_transposed.withColumn("_NAME_", F.regexp_extract(F.col("Metric"), "(.*)_[^_]+$", 1)) \
                             .withColumn("_STAT_", F.regexp_extract(F.col("Metric"), ".*_([^_]+)$", 1))
                             
df_scoring_input_num_var = df_transposed.groupBy("_NAME_").pivot("_STAT_").agg(F.first("Value"))
df_scoring_input_num_var = df_scoring_input_num_var.withColumnRenamed("_NAME_", "_name_") \
    .withColumn("_label_", F.col("_name_")) \
    .orderBy("_name_", "_label_")
df_scoring_input_num_var.write.format("delta").mode("overwrite").saveAsTable(f"scoring.Input_Num_Var_{runtype}_{muldate}")

def cleanup(dsn, runtype):
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
            # Drop tables with wildcard - not directly supported, requires listing and looping
            for table in spark.catalog.listTables("mulinter"):
                if table.name.startswith("lifestyle_engage") or table.name.startswith("order_curr_date"):
                    spark.sql(f"DROP TABLE IF EXISTS mulinter.{table.name}")
            spark.sql("DROP TABLE IF EXISTS mulinter.activities_sum")
            
        spark.sql("DROP TABLE IF EXISTS intermed.mid_key_appends_2")
        spark.sql("DROP TABLE IF EXISTS intermed.mid_key_appends1a_2")
        spark.sql("DROP TABLE IF EXISTS intermed.mid_key_appends2_2")
        spark.sql("DROP TABLE IF EXISTS intermed.mid_key_appends3_2")
        spark.sql("DROP TABLE IF EXISTS intermed.geo_cid_hid")
        spark.sql("DROP TABLE IF EXISTS intermed.cid_appends")
        spark.sql("DROP TABLE IF EXISTS intermed.geo_appends_rpm_dedup")
        spark.sql("DROP TABLE IF EXISTS intermed.household")
        for table in spark.catalog.listTables("intermed"):
            if table.name.startswith("emu"):
                spark.sql(f"DROP TABLE IF EXISTS intermed.{table.name}")

    else:
        print(f"Data set {dsn} does not exist")

cleanup("intermed.geo_appends", runtype)
#End-DBShift