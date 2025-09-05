%run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/Creds"
%run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/ReadLibnames"

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Assuming spark session is available as 'spark'
# Assuming connection variables 'conn', 'dsn', 'usern', 'passw' are defined from the %run command.
# For example:
# conn = "your_db_connection_alias"
# dsn = "your_dsn"
# usern = "your_user"
# passw = "your_password"
# ref2 = "your_db_schema"
# jdbc_url = "your_jdbc_url" # This needs to be configured based on dsn

enddt_date = datetime.now()
enddt = enddt_date.strftime('%Y-%m-%d')
startdt = (enddt_date - relativedelta(months=12)).strftime('%Y-%m-%d')
startdt_3mo = (enddt_date - relativedelta(months=3)).strftime('%Y-%m-%d')
startdt_1mo = (enddt_date - relativedelta(months=1)).strftime('%Y-%m-%d')
startdt_6mo = (enddt_date - relativedelta(months=6)).strftime('%Y-%m-%d')
startdt_12mo = (enddt_date - relativedelta(months=12)).strftime('%Y-%m-%d')
startdt_30days = (enddt_date - timedelta(days=30)).strftime('%Y-%m-%d')
startdt_365days = (enddt_date - timedelta(days=365)).strftime('%Y-%m-%d')
startdt_180days = (enddt_date - timedelta(days=180)).strftime('%Y-%m-%d')

print(f"{startdt} {enddt} {startdt_3mo} {startdt_1mo} {startdt_6mo} {startdt_12mo} {startdt_30days} {startdt_365days} {startdt_180days}")

spark.sql("DROP TABLE IF EXISTS work.conact_hist_mail")
spark.sql("DROP TABLE IF EXISTS work.contact_hist_mail_ob")
spark.sql("DROP TABLE IF EXISTS work.contact_obct")
spark.sql("DROP TABLE IF EXISTS work.contact_obct_mailer")
spark.sql("DROP TABLE IF EXISTS work.contact_obemail_mailers")
spark.sql("DROP TABLE IF EXISTS work.contact_obct_aca")
spark.sql("DROP TABLE IF EXISTS work.contact_obct_aca2")
spark.sql("DROP TABLE IF EXISTS work.contact_hist_2a1")
spark.sql("DROP TABLE IF EXISTS work.contact_hist_2a3")
spark.sql("DROP TABLE IF EXISTS work.contact_hist_phone_both")
spark.sql("DROP TABLE IF EXISTS work.contact_hist_2")
spark.sql("DROP TABLE IF EXISTS work.contact_inbct")
spark.sql("DROP TABLE IF EXISTS work.contact_ibmailer")
spark.sql("DROP TABLE IF EXISTS work.contact_click")
spark.sql("DROP TABLE IF EXISTS work.mailer_click")
spark.sql("DROP TABLE IF EXISTS work.contact_open")
spark.sql("DROP TABLE IF EXISTS work.mailer_open")
spark.sql("DROP TABLE IF EXISTS work.click_rate")
spark.sql("DROP TABLE IF EXISTS work.click_mailerid_transpose_past12")
spark.sql("DROP TABLE IF EXISTS work.sent_mailerid_transpose_past12")
spark.sql("DROP TABLE IF EXISTS work.contact_history_sum")

sql_query_conact_hist_mail = f"""
           select e.mid_key, count(e.mid_key) as mailct
	                from {ref2}.f_contact_history_analytic e
					left join {ref2}.d_contact_history_ib_analytic as b
					on e.d_contact_history_ib_key=b.d_contact_history_ib_key
                       where cast(contact_dt as date) >= '{startdt}'
                         	and cast(contact_dt as date) <= '{enddt}'
                            and e.d_contact_history_ib_key is not null
							and b.COMM_CHANNEL='M'
					group by e.mid_key
					order by e.mid_key
"""
df_conact_hist_mail_aa = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", f"({sql_query_conact_hist_mail}) as subq").option("user", usern).option("password", passw).load()
df_geo_appends = spark.table("intermed.geo_appends")
df_conact_hist_mail = df_conact_hist_mail_aa.join(df_geo_appends, on="mid_key", how="inner")

sql_query_contact_hist_mail_ob = f"""
           select e.mid_key, count(mid_key) as mailct_all,
		   /* added 20181002  */
		   Sum(case when cast(contact_dt as date) between '{startdt_365days}'
				and '{enddt}' then 1 Else 0 End) as mailct_past12
	                from {ref2}.f_contact_history_analytic e
					join {ref2}.d_campaign_analytic f
					on e.D_CAMPAIGN_KEY=f.D_CAMPAIGN_KEY
                       where cast(contact_dt as date) >= '{startdt}'
                         	and cast(contact_dt as date) <= '{enddt}'
							and comm_channel='M'
                         /*   and d_contact_history_ib_key is not null */
					group by mid_key
					order by mid_key
"""
df_contact_hist_mail_ob_aa = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", f"({sql_query_contact_hist_mail_ob}) as subq").option("user", usern).option("password", passw).load()
df_contact_hist_mail_ob = df_contact_hist_mail_ob_aa.join(df_geo_appends, on="mid_key", how="inner")

sql_query_contact_obct = f"""
select mid_key, Sum(case when cast(contact_dt as date) between '{startdt_6mo}'
		and '{enddt}' then 1 Else 0 End) as num_ct,
	Sum(case when cast(contact_dt as date) between '{startdt_30days}'
		and '{enddt}' then 1 Else 0 End) as num_sent_curmonth,
	Sum(case when cast(contact_dt as date) between '{startdt_365days}'
		and '{enddt}' then 1 Else 0 End) as num_sent_past12
from {ref2}.f_contact_history_analytic e
		join {ref2}.d_campaign_analytic f
		on e.D_CAMPAIGN_KEY=f.D_CAMPAIGN_KEY
where COMM_CHANNEL='E' and contact_direction='O'
group by mid_key
order by mid_key
"""
df_contact_obct_aa = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", f"({sql_query_contact_obct}) as subq").option("user", usern).option("password", passw).load()
df_contact_obct = df_contact_obct_aa.join(df_geo_appends, on="mid_key", how="inner")

sql_query_contact_obct_mailer = f"""
select mid_key, mailer_id, Sum(case when cast(contact_dt as date) between '{startdt_30days}'
		 and '{enddt}' then 1 Else 0 End) as num_sent,
	Sum(case when cast(contact_dt as date) between '{startdt_365days}'
		 and '{enddt}' then 1 Else 0 End) as num_sent_past12,
	Sum(case when cast(contact_dt as date) between '{startdt_180days}'
		 and '{enddt}' then 1 Else 0 End) as num_sent_past180
from {ref2}.f_contact_history_analytic e
		join {ref2}.d_campaign_analytic f
		on e.D_CAMPAIGN_KEY=f.D_CAMPAIGN_KEY
where COMM_CHANNEL='E' and contact_direction='O'
group by mid_key,mailer_id
order by mid_key
"""
df_contact_obct_mailer_aa = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", f"({sql_query_contact_obct_mailer}) as subq").option("user", usern).option("password", passw).load()
df_contact_obct_mailer = df_contact_obct_mailer_aa.join(df_geo_appends, on="mid_key", how="inner")

df_contact_obct_mailer.createOrReplaceTempView("contact_obct_mailer")
df_contact_obemail_mailers = spark.sql("""
	select mid_key,
	sum(case when num_sent_past180>0 then 1 else 0 end) as mailercount_sent_180,
	sum(case when num_sent>0 then 1 else 0 end) as mailercount_sent_30days
	from contact_obct_mailer
	group by mid_key
""")

sql_query_contact_obct_aca = f"""
select mid_key, 	Sum(case when cast(contact_dt as date) between '{startdt_365days}'
		and '{enddt}' then 1 Else 0 End) as care_ct
from {ref2}.f_contact_history_analytic e
		join {ref2}.d_campaign_analytic f
		on e.D_CAMPAIGN_KEY=f.D_CAMPAIGN_KEY
where contact_direction='O' and /*changed from comm_sponsor_ch 20170731 because equal to mailer_id*/ mailer_id
in ('AAHC', 'COHC','HDEM','HIAT','HIRX','VDE5','VDEY','CODZ','VDE6','VDE7','VDE8') and comm_channel in ('P')
group by mid_key
"""
df_contact_obct_aca = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", f"({sql_query_contact_obct_aca}) as subq").option("user", usern).option("password", passw).load()

sql_query_contact_obct_aca2 = f"""
select mid_key, 	Sum(case when cast(contact_dt as date) between '{startdt_365days}'
	  and '{enddt}'	then 1 Else 0 End) as care_ct
from {ref2}.f_contact_history_analytic e
		join {ref2}.d_campaign_analytic f
		on e.D_CAMPAIGN_KEY=f.D_CAMPAIGN_KEY
where contact_direction='O' and /*changed from comm_sponsor_ch 20170731 because equal to mailer_id*/ mailer_id
in ('AAHC', 'COHC','HDEM','HIAT','HIRX','VDE5','VDEY','CODZ','VDE6','VDE7','VDE8') and comm_channel in ('E')
group by mid_key
"""
df_contact_obct_aca2 = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", f"({sql_query_contact_obct_aca2}) as subq").option("user", usern).option("password", passw).load()

df_contact_obct_aca = df_contact_obct_aca.unionByName(df_contact_obct_aca2)

df_contact_obct_aca = df_contact_obct_aca.groupBy("mid_key").agg(F.sum("care_ct").alias("care_ct"))

sql_query_contact_hist_2a1 = f"""
select a.mid_key, count(a.mid_key) as call_freq,
	sum(case when contact_disposition='LIVE ANSWER' then 1 else 0 end) as live_answer_ct,
	sum(case when question_answered='Y' then 1 else 0 end) as poll_ct,
	sum(case when question_answered='X' then 1 else 0 end) as poll_noaskct,
	sum(case when question_answered='N' then 1 else 0 end) as poll_anact ,
	sum(case when contact_disposition in ('LIVE ANSWER','COMPLETE') then 1 else 0 end) as live_answer_comp_ct
from {ref2}.f_contact_history_analytic a
left join {ref2}.d_contact_history_ib_analytic b
on a.d_contact_history_ib_key=b.d_contact_history_ib_key
where COMM_CHANNEL='P' and cast(contact_dt as date) between '{startdt_12mo}'
		and '{enddt}'
group by mid_key
"""
df_contact_hist_2a1_aa = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", f"({sql_query_contact_hist_2a1}) as subq").option("user", usern).option("password", passw).load()
df_contact_hist_2a1_aa = df_contact_hist_2a1_aa.withColumn("pct_live", F.col("live_answer_ct") / F.col("call_freq")) \
                                           .withColumn("pct_live2", F.col("live_answer_comp_ct") / F.col("call_freq"))
df_contact_hist_2a1 = df_contact_hist_2a1_aa.join(df_geo_appends, on="mid_key", how="inner")
df_contact_hist_2a1 = df_contact_hist_2a1.select("mid_key", "call_freq", "live_answer_ct", "poll_ct", "poll_noaskct", "poll_anact", "pct_live", "pct_live2")

sql_query_contact_hist_2a3 = f"""
select a.mid_key,
	sum(case when contact_disposition='LIVE ANSWER'
		and cast(contact_dt as date) between '{startdt_3mo}' and '{enddt}' then 1 else 0 end)
			as liveanswer_freq_3,
	sum(case when contact_disposition='LIVE ANSWER'
		and cast(contact_dt as date) between '{startdt_6mo}' and '{startdt_3mo}' then 1 else 0 end)
			as liveanswer_freq3_6,
	sum(case when contact_disposition='LIVE ANSWER'
		and cast(contact_dt as date) between '{startdt_12mo}' and '{startdt_6mo}' then 1 else 0 end)
			as liveanswer_freq6_12,
	/* added 20190529 for 2019 rpm model  */
	sum(case when contact_disposition in ('LIVE ANSWER','COMPLETE')
		and cast(contact_dt as date) between '{startdt_3mo}' and '{enddt}' then 1 else 0 end)
			as liveanswer_comp_freq_3,
	sum(case when contact_disposition in ('LIVE ANSWER','COMPLETE')
		and cast(contact_dt as date) between '{startdt_6mo}' and '{startdt_3mo}' then 1 else 0 end)
			as liveanswer_comp_freq3_6,
	sum(case when contact_disposition in ('LIVE ANSWER','COMPLETE')
		and cast(contact_dt as date) between '{startdt_12mo}' and '{startdt_6mo}' then 1 else 0 end)
			as liveanswer_comp_freq6_12
from {ref2}.f_contact_history_analytic a
left join {ref2}.d_contact_history_ib_analytic b
on a.d_contact_history_ib_key=b.d_contact_history_ib_key
where COMM_CHANNEL='P'
group by mid_key
"""
df_contact_hist_2a3_aa = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", f"({sql_query_contact_hist_2a3}) as subq").option("user", usern).option("password", passw).load()
df_contact_hist_2a3 = df_contact_hist_2a3_aa.join(df_geo_appends, on="mid_key", how="inner")

sql_query_contact_hist_phone_both = f"""
select a.mid_key, count(a.mid_key) as call_freq_12mo_both
from {ref2}.f_contact_history_analytic a
join {ref2}.d_campaign_analytic CA
       on A.D_CAMPAIGN_KEY = CA.D_CAMPAIGN_KEY
where COMM_CHANNEL='P' and cast(contact_dt as date) between '{startdt_12mo}'
		and '{enddt}'
group by mid_key
"""
df_contact_hist_phone_both_aa = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", f"({sql_query_contact_hist_phone_both}) as subq").option("user", usern).option("password", passw).load()
df_contact_hist_phone_both = df_contact_hist_phone_both_aa.join(df_geo_appends, on="mid_key", how="inner")

df_contact_hist_2a1.createOrReplaceTempView("contact_hist_2a1")
df_contact_hist_2a3.createOrReplaceTempView("contact_hist_2a3")
df_contact_hist_2 = spark.sql("""
select coalesce(a.mid_key,b.mid_key) as mid_key,a.call_freq,a.live_answer_ct,a.poll_ct,a.poll_noaskct,
	a.poll_anact,a.pct_live,a.pct_live2,b.liveanswer_freq_3,b.liveanswer_freq3_6,b.liveanswer_freq6_12,
	b.liveanswer_comp_freq_3,b.liveanswer_comp_freq3_6,b.liveanswer_comp_freq6_12
from contact_hist_2a1 as a
full join contact_hist_2a3 as b
on a.mid_key=b.mid_key
""")

sql_query_contact_inbct = f"""
select mid_key,
	Sum(case when cast(contact_dt as date) between '{startdt_6mo}'
		 and '{enddt}' then 1 Else 0 End) as num_inb,
	/* added 20181113  */
	Sum(case when cast(contact_dt as date) between '{startdt_30days}'
		 and '{enddt}' then 1 Else 0 End) as num_inb_30days,
	/* added 20200324  */
	Sum(case when cast(contact_dt as date) between '{startdt_6mo}'
		and '{startdt_3mo}' then 1 Else 0 End) as num_inb_3_6mo,
	/* TB added 20211122  */
	Sum(case when cast(contact_dt as date) between '{startdt_3mo}'
        and '{startdt_1mo}' then 1 Else 0 End) as num_inb_1_3mo

from {ref2}.f_contact_history_analytic a
left join {ref2}.d_contact_history_ib_analytic b
on a.d_contact_history_ib_key=b.d_contact_history_ib_key
where transtype_key=8000 and comm_channel = 'E'
group by mid_key
"""
df_contact_inbct_aa = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", f"({sql_query_contact_inbct}) as subq").option("user", usern).option("password", passw).load()
df_contact_inbct = df_contact_inbct_aa.join(df_geo_appends, on="mid_key", how="inner")

sql_query_contact_ibmailer = f"""
select distinct mid_key, cast(mailer_id as char(7)) as mailer_id,
	Sum(case when contact_disposition='EC' and cast(contact_dt as date) between '{startdt_6mo}' and '{enddt}' then 1 Else 0 End) as num_click_mailer,
	Sum(case when contact_disposition='EC' and cast(contact_dt as date) between '{startdt_30days}' and '{enddt}' then 1 Else 0 End) as num_clicked_curr_mailer,
	Sum(case when contact_disposition='EC' and cast(contact_dt as date) between '{startdt_365days}' and '{enddt}' then 1 Else 0 End) as num_clicked_past12_mailer,
	/* added 20181001 for Medicare_em_attend and updated lo_medicare */
	Sum(case when contact_disposition='EC' and cast(contact_dt as date) between '{startdt_3mo}' and '{startdt_1mo}' then 1 Else 0 End) as num_clicked_1_3_mailer,
	Sum(case when contact_disposition='EO' and cast(contact_dt as date) between '{startdt_6mo}' and '{enddt}' then 1 Else 0 End) as num_open_mailer_6mo,
	Sum(case when contact_disposition='EO' and cast(contact_dt as date) between '{startdt_30days}' and '{enddt}' then 1 Else 0 End) as num_open_mailer_30days,
	Sum(case when contact_disposition='EO' and cast(contact_dt as date) between '{startdt_12mo}' and '{enddt}' then 1 Else 0 End) as num_open_mailer_12mo,
	/* added 20181002 for updated_lo_medicare */
	Sum(case when contact_disposition='EO' and cast(contact_dt as date) between '{startdt_3mo}' and '{startdt_1mo}' then 1 Else 0 End) as num_open_mailer_1_3mo,
	Sum(case when contact_disposition in ('EO','EC') and cast(contact_dt as date) between '{startdt_3mo}' and '{enddt}' then 1 Else 0 End) as num_ib_mailer_3mo,
	/* added 20181009 for work_jobs_attendee_em  */
	Sum(case when contact_disposition='EC' and cast(contact_dt as date) between '{startdt_6mo}' and '{startdt_3mo}' then 1 Else 0 End) as num_clicked_3_6_mailer,
	Sum(case when contact_disposition='EO' and cast(contact_dt as date) between '{startdt_6mo}' and '{startdt_3mo}' then 1 Else 0 End) as num_open_mailer_3_6mo,
	Sum(case when contact_disposition in ('EO','EC') and cast(contact_dt as date) between '{startdt_30days}' and '{enddt}' then 1 Else 0 End) as num_ib_mailer_30days,
	Sum(case when contact_disposition in ('EO','EC') and cast(contact_dt as date) between '{startdt_3mo}' and '{startdt_1mo}' then 1 Else 0 End) as num_ib_mailer_1_3mo,
	Sum(case when contact_disposition in ('EO','EC') and cast(contact_dt as date) between '{startdt_6mo}' and '{enddt}' then 1 Else 0 End) as num_ib_mailer_3_6mo,
	cast('' as char(1)) as extra_null
from {ref2}.f_contact_history_analytic a
left join {ref2}.d_contact_history_ib_analytic b
on a.d_contact_history_ib_key=b.d_contact_history_ib_key
left join {ref2}.d_campaign_analytic f
		on a.D_CAMPAIGN_KEY=f.D_CAMPAIGN_KEY
where transtype_key=8000 and b.comm_channel = 'E' and contact_direction='I'
group by mid_key, mailer_id
"""
df_contact_ibmailer_aa = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", f"({sql_query_contact_ibmailer}) as subq").option("user", usern).option("password", passw).load()
df_contact_ibmailer = df_contact_ibmailer_aa.join(df_geo_appends.select("mid_key"), on="mid_key", how="inner")

df_contact_ibmailer = df_contact_ibmailer.withColumn("num_open_mailer_12mo", F.when(F.col("num_open_mailer_12mo") > 0, 0).otherwise(F.col("num_open_mailer_12mo")))
df_contact_ibmailer = df_contact_ibmailer.withColumn("num_open_mailer_3_6mo", F.when(F.col("num_open_mailer_3_6mo") > 0, 0).otherwise(F.col("num_open_mailer_3_6mo")))
df_contact_ibmailer = df_contact_ibmailer.withColumn("num_open_mailer_6mo", F.when(F.col("num_open_mailer_6mo") > 0, 0).otherwise(F.col("num_open_mailer_6mo")))
df_contact_ibmailer = df_contact_ibmailer.withColumn("num_open_mailer_30days", F.when(F.col("num_open_mailer_30days") > 0, 0).otherwise(F.col("num_open_mailer_30days")))
df_contact_ibmailer = df_contact_ibmailer.withColumn("num_open_mailer_1_3mo", F.when(F.col("num_open_mailer_1_3mo") > 0, 0).otherwise(F.col("num_open_mailer_1_3mo")))

df_contact_ibmailer.createOrReplaceTempView("contact_ibmailer")
df_contact_click = spark.sql("""
select mid_key, Sum(num_click_mailer) as num_click,
	Sum(num_clicked_curr_mailer) as num_clicked_curmonth,
	Sum(num_clicked_past12_mailer) as num_clicked_past12,
	/* added 20181002 for updated lo_medicare */
	sum(num_clicked_1_3_mailer) as num_clicked_1_3mo,
	/* added 20181009 for work_jobs_attendee_em */
	sum(num_clicked_3_6_mailer) as num_clicked_3_6mo
from contact_ibmailer
group by mid_key
""")

df_mailer_click = spark.sql("""
select mid_key,
sum(case when num_clicked_past12_mailer>0 then 1 else 0 end) as mailercount_click,
sum(case when num_click_mailer>0 then 1 else 0 end) as mailercount_click_6mo,
/* added 20180517 for nps  */
sum(case when num_clicked_curr_mailer>0 then 1 else 0 end) as mailercount_click_30days,
/* added 20181001 for Medicare_em_attend  */
sum(case when num_clicked_1_3_mailer>0 then 1 else 0 end) as mailercount_click_1_3mo,
/* added 20181009 for work_jobs_attendee_em  */
sum(case when num_clicked_3_6_mailer>0 then 1 else 0 end) as mailercount_click_3_6mo
from contact_ibmailer
group by mid_key
""")

df_contact_open = spark.sql("""
select mid_key, sum(num_open_mailer_12mo) as num_open,
	Sum(num_open_mailer_6mo) as num_open_6mo,
	/* added 20180517 for nps  */
	Sum(num_open_mailer_30days) as num_open_30days,
	/* added 20180613 for fndn_pro_em */
	Sum(num_ib_mailer_3mo) as num_ib_3mo,
	/* added 20181002 for updated lo_medicare */
	Sum(num_open_mailer_1_3mo) as num_open_1_3mo,
	/* added 20181009 for work_jobs_attendee_em */
	Sum(num_open_mailer_3_6mo) as num_open_3_6mo,
	Sum(num_ib_mailer_30days) as num_ib_30days,
	Sum(num_ib_mailer_1_3mo) as num_ib_1_3mo,
	Sum(num_ib_mailer_3_6mo) as num_ib_3_6mo
	from contact_ibmailer
group by mid_key
""")

df_mailer_open = spark.sql("""
select mid_key, sum(case when num_open_mailer_12mo>0 then 1 else 0 end) as mailercount_open,
	sum(case when num_open_mailer_6mo>0 then 1 else 0 end) as mailercount_open_6mo,
	/* added 20181009 for work_jobs_attendee_em */
	sum(case when num_open_mailer_30days>0 then 1 else 0 end) as mailercount_open_30days,
	sum(case when num_open_mailer_3_6mo>0 then 1 else 0 end) as mailercount_open_3_6mo,
	sum(case when num_open_mailer_1_3mo>0 then 1 else 0 end) as mailercount_open_1_3mo
from contact_ibmailer
group by mid_key
""")

df_contact_click.createOrReplaceTempView("contact_click")
df_contact_open.createOrReplaceTempView("contact_open")
df_click_rate = spark.sql("""
select a.mid_key , num_click/num_open as click_rate,num_click/num_open_6mo as click_rate_6mo
from contact_click as a inner join
contact_open as b
on a.mid_key=b.mid_key
""")

df_click_transposed_base = df_contact_ibmailer.select("mid_key", "mailer_id", "num_clicked_past12_mailer")
df_click_pivoted = df_click_transposed_base.groupBy("mid_key").pivot("mailer_id").agg(F.first("num_clicked_past12_mailer"))
click_renamed_cols = [F.col(c).alias(f"{c}_click") if c != 'mid_key' else F.col(c) for c in df_click_pivoted.columns]
df_click_mailerid_transpose_past12 = df_click_pivoted.select(click_renamed_cols)

vlist_click_cols = [c for c in df_click_mailerid_transpose_past12.columns if c not in ('mid_key')]
vlist_click = ",".join(vlist_click_cols)
print(vlist_click)

non_key_cols = [c for c in df_click_mailerid_transpose_past12.columns if c != 'mid_key']
df_click_mailerid_transpose_past12 = df_click_mailerid_transpose_past12.fillna(0, subset=non_key_cols)

df_sent_transposed_base = df_contact_obct_mailer.select("mid_key", "mailer_id", "num_sent_past12")
df_sent_pivoted = df_sent_transposed_base.groupBy("mid_key").pivot("mailer_id").agg(F.first("num_sent_past12"))
sent_renamed_cols = [F.col(c).alias(f"{c}_sent") if c != 'mid_key' else F.col(c) for c in df_sent_pivoted.columns]
df_sent_mailerid_transpose_past12 = df_sent_pivoted.select(sent_renamed_cols)

vlist_sent_cols = [c for c in df_sent_mailerid_transpose_past12.columns if c not in ('mid_key')]
vlist_sent = ",".join(vlist_sent_cols)
print(vlist_sent)

vlist_sent_blank = " ".join(vlist_sent_cols)
print(vlist_sent_blank)
vlist_clicked_blank = vlist_sent_blank.replace("sent", "clickrate")
print(vlist_clicked_blank)

non_key_cols_sent = [c for c in df_sent_mailerid_transpose_past12.columns if c != 'mid_key']
df_sent_mailerid_transpose_past12 = df_sent_mailerid_transpose_past12.fillna(0, subset=non_key_cols_sent)

df_geo_appends_keys = spark.table("intermed.geo_appends").select("mid_key")
df_contact_history_sum = df_geo_appends_keys.alias("a") \
    .join(df_contact_inbct.alias("b"), "mid_key", "left") \
    .join(df_contact_click.alias("c"), "mid_key", "left") \
    .join(df_mailer_click.alias("d"), "mid_key", "left") \
    .join(df_contact_open.alias("e"), "mid_key", "left") \
    .join(df_mailer_open.alias("f"), "mid_key", "left") \
    .select(
        F.col("a.mid_key"),
        F.col("b.num_inb"),
        F.coalesce(F.col("b.num_inb_30days"), F.lit(0)).alias("num_inb_30days"),
        F.coalesce(F.col("b.num_inb_3_6mo"), F.lit(0)).alias("num_inb_3_6mo"),
        F.coalesce(F.col("b.num_inb_1_3mo"), F.lit(0)).alias("num_inb_1_3mo"),
        F.col("c.num_click"),
        F.coalesce(F.col("c.num_clicked_curmonth"), F.lit(0)).alias("num_clicked_curmonth"),
        F.coalesce(F.col("c.num_clicked_past12"), F.lit(0)).alias("num_clicked_past12"),
        F.coalesce(F.col("c.num_clicked_1_3mo"), F.lit(0)).alias("num_clicked_1_3mo"),
        F.coalesce(F.col("c.num_clicked_3_6mo"), F.lit(0)).alias("num_clicked_3_6mo"),
        F.col("d.mailercount_click"),
        F.col("d.mailercount_click_6mo"),
        F.col("d.mailercount_click_30days"),
        F.col("d.mailercount_click_1_3mo"),
        F.col("d.mailercount_click_3_6mo"),
        F.col("e.num_open"),
        F.col("e.num_open_6mo"),
        F.col("e.num_open_30days"),
        F.col("e.num_ib_3mo"),
        F.coalesce(F.col("e.num_open_1_3mo"), F.lit(0)).alias("num_open_1_3mo"),
        F.coalesce(F.col("e.num_open_3_6mo"), F.lit(0)).alias("num_open_3_6mo"),
        F.coalesce(F.col("e.num_ib_30days"), F.lit(0)).alias("num_ib_30days"),
        F.coalesce(F.col("e.num_ib_1_3mo"), F.lit(0)).alias("num_ib_1_3mo"),
        F.coalesce(F.col("e.num_ib_3_6mo"), F.lit(0)).alias("num_ib_3_6mo"),
        F.col("f.mailercount_open"),
        F.col("f.mailercount_open_6mo"),
        F.col("f.mailercount_open_30days"),
        F.col("f.mailercount_open_3_6mo"),
        F.col("f.mailercount_open_1_3mo")
    )

df_contact_history_sum_temp = df_contact_history_sum \
    .join(df_click_rate.alias("g"), "mid_key", "left") \
    .join(df_click_mailerid_transpose_past12.alias("h"), "mid_key", "left") \
    .join(df_sent_mailerid_transpose_past12.alias("i"), "mid_key", "left") \
    .join(df_contact_obct.alias("j"), "mid_key", "left") \
    .join(df_contact_obct_aca.alias("k"), "mid_key", "left")

select_exprs_2 = [F.col(c) for c in df_contact_history_sum.columns] + \
                 [F.col("g.click_rate"), F.col("g.click_rate_6mo")] + \
                 [F.col(c) for c in df_click_mailerid_transpose_past12.columns if c != "mid_key"] + \
                 [F.col(c) for c in df_sent_mailerid_transpose_past12.columns if c != "mid_key"] + \
                 [F.coalesce(F.col("j.num_sent_curmonth"), F.lit(0)).alias("num_sent_curmonth"),
                  F.col("j.num_sent_past12"),
                  F.col("j.num_ct"),
                  F.coalesce(F.col("k.care_ct"), F.lit(0)).alias("care_ct")]

df_contact_history_sum = df_contact_history_sum_temp.select(*select_exprs_2)

df_contact_history_sum_final = df_contact_history_sum \
    .join(df_contact_hist_2.alias("ll"), on="mid_key", how="left") \
    .join(df_conact_hist_mail.alias("mm"), on="mid_key", how="left") \
    .join(df_contact_obemail_mailers.alias("nn"), on="mid_key", how="left") \
    .join(df_contact_hist_mail_ob.alias("oo"), on="mid_key", how="left") \
    .join(df_contact_hist_phone_both.alias("pp"), on="mid_key", how="left")

select_exprs_final = [F.col(c) for c in df_contact_history_sum.columns] + \
                     [F.col(f"ll.{c}") for c in df_contact_hist_2.columns if c != 'mid_key'] + \
                     [F.col(f"mm.{c}") for c in df_conact_hist_mail.columns if c != 'mid_key'] + \
                     [F.col(f"nn.{c}") for c in df_contact_obemail_mailers.columns if c != 'mid_key'] + \
                     [F.col(f"oo.{c}") for c in df_contact_hist_mail_ob.columns if c != 'mid_key'] + \
                     [F.coalesce(F.col("pp.call_freq_12mo_both"), F.lit(0)).alias("call_freq_12mo_both")]

df_contact_history_sum = df_contact_history_sum_final.select(*select_exprs_final)
#End-DBShift