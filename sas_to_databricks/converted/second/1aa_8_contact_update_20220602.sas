import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from functools import reduce

#%run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/Creds"
#%run "/vg02/aarp_sas/aarp_projects/twalters/Data_Processing/ReadLibnames"

conn = "your_db_connection_alias"
dsn = "your_dsn"
usern = "your_user"
passw = "your_password"
jdbc_url = "jdbc:your_driver_here" 
ref2 = "your_remote_schema"

enddt_date = datetime.now()
enddt = enddt_date.strftime('%Y-%m-%d')
startdt = (enddt_date + relativedelta(months=-12)).strftime('%Y-%m-%d')
startdt_3mo = (enddt_date + relativedelta(months=-3)).strftime('%Y-%m-%d')
startdt_1mo = (enddt_date + relativedelta(months=-1)).strftime('%Y-%m-%d')
startdt_6mo = (enddt_date + relativedelta(months=-6)).strftime('%Y-%m-%d')
startdt_12mo = (enddt_date + relativedelta(months=-12)).strftime('%Y-%m-%d')
startdt_30days = (enddt_date - timedelta(days=30)).strftime('%Y-%m-%d')
startdt_365days = (enddt_date - timedelta(days=365)).strftime('%Y-%m-%d')
startdt_180days = (enddt_date - timedelta(days=180)).strftime('%Y-%m-%d')

print(f"{startdt} {enddt} {startdt_3mo} {startdt_1mo} {startdt_6mo} {startdt_12mo} {startdt_30days} {startdt_365days} {startdt_180days}")

df_geo_appends = spark.table("intermed.geo_appends")

sql_query_conact_hist_mail = f"""(
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
)"""
df_aa_1 = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", sql_query_conact_hist_mail).option("user", usern).option("password", passw).load()
df_conact_hist_mail = df_aa_1.join(df_geo_appends, on="mid_key", how="inner")
df_conact_hist_mail.write.format("delta").mode("overwrite").saveAsTable("intermed.conact_hist_mail")


sql_query_contact_hist_mail_ob = f"""(
           select e.mid_key, count(mid_key) as mailct_all,
		   Sum(case when cast(contact_dt as date) between '{startdt_365days}'
				and '{enddt}' then 1 Else 0 End) as mailct_past12
	                from {ref2}.f_contact_history_analytic e
					join {ref2}.d_campaign_analytic f
					on e.D_CAMPAIGN_KEY=f.D_CAMPAIGN_KEY
                       where cast(contact_dt as date) >= '{startdt}'
                         	and cast(contact_dt as date) <= '{enddt}'
							and comm_channel='M'
					group by mid_key
					order by mid_key
)"""
df_aa_2 = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", sql_query_contact_hist_mail_ob).option("user", usern).option("password", passw).load()
df_contact_hist_mail_ob = df_aa_2.join(df_geo_appends, on="mid_key", how="inner")
df_contact_hist_mail_ob.write.format("delta").mode("overwrite").saveAsTable("intermed.contact_hist_mail_ob")


sql_query_contact_obct = f"""(
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
)"""
df_aa_3 = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", sql_query_contact_obct).option("user", usern).option("password", passw).load()
df_contact_obct = df_aa_3.join(df_geo_appends, on="mid_key", how="inner")
df_contact_obct.write.format("delta").mode("overwrite").saveAsTable("intermed.contact_obct")

sql_query_contact_obct_mailer = f"""(
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
)"""
df_aa_4 = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", sql_query_contact_obct_mailer).option("user", usern).option("password", passw).load()
df_contact_obct_mailer = df_aa_4.join(df_geo_appends, on="mid_key", how="inner")
df_contact_obct_mailer.write.format("delta").mode("overwrite").saveAsTable("intermed.contact_obct_mailer")

df_contact_obct_mailer.createOrReplaceTempView("contact_obct_mailer")
df_contact_obemail_mailers = spark.sql("""
    SELECT
        mid_key,
        sum(case when num_sent_past180>0 then 1 else 0 end) as mailercount_sent_180,
        sum(case when num_sent>0 then 1 else 0 end) as mailercount_sent_30days
    FROM
        contact_obct_mailer
    GROUP BY
        mid_key
""")
df_contact_obemail_mailers.write.format("delta").mode("overwrite").saveAsTable("intermed.contact_obemail_mailers")

sql_query_contact_obct_aca = f"""(
select mid_key, 	Sum(case when cast(contact_dt as date) between '{startdt_365days}'
		and '{enddt}' then 1 Else 0 End) as care_ct
from {ref2}.f_contact_history_analytic e
		join {ref2}.d_campaign_analytic f
		on e.D_CAMPAIGN_KEY=f.D_CAMPAIGN_KEY
where contact_direction='O' and mailer_id
in ('AAHC', 'COHC','HDEM','HIAT','HIRX','VDE5','VDEY','CODZ','VDE6','VDE7','VDE8') and comm_channel in ('P')
group by mid_key
)"""
df_contact_obct_aca = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", sql_query_contact_obct_aca).option("user", usern).option("password", passw).load()

sql_query_contact_obct_aca2 = f"""(
select mid_key, 	Sum(case when cast(contact_dt as date) between '{startdt_365days}'
	  and '{enddt}'	then 1 Else 0 End) as care_ct
from {ref2}.f_contact_history_analytic e
		join {ref2}.d_campaign_analytic f
		on e.D_CAMPAIGN_KEY=f.D_CAMPAIGN_KEY
where contact_direction='O' and mailer_id
in ('AAHC', 'COHC','HDEM','HIAT','HIRX','VDE5','VDEY','CODZ','VDE6','VDE7','VDE8') and comm_channel in ('E')
group by mid_key
)"""
df_contact_obct_aca2 = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", sql_query_contact_obct_aca2).option("user", usern).option("password", passw).load()

df_contact_obct_aca_appended = df_contact_obct_aca.unionByName(df_contact_obct_aca2, allowMissingColumns=True)

df_contact_obct_aca_summary = df_contact_obct_aca_appended.groupBy("mid_key").agg(F.sum("care_ct").alias("care_ct"))
df_contact_obct_aca_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.contact_obct_aca")
df_contact_obct_aca = df_contact_obct_aca_summary

sql_query_contact_hist_2a1 = f"""(
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
)"""
df_aa_5 = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", sql_query_contact_hist_2a1).option("user", usern).option("password", passw).load()
df_contact_hist_2a1_joined = df_aa_5.join(df_geo_appends, on="mid_key", how="inner")
df_contact_hist_2a1 = df_contact_hist_2a1_joined.withColumn("pct_live", F.col("live_answer_ct") / F.col("call_freq")) \
                                              .withColumn("pct_live2", F.col("live_answer_comp_ct") / F.col("call_freq")) \
                                              .select("mid_key", "call_freq", "live_answer_ct", "poll_ct", "poll_noaskct", "poll_anact", "pct_live", "pct_live2")

sql_query_contact_hist_2a3 = f"""(
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
)"""
df_aa_6 = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", sql_query_contact_hist_2a3).option("user", usern).option("password", passw).load()
df_contact_hist_2a3 = df_aa_6.join(df_geo_appends, on="mid_key", how="inner")

sql_query_contact_hist_phone_both = f"""(
select a.mid_key, count(a.mid_key) as call_freq_12mo_both
from {ref2}.f_contact_history_analytic a
join {ref2}.d_campaign_analytic CA
       on A.D_CAMPAIGN_KEY = CA.D_CAMPAIGN_KEY
where COMM_CHANNEL='P' and cast(contact_dt as date) between '{startdt_12mo}'
		and '{enddt}'
group by mid_key
)"""
df_aa_7 = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", sql_query_contact_hist_phone_both).option("user", usern).option("password", passw).load()
df_contact_hist_phone_both = df_aa_7.join(df_geo_appends, on="mid_key", how="inner")
df_contact_hist_phone_both.write.format("delta").mode("overwrite").saveAsTable("intermed.contact_hist_phone_both")

df_contact_hist_2a1.createOrReplaceTempView("contact_hist_2a1")
df_contact_hist_2a3.createOrReplaceTempView("contact_hist_2a3")
df_contact_hist_2 = spark.sql("""
    SELECT
        coalesce(a.mid_key,b.mid_key) as mid_key,
        a.call_freq,
        a.live_answer_ct,
        a.poll_ct,
        a.poll_noaskct,
        a.poll_anact,
        a.pct_live,
        a.pct_live2,
        b.liveanswer_freq_3,
        b.liveanswer_freq3_6,
        b.liveanswer_freq6_12,
        b.liveanswer_comp_freq_3,
        b.liveanswer_comp_freq3_6,
        b.liveanswer_comp_freq6_12
    FROM
        contact_hist_2a1 as a
    FULL JOIN
        contact_hist_2a3 as b
        ON a.mid_key=b.mid_key
""")
df_contact_hist_2.write.format("delta").mode("overwrite").saveAsTable("intermed.contact_hist_2")

sql_query_contact_inbct = f"""(
select mid_key,
	Sum(case when cast(contact_dt as date) between '{startdt_6mo}'
		 and '{enddt}' then 1 Else 0 End) as num_inb,
	Sum(case when cast(contact_dt as date) between '{startdt_30days}'
		 and '{enddt}' then 1 Else 0 End) as num_inb_30days,
	Sum(case when cast(contact_dt as date) between '{startdt_6mo}'
		and '{startdt_3mo}' then 1 Else 0 End) as num_inb_3_6mo,
	Sum(case when cast(contact_dt as date) between '{startdt_3mo}'
        and '{startdt_1mo}' then 1 Else 0 End) as num_inb_1_3mo

from {ref2}.f_contact_history_analytic a
left join {ref2}.d_contact_history_ib_analytic b
on a.d_contact_history_ib_key=b.d_contact_history_ib_key
where transtype_key=8000 and comm_channel = 'E'
group by mid_key
)"""
df_aa_8 = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", sql_query_contact_inbct).option("user", usern).option("password", passw).load()
df_contact_inbct = df_aa_8.join(df_geo_appends, on="mid_key", how="inner")
df_contact_inbct.write.format("delta").mode("overwrite").saveAsTable("intermed.contact_inbct")

sql_query_contact_ibmailer = f"""(
select distinct mid_key, cast(mailer_id as char(7)) as mailer_id,
	Sum(case when contact_disposition='EC' and cast(contact_dt as date) between '{startdt_6mo}' and '{enddt}' then 1 Else 0 End) as num_click_mailer,
	Sum(case when contact_disposition='EC' and cast(contact_dt as date) between '{startdt_30days}' and '{enddt}' then 1 Else 0 End) as num_clicked_curr_mailer,
	Sum(case when contact_disposition='EC' and cast(contact_dt as date) between '{startdt_365days}' and '{enddt}' then 1 Else 0 End) as num_clicked_past12_mailer,
	Sum(case when contact_disposition='EC' and cast(contact_dt as date) between '{startdt_3mo}' and '{startdt_1mo}' then 1 Else 0 End) as num_clicked_1_3_mailer,
	Sum(case when contact_disposition='EO' and cast(contact_dt as date) between '{startdt_6mo}' and '{enddt}' then 1 Else 0 End) as num_open_mailer_6mo,
	Sum(case when contact_disposition='EO' and cast(contact_dt as date) between '{startdt_30days}' and '{enddt}' then 1 Else 0 End) as num_open_mailer_30days,
	Sum(case when contact_disposition='EO' and cast(contact_dt as date) between '{startdt_12mo}' and '{enddt}' then 1 Else 0 End) as num_open_mailer_12mo,
	Sum(case when contact_disposition='EO' and cast(contact_dt as date) between '{startdt_3mo}' and '{startdt_1mo}' then 1 Else 0 End) as num_open_mailer_1_3mo,
	Sum(case when contact_disposition in ('EO','EC') and cast(contact_dt as date) between '{startdt_3mo}' and '{enddt}' then 1 Else 0 End) as num_ib_mailer_3mo,
	Sum(case when contact_disposition='EC' and cast(contact_dt as date) between '{startdt_6mo}' and '{startdt_3mo}' then 1 Else 0 End) as num_clicked_3_6_mailer,
	Sum(case when contact_disposition='EO' and cast(contact_dt as date) between '{startdt_6mo}' and '{startdt_3mo}' then 1 Else 0 End) as num_open_mailer_3_6mo,
	Sum(case when contact_disposition in ('EO','EC') and cast(contact_dt as date) between '{startdt_30days}' and '{enddt}' then 1 Else 0 End) as num_ib_mailer_30days,
	Sum(case when contact_disposition in ('EO','EC') and cast(contact_dt as date) between '{startdt_3mo}' and '{startdt_1mo}' then 1 Else 0 End) as num_ib_mailer_1_3mo,
	Sum(case when contact_disposition in ('EO','EC') and cast(contact_dt as date) between '{startdt_6mo}' and '{enddt}' then 1 Else 0 End) as num_ib_mailer_3_6mo,
	cast('' as char(1)) as extra_null
from {ref2}.f_contact_history_analytic a
left join {ref2}.d_contact_history_ib_analytic b on a.d_contact_history_ib_key=b.d_contact_history_ib_key
left join {ref2}.d_campaign_analytic f on a.D_CAMPAIGN_KEY=f.D_CAMPAIGN_KEY
where transtype_key=8000 and b.comm_channel = 'E' and contact_direction='I'
group by mid_key, mailer_id
)"""
df_aa_9 = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", sql_query_contact_ibmailer).option("user", usern).option("password", passw).load()
df_contact_ibmailer = df_aa_9.join(df_geo_appends.select("mid_key"), on="mid_key", how="inner")
df_contact_ibmailer.write.format("delta").mode("overwrite").saveAsTable("intermed.contact_ibmailer")


df_contact_ibmailer = spark.table("intermed.contact_ibmailer")
df_contact_ibmailer = df_contact_ibmailer.withColumn("num_open_mailer_12mo", F.when(F.col("num_open_mailer_12mo") > 0, 0).otherwise(F.col("num_open_mailer_12mo")))
df_contact_ibmailer = df_contact_ibmailer.withColumn("num_open_mailer_3_6mo", F.when(F.col("num_open_mailer_3_6mo") > 0, 0).otherwise(F.col("num_open_mailer_3_6mo")))
df_contact_ibmailer = df_contact_ibmailer.withColumn("num_open_mailer_6mo", F.when(F.col("num_open_mailer_6mo") > 0, 0).otherwise(F.col("num_open_mailer_6mo")))
df_contact_ibmailer = df_contact_ibmailer.withColumn("num_open_mailer_30days", F.when(F.col("num_open_mailer_30days") > 0, 0).otherwise(F.col("num_open_mailer_30days")))
df_contact_ibmailer = df_contact_ibmailer.withColumn("num_open_mailer_1_3mo", F.when(F.col("num_open_mailer_1_3mo") > 0, 0).otherwise(F.col("num_open_mailer_1_3mo")))
df_contact_ibmailer.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("intermed.contact_ibmailer")

df_contact_ibmailer.createOrReplaceTempView("contact_ibmailer")

df_contact_click = spark.sql("""
    SELECT mid_key,
           Sum(num_click_mailer) as num_click,
           Sum(num_clicked_curr_mailer) as num_clicked_curmonth,
           Sum(num_clicked_past12_mailer) as num_clicked_past12,
           sum(num_clicked_1_3_mailer) as num_clicked_1_3mo,
           sum(num_clicked_3_6_mailer) as num_clicked_3_6mo
    FROM intermed.contact_ibmailer
    GROUP BY mid_key
""")
df_contact_click.write.format("delta").mode("overwrite").saveAsTable("intermed.contact_click")

df_mailer_click = spark.sql("""
    SELECT mid_key,
           sum(case when num_clicked_past12_mailer>0 then 1 else 0 end) as mailercount_click,
           sum(case when num_click_mailer>0 then 1 else 0 end) as mailercount_click_6mo,
           sum(case when num_clicked_curr_mailer>0 then 1 else 0 end) as mailercount_click_30days,
           sum(case when num_clicked_1_3_mailer>0 then 1 else 0 end) as mailercount_click_1_3mo,
           sum(case when num_clicked_3_6_mailer>0 then 1 else 0 end) as mailercount_click_3_6mo
    FROM intermed.contact_ibmailer
    GROUP BY mid_key
""")
df_mailer_click.write.format("delta").mode("overwrite").saveAsTable("intermed.mailer_click")

df_contact_open = spark.sql("""
    SELECT mid_key,
           sum(num_open_mailer_12mo) as num_open,
           Sum(num_open_mailer_6mo) as num_open_6mo,
           Sum(num_open_mailer_30days) as num_open_30days,
           Sum(num_ib_mailer_3mo) as num_ib_3mo,
           Sum(num_open_mailer_1_3mo) as num_open_1_3mo,
           Sum(num_open_mailer_3_6mo) as num_open_3_6mo,
           Sum(num_ib_mailer_30days) as num_ib_30days,
           Sum(num_ib_mailer_1_3mo) as num_ib_1_3mo,
           Sum(num_ib_mailer_3_6mo) as num_ib_3_6mo
    FROM intermed.contact_ibmailer
    GROUP BY mid_key
""")
df_contact_open.write.format("delta").mode("overwrite").saveAsTable("intermed.contact_open")

df_mailer_open = spark.sql("""
    SELECT mid_key,
           sum(case when num_open_mailer_12mo>0 then 1 else 0 end) as mailercount_open,
           sum(case when num_open_mailer_6mo>0 then 1 else 0 end) as mailercount_open_6mo,
           sum(case when num_open_mailer_30days>0 then 1 else 0 end) as mailercount_open_30days,
           sum(case when num_open_mailer_3_6mo>0 then 1 else 0 end) as mailercount_open_3_6mo,
           sum(case when num_open_mailer_1_3mo>0 then 1 else 0 end) as mailercount_open_1_3mo
    FROM intermed.contact_ibmailer
    GROUP BY mid_key
""")
df_mailer_open.write.format("delta").mode("overwrite").saveAsTable("intermed.mailer_open")

df_click_rate = df_contact_click.alias("a").join(df_contact_open.alias("b"), on="mid_key", how="inner") \
    .select(
        F.col("a.mid_key"),
        (F.col("a.num_click") / F.col("b.num_open")).alias("click_rate"),
        (F.col("a.num_click") / F.col("b.num_open_6mo")).alias("click_rate_6mo")
    )
df_click_rate.write.format("delta").mode("overwrite").saveAsTable("intermed.click_rate")

df_to_transpose_click = df_contact_ibmailer.select("mid_key", "mailer_id", "num_clicked_past12_mailer")
df_click_mailerid_transpose_past12 = df_to_transpose_click.groupBy("mid_key").pivot("mailer_id").agg(F.first("num_clicked_past12_mailer"))
df_click_mailerid_transpose_past12 = df_click_mailerid_transpose_past12.fillna(0)
df_click_mailerid_transpose_past12.write.format("delta").mode("overwrite").saveAsTable("intermed.click_mailerid_transpose_past12")

vlist_click_cols = [c for c in df_click_mailerid_transpose_past12.columns if c not in ['mid_key']]
vlist_click = ",".join(vlist_click_cols)
print(vlist_click)

df_to_transpose_sent = df_contact_obct_mailer.select("mid_key", "mailer_id", "num_sent_past12")
df_sent_mailerid_transpose_past12 = df_to_transpose_sent.groupBy("mid_key").pivot("mailer_id").agg(F.first("num_sent_past12"))
df_sent_mailerid_transpose_past12 = df_sent_mailerid_transpose_past12.fillna(0)
df_sent_mailerid_transpose_past12.write.format("delta").mode("overwrite").saveAsTable("intermed.sent_mailerid_transpose_past12")

vlist_sent_cols = [c for c in df_sent_mailerid_transpose_past12.columns if c not in ['mid_key']]
vlist_sent = ",".join(vlist_sent_cols)
print(vlist_sent)

vlist_sent_blank = " ".join(vlist_sent_cols)
print(vlist_sent_blank)
vlist_clicked_blank = vlist_sent_blank.replace("sent", "clickrate")
print(vlist_clicked_blank)

df_contact_history_sum_base = df_geo_appends.select("mid_key") \
    .join(df_contact_inbct, "mid_key", "left") \
    .join(df_contact_click, "mid_key", "left") \
    .join(df_mailer_click, "mid_key", "left") \
    .join(df_contact_open, "mid_key", "left") \
    .join(df_mailer_open, "mid_key", "left") \
    .select(
        F.col("mid_key"),
        F.col("num_inb"),
        F.coalesce(F.col("num_inb_30days"), F.lit(0)).alias("num_inb_30days"),
        F.coalesce(F.col("num_inb_3_6mo"), F.lit(0)).alias("num_inb_3_6mo"),
        F.coalesce(F.col("num_inb_1_3mo"), F.lit(0)).alias("num_inb_1_3mo"),
        F.col("num_click"),
        F.coalesce(F.col("num_clicked_curmonth"), F.lit(0)).alias("num_clicked_curmonth"),
        F.coalesce(F.col("num_clicked_past12"), F.lit(0)).alias("num_clicked_past12"),
        F.coalesce(F.col("num_clicked_1_3mo"), F.lit(0)).alias("num_clicked_1_3mo"),
        F.coalesce(F.col("num_clicked_3_6mo"), F.lit(0)).alias("num_clicked_3_6mo"),
        F.col("mailercount_click"), F.col("mailercount_click_6mo"), F.col("mailercount_click_30days"),
        F.col("mailercount_click_1_3mo"), F.col("mailercount_click_3_6mo"),
        F.col("num_open"), F.col("num_open_6mo"), F.col("num_open_30days"), F.col("num_ib_3mo"),
        F.coalesce(F.col("num_open_1_3mo"), F.lit(0)).alias("num_open_1_3mo"),
        F.coalesce(F.col("num_open_3_6mo"), F.lit(0)).alias("num_open_3_6mo"),
        F.coalesce(F.col("num_ib_30days"), F.lit(0)).alias("num_ib_30days"),
        F.coalesce(F.col("num_ib_1_3mo"), F.lit(0)).alias("num_ib_1_3mo"),
        F.coalesce(F.col("num_ib_3_6mo"), F.lit(0)).alias("num_ib_3_6mo"),
        F.col("mailercount_open"), F.col("mailercount_open_6mo"), F.col("mailercount_open_30days"),
        F.col("mailercount_open_3_6mo"), F.col("mailercount_open_1_3mo")
    )
df_contact_history_sum_base.write.format("delta").mode("overwrite").saveAsTable("intermed.contact_history_sum")

df_contact_history_sum = spark.table("intermed.contact_history_sum")
df_contact_history_sum_step2 = df_contact_history_sum \
    .join(df_click_rate, "mid_key", "left") \
    .join(df_click_mailerid_transpose_past12, "mid_key", "left") \
    .join(df_sent_mailerid_transpose_past12, "mid_key", "left") \
    .join(df_contact_obct, "mid_key", "left") \
    .join(df_contact_obct_aca, "mid_key", "left") \
    .withColumn("num_sent_curmonth", F.coalesce(F.col("num_sent_curmonth"), F.lit(0))) \
    .withColumn("care_ct", F.coalesce(F.col("care_ct"), F.lit(0)))
df_contact_history_sum_step2.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("intermed.contact_history_sum")

df_contact_history_sum = spark.table("intermed.contact_history_sum")
df_contact_history_sum_final = df_contact_history_sum \
    .join(df_contact_hist_2, "mid_key", "left") \
    .join(df_conact_hist_mail, "mid_key", "left") \
    .join(df_contact_obemail_mailers, "mid_key", "left") \
    .join(df_contact_hist_mail_ob, "mid_key", "left") \
    .join(df_contact_hist_phone_both, "mid_key", "left") \
    .withColumn("call_freq_12mo_both", F.coalesce(F.col("call_freq_12mo_both"), F.lit(0)))
df_contact_history_sum_final.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("intermed.contact_history_sum")

#End-DBShift