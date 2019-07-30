# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC --Number of violation per month in 2017?
# MAGIC 
# MAGIC select count(summons_number), month
# MAGIC from dm_nyu_parking.fact_nyu_parking_violation_info
# MAGIC where year='2017'
# MAGIC group by month;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Number of violations per time of day (when do most violations happen)?
# MAGIC 
# MAGIC select count(summons_number) as frequency, violation_time
# MAGIC from dm_nyu_parking.fact_nyu_parking_violation_info
# MAGIC group by violation_time
# MAGIC order by frequency desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Reason of violation per location?
# MAGIC 
# MAGIC select violation_description, violation_location
# MAGIC from dm_nyu_parking.fact_nyu_parking_violation_info a, dm_nyu_parking.dim_violation_location_info b
# MAGIC where a.location_id = b.location_id
# MAGIC group by violation_description, violation_location
# MAGIC Limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Reason of violation per location?
# MAGIC 
# MAGIC select violation_description, violation_location
# MAGIC from dm_nyu_parking.fact_nyu_parking_violation_info a, dm_nyu_parking.dim_violation_location_info b
# MAGIC where a.location_id = b.location_id
# MAGIC group by violation_description, violation_location
# MAGIC Limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Number violations issued per police officer or precinct?
# MAGIC 
# MAGIC select count(summons_number), issuer_precinct
# MAGIC from dm_nyu_parking.fact_nyu_parking_violation_info a, dm_nyu_parking.dim_issuer_info b
# MAGIC where a.issuer_id = b.issuer_id
# MAGIC group by issuer_precinct;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --SHOW COLUMNS IN parking_violations_issued___fiscal_year_2017_9a659_csv;-
# MAGIC --select * from dim_issuer_info Limit 1;
# MAGIC 
# MAGIC --Most common reasons of all violation (Top-k)
# MAGIC 
# MAGIC select count(*) as frequency, violation_description
# MAGIC from dm_nyu_parking.fact_nyu_parking_violation_info
# MAGIC group by violation_description
# MAGIC order by frequency desc
# MAGIC Limit 10;
