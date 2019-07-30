# Databricks notebook source
#%fs rm -r /user/hive/warehouse/df_nyu_parking_violation_info
#%fs rm -r /user/hive/warehouse/fact_nyu_parking_violation_info

#dbutils.fs.rm("/user/hive/warehouse/dm_nyu_parking.db/dim_violation_location_info",recurse=True)
#dbutils.fs.rm("/df_nyu_parking_violation_info",recurse=True)


#dbutils.fs.rm("/user/hive/warehouse/dm_nyu_parking.db/fact_nyu_parking_violation_info",recurse=True)
#dbutils.fs.rm("/user/hive/warehouse/dm_nyu_parking.db/agg_nyu_parking_violation_info",recurse=True)



# COMMAND ----------

# MAGIC %sql 
# MAGIC --select * from fact_nyu_parking_violation_info Limit 1;

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC --create database dm_nyu_parking;
# MAGIC --drop table default.fact_nyu_parking_violation_info;
# MAGIC 
# MAGIC --drop table dim_issuer_info;
# MAGIC --drop table dim_vehicles_info;
# MAGIC --drop table dim_vehicle_registration_info;
# MAGIC --drop table dm_nyu_parking.dim_violation_location_info;
# MAGIC --drop table dim_legal_info;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /***
# MAGIC CREATE TABLE dm_nyu_parking.fact_nyu_parking_violation_info
# MAGIC (
# MAGIC summons_number long,
# MAGIC issue_date string,
# MAGIC violation_code integer,
# MAGIC issuing_agency string,
# MAGIC vehicle_expiration_date integer,
# MAGIC violation_precinct integer,
# MAGIC violation_time string,
# MAGIC time_first_observed string,
# MAGIC date_first_observed integer,
# MAGIC days_parking_in_effect string,
# MAGIC from_hours_in_effect string,
# MAGIC to_hours_in_effect string,
# MAGIC unregistered_vehicle integer,
# MAGIC meter_number string,
# MAGIC feet_from_curb integer,
# MAGIC violation_post_code string,
# MAGIC violation_description string,
# MAGIC no_standing_or_stopping_violation string,
# MAGIC hydrant_violation string,
# MAGIC double_parking_violation string,
# MAGIC vehicle_registration_plate_id long,
# MAGIC issuer_id long,
# MAGIC location_id long,
# MAGIC vehicle_id long,
# MAGIC legal_id long,
# MAGIC year integer,
# MAGIC month integer,
# MAGIC day integer
# MAGIC )
# MAGIC stored as PARQUET
# MAGIC LOCATION '/user/hive/warehouse/dm_nyu_parking.db/fact_nyu_parking_violation_info';
# MAGIC ***/
