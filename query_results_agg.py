# Databricks notebook source
%sql

/***
year:integer
month:integer
day:integer
violation_time:string
location_id:long
issuer_id:long
vehicle_registration_plate_id:long
violation_description:string
count_summons_number:long
count_no_stand_number:long
count_hydrant_violation_number:long
count_double_parking_violation_number:long
max_days_parking_in_effect_number:string
min_days_parking_in_effect_number:string
max_feet_from_curb_number:integer
min_feet_from_curb_number:integer
***/

-Number of violation per month in 2017

select count(count_summons_number), month
from dm_nyu_parking.agg_nyu_parking_violation_info
where year='2017'
group by month;

# COMMAND ----------

%sql

-Number of violations per time of day (when do most violations happen)

select count(count_summons_number) as frequency, violation_time
from dm_nyu_parking.agg_nyu_parking_violation_info
group by violation_time
order by frequency desc;

# COMMAND ----------

%sql

-Reason of violation per location

select violation_description, violation_location
from dm_nyu_parking.agg_nyu_parking_violation_info a, dm_nyu_parking.dim_violation_location_info b
where a.location_id = b.location_id
group by violation_description, violation_location
Limit 10;

# COMMAND ----------

%sql

-Reason of violation per location

select violation_description, violation_location
from dm_nyu_parking.agg_nyu_parking_violation_info a, dm_nyu_parking.dim_violation_location_info b
where a.location_id = b.location_id
group by violation_description, violation_location
Limit 10;

# COMMAND ----------

%sql

-Number violations issued per police officer or precinct

select count(count_summons_number), issuer_precinct
from dm_nyu_parking.agg_nyu_parking_violation_info a, dm_nyu_parking.dim_issuer_info b
where a.issuer_id = b.issuer_id
group by issuer_precinct;

# COMMAND ----------

%sql

-SHOW COLUMNS IN parking_violations_issued___fiscal_year_2017_9a659_csv;-
-select * from dim_issuer_info Limit 1;

-Most common reasons of all violation (Top-k)

select count(*) as frequency, violation_description
from dm_nyu_parking.agg_nyu_parking_violation_info
group by violation_description
order by frequency desc
Limit 10;
