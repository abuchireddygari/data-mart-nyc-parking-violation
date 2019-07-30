# Databricks notebook source
import re

from datetime import date, datetime, timedelta
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

now = datetime.now()

if __name__ == "__main__":

    startTime = datetime.now()
    spark = SparkSession \
                    .builder \
                    .appName("agg_nyc_parking_violation_info_job") \
                    .config("spark.memory.useLegacyMode", "false") \
                    .config("spark.default.parallelism", "5") \
                    .config("spark.sql.shuffle.partitions", "5") \
                    .config("spark.shuffle.consolidateFiles", "true") \
                    .config("spark.port.maxRetries", "5") \
                    .config("spark.rdd.compress", "true") \
                    .config("hive.exec.dynamic.partition", "true") \
                    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
                    .config("spark.shuffle.service.enabled", "true") \
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                    .config("spark.speculation", "true") \
                    .config("spark.sql.orc.filterPushdown", "true") \
                    .config("spark.sql.caseSensitive", "true") \
                    .config("spark.sql.hive.metastorePartitionPruning", "true") \
                    .config("spark.sql.broadcastTimeout",  "600") \
                    .config("spark.network.timeout","600") \
                    .config("spark.speculation","true") \
                    .config("spark.speculation.interval","1s") \
                    .config("spark.speculation.quantile","0.75") \
                    .config("spark.speculation.multiplier","1.5") \
                    .enableHiveSupport() \
                    .getOrCreate()
    
    df_nyu_parking_fact = spark.sql('select * from dm_nyu_parking.fact_nyu_parking_violation_info')
    
    '''
    Aggregrate columns
    
    Total violations in day
    Total no stand/stop violations in day
    Total hydrant violations in day
    Total double parking violations in day
    Max parking days
    Min parking time
    Max feet from curb
    Min feet from curb
    '''
    
    df_nyu_parking_agg = df_nyu_parking_fact \
                                      .groupBy("year", 
                                               "month",
                                               "day",
                                               "violation_time",
                                               "location_id",
                                               "issuer_id",
                                               "vehicle_registration_plate_id",
                                               "violation_description") \
                                      .agg(F.count("summons_number").alias("count_summons_number"), 
                                            F.count("no_standing_or_stopping_violation").alias("count_no_stand_number"),
                                            F.count("hydrant_violation").alias("count_hydrant_violation_number"),
                                            F.count("double_parking_violation").alias("count_double_parking_violation_number"),
                                            F.max("days_parking_in_effect").alias("max_days_parking_in_effect_number"),
                                            F.min("days_parking_in_effect").alias("min_days_parking_in_effect_number"),
                                            F.max("feet_from_curb").alias("max_feet_from_curb_number"),
                                            F.min("feet_from_curb").alias("min_feet_from_curb_number"))
      
    df_nyu_parking_agg.write.format("parquet").partitionBy('year','month','day').mode("overwrite").insertInto("dm_nyu_parking.agg_nyu_parking_violation_info")  
    
    
    ################################################################################ END OF SCRIPT/ NOTES BELOW ######################################################################
    
    '''
    summons_number|plate_id|registration_state|plate_type|issue_date|violation_code|vehicle_body_type
    |vehicle_make|issuing_agency|street_code1|street_code2|street_code3|vehicle_expiration_date|violation_location
    |violation_precinct|issuer_precinct|issuer_code|issuer_command|issuer_squad|violation_time|time_first_observed
    |violation_county|violation_in_front_of_or_opposite|house_number|street_name|intersecting_street|date_first_observed|law_section|sub_division
    |violation_legal_code|days_parking_in_effect
    |from_hours_in_effect|to_hours_in_effect|vehicle_color|unregistered_vehicle|vehicle_year
    |meter_number|feet_from_curb|violation_post_code|violation_description|no_standing_or_stopping_violation|hydrant_violation|double_parking_violation|
    '''
    
    
