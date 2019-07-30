## Data Mart - NYC Parking Violation 

### Design

#### Fact Table is parking_violation_ticket_info :

##### Fact table is partitioned based on state (NYC) and year/month/day

##### Aggregate Columns:

* Total violations in day
* Total no stand/stop violations in day
* Total hydrant violations in day
* Total double parking violations in day
* Max parking days
* Min parking time
* Max feet from curb
* Min feet from curb


#### IMPLEMENTED ER DIAGRAM (STAR SCHEMA)

		- ![ER image](https://github.com/abuchireddygari/data-mart-nyc-parking-violation/blob/master/er1.png)


#### Scenarios:
* Find if the plate used is valid, what if fake plate is used?
* Find number of hydrants in the area, are they within the city limits?
* Accuracy of NTA device, do you need to replace the device?
* What’s causing more parking violations, is it because there’s no public parking nearby?
* Cause for more parking violations in the area, is it because new ice cream/ retail shop opened or airport nearby or event happening in the area?
* Can you make more money if you open a commercial parking garage in the area?
* Are the violations valid?
* Which agency issued violations more in a year?


#### Batch Processing:
	The job runs daily to aggregate the data for the aggregation columns specified above

#### ETL :
	
##### Fact and Dimension tables ETL job:


Created dimensions and facts and stored as parquet. Fact is stored based on partition column(year/month/day).

Due to limitations in data storage, only 2017  issue date year is processed 

##### Database:
	dm_nyu_parking

##### Dimension Tables and Columns:
	
* dim_vehicles_registration_info:
* vehicle_registration_plate_id:long
* plate_id:string
* registration_state:string
* unregistered_vehicle:integer
* Plate_type:string

##### dim_issuer_info:
* issuer_id:long
* issuer_precinct:integer
* issuer_code:integer
* issuer_command:string
* Issuer_squad:string
* issuing_agency:string


##### dim_vehicles_info:
* vehicle_id:long
* vehicle_body_type:string
* vehicle_make:string
* vehicle_year:integer
* Vehicle_color:string
* vehicle_expiration_date:integer


##### dim_violation_location_info:
* location_id:long
* violation_location:integer
* violation_county:string
* violation_in_front_of_or_opposite:string
* house_number:string
* street_name:string
* intersecting_street:string
* street_code1:integer
* street_code2:integer
* Street_code3:integer


##### dim_legal_info:
* legal_id:long
* law_section:integer
* sub_division:string
* Violation_legal_code:string


##### Fact table columns:

##### Fact_nyu_parking_violation_info:
* parking_violation_id:bigint
* summons_number:long
* issue_date:date
* violation_code:integer
* violation_precinct:integer
* violation_time:string
* time_first_observed:string
* date_first_observed:integer
* days_parking_in_effect:string
* from_hours_in_effect:string
* to_hours_in_effect:string
* unregistered_vehicle:integer
* meter_number:string
* feet_from_curb:integer
* violation_post_code:string
* violation_description:string
* no_standing_or_stopping_violation:string
* hydrant_violation:string
* double_parking_violation:string
* vehicle_registration_plate_id:long
* issuer_id:long
* location_id:long
* vehicle_id:long
* legal_id:long
* year:integer
* month:integer
* day:integer



I’ve used a counter for dimension id but you can also use hashkey but counter, which is integer is faster than hashkey, disadvantage of counter, in my logic, is it’s not continuous increment.  

#### Code:
  https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5041140880827814/4014305827219043/2252038084290521/latest.html


#### Agg ETL job:

* agg_nyu_parking_violation_info:
* year:integer
* month:integer
* day:integer
* violation_time:string
* location_id:long
* issuer_id:long
* vehicle_registration_plate_id:long
* violation_description:string
* count_summons_number:long
* count_no_stand_number:long
* count_hydrant_violation_number:long
* count_double_parking_violation_number:long
* max_days_parking_in_effect_number:string
* min_days_parking_in_effect_number:string
* max_feet_from_curb_number:integer
* Min_feet_from_curb_number:integer
	

##### Aggregrate columns:
    
    * Total violations in day
    * Total no stand/stop violations in day
    * Total hydrant violations in day
    * Total double parking violations in day
    * Max parking days
    * Min parking time
    * Max feet from curb
    * Min feet from curb


##### Code:

https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5041140880827814/3333566614432119/2252038084290521/latest.html

IDEAL ER DIAGRAM (SNOWFLAKE SCHEMA)
	- ![ER2 image](https://github.com/abuchireddygari/data-mart-nyc-parking-violation/blob/master/er2.png)



#### Query Results

##### With Fact 

https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5041140880827814/4014305827219070/2252038084290521/latest.html


##### With Agg

https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5041140880827814/2634234997705643/2252038084290521/latest.html




