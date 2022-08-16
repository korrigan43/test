-------------------------------------------------------
-- CHECK THAT NO TASK ARE RUNNING & SUSPEND TASKS    --
-------------------------------------------------------
use role SSO_SNOWFLAKE__BI_CORPO__DEV;
use warehouse SSO_SNOWFLAKE__BI_CORPO__DEV;

select *
  from table(information_schema.task_history())
  where NAME LIKE 'PULSAR%' AND DATABASE_NAME NOT LIKE '%UAT'
  order by scheduled_time;

USE ROLE SERVICE_BI_CORPO_S3PULSAR_EXTRACT_PROD;
ALTER TASK DATASTORE_DISTRIBUTION_CENTERS.RAW.PULSAR_DISTRIBUTION_CENTERS_INBOUND_FLOW_PACKAGE_LABEL_STATUS_TASK SUSPEND;
ALTER TASK DATASTORE_DISTRIBUTION_CENTERS.RAW.PULSAR_DISTRIBUTION_CENTERS_INBOUND_FLOW_PACKAGE_LABELS_TASK SUSPEND;
ALTER TASK DATASTORE_TRANSPORTATION.RAW.PULSAR_TRANSPORTATION_PLAN_ROUTE_OPTIMIZATIONS_TASK SUSPEND;

-------------------------------------------------------
-- CREATE NEW STORED PROC AND TASKS IN UTILITIES_DB  --
-------------------------------------------------------
USE ROLE SERVICE_BI_CORPO_S3PULSAR_EXTRACT_PROD;

-- Execute the 4 scripts (PROD Portion) located in Code Repo "BI_Snowflake\UTILITIES_DB\TASKS"

--Wait for at least 20 minutes for the next schedule to be triggered and check execution
select *
  from table(information_schema.task_history())
  where NAME LIKE 'PULSAR%' AND DATABASE_NAME = 'UTILITIES_DB'
  order by scheduled_time;

-------------------------------------------------------
-- DELETE OLD STORED PROCS and TASKS                 --
-------------------------------------------------------

----- DATASTORE_TRANSPORTATION ----- 
use role SERVICE_BI_CORPO_S3PULSAR_EXTRACT_PROD;
use database DATASTORE_TRANSPORTATION;
use schema RAW;

show tasks;
drop task PULSAR_TRANSPORTATION_PLAN_ROUTE_OPTIMIZATIONS_TASK;
show tasks;

show procedures;
drop procedure LOAD_S3_STAGE_PARTITIONED_MINUTE_TO_TABLE_P(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR);
show procedures;


----- DATASTORE_DISTRIBUTION_CENTERS ----- 
use role SERVICE_BI_CORPO_S3PULSAR_EXTRACT_PROD;
use database DATASTORE_DISTRIBUTION_CENTERS;
use schema RAW;

show tasks;
drop task PULSAR_DISTRIBUTION_CENTERS_INBOUND_FLOW_PACKAGE_LABELS_TASK;
drop task PULSAR_DISTRIBUTION_CENTERS_INBOUND_FLOW_PACKAGE_LABEL_STATUS_TASK;
show tasks;

show procedures;
drop procedure LOAD_S3_STAGE_PARTITIONED_MINUTE_TO_TABLE_P(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR);
show procedures;
