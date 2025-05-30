DROP SCHEMA IF EXISTS db_etl_practice_ext
DROP EXTERNAL DATABASE;

CREATE EXTERNAL SCHEMA db_etl_practice_ext
FROM DATA CATALOG 
DATABASE 'etl-practice-catalog-05262025' 
IAM_ROLE 'arn:aws:iam::827763788543:role/service-role/AmazonRedshift-CommandsAccessRole-20250527T164155'
CREATE EXTERNAL DATABASE IF NOT EXISTS;