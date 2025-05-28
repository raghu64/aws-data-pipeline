# aws-data-pipeline
AWS End to End ETL pipeline using S3, AWS Glue, EMR, Redshift, AWS Managed Apache Airflow.

### Architecture Diagram

![Architecture Diagram](./docs/Architecture%20diagram.svg)


Workflow:
1. Use Glue Crawler to pull the schema of raw data from the S3 bucket's folder.
2. Create Glue ETL script (job) to read the raw data from S3, Transform and the save it back to S3 path. This will also create/update Data catalog in Glue when the data trasnformed files are written to S3. This job needs the following parameters:
    * bucket: S3 bucket name
    * ip_path: input files path within the S3 bucket
    * op_path: output files path where the transformed files need to be sent to within the S3 bucket.
    * catalog_database: name of the database where the output files catables needs to be created/updated
3. Create EMR Cluster on EC2
    * custom bundle: Spark, Hadoop
    * enable use for spark table metadata
    * remove task instance group
    * EC2 security groups (firewall): Additional security groups - default (for both primary node, and core and task nodes)
    * create key pair for SSH to the cluster
    * create new EMR service role
    * create new EC2 instance profile for EMR, to specific S3 bucket and if needed add additional policies to this role.
4. SSH into EMR Cluster to run the script. This will read the data from the transformed S3 path. Then perform calculation to create new files with aggregated analytical data and save it back in S3. This job needs the following paramerters:
    * bucket: S3 bucket name
    * ip_path: input files path within the S3 bucket
    * op_path: output files path where the aggregated data files need to be sent to within the S3 bucket.
5. Create RedShift Cluster. (RedShift -> Provisioned cluester dashboard)
6. Create Managed Apache Airflow (MWAA)
    1. Before creating Airflow environment, check and create 2 private subnets in the VPC.
    2. Create NAT Gateway
    3. create route table, and then add route for all the network (0.0.0.0/0) to NAT gateway that we created above
    4. create s3 bucket to store DAG files. recommended to have the versioning enabled, and block all public access to the bucket
    5. After creating the environment,
        * add policies to role to give access to EMR, S3, Glue, Redshift
        * add MWAA security group to redshift's security groups inboud rules on port 5439
        * create new EMR cluster to run the job

### Data Flow
![Data Flow](./docs/Data%20Flow.png)