from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, sum, avg, min, max, col, countDistinct
import sys
import boto3
import time
from botocore.exceptions import ClientError

'''
parameters:
    bucket: S3 bucket name
    ip_path: S3 input folder path
    out_path: S3 output folder path
    
script usage:
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory \
  etl-practice-emr-job.py \
  <S3 bucket> \
  <S3 ip_path> \
  <S3 op_path> 

Eg: spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory \
  etl-practice-emr-job.py \
  etl-practice-s3-05262025 \
  transformed \
  analytics
'''

class DataProcessor:

    def __init__(self):
        self.spark = SparkSession.builder \
                        .appName('DataProcessor') \
                        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
                        .config('spark.sql.hive.convertMetastoreParquet', 'false') \
                        .config('hive.metastore.client.factory.class', 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory') \
                        .enableHiveSupport() \
                        .getOrCreate()
        
        self.bucket = sys.argv[1]
        self.ip_path = sys.argv[2]
        self.op_path = sys.argv[3]
        # self.catalog_database = sys.argv[4]
        # self.crawler_name=sys.argv[5]
        # self.crawler_role = sys.argv[6]
        self.glue = boto3.client('glue', region_name='us-east-1')

    """
    Read data from S3
    """
    def read_data(self, table: str) -> DataFrame:
        return self.spark.read.parquet(f's3a://{self.bucket}/{self.ip_path}/{table}/*')
    

    """
    Calculate the custer metrics based on the transactions
    """
    def calculate_customer_metrics(self) -> DataFrame:
        transactions = self.read_data('transactions')

        return transactions.groupBy('customer_id').agg(
                count('transaction_id').alias('total_transactions'),
                sum(col('quantity') * col('unit_price')).alias('total_spent'),
                avg(col('quantity') * col('unit_price')).alias('avg_transaction_amount'),
                min('transaction_date').alias('first_purchase'),
                max('transaction_date').alias('last_purchase')
            )
    

    """
    Calculate product metrics based on the transaction and product category, and subcategory
    """
    def calculate_product_metrics(self) -> DataFrame:
        transactions = self.read_data('transactions')
        products = self.read_data('products')

        return transactions.join(products, 'product_id', 'inner') \
                .groupBy('category', 'subcategory') \
                .agg(
                    sum('quantity').alias('total_units_sold'),
                    sum(col('quantity') * col('unit_price')).alias('total_revenue'),
                    avg('unit_price').alias('avg_price'),
                    countDistinct('customer_id').alias('unique_customers')
                )

    """
    XXX Not Working
    Run creawler to create/update the data catalog in Glue.
    If the crawler doesn't exists then create one and then run it.
    """
    # def run_crawler(self, wait_for_completion: bool = True):
    #     try:
    #         self.glue.get_crawler(Name=self.crawler_name)
    #         print(f"Crawler '{self.crawler_name}' already exists.")
    #     except self.glue.exceptions.EntityNotFoundException:
    #         print(f"Crawler '{self.crawler_name}' not found. Creating...")
    #         self.glue.create_crawler(
    #             Name=self.crawler_name,
    #             Role=self.crawler_role,
    #             DatabaseName=self.catalog_database,
    #             TablePrefix="",
    #             Targets={
    #                 "S3Targets": [{
    #                     "Path": f's3://{self.bucket}/{self.op_path}/'
    #                 }]
    #             },
    #             SchemaChangePolicy={
    #                 'UpdateBehavior': 'UPDATE_IN_DATABASE',
    #                 'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
    #             }
    #         )
    #         print(f"Crawler '{self.crawler_name}' created.")

    #         self.glue.start_crawler(Name=self.crawler_name)

    #         if wait_for_completion:
    #             self.wait_for_crawler_to_complete()

    """
    Poll for the crawler status 
    """    
    # def wait_for_crawler_to_complete(self, poll_interval: int = 10):
    #     print(f"Waiting for crawler '{self.crawler_name}' to complete...")
    #     while True:
    #         response = self.glue.get_crawler(Name=self.crawler_name)
    #         state = response['Crawler']['State']

    #         if state == 'READY':
    #             print(f"Crawler '{self.crawler_name}' completed.")
    #             break
    #         else:
    #             print(f"Crawler state: {state}")
    #             time.sleep(poll_interval)

    """
    Write the final metrics data to S3
    """
    def write_to_s3(self, df: DataFrame, table: str, format: str = 'parquet'):
        path = f's3://{self.bucket}/{self.op_path}/{table}'
        df.write.format(format).mode('overwrite').save(path)
    

    def run(self):
        customer_metrics_df = self.calculate_customer_metrics()
        product_metrics_df = self.calculate_product_metrics()

        self.write_to_s3(customer_metrics_df, 'customer_metrics')
        self.write_to_s3(product_metrics_df, 'product_metrics')

        # self.run_crawler()

if __name__=="__main__":
    processor = DataProcessor()
    processor.run()