from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, sum, avg, min, max, col, countDistinct
import sys

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


if __name__=="__main__":
    processor = DataProcessor()
    processor.run()