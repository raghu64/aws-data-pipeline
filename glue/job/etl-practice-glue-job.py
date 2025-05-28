'''
parameters
--bucket <>
--ip_path <>
--op_path <>
--catalog_database <>
'''

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, col, lower, upper, initcap

class ETLJOB:
    def __init__(self):
        params = ['bucket', 'ip_path', 'op_path', 'catalog_database']
        args = getResolvedOptions(sys.argv, params)

        sc = SparkContext()
        self.glueContext = GlueContext(sc)
        self.spark = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        
        self.bucket = args['bucket']
        self.ip_path = args['ip_path']
        self.op_path = args['op_path']
        self.catalog_database = args['catalog_database']
        
        
    """
    Read raw data from S3 as DataFrame and return it.
    """
    def read_raw_data(self, file: str, format: str = 'csv') -> DataFrame:
       return self.spark.read.format(format) \
                    .option('header', 'true') \
                    .option('inferSchema', 'true') \
                    .load(f's3://{self.bucket}/{self.ip_path}/{file}')
    
    """
    Cleanse, Transform, and Standardize Customers data.
    """
    def process_customers(self, df: DataFrame) -> DataFrame:
        return df.dropna(subset=['email']) \
                .withColumn('email', lower(col('email'))) \
                .withColumn('country', upper(col('country'))) \
                .withColumn('load_date', current_date())
    
    """
    Cleanse, Transform, and Standardize Products data.
    """
    def process_products(self, df: DataFrame) -> DataFrame:
        return df.dropna(subset=['price', 'supplier_id']) \
                .withColumn('category', initcap(col('category'))) \
                .withColumn('subcategory', initcap(col('subcategory'))) \
                .withColumn('load_date', current_date())
    
    """
    Standardise Transaction data.
    """
    def process_transactions(self, df: DataFrame) -> DataFrame:
        return df.dropna(subset=['quantity','unit_price']) \
                .withColumn('load_date', current_date())
    
    """
    Write the DataFrame into S3
    """
    def write_to_s3(self, df: DataFrame, table: str, format: str = 'csv'):
        path = f's3://{self.bucket}/{self.op_path}/{table}'
        
        ## To save data into S3 an also 
        final_dyf = DynamicFrame.fromDF(df, self.glueContext, 'final_dyf')
        
        ## save data in s3
        s3sink = self.glueContext.getSink(
            path=path,
            connection_type='s3',
            updateBehavior='UPDATE_IN_DATABASE',
            partitionKeys=[],
            enableUpdateCatalog=True
        )
        
        # create/update glue catalog for the new files 
        s3sink.setCatalogInfo(
            catalogDatabase=self.catalog_database,
            catalogTableName=table
        )
        
        s3sink.setFormat('csv')
        s3sink.writeFrame(final_dyf)
    
    def run(self):
        # read raw data from s3
        customers_df = self.read_raw_data('customers.csv', 'csv')
        products_df = self.read_raw_data('products.csv', 'csv')
        transactions_df = self.read_raw_data('transactions.csv', 'csv')
        
        
        # process the data
        processed_customers = self.process_customers(customers_df)
        processed_products = self.process_products(products_df)
        processed_transactions = self.process_transactions(transactions_df)
        
        
        # write processed data back into s3
        self.write_to_s3(processed_customers, 'customers')
        self.write_to_s3(processed_products, 'products')
        self.write_to_s3(processed_transactions, 'transactions')
        
        self.job.commit()
        
if __name__ == "__main__":
    etl = ETLJOB()
    etl.run()
