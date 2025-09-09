import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging

#Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OlistDataLoader: 
    def __init__(self): 
        self.spark = (
            SparkSession.builder
                .appName("OlistDataLoader")
                .master("spark://spark-master:7077")
                .config("spark.jars", "/usr/local/spark/jars/postgresql-42.3.1.jar")
                .getOrCreate()
)
        # self.spark = SparkSession.builder \
            # .appName("OlistDataLoader") \
            # .master("spark://spark-master:7077") \
            # .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.3.1.jar") \
            # .config("spark.sql.adaptive.enabled", "true") \
            # .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            # .getOrCreate()
            
        # Database connection properties
        self.db_properties = {
            "user": "ecommerce_user",
            "password": "ecommerce_pass",
            "driver": "org.postgresql.Driver"
        }
        self.db_url = "jdbc:postgresql://postgres:5432/ecommerce_db"

    def load_csv_to_spark(self, file_path, schema=None):
        """Load CSV file to Spark DataFrame with optional schema"""
        try:
            if schema:
                df = self.spark.read.option("header", True).schema(schema).csv(file_path)
            else:
                df = self.spark.read.option("header", True).option("inferSchema", True).csv(file_path)
            
            logger.info(f"Loaded {df.count():,} rows from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Error loading {file_path}: {str(e)}")
            return None  

    def write_to_postgres(self, df, table_name, schema="raw_data", mode="overwrite"):
        """Write DataFrame to PostgreSQL"""
        logger.info(f"Attempting to write {df.count():,} rows to {schema}.{table_name}...")
        try:
            df.write \
                .format("jdbc") \
                .option("url", self.db_url) \
                .option("dbtable", f"{schema}.{table_name}") \
                .options(**self.db_properties) \
                .mode(mode) \
                .save()
            
            logger.info(f"Written {df.count():,} rows to {schema}.{table_name}")
        except Exception as e:
            logger.error(f"Error writing to {schema}.{table_name}: {str(e)}")

    def load_olist_dataset(self):
        """Load all Olist dataset files"""
        data_path = "/opt/spark-data/raw"
        # Check if data directory exists
        if not os.path.exists(data_path):
            logger.error(f"Data directory not found: {data_path}")
            return {}

        # Define file mappings
        files_to_load = {
            "customers": f"{data_path}/olist_customers_dataset.csv",
            "orders": f"{data_path}/olist_orders_dataset.csv", 
            "order_items": f"{data_path}/olist_order_items_dataset.csv",
            "products": f"{data_path}/olist_products_dataset.csv"
        }
        
        loaded_dfs = {}
        
        for table_name, file_path in files_to_load.items():
            if os.path.exists(file_path):
                logger.info(f"Loading {table_name} from {file_path}")
                df = self.load_csv_to_spark(file_path)
                
                if df is not None:
                    loaded_dfs[table_name] = df
                    self.write_to_postgres(df, table_name)
                else:
                    logger.warning(f"Failed to load {table_name}")
            else:
                logger.warning(f"File not found: {file_path}")
        
        return loaded_dfs
    

    def validate_data_quality(self, dfs):
        """Basic data quality validation"""
        logger.info("Running data quality checks...")
        
        for table_name, df in dfs.items():
            null_counts = {}
            for col in df.columns:
                null_count = df.filter(df[col].isNull()).count()
                if null_count > 0:
                    null_counts[col] = null_count
            
            if null_counts:
                logger.warning(f"{table_name} has null values: {null_counts}")
            else:
                logger.info(f"{table_name} has no null values")
    
    def close(self):
        """Close Spark session"""
        self.spark.stop()


def main():
    """Main execution function"""  
    logger.info("Starting Olist Data Loading Process...")
    
    loader = OlistDataLoader()
    
    try:
        # Load all datasets
        dfs = loader.load_olist_dataset()
        
        # Validate data quality
        if dfs:
            loader.validate_data_quality(dfs)
            logger.info("Data loading completed successfully!")
        else:
            logger.error("No data was loaded")
    
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    
    finally:
        loader.close()

if __name__ == "__main__":
    main()