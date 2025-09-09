from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EcommerceETLPipeline:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("EcommerceETL") \
            .master("spark://spark-master:7077") \
            .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.3.1.jar") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
            .getOrCreate()
        
        self.db_properties = {
            "user": "ecommerce_user",
            "password": "ecommerce_pass", 
            "driver": "org.postgresql.Driver"
        }
        self.db_url = "jdbc:postgresql://postgres:5432/ecommerce_db"
    
    def read_from_postgres(self, table_name, schema="raw_data"):
        """Read data from PostgreSQL"""
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.db_url) \
            .option("dbtable", f"{schema}.{table_name}") \
            .options(**self.db_properties) \
            .load()
    
    def write_to_postgres(self, df, table_name, schema="staging", mode="overwrite"):
        """Write DataFrame to PostgreSQL"""
        df.write \
            .format("jdbc") \
            .option("url", self.db_url) \
            .option("dbtable", f"{schema}.{table_name}") \
            .options(**self.db_properties) \
            .mode(mode) \
            .save()
    
    def process_customer_metrics(self):
        """Calculate comprehensive customer metrics"""
        logger.info("Processing customer metrics...")
        
        # Read data
        customers_df = self.read_from_postgres("customers")
        orders_df = self.read_from_postgres("orders")
        order_items_df = self.read_from_postgres("order_items")
        products_df = self.read_from_postgres("products")
        
        # Filter delivered orders only
        delivered_orders = orders_df.filter(col("order_status") == "delivered")
        
        # Join orders with items and products
        order_details = delivered_orders \
            .join(order_items_df, "order_id") \
            .join(products_df, "product_id")
        
        # Calculate order totals
        order_totals = order_details.groupBy("order_id", "customer_id") \
            .agg(
                sum(col("price") + col("freight_value")).alias("order_total"),
                first("order_purchase_timestamp").alias("order_date")
            )
        
        # Calculate customer metrics
        customer_metrics = order_totals.groupBy("customer_id") \
            .agg(
                count("order_id").alias("total_orders"),
                sum("order_total").alias("total_spent"),
                avg("order_total").alias("avg_order_value"),
                min("order_date").alias("first_order_date"),
                max("order_date").alias("last_order_date")
            )
        
        # Calculate customer lifetime in days
        customer_metrics = customer_metrics.withColumn(
            "customer_lifetime_days",
            datediff(col("last_order_date"), col("first_order_date"))
        )
        
        # Find favorite category per customer
        customer_categories = order_details.groupBy("customer_id", "product_category_name") \
            .agg(count("*").alias("category_orders")) \
            .withColumn(
                "rn", 
                row_number().over(
                    Window.partitionBy("customer_id")
                    .orderBy(desc("category_orders"))
                )
            ) \
            .filter(col("rn") == 1) \
            .select("customer_id", col("product_category_name").alias("favorite_category"))
        
        # Join with favorite category
        final_metrics = customer_metrics.join(customer_categories, "customer_id", "left")
        
        # Write to staging
        self.write_to_postgres(final_metrics, "customer_metrics", "staging")
        
        logger.info(f"Processed {final_metrics.count():,} customer metrics")
        return final_metrics
    
    def process_daily_sales(self):
        """Calculate daily sales analytics"""
        logger.info("Processing daily sales...")
        
        orders_df = self.read_from_postgres("orders")
        order_items_df = self.read_from_postgres("order_items")
        
        # Filter delivered orders
        delivered_orders = orders_df.filter(col("order_status") == "delivered")
        
        # Join with order items
        order_details = delivered_orders.join(order_items_df, "order_id")
        
        # Calculate daily metrics
        daily_sales = order_details \
            .withColumn("sale_date", to_date(col("order_purchase_timestamp"))) \
            .groupBy("sale_date") \
            .agg(
                count("order_id").alias("total_orders"),
                sum(col("price") + col("freight_value")).alias("total_revenue"),
                countDistinct("customer_id").alias("unique_customers"),
                avg(col("price") + col("freight_value")).alias("avg_order_value")
            ) \
            .orderBy("sale_date")
        
        # Write to analytics schema
        self.write_to_postgres(daily_sales, "daily_sales", "analytics")
        
        logger.info(f"Processed {daily_sales.count():,} days of sales data")
        return daily_sales
    
    def process_product_performance(self):
        """Calculate product performance metrics"""
        logger.info("Processing product performance...")
        
        products_df = self.read_from_postgres("products")
        order_items_df = self.read_from_postgres("order_items")
        orders_df = self.read_from_postgres("orders")
        
        # Filter delivered orders
        delivered_orders = orders_df.filter(col("order_status") == "delivered")
        
        # Join order items with delivered orders
        valid_items = order_items_df.join(delivered_orders.select("order_id"), "order_id")
        
        # Calculate product metrics
        product_performance = valid_items.groupBy("product_id") \
            .agg(
                sum("order_item_id").alias("total_quantity_sold"),
                sum("price").alias("total_revenue"),
                avg("price").alias("avg_price"),
                countDistinct("order_id").alias("total_orders")
            )
        
        # Join with product details
        product_performance = product_performance \
            .join(products_df, "product_id") \
            .select(
                "product_id",
                "product_category_name",
                "total_quantity_sold",
                "total_revenue", 
                "avg_price",
                "total_orders"
            ) \
            .orderBy(desc("total_revenue"))
        
        # Write to analytics schema
        self.write_to_postgres(product_performance, "product_performance", "analytics")
        
        logger.info(f"Processed {product_performance.count():,} products")
        return product_performance
    
    def run_full_pipeline(self):
        """Execute complete ETL pipeline"""
        logger.info("Starting E-commerce ETL Pipeline...")
        
        try:
            # Process all analytics
            customer_metrics = self.process_customer_metrics()
            daily_sales = self.process_daily_sales()
            product_performance = self.process_product_performance()
            
            logger.info("ETL Pipeline completed successfully!")
            
            # Return summary stats
            return {
                "customers_processed": customer_metrics.count(),
                "days_analyzed": daily_sales.count(),
                "products_analyzed": product_performance.count()
            }
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
        
        finally:
            self.spark.stop()

def main():
    pipeline = EcommerceETLPipeline()
    results = pipeline.run_full_pipeline()
    logger.info(f"Pipeline Results: {results}")

if __name__ == "__main__":
    main()
