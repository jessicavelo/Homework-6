# spark_session.py
import os
from pyspark.sql import SparkSession

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WAREHOUSE = os.path.join(BASE_DIR, "spark-warehouse")
METASTORE = os.path.join(BASE_DIR, "metastore_db")

def get_spark(app_name):
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.warehouse.dir", WAREHOUSE)
            .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                    f"jdbc:derby:;databaseName={METASTORE};create=true")
            .config("spark.driver.memory", "4g")
            .enableHiveSupport()
            .getOrCreate())