import os, time
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

WAREHOUSE = "C:/Users/jessi/sjsu-data226-FA26/week7/spark-warehouse"
METASTORE = "C:/Users/jessi/sjsu-data226-FA26/week7/metastore_db"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AQE Skew Join Demo") \
    .enableHiveSupport() \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.warehouse.dir", WAREHOUSE) \
    .config("spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={METASTORE};create=true") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.sql.autoBroadcastJoinThreshold", "1MB") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "16MB") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "16MB") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.sql("USE demo")

def run_skew_bench(aqe_on):
    spark.conf.set("spark.sql.adaptive.enabled", str(aqe_on).lower())
    print(f"\n=== AQE Skew Join Enabled: {aqe_on} ===")
    # No WHERE filter: the full 20M-row sales table joins items, so the
    # 80% skew on item_id = 100 lands in a single shuffle partition.
    df = spark.sql("""
        SELECT date, sum(quantity * price) AS total_sales
        FROM sales s JOIN items i ON s.item_id = i.id
        GROUP BY 1 ORDER BY 2 DESC
    """)

    print("--- plan BEFORE execution ---")
    df.explain()

    start = time.perf_counter()
    df.collect()
    print(f"Execution Time: {time.perf_counter() - start:.4f}s")

    print("--- plan AFTER execution ---")
    df.explain()

run_skew_bench(True)
run_skew_bench(False)

try:
    input("\nCheck 'AQEShuffleRead skewed' at http://localhost:4040. Press Enter to exit...")
except EOFError:
    pass
spark.stop()
