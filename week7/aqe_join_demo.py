import os, time
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

WAREHOUSE = "C:/Users/jessi/sjsu-data226-FA26/week7/spark-warehouse"
METASTORE = "C:/Users/jessi/sjsu-data226-FA26/week7/metastore_db"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AQE Join Demo") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", WAREHOUSE) \
    .config("javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={METASTORE};create=true") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.sql.autoBroadcastJoinThreshold", "1MB") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.sql("USE demo")

def run_join_bench(aqe_on):
    spark.conf.set("spark.sql.adaptive.enabled", str(aqe_on).lower())
    start = time.perf_counter()
    # Filter 'price < 10' reduces items size to trigger Broadcast Join
    df = spark.sql("""
        SELECT date, sum(quantity * price) AS total_sales
        FROM sales s JOIN items i ON s.item_id = i.id
        WHERE price < 10 GROUP BY 1 ORDER BY 2 DESC
    """)

    print("--- plan BEFORE execution ---")
    df.explain()

    start = time.perf_counter()
    df.collect()
    print(f"Execution Time: {time.perf_counter() - start:.4f}s")

    print("--- plan AFTER execution ---")
    df.explain()

    spark.sql("SELECT date, sum(quantity * price) AS total_sales FROM sales s JOIN items i ON s.item_id = i.id WHERE price < 10 GROUP BY 1 ORDER BY 2 DESC").collect()

run_join_bench(True)
run_join_bench(False)

input("\nCheck 'BroadcastHashJoin' at http://localhost:4040. Press Enter to exit...")
spark.stop()