import os
# Force Spark to bind to localhost
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

#Absolute path to create table
WAREHOUSE = "C:/Users/jessi/sjsu-data226-FA26/week7/spark-warehouse"
METASTORE = "C:/Users/jessi/sjsu-data226-FA26/week7/metastore_db"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("gen-sample-data-10M") \
    .enableHiveSupport() \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.warehouse.dir", WAREHOUSE) \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS demo")
spark.sql("USE demo")

# Scaled to 600,000 items
print("Generating 600K row Items table...")
spark.sql("""
CREATE TABLE IF NOT EXISTS items USING parquet AS
SELECT id, CAST(rand() * 1000 AS INT) AS price FROM RANGE(600000)
""")

# Scaled to 20,000,000 sales
SKEW_KEY, SKEW_FRACTION, TOLERANCE = 100, 0.80, 0.02
N_ITEMS, N_SALES, SEED = 600000, 20000000, 226

def measure_skew():
    r = spark.sql(f"""
        SELECT COUNT(*) AS n,
               SUM(CASE WHEN item_id = {SKEW_KEY} THEN 1 ELSE 0 END) AS hot
        FROM sales
    """).first()
    n = r["n"] or 0
    return n, ((r["hot"] or 0) / n if n else 0.0)

rebuild = not spark.catalog.tableExists("demo", "sales")
if not rebuild:
    rows, share = measure_skew()
    print(f"Existing sales: {rows:,} rows, item_id={SKEW_KEY} share = {share:.2%}")
    rebuild = abs(share - SKEW_FRACTION) > TOLERANCE

if rebuild:
    print(f"Generating {N_SALES:,} row skewed Sales table...")
    spark.sql("DROP TABLE IF EXISTS sales")
    spark.sql(f"""
    CREATE TABLE sales USING parquet AS
    SELECT
      CASE WHEN rand({SEED}) < {SKEW_FRACTION} THEN {SKEW_KEY}
           ELSE CAST(rand({SEED+1}) * {N_ITEMS} AS INT)
      END AS item_id,
      CAST(rand({SEED+2}) * 100 AS INT) AS quantity,
      DATE_ADD(current_date(), - CAST(rand({SEED+3}) * 360 AS INT)) AS date
    FROM RANGE({N_SALES})
    """)
else:
    print("Sales table already holds the required skew - reusing it.")

rows, share = measure_skew()
print(f"Verification: {rows:,} rows, item_id={SKEW_KEY} share = {share:.2%}")
if abs(share - SKEW_FRACTION) > TOLERANCE:
    raise SystemExit(f"ERROR: skew is {share:.2%}, expected {SKEW_FRACTION:.0%}")
spark.sql("SELECT item_id, COUNT(*) AS c FROM sales GROUP BY item_id ORDER BY c DESC LIMIT 5").show()

print(f"Tables saved permanently in: {WAREHOUSE}")
input("Wait for UI inspection at http://localhost:4040. Press Enter to stop...")
spark.stop()
