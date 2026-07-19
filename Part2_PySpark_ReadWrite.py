# Databricks notebook source
# MAGIC %md
# MAGIC # PART 2 — Reading & Writing Data 
# MAGIC ### PySpark on Databricks — Complete Learning Series
# MAGIC
# MAGIC **How to use this notebook:**
# MAGIC 1. `Workspace -> Import -> File` → upload this `.py` file (auto-detected as a notebook).
# MAGIC 2. Attach to any cluster (works on **Databricks Community Edition** too — call-outs below mark anything that needs a paid workspace).
# MAGIC 3. Run top-to-bottom with `Shift+Enter`. This notebook is **self-contained** — it generates its own
# MAGIC    sample CSV/JSON/Parquet/ORC/Text files first, so you don't need to upload anything.
# MAGIC 4. Practice Questions + Solutions at the end.
# MAGIC
# MAGIC **Topics covered in Part 2:**
# MAGIC 10. Reading CSV, JSON, Parquet, ORC, Text files
# MAGIC 11. Read options (header, inferSchema, delimiter, multiline) + modes (PERMISSIVE/DROPMALFORMED/FAILFAST) + error handling
# MAGIC 12. Writing files (CSV, JSON, Parquet) with modes: overwrite, append, ignore, error
# MAGIC 13. Partitioned read/write (`partitionBy`)
# MAGIC 14. Reading from JDBC (SQL databases)
# MAGIC 15. Reading from Delta Lake tables
# MAGIC 16. DBFS paths, mounting cloud storage (ADLS/S3), Unity Catalog volumes
# MAGIC 17. 🎯 Practice Questions with Solutions
# MAGIC
# MAGIC > ⚠️ **Databricks Community Edition (CE) note (read this first):**
# MAGIC > CE gives you a free single-node cluster with DBFS storage, and **Delta Lake works fully**.
# MAGIC > However CE does **NOT** support: **Unity Catalog**, mounting your own cloud storage account
# MAGIC > (ADLS/S3) since you have no cloud credentials tied to it, and some Jobs/cluster-scaling features.
# MAGIC > Everywhere this matters, you'll see a boxed **"CE ALTERNATIVE"** note telling you exactly what
# MAGIC > to use instead so every example in this notebook still runs on CE.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup — Generate Sample Files to Practice With
# MAGIC We create one Employee dataset and write it out in every format, so every "read" example
# MAGIC below has a real file to read back. This makes the notebook 100% runnable anywhere.

# COMMAND ----------

# MAGIC %md
# MAGIC DButils

# COMMAND ----------

# 2) Create a folder in DBFS (like "mkdir") - useful to organize your own practice files
dbutils.fs.mkdirs("/Volumes/ml/ai_schema/somedatavolume/new")      # creates a folder (no error if it already exists)
display(dbutils.fs.ls("/Volumes/ml/ai_schema/somedatavolume/"))          # confirm the folder was created

# COMMAND ----------

# 3) Widgets: create interactive input parameters at the top of your notebook (super useful for reusable notebooks / jobs)
dbutils.widgets.text("dept_filter", "IT")              # creates a text-box widget named "dept_filter" with default value "IT"
selected_dept = dbutils.widgets.get("dept_filter")      # read the current value typed by the user in the widget box
print("You selected department:", selected_dept)
# dbutils.widgets.remove("dept_filter")                 # uncomment to remove the widget when done

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Base folder for all Part 2 practice files (works on CE and full Databricks alike - plain DBFS path)
base_path = "/Volumes/pysparkcatalog/pyspark_schema/files/pyspark_part2_sample_files/write/"
dbutils.fs.mkdirs(base_path)          # create the folder (no error if it already exists)

# Our sample Employee data (reused throughout Part 2)
employee_data = [
    (1, "Amit",  "IT",     55000, 28, "Pune"),
    (2, "Sneha", "HR",     48000, 32, "Mumbai"),
    (3, "Raj",   "Sales",  62000, 25, "Delhi"),
    (4, "Priya", "IT",     71000, 30, "Pune"),
    (5, "Karan", "Sales",  39000, 27, "Delhi"),
    (6, "Neha",  "HR",     54000, 29, "Mumbai"),
    (7, "Vikas", "IT",     67000, 35, "Bangalore"),
    (8, "Meena", "Sales",  45000, 24, "Delhi"),
]
employee_columns = ["id", "name", "department", "salary", "age", "city"]
df_source = spark.createDataFrame(employee_data, employee_columns)
df_source.show()

# Write this ONE dataframe out to CSV, JSON, Parquet, ORC, and plain Text so we have files to read in Section 1.
# df_source.write.mode("overwrite").option("header", True).csv(f"{base_path}/employees_csv")
# df_source.write.mode("overwrite").json(f"{base_path}/employees_json")
# df_source.write.mode("overwrite").parquet(f"{base_path}/employees_parquet")
# df_source.write.mode("overwrite").orc(f"{base_path}/employees_orc")

df_source.selectExpr("concat_ws(',', id, name, department, salary, age, city) as line") \
         .write.mode("overwrite").text(f"{base_path}/employees_text")   # text() needs a SINGLE string column

print("Sample files created under:", base_path)
display(dbutils.fs.ls(base_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ORC format:ORC (which stands for Optimized Row Columnar) is a free, open-source, column-oriented data storage format.
# MAGIC
# MAGIC id column
# MAGIC ---------
# MAGIC 1
# MAGIC 2
# MAGIC 3
# MAGIC
# MAGIC name column
# MAGIC -----------
# MAGIC Amit
# MAGIC Neha
# MAGIC Rahul
# MAGIC
# MAGIC salary column
# MAGIC -------------
# MAGIC 50000
# MAGIC 54000
# MAGIC 70000

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Reading CSV, JSON, Parquet, ORC, Text Files
# MAGIC
# MAGIC ```
# MAGIC   File Format Cheat Sheet
# MAGIC   ┌───────────┬────────────────────────────┬──────────────────────────────────┐
# MAGIC   │  Format   │  Stores schema internally?  │  Best use case                    │
# MAGIC   ├───────────┼────────────────────────────┼──────────────────────────────────┤
# MAGIC   │  CSV      │  No (plain text, row/col)   │  Excel exports, simple data dumps │
# MAGIC   │  JSON     │  No (self-describing keys)  │  Semi-structured / nested data     │
# MAGIC   │  Parquet  │  YES (columnar + schema)    │  Big data analytics (FAST, default)│
# MAGIC   │  ORC      │  YES (columnar + schema)    │  Hive-heavy environments            │
# MAGIC   │  Text     │  No (one column per line)   │  Logs, unstructured raw text        │
# MAGIC   │  Delta    │  YES (+ transaction log)    │  Production lakehouse tables (Part 15)│
# MAGIC   └───────────┴────────────────────────────┴──────────────────────────────────┘
# MAGIC ```
# MAGIC **Real-life analogy:** CSV/Text are like a plain hand-written list — readable but Spark must
# MAGIC guess the structure. Parquet/ORC are like a pre-labeled, pre-organized filing cabinet —
# MAGIC Spark can jump straight to the columns/rows it needs without reading everything (this is
# MAGIC called **columnar storage** + **predicate pushdown**), making them dramatically faster.

# COMMAND ----------

base_path = "/Volumes/pysparkcatalog/pyspark_schema/files/pyspark_part2_sample_files"

# COMMAND ----------

# ---- 10.1 Reading CSV ----
df_csv = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/employees.csv")
df_csv.show()
df_csv.printSchema()
#inferSchema tells Spark to automatically detect the data type of each column instead of reading everything as a string.

# COMMAND ----------

# ---- 10.2 Reading JSON ----
df_json = spark.read.json(f"{base_path}/employees.json")   # JSON is self-describing -> schema inferred automatically, no options needed
df_json.show()
df_json.printSchema()

# COMMAND ----------

# ---- 10.3 Reading Parquet (schema is stored INSIDE the file - no inferSchema/header options needed at all) ----
df_parquet = spark.read.parquet(f"{base_path}/employees.parquet")
df_parquet.show()
df_parquet.printSchema()          # notice: correct types (int, double) came back automatically - Parquet remembers them!

# COMMAND ----------

# ---- 10.4 Reading ORC ----
df_orc = spark.read.orc(f"{base_path}/employees_sample.orc")
df_orc.show()

# COMMAND ----------

# ---- 10.5 Reading plain Text (every line becomes ONE row in a single column called "value") ----
df_text = spark.read.text(f"{base_path}/employees.txt")
df_text.show(truncate=False)
df_text.printSchema()             # always just one column: "value" (StringType)

# You'd typically split this manually afterward:
from pyspark.sql.functions import split, col
df_text_split = df_text.withColumn("parts", split(col("value"), ","))
df_text_split.select(
    col("parts")[0].alias("id"),
    col("parts")[1].alias("name"),
    col("parts")[2].alias("department")
).show()

# COMMAND ----------

# ---- 10.6 Generic reader syntax (format() + load()) — works for ANY format, useful when the format is a variable ----
df_generic = spark.read.format("csv").option("header", True).option("inferSchema", True).load(f"{base_path}/employees_csv")
df_generic.show(3)
# This is EXACTLY equivalent to spark.read.csv(...) - "format().load()" is just the more general/explicit syntax,
# and it's the ONLY way to read some formats like "delta", "jdbc", "avro".

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Read Options + Modes (PERMISSIVE / DROPMALFORMED / FAILFAST) + Error Handling
# MAGIC
# MAGIC ### Common read options
# MAGIC | Option | Purpose | Example |
# MAGIC |---|---|---|
# MAGIC | `header` | First row is column names? (CSV) | `.option("header", True)` |
# MAGIC | `inferSchema` | Auto-detect column data types (CSV) — slower on big files, scans data twice | `.option("inferSchema", True)` |
# MAGIC | `delimiter` / `sep` | Character separating columns (CSV) | `.option("delimiter", ";")` |
# MAGIC | `multiline` | Allow a single JSON/CSV record to span multiple lines | `.option("multiline", True)` |
# MAGIC | `quote` | Character used to wrap values containing the delimiter | `.option("quote", "\"")` |
# MAGIC | `escape` | Escape character inside quoted values | `.option("escape", "\\")` |
# MAGIC | `dateFormat` | Pattern to parse date strings | `.option("dateFormat", "yyyy-MM-dd")` |
# MAGIC | `nullValue` | String that should be treated as NULL | `.option("nullValue", "NA")` |
# MAGIC | `mode` | How to handle BAD/malformed records (see below) | `.option("mode", "PERMISSIVE")` |
# MAGIC | `columnNameOfCorruptRecord` | Column to store the raw bad record when mode=PERMISSIVE | `.option("columnNameOfCorruptRecord","_corrupt")` |
# MAGIC
# MAGIC ### The 3 parsing modes (CSV & JSON)
# MAGIC ```
# MAGIC   PERMISSIVE (DEFAULT)
# MAGIC     - Keeps ALL rows, sets fields it couldn't parse to NULL,
# MAGIC       and (optionally) stores the raw bad line in a "_corrupt_record" column.
# MAGIC     - Best for: exploring messy data without losing any rows.
# MAGIC
# MAGIC   DROPMALFORMED
# MAGIC     - SILENTLY DROPS any row that doesn't match the schema. No error, no trace.
# MAGIC     - Best for: quick analysis where a few bad rows genuinely don't matter.
# MAGIC     - ⚠ Risk: you can silently lose data without realizing it - use with caution.
# MAGIC
# MAGIC   FAILFAST
# MAGIC     - THROWS AN EXCEPTION immediately the moment it hits ANY malformed record.
# MAGIC     - Best for: production pipelines where bad data must STOP the job (data quality gate).
# MAGIC ```
# MAGIC **Real-life analogy:** Think of a security guard checking IDs at a club entrance.
# MAGIC - PERMISSIVE = lets everyone in, but flags the fake IDs on a clipboard.
# MAGIC - DROPMALFORMED = quietly turns away anyone with a bad ID, no questions asked.
# MAGIC - FAILFAST = shuts down the entire club the second ONE fake ID is spotted.

# COMMAND ----------

# ---- Create a MESSY csv file on purpose (with a bad row) so we can practice error handling ----

base_path = "/Volumes/pysparkcatalog/pyspark_schema/files/pyspark_part2_sample_files"

messy_csv_content = """id,name,department,salary
1,Amit,IT,55000
2,Sneha,HR,48000
BAD_ROW_HERE_ONLY_TWO_FIELDS,IT,67.005
4,Priya,IT,71000"""

dbutils.fs.put(f"{base_path}/messy_data2.csv", messy_csv_content, overwrite=True)   # write raw text directly to DBFS

# COMMAND ----------

base_path = "/Volumes/pysparkcatalog/pyspark_schema/files/pyspark_part2_sample_files/messy_data2.csv"
df=spark.read.option("header", True).csv(base_path)
df.display()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
#---- 11.1 PERMISSIVE mode (default): bad row survives as NULLs + optionally captured ----

messy_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("_corrupt_record", StringType(), True),   # extra column to CATCH the raw bad line
])

df_permissive = (
    spark.read
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")   # tell Spark WHERE to store the raw bad line
    .schema(messy_schema)                                       # NOTE: columnNameOfCorruptRecord requires an explicit schema!
    .csv(f"{base_path}")
)
df_permissive.show(truncate=False)   # the bad row shows as NULL fields + its raw text in "_corrupt_record"

# COMMAND ----------

# ---- 11.2 DROPMALFORMED mode: bad row silently disappears ----
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
df_dropmalformed = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("mode", "DROPMALFORMED")
    .csv(f"{base_path}")
)
df_dropmalformed = df_dropmalformed.withColumn(
    "id", col("id").cast("int")
)
df_dropmalformed.show()   # only 3 GOOD rows remain - the bad row is gone with no warning printed

# COMMAND ----------

# ---- 11.3 FAILFAST mode: throws an exception -> wrap in try/except for proper error handling ----
try:
    df_failfast = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "FAILFAST")
        .csv(f"{base_path}")
    )
    df_failfast.show()          # this line will NEVER be reached if a bad row exists
except Exception as e:
    print("❌ FAILFAST caught a bad record! Stopping pipeline as expected.")
    print("Error detail (truncated):", str(e)[:300])   # print first 300 chars of the error message

# COMMAND ----------

# MAGIC %md ### 11.4 More error-handling patterns for production pipelines

# COMMAND ----------

# Pattern A: Defensive read — wrap ANY read in try/except so one bad file doesn't crash your whole job
def safe_read_csv(path):
    """Reads a CSV safely; returns None (and logs the error) instead of crashing the whole notebook/job."""
    try:
        return spark.read.option("header", True).option("inferSchema", True).csv(path)
    except Exception as err:
        print(f"⚠️ Failed to read {path}: {err}")
        return None

result_df = safe_read_csv(f"{base_path}")
if result_df is not None:
    result_df.show(3)

# COMMAND ----------

# Pattern B: Isolate & inspect bad records using PERMISSIVE mode + _corrupt_record, then quarantine them
good_records = df_permissive.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
bad_records  = df_permissive.filter(col("_corrupt_record").isNotNull())

print("✅ Good records:")
good_records.show()
print("❌ Bad/quarantined records (send these to a review folder or alert a data-quality dashboard):")
bad_records.show(truncate=False)

# COMMAND ----------

# ---- Multiline JSON example (a JSON record that spans several lines - common in exported/pretty-printed JSON) ----
multiline_json = """[
  {
    "id": 1,
    "name": "Amit",
    "department": "IT"
  },
  {
    "id": 2,
    "name": "Sneha",
    "department": "HR"
  }
]"""
dbutils.fs.put(f"{base_path}/multiline.json", multiline_json, overwrite=True)

# WITHOUT multiline=True, Spark expects ONE JSON object PER LINE and this file would fail/return garbage.
df_multiline = spark.read.option("multiline", True).json(f"{base_path}/multiline.json")
df_multiline.show()

# COMMAND ----------

# ---- Custom delimiter example (e.g. pipe-separated values) ----
base_path = "/Volumes/pysparkcatalog/pyspark_schema/files/pyspark_part2_sample_files/"
pipe_data = "id|name|department\n1|Amit|IT\n2|Sneha|HR"
dbutils.fs.put(f"{base_path}/pipe_data.csv", pipe_data, overwrite=True)

df_pipe = spark.read.option("header", True).option("delimiter", "|").option("inferSchema", True).csv(f"{base_path}/pipe_data.csv")
df_pipe.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Writing Files (CSV, JSON, Parquet) with Modes: overwrite, append, ignore, error
# MAGIC
# MAGIC | Mode | Behavior | Real-life analogy |
# MAGIC |---|---|---|
# MAGIC | `overwrite` | DELETES existing data at the path and writes fresh | Erasing a whiteboard before writing new notes |
# MAGIC | `append` | ADDS new data to whatever already exists at the path | Adding a new page to an existing notebook |
# MAGIC | `ignore` | If data ALREADY exists at the path, do NOTHING (no error, no write) | "Don't overwrite my file if it's already there" |
# MAGIC | `error` / `errorifexists` (DEFAULT) | Throws an EXCEPTION if the path already has data | A safety lock preventing accidental overwrites |

# COMMAND ----------

# ============================================================
# WRITE MODE: error / errorifexists
# ============================================================

# 1. Create sample DataFrame
data = [
    (1, "Amit", "IT", 55000),
    (2, "Sneha", "HR", 48000),
    (3, "Rahul", "Finance", 62000),
    (4, "Priya", "IT", 71000)
]

columns = ["id", "name", "department", "salary"]

df_source = spark.createDataFrame(data, columns)

print("Source Data:")
df_source.show()


# 2. Define paths
base_path = "/Volumes/pysparkcatalog/pyspark_schema/files/pyspark_part2_sample_files"
output_path = f"{base_path}/write_demo"


# 3. Optional cleanup - makes the cell runnable again from scratch
# dbutils.fs.rm(f"{output_path}_v1", True)


# 4. FIRST WRITE
# "error" is also Spark's default write mode.
# It works because the path does not exist yet.

df_source.write \
    .mode("error") \
    .option("header", True) \
    .csv(f"{output_path}_v1")

print("✅ First write successful")


# 5. SECOND WRITE to the SAME path
# This fails because the output path already exists.

try:
    df_source.write \
        .mode("error") \
        .option("header", True) \
        .csv(f"{output_path}_v1")

except Exception as e:
    print("❌ 'error' mode blocked the second write as expected")
    print("Error:", str(e)[:500])


# 6. Read the successfully written CSV files
df_result = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(f"{output_path}_v1")

print("Data already present in output path:")
df_result.show()

# COMMAND ----------

# ---- ignore ----
df_source.write.mode("ignore").csv(f"{output_path}_v1")     # path already exists from above -> WRITE IS SILENTLY SKIPPED
print("Row count still same as before (write was skipped):", spark.read.csv(f"{output_path}_v1").count())

# COMMAND ----------

# ---- overwrite ----
df_half = df_source.filter(col("department") == "IT")        # only 2 rows
df_half.write.mode("overwrite").option("header", True).csv(f"{output_path}_v1")
print("Row count after OVERWRITE with only IT dept:", spark.read.option("header", True).csv(f"{output_path}_v1").count())
df_read=spark.read.option("header", True).csv(f"{output_path}_v1")
df_read.show()

# COMMAND ----------

# ---- append ----
df_more = df_source.filter(col("department") == "HR")        # 3 more rows
df_more.write.mode("append").option("header", True).csv(f"{output_path}_v1")
print("Row count after APPEND (IT rows + HR rows):", spark.read.option("header", True).csv(f"{output_path}_v1").count())
df_read=spark.read.option("header", True).csv(f"{output_path}_v1")
df_read.show()

# COMMAND ----------

# ---- Writing JSON and Parquet with modes works identically ----
df_source.write.mode("overwrite").json(f"{output_path}_json")
df_source.write.mode("overwrite").parquet(f"{output_path}_parquet")

# Compression option (Parquet defaults to snappy already, but you can control it)
df_source.write.mode("overwrite").option("compression", "gzip").parquet(f"{output_path}_parquet_gzip")
print("Files written with different compression codecs - check file sizes:")
display(dbutils.fs.ls(f"{output_path}_parquet"))
display(dbutils.fs.ls(f"{output_path}_parquet_gzip"))

# COMMAND ----------

# ---- coalesce(1) / repartition(1) before writing -> controls HOW MANY output files are created ----
# By default Spark writes ONE FILE PER PARTITION (could be dozens of small files!). For a small demo dataset,
# it's common to force a SINGLE output file so it's easy to share/inspect.
df_source.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_path}_single_file")
display(dbutils.fs.ls(f"{output_path}_single_file"))   # notice: one part-0000....csv file (plus a _SUCCESS marker file)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Partitioned Read/Write (`partitionBy`)
# MAGIC
# MAGIC **What is partitioning on write?** Instead of one giant file, Spark creates a **folder per
# MAGIC unique value** of the column(s) you choose. This lets future reads SKIP entire folders that
# MAGIC don't match a filter (called **partition pruning**) — massively speeding up queries on huge datasets.
# MAGIC
# MAGIC **Real-life analogy:** Instead of keeping all invoices in one giant pile, you file them into
# MAGIC labeled drawers by "Year" and "Month". Looking for March 2024 invoices? Open ONE drawer,
# MAGIC ignore the rest — instead of flipping through the entire pile.
# MAGIC
# MAGIC ```
# MAGIC   employees_partitioned/
# MAGIC       ├── department=IT/
# MAGIC       │      └── part-0000.parquet
# MAGIC       ├── department=HR/
# MAGIC       │      └── part-0000.parquet
# MAGIC       └── department=Sales/
# MAGIC              └── part-0000.parquet
# MAGIC ```

# COMMAND ----------

# ---- Writing partitioned by ONE column ----
partitioned_path = f"{base_path}/employees_partitioned"
df_source.write.mode("overwrite").partitionBy("department").parquet(partitioned_path)

display(dbutils.fs.ls(partitioned_path))   # notice folders: department=IT, department=HR, department=Sales

# COMMAND ----------

# ---- Writing partitioned by MULTIPLE columns (creates nested folders) ----
partitioned_path_multi = f"{base_path}/employees_partitioned_multi"
df_source.write.mode("overwrite").partitionBy("department", "city").parquet(partitioned_path_multi)
display(dbutils.fs.ls(partitioned_path_multi))                       # top level: department=IT, department=HR...
display(dbutils.fs.ls(f"{partitioned_path_multi}/department=IT"))     # inside IT: city=Pune, city=Bangalore...

# COMMAND ----------

# ---- Reading a partitioned dataset back ----
df_read_partitioned = spark.read.parquet(partitioned_path)
df_read_partitioned.printSchema()      # "department" comes back as a normal column even though it was a FOLDER name!
df_read_partitioned.show()

# ---- Partition pruning in action: filtering on the partition column is SUPER fast (skips other folders entirely) ----
df_it_only = spark.read.parquet(partitioned_path).filter(col("department") == "IT")
df_it_only.explain()      # look for "PartitionFilters" in the plan -> proof Spark skipped HR/Sales folders on disk
df_it_only.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Reading from JDBC (SQL Databases)
# MAGIC
# MAGIC JDBC lets Spark connect DIRECTLY to relational databases (MySQL, PostgreSQL, SQL Server,
# MAGIC Oracle, etc.) and read/write tables as DataFrames.
# MAGIC
# MAGIC **Requirements:**
# MAGIC 1. The database's **JDBC driver JAR** must be installed on the cluster (`Compute -> Libraries -> Install New -> Maven`, search e.g. `mysql:mysql-connector-java`).
# MAGIC 2. Network connectivity from the cluster to the database (firewall/VPC rules).
# MAGIC 3. A valid connection URL, username, and password (use **Databricks Secrets**, never hardcode passwords!).
# MAGIC
# MAGIC > ⚠️ **Community Edition note:** CE clusters CAN install JDBC driver JARs and CAN reach public
# MAGIC > internet databases, so JDBC reads DO work on CE — **as long as your database allows inbound
# MAGIC > connections from Databricks' cloud IP range** (most personal/local databases behind a home
# MAGIC > router will NOT be reachable). For practicing without a real database, use a small public
# MAGIC > demo DB, or simply skip to the code pattern below and swap in your own working credentials later.

# COMMAND ----------

# ---- Standard JDBC read pattern (syntax reference - replace placeholders with your real DB details) ----

jdbc_url = "jdbc:mysql://<host>:<port>/<database>"     # e.g. jdbc:mysql://mydb.company.com:3306/sales_db

connection_properties = {
    "user": dbutils.secrets.get(scope="my-scope", key="db-username"),   # NEVER hardcode credentials - use Secrets!
    "password": dbutils.secrets.get(scope="my-scope", key="db-password"),
    "driver": "com.mysql.cj.jdbc.Driver"                                  # driver class name (varies per DB vendor)
}

# Reading an ENTIRE table:
# df_jdbc = spark.read.jdbc(url=jdbc_url, table="employees", properties=connection_properties)

# Reading with a CUSTOM SQL QUERY instead of a full table (push filtering down to the DB - very efficient):
# query = "(SELECT id, name, salary FROM employees WHERE department = 'IT') AS subquery"
# df_jdbc_query = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)

# Alternative syntax using format()/option() (equivalent to the above, just more explicit):
# df_jdbc2 = (
#     spark.read.format("jdbc")
#     .option("url", jdbc_url)
#     .option("dbtable", "employees")
#     .option("user", "myuser")
#     .option("password", "mypassword")
#     .option("driver", "com.mysql.cj.jdbc.Driver")
#     .option("numPartitions", 4)          # read in PARALLEL using 4 tasks (requires partitionColumn/lowerBound/upperBound too)
#     .option("partitionColumn", "id")
#     .option("lowerBound", 1)
#     .option("upperBound", 10000)
#     .load()
# )

print("ℹ️ JDBC code shown above is reference syntax - connect it to your own database to run it.")
print("Parallel JDBC reads (numPartitions/partitionColumn/lowerBound/upperBound) avoid pulling millions of rows through a SINGLE connection.")

# COMMAND ----------

# MAGIC %md ### Writing back to a database via JDBC (reference syntax)

# COMMAND ----------

# df_source.write.jdbc(
#     url=jdbc_url,
#     table="employees_output",
#     mode="overwrite",                 # same modes as file writes: overwrite / append / ignore / error
#     properties=connection_properties
# )
print("ℹ️ Writing DataFrames to a SQL table uses the exact same .write.mode(...) pattern you already learned in Section 12.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Reading from Delta Lake Tables
# MAGIC
# MAGIC Delta Lake is an open-source storage layer that adds **ACID transactions, time travel, schema
# MAGIC enforcement, and upserts (MERGE)** on top of Parquet files. It's the DEFAULT table format in
# MAGIC Databricks. (Full deep-dive is in Part 9 — this section covers just reading/writing basics.)
# MAGIC
# MAGIC > ✅ **Community Edition note:** Delta Lake is **fully supported on CE** — no limitations here at all!

# COMMAND ----------

# ---- Writing a Delta table (path-based) ----
delta_path = f"{base_path}/employees_delta"
df_source.write.format("delta").mode("overwrite").save(delta_path)

# ---- Reading it back ----
df_delta = spark.read.format("delta").load(delta_path)
df_delta.show()

# COMMAND ----------

# ---- Registering as a proper managed/external TABLE so you can query it with plain SQL ----
spark.sql(f"CREATE TABLE IF NOT EXISTS employees_delta_tbl USING DELTA LOCATION '{delta_path}'")

# Now query using SQL directly:
spark.sql("SELECT department, COUNT(*) as emp_count, AVG(salary) as avg_salary FROM employees_delta_tbl GROUP BY department").show()

# Or query using the DataFrame API on the TABLE (instead of a path):
spark.table("employees_delta_tbl").show()

# COMMAND ----------

# ---- Quick preview of what makes Delta special: DESCRIBE HISTORY (full time-travel is covered in Part 9) ----
spark.sql("DESCRIBE HISTORY employees_delta_tbl").select("version", "timestamp", "operation").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. DBFS Paths, Mounting Cloud Storage (ADLS/S3), Unity Catalog Volumes
# MAGIC
# MAGIC ### DBFS path types you'll see:
# MAGIC | Path style | Meaning |
# MAGIC |---|---|
# MAGIC | `/tmp/...` or `/FileStore/...` | A path INSIDE DBFS (Databricks' built-in distributed file system) |
# MAGIC | `dbfs:/tmp/...` | Same as above, just with the explicit `dbfs:` scheme prefix |
# MAGIC | `/mnt/my-mount/...` | A MOUNT POINT — a shortcut/symlink DBFS keeps to an external cloud storage container |
# MAGIC | `abfss://container@account.dfs.core.windows.net/...` | Direct ADLS Gen2 path (no mount needed, used with Unity Catalog credentials) |
# MAGIC | `/Volumes/catalog/schema/volume_name/...` | A **Unity Catalog Volume** path — the modern replacement for mounts |
# MAGIC
# MAGIC ### Mounting external cloud storage (legacy pattern, still common in older workspaces)
# MAGIC ```python
# MAGIC dbutils.fs.mount(
# MAGIC     source = "abfss://mycontainer@mystorageaccount.dfs.core.windows.net/",
# MAGIC     mount_point = "/mnt/my-adls-mount",
# MAGIC     extra_configs = {
# MAGIC         "fs.azure.account.auth.type": "OAuth",
# MAGIC         "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC         "fs.azure.account.oauth2.client.id": dbutils.secrets.get("scope", "client-id"),
# MAGIC         "fs.azure.account.oauth2.client.secret": dbutils.secrets.get("scope", "client-secret"),
# MAGIC         "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenant-id>/oauth2/token"
# MAGIC     }
# MAGIC )
# MAGIC # After mounting, read/write it exactly like any DBFS path:
# MAGIC # spark.read.csv("/mnt/my-adls-mount/some_file.csv")
# MAGIC ```
# MAGIC
# MAGIC ### Unity Catalog Volumes (the modern, governed replacement for mounts)
# MAGIC ```python
# MAGIC # Once a Volume is created by an admin:
# MAGIC # spark.read.csv("/Volumes/main_catalog/sales_schema/raw_files/employees.csv")
# MAGIC ```
# MAGIC Volumes give fine-grained permissions (who can read/write which folder) governed centrally,
# MAGIC instead of every user needing cloud storage keys.
# MAGIC
# MAGIC > ⚠️ **Community Edition note — READ CAREFULLY:**
# MAGIC > - `dbutils.fs.mount(...)` requires a real cloud storage account (Azure/AWS) + credentials — **CE typically has no cloud account attached**, so mounting your own storage generally isn't possible on CE.
# MAGIC > - **Unity Catalog is NOT available on Community Edition at all** — there is no `/Volumes/...` path and no `catalog.schema.table` 3-level namespace; CE only has the legacy single-level `hive_metastore` with `database.table`.
# MAGIC >
# MAGIC > **✅ CE ALTERNATIVES that work everywhere:**
# MAGIC > 1. Use plain **DBFS paths** (`/tmp/...`, `/FileStore/...`) exactly like every example in this notebook does — no mounting needed for practice/learning.
# MAGIC > 2. To bring your OWN files into CE: use the notebook UI **`File -> Upload Data`**, or drag-and-drop into `/FileStore/tables/` via the **Data** tab in the sidebar.
# MAGIC > 3. To pull public sample datasets: Databricks pre-loads many at **`/databricks-datasets/`** (available on CE too) — try `dbutils.fs.ls("/databricks-datasets")`.
# MAGIC > 4. To fetch a file from the internet on CE, use a shell cell: `%sh wget -O /tmp/file.csv <url>` then move it into DBFS with `dbutils.fs.cp("file:/tmp/file.csv", "dbfs:/tmp/file.csv")`.

# COMMAND ----------

# ---- Practical DBFS path operations you'll use constantly ----
display(dbutils.fs.ls(base_path))              # list files/folders
dbutils.fs.cp(f"{base_path}/employees_csv", f"{base_path}/employees_csv_backup", recurse=True)  # copy a folder
print("Does backup exist?", any(f.name.startswith("employees_csv_backup") for f in dbutils.fs.ls(base_path)))

# dbutils.fs.rm(f"{base_path}/employees_csv_backup", recurse=True)   # uncomment to delete the backup folder + all contents
# dbutils.fs.head(f"{base_path}/employees_text/part-00000")          # peek at the first bytes of a raw file (uncomment to try)

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎯 PRACTICE QUESTIONS — Part 2
# MAGIC Try each question yourself first, then check the solution cell below it.
# MAGIC All questions reuse `df_source` (the Employee dataset) and the `base_path` folder from Section 0.

# COMMAND ----------

# MAGIC %md
# MAGIC | Q# | Question | Notes / Expected Result |
# MAGIC |---|---|---|
# MAGIC | 1 | Write `df_source` as JSON to `{base_path}/practice/q1_json`, then read it back and show it | JSON round-trip |
# MAGIC | 2 | Read `messy_data.csv` using `DROPMALFORMED` mode and count how many rows survived | Should be 3 rows |
# MAGIC | 3 | Read `messy_data.csv` using `PERMISSIVE` mode with a `_corrupt_record` column, then show ONLY the corrupt row | 1 row shown |
# MAGIC | 4 | Write `df_source` to `{base_path}/practice/q4_append` twice using `append` mode, then confirm row count doubled | 16 rows |
# MAGIC | 5 | Write `df_source` partitioned by `city` to `{base_path}/practice/q5_partitioned` | Check folder names = city=Pune, city=Mumbai, etc. |
# MAGIC | 6 | Read back the Q5 output and filter for `city = 'Pune'` only, then call `.explain()` to confirm partition pruning | Look for PartitionFilters in the plan |
# MAGIC | 7 | Read the pipe-delimited file `{base_path}/pipe_data.csv` (delimiter `\|`) into a DataFrame | 2 rows, 3 columns |
# MAGIC | 8 | Write `df_source` as a SINGLE output CSV file (use `coalesce`) to `{base_path}/practice/q8_singlefile` | Only 1 part file + _SUCCESS |
# MAGIC | 9 | Write `df_source` as a Delta table at `{base_path}/practice/q9_delta`, then read it back and filter `salary > 50000` | 5 rows |
# MAGIC | 10 | Try writing to `{base_path}/practice/q4_append` using mode `error` and catch the exception gracefully | Should print a caught-error message, not crash |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ SOLUTIONS

# COMMAND ----------

# Q1: JSON round-trip
df_source.write.mode("overwrite").json(f"{base_path}/practice/q1_json")
spark.read.json(f"{base_path}/practice/q1_json").show()

# COMMAND ----------

# Q2: DROPMALFORMED row count
q2_df = spark.read.option("header", True).option("inferSchema", True).option("mode", "DROPMALFORMED").csv(f"{base_path}/messy_data.csv")
print("Surviving row count:", q2_df.count())
q2_df.show()

# COMMAND ----------

# Q3: PERMISSIVE mode - show only the corrupt row
q3_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("_corrupt_record", StringType(), True),
])
q3_df = (
    spark.read.option("header", True).option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(q3_schema)
    .csv(f"{base_path}/messy_data.csv")
)
q3_df.filter(col("_corrupt_record").isNotNull()).show(truncate=False)

# COMMAND ----------

# Q4: Append twice, confirm doubled row count
q4_path = f"{base_path}/practice/q4_append"
df_source.write.mode("overwrite").option("header", True).csv(q4_path)   # first write (8 rows)
df_source.write.mode("append").option("header", True).csv(q4_path)      # second write appends 8 more rows
print("Total rows after 2 writes:", spark.read.option("header", True).csv(q4_path).count())

# COMMAND ----------

# Q5: Partition by city
q5_path = f"{base_path}/practice/q5_partitioned"
df_source.write.mode("overwrite").partitionBy("city").parquet(q5_path)
display(dbutils.fs.ls(q5_path))

# COMMAND ----------

# Q6: Filter partitioned data + explain() to confirm pruning
q6_df = spark.read.parquet(q5_path).filter(col("city") == "Pune")
q6_df.explain()          # look for "PartitionFilters: [isnotnull(city#.. ), (city#.. = Pune)]" in the output
q6_df.show()

# COMMAND ----------

# Q7: Read pipe-delimited file
spark.read.option("header", True).option("delimiter", "|").option("inferSchema", True).csv(f"{base_path}/pipe_data.csv").show()

# COMMAND ----------

# Q8: Single output file using coalesce
q8_path = f"{base_path}/practice/q8_singlefile"
df_source.coalesce(1).write.mode("overwrite").option("header", True).csv(q8_path)
display(dbutils.fs.ls(q8_path))    # exactly ONE part-*.csv file + a _SUCCESS marker

# COMMAND ----------

# Q9: Delta table write + read + filter
q9_path = f"{base_path}/practice/q9_delta"
df_source.write.format("delta").mode("overwrite").save(q9_path)
spark.read.format("delta").load(q9_path).filter(col("salary") > 50000).show()

# COMMAND ----------

# Q10: Catch an 'error' mode exception gracefully
try:
    df_source.write.mode("error").csv(q4_path)     # q4_append path already has data from Q4
except Exception as e:
    print("✅ Caught expected exception - path already exists. Error (truncated):", str(e)[:150])

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Part 2 Summary — What You Learned
# MAGIC - Reading **CSV, JSON, Parquet, ORC, Text** — and WHY Parquet/ORC (columnar + embedded schema) beat CSV/JSON for big data
# MAGIC - Key **read options**: header, inferSchema, delimiter, multiline, quote/escape, nullValue, dateFormat
# MAGIC - The 3 **parsing modes** — PERMISSIVE (keep + flag), DROPMALFORMED (silently drop), FAILFAST (crash immediately) — and how to pick the right one
# MAGIC - **Error-handling patterns**: try/except wrappers, `_corrupt_record` quarantine pattern, defensive read functions
# MAGIC - **Write modes**: overwrite, append, ignore, error/errorifexists — with live before/after row-count proof
# MAGIC - **partitionBy()** for folder-based partitioning + **partition pruning** proof via `.explain()`
# MAGIC - **JDBC** reference syntax for connecting to relational databases (with Secrets-based credentials, parallel reads)
# MAGIC - **Delta Lake** basics: write, read, register as SQL table, `DESCRIBE HISTORY`
# MAGIC - **DBFS paths, mounts, and Unity Catalog Volumes** — plus clear **Community Edition alternatives** for each
# MAGIC - **10 hands-on practice questions with full solutions**
# MAGIC
# MAGIC ### 🔜 Coming in Part 3: Transformations (Core)
# MAGIC select/selectExpr, filter, withColumn deep-dive, string functions, date/time functions, null handling,
# MAGIC and dozens more transformation functions — with the same style: full examples + practice questions.
