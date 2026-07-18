# Databricks notebook source
# MAGIC %md
# MAGIC # PART 1 — PySpark Fundamentals & Environment (EXPANDED EDITION)
# MAGIC ### A complete beginner-to-expert PySpark learning series (Databricks Notebook)
# MAGIC
# MAGIC **How to use this notebook:**
# MAGIC 1. Import this file into Databricks: `Workspace -> Import -> File` (upload this .py file — Databricks auto-detects it as a notebook because of the `# Databricks notebook source` header).
# MAGIC 2. Attach it to any running cluster.
# MAGIC 3. Run cells top-to-bottom with `Shift + Enter`.
# MAGIC 4. Every code cell has line-by-line comments. Read → Run → Modify values yourself → Re-run.
# MAGIC 5. A full **Practice Questions + Solutions** section is at the end — try the question yourself FIRST, then check the solution cell.
# MAGIC
# MAGIC **Topics covered in Part 1:**
# MAGIC 1. What is Spark, Spark Architecture (Driver, Executors, Cluster Manager, Jobs/Stages/Tasks)
# MAGIC 2. Databricks Workspace Basics (Notebooks, Clusters, DBFS, Unity Catalog, dbutils)
# MAGIC 3. RDD vs DataFrame vs Dataset (with extra RDD transformation examples)
# MAGIC 4. SparkSession vs SparkContext
# MAGIC 5. Creating DataFrames — list, dict, RDD, range, Pandas, empty DataFrame
# MAGIC 6. Schema — StructType, StructField, Data Types, Nested Schemas, Arrays/Maps
# MAGIC 7. show(), display(), printSchema(), describe(), summary() + extra inspection tricks
# MAGIC 8. select, filter/where, withColumn, drop, alias — with MANY more transformation examples
# MAGIC 9. Column expressions — col(), expr(), lit()
# MAGIC 10. 🎯 Practice Questions with step-by-step Solutions (15 questions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What is Spark? Spark Architecture
# MAGIC
# MAGIC ### What is Apache Spark?
# MAGIC Apache Spark is a **distributed, in-memory data processing engine**. Instead of processing
# MAGIC a huge file on ONE machine (slow, may not even fit in memory), Spark **splits the data into
# MAGIC chunks (partitions) and processes those chunks in parallel across many machines** (a cluster).
# MAGIC
# MAGIC **Real-life analogy:**
# MAGIC Imagine you must count all books in a 10-floor library by yourself — it would take hours.
# MAGIC Now imagine you hire 10 people, one per floor, all counting **at the same time**, and then
# MAGIC you add up their totals. That's exactly what Spark does with data: it divides the work
# MAGIC (partitions) and does it in parallel (distributed computing).
# MAGIC
# MAGIC ### Spark Architecture — 3 Main Components
# MAGIC
# MAGIC ```
# MAGIC                         ┌────────────────────────────┐
# MAGIC                         │        CLUSTER MANAGER      │
# MAGIC                         │  (YARN / Kubernetes / Databricks) │
# MAGIC                         │  Decides which machines      │
# MAGIC                         │  (nodes) are available        │
# MAGIC                         └───────────────┬──────────────┘
# MAGIC                                          │ allocates resources
# MAGIC                                          ▼
# MAGIC   ┌───────────────────────────────────────────────────────────────────┐
# MAGIC   │                              DRIVER                                │
# MAGIC   │  - Runs your main() program / notebook code                        │
# MAGIC   │  - Creates the SparkSession                                        │
# MAGIC   │  - Builds the DAG (Directed Acyclic Graph) of operations           │
# MAGIC   │  - Splits work into JOBS -> STAGES -> TASKS, sends to executors     │
# MAGIC   │  - Collects final results back                                     │
# MAGIC   └───────────────┬───────────────────────┬───────────────┬────────────┘
# MAGIC                   │                       │               │
# MAGIC                   ▼                       ▼               ▼
# MAGIC          ┌─────────────────┐   ┌─────────────────┐  ┌─────────────────┐
# MAGIC          │   EXECUTOR 1    │   │   EXECUTOR 2    │  │   EXECUTOR 3    │
# MAGIC          │  runs TASKS     │   │  runs TASKS     │  │  runs TASKS     │
# MAGIC          │  holds data in  │   │  holds data in  │  │  holds data in  │
# MAGIC          │  memory/disk    │   │  memory/disk    │  │  memory/disk    │
# MAGIC          └─────────────────┘   └─────────────────┘  └─────────────────┘
# MAGIC              (Worker Node 1)       (Worker Node 2)      (Worker Node 3)
# MAGIC ```
# MAGIC
# MAGIC | Component | Role | Real-life analogy |
# MAGIC |---|---|---|
# MAGIC | **Driver** | The "brain" — plans the work, breaks it into tasks, coordinates everything | Restaurant Manager who takes the order and assigns dishes to chefs |
# MAGIC | **Executors** | Worker processes that actually do the computation and hold data in memory/disk | Chefs, each cooking their assigned dish |
# MAGIC | **Cluster Manager** | Allocates machines/resources (CPU, RAM) to the Driver & Executors | Restaurant Owner who decides how many chefs are hired today |
# MAGIC
# MAGIC ### Job → Stage → Task hierarchy (how the Driver breaks down work)
# MAGIC ```
# MAGIC   ACTION (e.g. .show(), .count())
# MAGIC        │
# MAGIC        ▼
# MAGIC      JOB  (one job is triggered per ACTION)
# MAGIC        │
# MAGIC        ├── STAGE 1  (a group of transformations that don't need a SHUFFLE)
# MAGIC        │      ├── Task 1  (runs on partition 1)
# MAGIC        │      ├── Task 2  (runs on partition 2)
# MAGIC        │      └── Task 3  (runs on partition 3)
# MAGIC        │
# MAGIC        └── STAGE 2  (new stage starts whenever data must be SHUFFLED, e.g. groupBy/join)
# MAGIC               ├── Task 1
# MAGIC               └── Task 2
# MAGIC ```
# MAGIC **Key idea — Transformations vs Actions:**
# MAGIC - **Transformations** (select, filter, withColumn...) are **LAZY** — Spark just builds a plan, nothing runs yet.
# MAGIC - **Actions** (show, count, collect, write...) **TRIGGER** actual execution across the cluster.
# MAGIC This laziness lets Spark's Catalyst Optimizer look at your ENTIRE chain of transformations and find the fastest execution plan before running anything — like a GPS calculating the best route before you start driving, instead of turn-by-turn with no overview.

# COMMAND ----------

# In Databricks, a SparkSession called `spark` is ALREADY created for you automatically.
# You do NOT need to create it yourself in a Databricks notebook (unlike plain PySpark scripts).

# Let's inspect the Driver and cluster info to see the architecture in action.
print("Spark Version:", spark.version)                       # Shows the Spark engine version running on the Driver
print("App Name:", spark.sparkContext.appName)                # Name of this Spark Application
print("Master:", spark.sparkContext.master)                   # Shows cluster manager connection info
print("Default Parallelism (approx total cores across executors):",
      spark.sparkContext.defaultParallelism)                  # How many tasks can run in parallel = total cores available

# COMMAND ----------

# ---- See Jobs/Stages/Tasks live: run a transformation chain then trigger an action ----
sample_df = spark.range(1, 1000001)                    # LAZY: just builds a plan for numbers 1 to 1,000,000 (nothing executes yet)
sample_df2 = sample_df.withColumn("double_val", sample_df.id * 2)   # LAZY: still just a plan
sample_df3 = sample_df2.filter(sample_df2.double_val > 500000)       # LAZY: still just a plan

print("Nothing has run yet - transformations are lazy!")

result_count = sample_df3.count()                       # ACTION: THIS triggers a real JOB on the cluster
print("Count of rows where double_val > 500000:", result_count)
# TIP: Open the "Spark UI" tab (top of notebook results) -> Jobs -> you'll see this single .count() created 1 Job with Stages/Tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Databricks Workspace Basics
# MAGIC
# MAGIC | Concept | What it is | Real-life analogy |
# MAGIC |---|---|---|
# MAGIC | **Notebook** | A file made of cells (code / markdown) where you write & run PySpark, SQL, Scala, R | A lab notebook where you write experiments and see results instantly |
# MAGIC | **Cluster** | A group of virtual machines (1 Driver + N Executors) that actually run your code | The kitchen + staff of a restaurant |
# MAGIC | **DBFS** | A distributed file system mounted on your workspace, backed by cloud storage (S3/ADLS) | A shared network drive everyone in the company can access |
# MAGIC | **Unity Catalog** | Centralized governance: organizes tables as `catalog.schema.table`, manages permissions, lineage | A company-wide library catalog tracking who owns/can-access which book (table) |
# MAGIC | **dbutils** | Databricks-only utility library to interact with files, secrets, widgets, notebooks | The Swiss-army-knife toolbox for the workspace |
# MAGIC
# MAGIC ### Unity Catalog hierarchy:
# MAGIC ```
# MAGIC Metastore
# MAGIC   └── Catalog        (e.g. "sales_catalog")
# MAGIC         └── Schema (Database)   (e.g. "retail")
# MAGIC               └── Table          (e.g. "customers")
# MAGIC ```
# MAGIC Fully qualified table name: `sales_catalog.retail.customers`

# COMMAND ----------

# 1) List files in DBFS root - shows you what's available in the distributed file system
display(dbutils.fs.ls("/databricks-datasets"))   # display() renders a nice interactive table (Databricks-only function)

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

# 4) Check current cluster's Spark configuration (driver + executor memory settings, etc.)
for conf in spark.sparkContext.getConf().getAll()[:10]:   # only first 10 settings, there can be 100+
    print(conf)                                            # each conf is a (key, value) tuple

# COMMAND ----------

# 5) See what catalogs/schemas/tables exist (Unity Catalog / Hive Metastore)
for c in spark.catalog.listCatalogs():
    print(c.name)   # lists all catalogs available to you

display(spark.catalog.listDatabases())    # lists schemas/databases in the current catalog

for t in spark.catalog.listTables():
    print(t.name)     # lists tables in the CURRENT database

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. RDD vs DataFrame vs Dataset
# MAGIC
# MAGIC ```
# MAGIC   RDD (Resilient Distributed Dataset)
# MAGIC     - Lowest-level API. A distributed collection of Python/Java/Scala OBJECTS.
# MAGIC     - No schema awareness (Spark doesn't know column names/types).
# MAGIC     - You write manual transformation logic (map, filter, reduce, flatMap...).
# MAGIC     - Slower - no Catalyst optimizer help.
# MAGIC
# MAGIC   DataFrame
# MAGIC     - Distributed collection organized into named COLUMNS (like a SQL table / Excel sheet).
# MAGIC     - Has a SCHEMA. Optimized by Catalyst + Tungsten. THIS COURSE FOCUSES HERE.
# MAGIC
# MAGIC   Dataset
# MAGIC     - Type-safe DataFrame. Scala/Java ONLY. Does NOT exist in PySpark.
# MAGIC ```
# MAGIC
# MAGIC | Feature | RDD | DataFrame | Dataset (Scala/Java only) |
# MAGIC |---|---|---|---|
# MAGIC | Schema | No | Yes | Yes |
# MAGIC | Optimization (Catalyst) | No | Yes | Yes |
# MAGIC | Ease of use | Hard (manual functions) | Easy (SQL-like) | Easy, but Scala/Java only |
# MAGIC | Speed | Slowest | Fast | Fast |
# MAGIC | Available in PySpark? | Yes | Yes | **No** |
# MAGIC
# MAGIC **Real-life analogy:** RDD is like a box of loose, unlabeled items — you must remember what's
# MAGIC inside each item. A DataFrame is like an Excel spreadsheet with column headers.

# COMMAND ----------

# ---- Example A: Same task done with RDD vs DataFrame ----
data = [("Amit", 50000), ("Sneha", 60000), ("Raj", 45000)]

# 1) RDD approach
rdd = spark.sparkContext.parallelize(data)          # Convert Python list into a distributed RDD
rdd_result = rdd.filter(lambda x: x[1] > 48000).map(lambda x: x[0])   # manual lambdas, no column names
print("RDD Result:", rdd_result.collect())          # collect() pulls all distributed data back to Driver



# COMMAND ----------

# 2) DataFrame approach- List of tuple
data = [("Amit", 50000), ("Sneha", 60000), ("Raj", 45000)]
df = spark.createDataFrame(data, ["name", "salary"])
df_result = df.filter(df.salary > 48000).select("name")
df_result.show()

# COMMAND ----------

#DF using list of dict
data = [
    {"name": "Amit", "salary": 50000},
    {"name": "Sneha", "salary": 60000},
    {"name": "Raj", "salary": 45000}
]

df = spark.createDataFrame(data)

df.filter(df.salary > 48000).select("name").show()

# COMMAND ----------

# ---- Example B: More common RDD transformations ----

words_rdd = spark.sparkContext.parallelize([
    "pyspark is fun",
    "spark is fast",
    "pyspark is powerful"
])

# map(): transforms EACH element (1-to-1)
upper_rdd = words_rdd.map(lambda line: line.upper())
print("map() ->", upper_rdd.collect())

# Output:
# map() -> ['PYSPARK IS FUN', 'SPARK IS FAST', 'PYSPARK IS POWERFUL']


# flatMap(): transforms each element into MULTIPLE elements (1-to-many)
split_words = words_rdd.flatMap(lambda line: line.split(" "))
print("flatMap() ->", split_words.collect())

# Output:
# flatMap() ->
# ['pyspark', 'is', 'fun',
#  'spark', 'is', 'fast',
#  'pyspark', 'is', 'powerful']


# reduceByKey(): classic Word Count example
word_pairs = split_words.map(lambda word: (word, 1))
print("Word Pairs ->", word_pairs.collect())

# Output:
# Word Pairs ->
# [('pyspark', 1), ('is', 1), ('fun', 1),
#  ('spark', 1), ('is', 1), ('fast', 1),
#  ('pyspark', 1), ('is', 1), ('powerful', 1)]


word_counts = word_pairs.reduceByKey(lambda a, b: a + b)
print("reduceByKey() Word Count ->", word_counts.collect())

# Output:
# reduceByKey() Word Count ->
# [('pyspark', 2),
#  ('is', 3),
#  ('fun', 1),
#  ('spark', 1),
#  ('fast', 1),
#  ('powerful', 1)]

# COMMAND ----------

from pyspark.sql.functions import col, upper, split, explode, count

# -------------------------------------------------------------------
# Create a DataFrame
# Each tuple represents one row with a single column named "sentence"
# -------------------------------------------------------------------
data = [
    ("pyspark is fun",),
    ("spark is fast",),
    ("pyspark is powerful",)
]

df = spark.createDataFrame(data, ["sentence"])


# -------------------------------------------------------------------
# map() Equivalent
# RDD: map()
# DataFrame: select() with a function
#
# upper() converts every sentence to uppercase.
# A new DataFrame is returned (original DataFrame is unchanged).
# -------------------------------------------------------------------
upper_df = df.select(
    upper(col("sentence")).alias("sentence")
)

display(upper_df)


# -------------------------------------------------------------------
# flatMap() Equivalent
# RDD: flatMap()
# DataFrame: split() + explode()
#
# split() converts each sentence into an array of words.
# explode() creates one row for each word in the array.
# -------------------------------------------------------------------
split_words = df.select(
    explode(
        split(col("sentence"), " ")
    ).alias("word")
)

display(split_words)


# -------------------------------------------------------------------
# reduceByKey() Equivalent
# RDD: reduceByKey()
# DataFrame: groupBy() + agg()
#
# groupBy("word") groups identical words together.
# count("*") counts how many times each word appears.
# -------------------------------------------------------------------
word_counts = (
    split_words
        .groupBy("word")
        .agg(count("*").alias("count"))
)

display(word_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. SparkSession vs SparkContext
# MAGIC
# MAGIC ```
# MAGIC   SparkContext (sc)     - ORIGINAL entry point (pre Spark 2.0). Needed mainly for RDD operations.
# MAGIC   SparkSession (spark)  - Introduced in Spark 2.0. Single unified entry point for
# MAGIC                           DataFrame, SQL, Streaming, Delta Lake, Hive - everything.
# MAGIC ```
# MAGIC **Real-life analogy:** SparkContext is like the electricity connection to a building.
# MAGIC SparkSession is the entire smart-home control panel managing electricity, water,
# MAGIC security — ONE interface for everything.

# COMMAND ----------

# In a plain (non-Databricks) PySpark script, you'd create it manually like this:
#
# from pyspark.sql import SparkSession
# spark = SparkSession.builder \
#             .appName("MyFirstApp") \        # Name shown in Spark UI
#             .master("local[*]") \           # Run locally using all CPU cores (only for local testing)
#             .config("spark.sql.shuffle.partitions", "8") \   # Example: set any Spark config
#             .getOrCreate()                  # Reuses existing session if one exists, else creates new
#
# NOTE: In Databricks, DO NOT run the above manually - `spark` already exists!

sc = spark.sparkContext
print("SparkContext App Name:", sc.appName)
print("SparkSession object:", spark)

# Getting the CURRENTLY active session anywhere in your code (very useful inside functions/modules):
from pyspark.sql import SparkSession
active_spark = SparkSession.getActiveSession()
print("Active session app name:", active_spark.sparkContext.appName)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Creating DataFrames — list, dict, RDD, range, Pandas, empty

# COMMAND ----------

# ---- 5.1 From a LIST of tuples ----
employee_list = [
    (1, "Amit",  "IT",     55000, 28),
    (2, "Sneha", "HR",     48000, 32),
    (3, "Raj",   "Sales",  62000, 25),
    (4, "Priya", "IT",     71000, 30),
    (5, "Karan", "Sales",  39000, 27),
    (6, "Neha",  "HR",     54000, 29),
    (7, "Vikas", "IT",     67000, 35),
    (8, "Meena", "Sales",  45000, 24),
]
columns = ["id", "name", "department", "salary", "age"]
df_from_list = spark.createDataFrame(employee_list, columns)
df_from_list.show()

# COMMAND ----------

# ---- 5.2 From a LIST of DICTIONARIES ----
employee_dicts = [
    {"id": 1, "name": "Amit",  "department": "IT", "salary": 55000},
    {"id": 2, "name": "Sneha", "department": "HR", "salary": 48000},
]
df_from_dict = spark.createDataFrame(employee_dicts)   # Spark infers column names FROM dict keys
df_from_dict.show()

# COMMAND ----------

# ---- 5.3 From an RDD ----
rdd_emp = spark.sparkContext.parallelize(employee_list)
df_from_rdd = rdd_emp.toDF(columns)                      # .toDF() converts RDD -> DataFrame
df_from_rdd.show()

# COMMAND ----------

# ---- 5.4 From a RANGE ----
df_range = spark.range(1, 11)          # single column "id", values 1 to 10
df_range.show(5)

df_range2 = spark.range(0, 20, 2)      # spark.range(start, end, step) -> 0,2,4,...,18
df_range2.show()

# COMMAND ----------

# ---- 5.5 From a PANDAS DataFrame (common when converting existing pandas code to Spark) ----
import pandas as pd
pandas_df = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Amit", "Sneha", "Raj"],
    "salary": [55000, 48000, 62000]
})
df_from_pandas = spark.createDataFrame(pandas_df)        # convert pandas -> Spark DataFrame
df_from_pandas.show()
df_from_pandas.printSchema()

# Reverse: Spark DataFrame -> Pandas (careful! this pulls ALL data to the Driver, only use on small data)
back_to_pandas = df_from_pandas.toPandas()
print(type(back_to_pandas))

# COMMAND ----------

# ---- 5.6 Create an EMPTY DataFrame with a defined schema (useful as a placeholder / accumulator pattern) ----
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

empty_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
])
df_empty = spark.createDataFrame([], schema=empty_schema)   # empty list of rows + a schema
df_empty.show()
df_empty.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Schema — StructType, StructField, Data Types, Nested Schemas

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType,
    ArrayType, MapType
)

# ---- 6.1 Simple flat schema ----
employee_schema = StructType([
    StructField("id",         IntegerType(), False),  # NOT nullable - id must always exist
    StructField("name",       StringType(),  True),
    StructField("department", StringType(),  True),
    StructField("salary",     DoubleType(),  True),
    StructField("age",        IntegerType(), True),
])

employee_data = [
    (1, "Amit",  "IT",    55000.0, 28),
    (2, "Sneha", "HR",    48000.0, 32),
    (3, "Raj",   "Sales", 62000.0, 25),
    (4, "Priya", "IT",    71000.0, 30),
    (5, "Ravi",  "Sales", 58000.0, 35),
    (6, "Karan", "Sales", 39000.0, 27),
]
df_emp = spark.createDataFrame(employee_data, schema=employee_schema)
df_emp.printSchema()
df_emp.show()

# COMMAND ----------

# ---- 6.2 NESTED schema (a column that itself contains multiple sub-fields, like a JSON object) ----
# Real-life example: each employee has an "address" object with street/city/pincode
nested_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("address", StructType([                      # <-- nested StructType inside a StructField
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("pincode", StringType(), True),
    ]), True)
])

nested_data = [
    (1, "Amit", ("MG Road", "Pune", "411001")),
    (2, "Sneha", ("Park Street", "Kolkata", "700016")),
    (3, "Raj",   ("Churchgate", "Mumbai", "400001)")),
    (4, "Priya", ["IT", "SQL", "Spark"]),                   # can also pass a LIST directly
    (5, "Karan", ["Sales", "Data Science", "AWS"]),          # can also pass a LIST directly
    (6, "Neha",  ["HR", "Recruiting", "Talent Acquisition"]),
]
df_nested = spark.createDataFrame(nested_data, schema=nested_schema)
df_nested.show(truncate=False)
df_nested.printSchema()                                       # notice "address" shows as a nested tree

# Accessing a nested field uses DOT notation:
df_nested.select("name", "address.city", "address.pincode").show()

# COMMAND ----------

# ---- 6.3 ArrayType and MapType columns ----
# Real-life example: each employee has multiple "skills" (array) and "ratings" per year (map)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType,
    ArrayType, MapType
)
from pyspark.sql.functions import col
array_map_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("skills", ArrayType(StringType()), True),          # a LIST of strings
    StructField("yearly_rating", MapType(StringType(), IntegerType()), True)  # a DICT: {"2023": 4, "2024": 5}
])

array_map_data = [
    (1, ["Python", "SQL", "Spark"], {"2023": 4, "2024": 5}),
    (2, ["Java", "Scala"],          {"2023": 3, "2024": 4}),
]
df_array_map = spark.createDataFrame(array_map_data, schema=array_map_schema)
df_array_map.show(truncate=False)
df_array_map.printSchema()
# Select the first element (index starts from 0)
df_array_map.select(
    col("skills")[1].alias("second_skill")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Common PySpark Data Types** (from `pyspark.sql.types`):
# MAGIC
# MAGIC | Type | Python equivalent | Example |
# MAGIC |---|---|---|
# MAGIC | StringType | str | "Amit" |
# MAGIC | IntegerType | int (32-bit) | 28 |
# MAGIC | LongType | int (64-bit) | 9999999999 |
# MAGIC | DoubleType | float | 55000.50 |
# MAGIC | FloatType | float (32-bit) | 3.14 |
# MAGIC | BooleanType | bool | True/False |
# MAGIC | DateType | datetime.date | 2024-01-15 |
# MAGIC | TimestampType | datetime.datetime | 2024-01-15 10:30:00 |
# MAGIC | ArrayType(elementType) | list | [1,2,3] |
# MAGIC | MapType(keyType,valType) | dict | {"a":1} |
# MAGIC | StructType | nested object | {"street":"MG Road","city":"Pune"} |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. show(), display(), printSchema(), describe(), summary()

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType,
    ArrayType, MapType
)

# ---- 6.1 Simple flat schema ----
employee_schema = StructType([
    StructField("id",         IntegerType(), False),  # NOT nullable - id must always exist
    StructField("name",       StringType(),  True),
    StructField("department", StringType(),  True),
    StructField("salary",     DoubleType(),  True),
    StructField("age",        IntegerType(), True),
])

employee_data = [
    (1, "Amit",  "IT",    55000.0, 28),
    (2, "Sneha", "HR",    48000.0, 32),
    (3, "Raj",   "Sales", 62000.0, 25),
    (4, "Priya", "IT",    71000.0, 30),
    (5, "Ravi",  "Sales", 58000.0, 35),
    (6, "Karan", "Sales", 39000.0, 27),
]
df_emp = spark.createDataFrame(employee_data, schema=employee_schema)
df_emp.printSchema()
df_emp.show()

# COMMAND ----------

df_emp.show()                 # default: top 20 rows, truncates long strings
df_emp.show(2)                 # only top 2 rows
df_emp.show(truncate=False)    # full column content, no truncation
df_emp.show(2, vertical=True)  # NEW: prints each row VERTICALLY (great for wide tables with many columns)

# COMMAND ----------

display(df_emp)   # Databricks-only: interactive UI table - sort columns, add a chart, download as CSV

# COMMAND ----------

df_emp.printSchema()   # Tree-style: column names + data types + nullable flag

# COMMAND ----------

df_emp.describe().show()                 # count, mean, stddev, min, max for numeric columns
df_emp.describe("salary", "age").show()   # NEW: run describe() on SPECIFIC columns only

# COMMAND ----------

df_emp.summary().show()                                          # describe() PLUS 25/50/75 percentiles
df_emp.summary("count", "min", "25%", "50%", "75%", "max").show() # pick only specific stats

# COMMAND ----------

# More useful inspection tricks:
print("Row count:", df_emp.count())                # total number of rows (an ACTION)
print("Column count:", len(df_emp.columns))         # number of columns
print("Column names:", df_emp.columns)              # list of column name strings
print("Data types:", df_emp.dtypes)                 # list of (column_name, type) tuples
df_emp.select("department").distinct().show()        # unique values in a column
print("Is DataFrame empty?", df_emp.isEmpty())        # quick check if 0 rows (Spark 3.3+)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. select, filter/where, withColumn, drop, alias — WITH MANY MORE EXAMPLES

# COMMAND ----------

# MAGIC %md ### 8.1 select() — more patterns

# COMMAND ----------

df_emp.select("name", "department").show()                 # keep specific columns
df_emp.select(df_emp.name, df_emp.salary).show()             # dot-notation
df_emp.select(df_emp["name"], df_emp["salary"]).show()        # bracket-notation (useful when column name has spaces/special chars)
df_emp.select("*").show()                                      # select ALL columns (like SQL SELECT *)
df_emp.select([c for c in df_emp.columns if c != "age"]).show()  # select all columns EXCEPT one, using a Python list comprehension

# Create an empty list
cols = []

# Add all columns except "age"
for c in df_emp.columns:
    if c != "age":
        cols.append(c)

# Select those columns
df_emp.select(cols).show()

# COMMAND ----------

# MAGIC %md ### 8.2 filter() / where() — many condition types

# COMMAND ----------

df_emp.filter(df_emp.salary > 50000).show()                          # greater than
df_emp.where(df_emp.department == "IT").show()                        # equal to
df_emp.filter(df_emp.department != "IT").show()                       # not equal to
df_emp.filter((df_emp.department == "IT") & (df_emp.salary > 60000)).show()  # AND
df_emp.filter((df_emp.department == "IT") | (df_emp.department == "HR")).show()  # OR
df_emp.filter(~(df_emp.department == "IT")).show()                     # NOT (everyone NOT in IT)

df_emp.filter(df_emp.department.isin("IT", "HR")).show()               # isin() -> matches ANY value in the list
df_emp.filter(df_emp.name.like("A%")).show()                            # like() -> SQL-style pattern match (names starting with "A")
df_emp.filter(df_emp.salary.between(45000, 65000)).show()               # between() -> inclusive range check
df_emp.filter(df_emp.age.isNull()).show()                                # rows where age IS NULL
df_emp.filter(df_emp.age.isNotNull()).show()                             # rows where age IS NOT NULL
df_emp.filter("salary > 50000 AND department = 'IT'").show()             # filter using a raw SQL-string condition (alternative style)

# COMMAND ----------

# MAGIC %md ### 8.3 withColumn() — many transformation patterns

# COMMAND ----------

from pyspark.sql.functions import col, when, round as spark_round, concat, lit, upper, lower

# Add a new calculated column
df_emp.withColumn("bonus", col("salary") * 0.10).show()

# Add MULTIPLE new columns by chaining withColumn() calls
df_multi = (
    df_emp
    .withColumn("bonus", col("salary") * 0.10)                      # 10% bonus
    .withColumn("total_pay", col("salary") + col("salary") * 0.10)   # salary + bonus
    .withColumn("total_pay_rounded", spark_round(col("total_pay"), 2)) # round to 2 decimals
)
df_multi.show()

# withColumn + when/otherwise -> conditional (like CASE WHEN / IF-ELSE) column
df_emp.withColumn(
    "salary_band",
    when(col("salary") >= 60000, "High")          # IF salary >= 60000
    .when(col("salary") >= 48000, "Medium")        # ELSE IF salary >= 48000
    .otherwise("Low")                                # ELSE
).show()

# withColumn + string functions: build a full display label
df_emp.withColumn("employee_label", concat(upper(col("name")), lit(" - "), col("department"))).show(truncate=False)

# Modify (overwrite) an EXISTING column - cast age to double
df_emp.withColumn("age", col("age").cast("double")).printSchema()

# COMMAND ----------

# MAGIC %md ### 8.4 drop() — more patterns

# COMMAND ----------

df_emp.drop("age").show()                       # drop ONE column
df_emp.drop("age", "department").show()          # drop MULTIPLE columns at once

df_dup_col = df_emp.withColumn("id_copy", col("id"))
df_dup_col.drop(df_dup_col.id_copy).show()        # drop using column object reference (useful after joins with duplicate names)

# COMMAND ----------

# MAGIC %md ### 8.5 alias() — more patterns

# COMMAND ----------

df_emp.select(
    col("name").alias("employee_name"),
    col("salary").alias("monthly_salary"),
    (col("salary") * 12).alias("annual_salary")     # alias also works on computed expressions
).show()

# withColumnRenamed() -> permanently renames a column ON the DataFrame
df_renamed = df_emp.withColumnRenamed("name", "employee_name").withColumnRenamed("salary", "monthly_salary")
df_renamed.show()

# Renaming MANY columns at once using a dictionary + reduce pattern
rename_map = {"id": "emp_id", "name": "emp_name", "department": "dept"}
df_bulk_renamed = df_emp
for old_name, new_name in rename_map.items():         # loop through dictionary, renaming one at a time
    df_bulk_renamed = df_bulk_renamed.withColumnRenamed(old_name, new_name)
df_bulk_renamed.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Column Expressions: col(), expr(), lit()
# MAGIC
# MAGIC | Function | Purpose | Example |
# MAGIC |---|---|---|
# MAGIC | `col("name")` | Reference an existing column by name | `col("salary") > 50000` |
# MAGIC | `expr("...")` | SQL-like STRING expression parsed & run by Spark | `expr("salary * 1.1")` |
# MAGIC | `lit(value)` | CONSTANT/LITERAL column — same value in every row | `lit("India")` |

# COMMAND ----------

from pyspark.sql.functions import col, expr, lit

df_emp.select(col("name"), col("salary")).show()
df_emp.filter(col("salary") > 50000).show()

# expr(): SQL-style string expressions
df_emp.select(col("name"), expr("salary * 1.10 AS salary_after_hike")).show()
df_emp.select(col("name"), expr("CASE WHEN salary > 60000 THEN 'High' ELSE 'Normal' END AS salary_band")).show()
df_emp.filter(expr("salary > 50000 AND department = 'IT'")).show()      # expr() also works inside filter()

# lit(): constants
df_emp.withColumn("country", lit("India")).show()
df_emp.withColumn("tax_rate", lit(0.10)).show()
df_emp.withColumn("tax_amount", col("salary") * lit(0.10)).show()

from pyspark.sql.functions import array
df_emp.withColumn("fixed_tags", array(lit("employee"), lit("active"))).show(truncate=False)   # lit() inside array() -> constant array column

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎯 PRACTICE QUESTIONS — Part 1
# MAGIC Try to solve each question YOURSELF in a blank cell before scrolling to the solution cell below it.
# MAGIC All questions use this **Employee dataset** (re-created below so it's always available).

# COMMAND ----------

# Practice dataset - run this cell first, all questions below use "df_practice"
practice_data = [
    (1, "Amit",  "IT",     55000, 28, "Pune"),
    (2, "Sneha", "HR",     48000, 32, "Mumbai"),
    (3, "Raj",   "Sales",  62000, 25, "Delhi"),
    (4, "Priya", "IT",     71000, 30, "Pune"),
    (5, "Karan", "Sales",  39000, 27, "Delhi"),
    (6, "Neha",  "HR",     54000, 29, "Mumbai"),
    (7, "Vikas", "IT",     67000, 35, None),
    (8, "Meena", "Sales",  45000, 24, "Delhi"),
    (9, "Ravi",  "IT",     58000, None, "Pune"),
    (10,"Alka",  "HR",     51000, 26, "Mumbai"),
]
practice_columns = ["id", "name", "department", "salary", "age", "city"]
df_practice = spark.createDataFrame(practice_data, practice_columns)
df_practice.show()

# COMMAND ----------

# MAGIC %md
# MAGIC | Q# | Question | Columns Expected in Output |
# MAGIC |---|---|---|
# MAGIC | 1 | Select only `name` and `city` columns | name, city |
# MAGIC | 2 | Find all employees in the "IT" department | id, name, department, salary, age, city |
# MAGIC | 3 | Find employees with salary between 45000 and 60000 | id, name, department, salary, age, city |
# MAGIC | 4 | Find employees whose name starts with letter "A" | id, name, department, salary, age, city |
# MAGIC | 5 | Find employees who are in "IT" OR "HR" department | id, name, department, salary, age, city |
# MAGIC | 6 | Add a new column `annual_salary` = salary * 12 | ..., annual_salary |
# MAGIC | 7 | Add a column `seniority` : 'Senior' if age > 30 else 'Junior' | ..., seniority |
# MAGIC | 8 | Rename `salary` column to `monthly_salary` | id, name, department, monthly_salary, age, city |
# MAGIC | 9 | Drop the `city` column entirely | id, name, department, salary, age |
# MAGIC | 10 | Find employees whose `city` is NULL (missing) | id, name, department, salary, age, city |
# MAGIC | 11 | Find employees whose `age` is NULL, and replace missing age with 0 in a new column `age_filled` | ..., age_filled |
# MAGIC | 12 | Create a column `full_label` = "NAME (DEPARTMENT)" in uppercase e.g. "AMIT (IT)" | ..., full_label |
# MAGIC | 13 | Select all columns EXCEPT `salary` and `age` | id, name, department, city |
# MAGIC | 14 | Using `expr()`, create column `tax` = 10% of salary, rounded to 2 decimals | ..., tax |
# MAGIC | 15 | Find employees NOT in the "Sales" department, sorted logic not needed — just filter | id, name, department, salary, age, city |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ SOLUTIONS
# MAGIC (Scroll up and try first! Solutions are below, one per cell, clearly numbered.)

# COMMAND ----------

# Q1: Select only name and city columns
df_practice.select("name", "city").show()

# COMMAND ----------

# Q2: Employees in IT department
df_practice.filter(col("department") == "IT").show()

# COMMAND ----------

# Q3: Salary between 45000 and 60000
df_practice.filter(col("salary").between(45000, 60000)).show()

# COMMAND ----------

# Q4: Names starting with "A"
df_practice.filter(col("name").like("A%")).show()

# COMMAND ----------

# Q5: Department is IT or HR
df_practice.filter(col("department").isin("IT", "HR")).show()

# COMMAND ----------

# Q6: Add annual_salary = salary * 12
df_practice.withColumn("annual_salary", col("salary") * 12).show()

# COMMAND ----------

# Q7: seniority = 'Senior' if age > 30 else 'Junior'
df_practice.withColumn(
    "seniority",
    when(col("age") > 30, "Senior").otherwise("Junior")
).show()

# COMMAND ----------

# Q8: Rename salary -> monthly_salary
df_practice.withColumnRenamed("salary", "monthly_salary").show()

# COMMAND ----------

# Q9: Drop the city column
df_practice.drop("city").show()

# COMMAND ----------

# Q10: Employees where city IS NULL
df_practice.filter(col("city").isNull()).show()

# COMMAND ----------

# Q11: Replace missing age with 0 in a NEW column age_filled (keeps original "age" untouched)
from pyspark.sql.functions import coalesce
df_practice.withColumn("age_filled", coalesce(col("age"), lit(0))).show()
# coalesce(col_A, col_B) returns col_A's value if it's NOT null, otherwise falls back to col_B (here, the constant 0)

# COMMAND ----------

# Q12: full_label = "NAME (DEPARTMENT)" in uppercase, e.g. "AMIT (IT)"
df_practice.withColumn(
    "full_label",
    upper(concat(col("name"), lit(" ("), col("department"), lit(")")))
).show(truncate=False)

# COMMAND ----------

# Q13: Select all columns except salary and age
cols_to_keep = [c for c in df_practice.columns if c not in ("salary", "age")]
df_practice.select(cols_to_keep).show()

# COMMAND ----------

# Q14: Using expr(), tax = 10% of salary rounded to 2 decimals
df_practice.select("*", expr("ROUND(salary * 0.10, 2) AS tax")).show()

# COMMAND ----------

# Q15: Employees NOT in Sales department
df_practice.filter(col("department") != "Sales").show()
# Alternative using ~ (NOT) operator:
df_practice.filter(~(col("department") == "Sales")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Part 1 Summary — What You Learned
# MAGIC - Spark's distributed architecture: **Driver, Executors, Cluster Manager**, and the **Job → Stage → Task** execution model
# MAGIC - **Lazy transformations vs eager actions** — why this makes Spark fast
# MAGIC - Databricks workspace essentials: **Notebooks, Clusters, DBFS, Unity Catalog, dbutils, widgets**
# MAGIC - Why **DataFrames** beat RDDs, plus classic RDD patterns (map, flatMap, reduceByKey)
# MAGIC - **SparkSession** as your single entry point
# MAGIC - 6 ways to **create DataFrames**: list, dict, RDD, range, Pandas, empty-with-schema
# MAGIC - **Schemas**: flat, nested (StructType-in-StructType), ArrayType, MapType
# MAGIC - Deep inspection: **show, display, printSchema, describe, summary, dtypes, isEmpty**
# MAGIC - Extensive transformation practice: **select, filter/where (isin/like/between/isNull), withColumn (when/otherwise, chaining), drop, alias/withColumnRenamed**
# MAGIC - Column tools: **col(), expr(), lit()**
# MAGIC - **15 hands-on practice questions with full solutions**
# MAGIC
# MAGIC ### 🔜 Coming in Part 2: Reading & Writing Data
# MAGIC CSV/JSON/Parquet/ORC read & write, read options, partitioned I/O, JDBC, Delta Lake reads,
# MAGIC DBFS mounting & Unity Catalog volumes — with the same style: full examples + practice questions.
