# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum, expr, lpad, avg
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Load all the chunks using a wildcard
df = spark.read.json("/FileStore/tables/chunk_*.json")

# Show schema and sample data
df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_extract, count

# Define a regex pattern to capture the various common misspellings
pattern = "(ÎNTREPRINDERE|iNTREPRINDERE|INTREPRINDERE|INTEPRINDERE|ÎNREPRINDERE|ÎNTREPRIDERE|ÎNTREPRINDRE|ÎNTREPRINERE|INTREPRINDEREA)"

# Filter names containing any of the variants
__dw_df_0_in = df
__dw_df_0 = __dw_df_0_in.filter(col("nume").rlike(pattern))

# Extract the matched spelling variant
__dw_df_1 = __dw_df_0.withColumn("spelling_variant", regexp_extract("nume", pattern, 0))

# Group by variant and count
__dw_df_agg = __dw_df_1.groupBy("spelling_variant").agg(count("*").alias("count"))

# Display as pie chart
display(__dw_df_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Flatten the years nested structs

# COMMAND ----------

years = ["2018", "2019", "2020", "2021", "2022", "2023"]

dfs = []

for year in years:
    df_year = df.select(
        col("cui"),
        col("nume"),
        col("adresa.judet").alias("judet"), 
        col("adresa.localitate").alias("localitate"), 
        col(f"bilant.`{year}`.*")    # Flatten the struct
    ).withColumn("year", lit(year))  # Create new column with year as literal
    dfs.append(df_year)

# Union all years
df_flat = dfs[0]
for df_year in dfs[1:]:
    df_flat = df_flat.unionByName(df_year)

display(df_flat)


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Geo/ location grouped analysis

# COMMAND ----------

geo_group_df = df.groupBy("adresa.judet").count().orderBy("count", ascending=False)

display(geo_group_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Profit/ loss per year

# COMMAND ----------

profit_loss_df = df_flat \
    .filter((col("profitNet").isNotNull()) | (col("pierdereNet").isNotNull())) \
    .select("cui", "nume", "year", "profitNet", "pierdereNet") \

display(profit_loss_df)

# COMMAND ----------

# Exclude year 2020
df_profit_loss_geo = df_flat.filter(col("year") != "2020") \
    .groupBy("judet", "year") \
    .agg(
        avg("profitNet").alias("total_profit"),
        avg("pierdereNet").alias("total_loss"),
        avg("cifraAfaceri")
    ) \
    .orderBy("judet", "year")

# Add a net_result column (profit - loss)
df_profit_loss_geo = df_profit_loss_geo.withColumn(
    "net_result", expr("total_profit - total_loss")
)

display(df_profit_loss_geo)

# COMMAND ----------

# Step 3: Aggregate net_result across all years per judet
df_top_judete = df_profit_loss_geo.groupBy("judet").agg(
    avg("avg(cifraAfaceri)").alias("cifra_afaceri")
).orderBy(col("cifra_afaceri").desc()).limit(10)

display(df_top_judete)

# COMMAND ----------

# 1. Load the CAEN CSV and rename column to avoid ambiguity
df_caen = (
    spark.read
    .option("header", True)
    .option("delimiter", ";")
    .csv("/FileStore/tables/caen_map.csv")
    .withColumnRenamed("caen", "caen_descr")
)

# 2. Normalize CAEN code in df_flat: cast to int, then string
df_flat = df_flat.withColumn("caen_int", col("caen").cast(IntegerType()))
df_flat = df_flat.withColumn("caen_str", col("caen_int").cast(StringType()))

# 3. Join on normalized caen_str == caen_descr
df_with_descriere = df_flat.join(
    df_caen,
    df_flat["caen_str"] == df_caen["caen_descr"],
    how="left"
)

# 4. Group by industry code and description
df_profit_by_industry = df_with_descriere.groupBy("caen_str", "descriere").agg(
    avg("profitNet").alias("total_profit")
).orderBy(col("total_profit").desc())

# 5. Display results
display(df_profit_by_industry)

# COMMAND ----------


# First 10 industries by total profit (least profitable)
df_least_profitable = df_profit_by_industry.orderBy(col("total_profit").asc()).limit(10)

# Last 10 industries by total profit (most profitable)
df_most_profitable = df_profit_by_industry.orderBy(col("total_profit").desc()).limit(10)

# Show both DataFrames
print("Least Profitable Industries:")
display(df_least_profitable)

print("Most Profitable Industries:")
display(df_most_profitable)