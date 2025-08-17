# Databricks notebook source
# MAGIC %md
# Gold Layer - para reportes

# MAGIC %python
from pyspark.sql.functions import sum, round, col

# Cargar desde Silver
df_silver = spark.read.table("databricksarea.silver.ecommerce_sales_silver")

# AGRUPAR VENTAS POR CATEGORÍA
df_gold = (
    df_silver
    .groupBy("Product_Category")
    .agg(
        round(sum("Total_Sales"), 2).alias("Total_Sales"),
        round(sum("Units_Sold"), 0).alias("Total_Units_Sold")
    )
    .orderBy(col("Total_Sales").desc())
)

# Guardar como tabla Gold
df_gold.write.mode("overwrite").format("delta") \
    .saveAsTable("databricksarea.gold.ecommerce_sales_gold")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- %sql
# MAGIC SELECT Product_Category, Total_Sales
# MAGIC FROM databricksarea.gold.ecommerce_sales_gold
# MAGIC ORDER BY Total_Sales DESC
# MAGIC