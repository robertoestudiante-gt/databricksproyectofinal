# Databricks notebook source
# MAGIC %md
# 🪙 Silver Layer - Limpieza y Transformación

# MAGIC %python
from pyspark.sql.functions import col

# Cargar desde la tabla Bronze
df_bronze = spark.read.table("databricksarea.bronze.ecommerce_sales_bronze")

# LIMPIEZAS Y TRANSFORMACIONES
df_silver = (
    df_bronze
    .dropDuplicates()
    .dropna(subset=["Product_Category", "Price", "Units_Sold"])  # columnas esenciales
    .withColumn("Price", col("Price").cast("double"))
    .withColumn("Units_Sold", col("Units_Sold").cast("int"))
    .withColumn("Total_Sales", (col("Price") * col("Units_Sold")).cast("double"))
)

# Guardar como tabla en capa Silver
df_silver.write.mode("overwrite").format("delta") \
    .saveAsTable("databricksarea.silver.ecommerce_sales_silver")

