# Databricks notebook source
"""
ETL - CAPA BRONZE
Dataset: Ecommerce_Sales_Prediction_Dataset.csv
Ubicación: abfss://raw@cuentarcalmacenamiento.dfs.core.windows.net/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Crear la sesión de Spark
spark = SparkSession.builder.appName("ETL_Bronze").getOrCreate()

# Ruta de origen
raw_path = "abfss://raw@cuentarcalmacenamiento.dfs.core.windows.net/Ecommerce_Sales_Prediction_Dataset.csv"

# Leer el archivo CSV desde el external location
df_raw = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(raw_path)

# Mostrar algunas filas del DataFrame
display(df_raw)

# Escribir la capa Bronze en formato Delta
df_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("databricksarea.bronze.ecommerce_sales_bronze")

print("✅ Datos cargados en la capa Bronze correctamente")

# COMMAND ----------

# BRONZE - Cargar CSV desde almacenamiento en formato Delta

from pyspark.sql.functions import *

file_path = "abfss://raw@cuentarcalmacenamiento.dfs.core.windows.net/Ecommerce_Sales_Prediction_Dataset.csv"

# 1. Leer CSV desde Data Lake
df_bronze = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(file_path)
)

# 2. Mostrar los datos
display(df_bronze)

# 3. Guardar en tabla Delta en el esquema bronze
df_bronze.write.format("delta").mode("overwrite").saveAsTable("databricksarea.bronze.ecommerce_sales_bronze")


# COMMAND ----------

df_bronze = spark.table("databricksarea.bronze.ecommerce_sales_bronze")
display(df_bronze)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Product_Category, SUM(Price*Units_Sold) as Total_Sales
# MAGIC FROM databricksarea.bronze.ecommerce_sales_bronze
# MAGIC GROUP BY Product_Category
# MAGIC ORDER BY Total_Sales DESC
# MAGIC