
# Proyecto ETL con Arquitectura Medallion usando Azure Databricks

Este repositorio contiene un proyecto de ejemplo para demostrar el uso de la arquitectura Medallion (Bronze, Silver, Gold)
implementada con PySpark en Azure Databricks. El dataset utilizado es `Ecommerce_Sales_Prediction_Dataset.csv` descargado desde Kaggle.

## Estructura del Proyecto

```
.
├── notebooks/
│   ├── 1_bronze_etl.py
│   ├── 2_silver_etl.py
│   └── 3_gold_etl.py
├── data/
│   └── Ecommerce_Sales_Prediction_Dataset.csv
├── ETL_Medallion_Databricks_Roberto.pptx
└── README.md
```

## Arquitectura

El proyecto sigue el enfoque de la arquitectura Medallion:

- **Raw**: El dataset original se almacena en un Azure Storage Account (contenedor: `raw`).
- **Bronze**: Datos crudos cargados en formato Delta Lake desde el CSV.
- **Silver**: Datos limpios y transformados con tipos correctos.
- **Gold**: Datos agregados y modelados para análisis.

## Herramientas Usadas

- Azure Storage Account (con HNS activado)
- Azure Databricks (Unity Catalog habilitado)
- PySpark
- Power BI (conexión directa vía token y Databricks SQL endpoint)
- Panel de power bi publico https://app.powerbi.com/links/xMIb8e_wNg?ctid=9095890d-5421-4305-8de7-9346ffcf0a9d&pbi_source=linkShare
- GitHub (para versionado y documentación)
- Databricks Dashboards
- Panel publico de databricks: https://adb-4339558086818032.12.azuredatabricks.net/dashboardsv3/01f07b4178d615cea10839c4d5c6a6a4/published?o=4339558086818032
  
<img width="1382" height="49" alt="image" src="https://github.com/user-attachments/assets/b9d0aa9d-e9b5-49c4-8e6a-14028660507c" />


## Notebooks

- `bronze_etl.py`: Lectura y carga inicial desde CSV.
- `silver_etl.py`: Limpieza y transformación de datos.
- `gold_etl.py`: Agregación y generación de métricas.

## Visualización

El dataset final (Gold) fue conectado a Power BI para crear visualizaciones de ventas por categoría y producto.

---

**Autor:** Roberto Herrera (2025)  
**Licencia:** MIT

