# Databricks notebook source

# ============================================================
# PROYECTO: SMARTDATA E-COMMERCE
# NOTEBOOK: 01_Ingesta_Raw_Bronze
# OBJETIVO: Ingestar archivos CSV desde ADLS Gen2 a Bronze
# ============================================================

from pyspark.sql import functions as F
from datetime import datetime

# 1. CONFIGURACION GENERAL

CATALOGO = "smartdata_dev"
ESQUEMA = "bronze"

RUTA_RAW = (
    "abfss://raw@stsmartdatacomercial01."
    "dfs.core.windows.net/ecommerce/"
)

# Archivos originales y tablas destino
ARCHIVOS = {
    "aisles.csv": "aisles",
    "departments.csv": "departments",
    "order_products__prior.csv": "order_products_prior",
    "order_products__train.csv": "order_products_train",
    "orders.csv": "orders",
    "products.csv": "products",
    "sample_submission.csv": "sample_submission"
}

print("======================================")
print("SMARTDATA - INGESTA RAW A BRONZE")
print("======================================")
print("Catalogo:", CATALOGO)
print("Esquema:", ESQUEMA)
print("Origen:", RUTA_RAW)
print("Archivos:", len(ARCHIVOS))

# 2. VALIDAR CATALOGO Y ESQUEMA

spark.sql(f"USE CATALOG `{CATALOGO}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOGO}`.`{ESQUEMA}`")

print("Catalogo y esquema disponibles.")

# 3. FUNCION DE INGESTA

def ingestar_archivo(nombre_archivo, nombre_tabla):

    ruta_archivo = RUTA_RAW + nombre_archivo

    tabla_destino = (
        f"{CATALOGO}.{ESQUEMA}.{nombre_tabla}"
    )

    print("\n--------------------------------------")
    print("Procesando:", nombre_archivo)
    print("Destino:", tabla_destino)

    # Leer archivo CSV desde ADLS Gen2
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .load(ruta_archivo)
    )

    # Metadatos de trazabilidad
    df_bronze = (
        df
        .withColumn(
            "fecha_ingesta",
            F.current_timestamp()
        )
        .withColumn(
            "archivo_origen",
            F.lit(nombre_archivo)
        )
        .withColumn(
            "ruta_origen",
            F.lit(ruta_archivo)
        )
    )

    # Guardar tabla Delta administrada
    # Overwrite permite repetir la carga inicial
    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabla_destino)
    )

    # Validar registros almacenados
    total = spark.table(tabla_destino).count()

    print("Estado: CORRECTO")
    print("Registros:", total)

    return {
        "archivo": nombre_archivo,
        "tabla": tabla_destino,
        "registros": total,
        "estado": "OK"
    }


# 4. EJECUTAR INGESTA DE LOS 7 DATASETS

resultados = []

for archivo, tabla in ARCHIVOS.items():

    try:
        resultado = ingestar_archivo(
            archivo,
            tabla
        )

        resultados.append(resultado)

    except Exception as e:

        print("ERROR:", archivo)
        print(str(e))

        resultados.append({
            "archivo": archivo,
            "tabla": f"{CATALOGO}.{ESQUEMA}.{tabla}",
            "registros": None,
            "estado": "ERROR"
        })


# 5. RESUMEN DE EJECUCION

df_resultados = spark.createDataFrame(
    resultados
)

print("\n======================================")
print("RESUMEN DE INGESTA BRONZE")
print("======================================")

display(df_resultados)

errores = [
    r for r in resultados
    if r["estado"] == "ERROR"
]

if errores:
    raise RuntimeError(
        f"La ingesta termino con {len(errores)} errores."
    )

print("INGESTA BRONZE COMPLETADA")


# COMMAND ----------

# Verificar registros originales del archivo products.csv

RUTA_PRODUCTOS = (
    "abfss://raw@stsmartdatacomercial01."
    "dfs.core.windows.net/ecommerce/products.csv"
)

# Leer líneas originales sin interpretar columnas
df_raw = spark.read.text(RUTA_PRODUCTOS)

# Buscar los productos afectados
display(
    df_raw.filter(
        df_raw.value.rlike(
            r"^(404|2449|5934|16435|22958|23394|35348|36421|37093|40034|44425);"
        )
    )
)