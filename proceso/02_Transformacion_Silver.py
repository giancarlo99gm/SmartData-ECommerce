# Databricks notebook source

# ============================================================
# PROYECTO: SMARTDATA E-COMMERCE
# NOTEBOOK: 02_Transformacion_Silver
# OBJETIVO: Limpieza, tipificacion y calidad de datos
# TECNOLOGIA: Azure Databricks + PySpark + Delta Lake
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.window import Window

# ============================================================
# 1. CONFIGURACION GENERAL
# ============================================================


# Configuración del ambiente DEV / PROD
dbutils.widgets.text("catalogo", "smartdata_dev")

CATALOGO = dbutils.widgets.get("catalogo")

if CATALOGO not in ["smartdata_dev", "smartdata_prod"]:
    raise ValueError(
        f"Catálogo no permitido: {CATALOGO}"
    )

print(f"Catálogo seleccionado: {CATALOGO}")

ESQUEMA_ORIGEN = "bronze"
ESQUEMA_DESTINO = "silver"

spark.sql(f"USE CATALOG `{CATALOGO}`")

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"`{CATALOGO}`.`{ESQUEMA_DESTINO}`"
)

print("======================================")
print("SMARTDATA - TRANSFORMACION SILVER")
print("======================================")

# ============================================================
# 2. CONFIGURACION DE TABLAS Y TIPOS DE DATOS
# ============================================================

CONFIG_TABLAS = {

    "aisles": {
        "enteros": ["aisle_id"],
        "decimales": [],
        "claves": ["aisle_id"]
    },

    "departments": {
        "enteros": ["department_id"],
        "decimales": [],
        "claves": ["department_id"]
    },

    "products": {
        "enteros": [
            "product_id",
            "aisle_id",
            "department_id"
        ],
        "decimales": [],
        "claves": ["product_id"]
    },

    "orders": {
        "enteros": [
            "order_id",
            "user_id",
            "order_number",
            "order_dow",
            "order_hour_of_day"
        ],
        "decimales": [
            "days_since_prior_order"
        ],
        "claves": ["order_id"]
    },

    "order_products_prior": {
        "enteros": [
            "order_id",
            "product_id",
            "add_to_cart_order",
            "reordered"
        ],
        "decimales": [],
        "claves": ["order_id", "product_id"]
    },

    "order_products_train": {
        "enteros": [
            "order_id",
            "product_id",
            "add_to_cart_order",
            "reordered"
        ],
        "decimales": [],
        "claves": ["order_id", "product_id"]
    },

    "sample_submission": {
        "enteros": ["order_id"],
        "decimales": [],
        "claves": ["order_id"]
    }
}

# ============================================================
# 3. FUNCION DE LIMPIEZA Y TRANSFORMACION
# ============================================================

def transformar_silver(nombre_tabla, configuracion):

    origen = (
        f"{CATALOGO}.{ESQUEMA_ORIGEN}.{nombre_tabla}"
    )

    destino = (
        f"{CATALOGO}.{ESQUEMA_DESTINO}.{nombre_tabla}"
    )

    print("\n--------------------------------------")
    print("Procesando:", nombre_tabla)

    # Leer datos Bronze
    df = spark.table(origen)

    # Normalizar nombres de columnas
    for columna in df.columns:

        nuevo_nombre = columna.strip().lower()

        if columna != nuevo_nombre:
            df = df.withColumnRenamed(
                columna,
                nuevo_nombre
            )

    # Limpiar espacios y cadenas vacias
    for columna, tipo in df.dtypes:

        if tipo == "string":

            df = df.withColumn(
                columna,
                F.when(
                    F.trim(F.col(columna)) == "",
                    F.lit(None)
                ).otherwise(
                    F.trim(F.col(columna))
                )
            )

    # Validar columnas esperadas
    columnas_requeridas = (
        configuracion["enteros"]
        + configuracion["decimales"]
        + configuracion["claves"]
    )

    faltantes = sorted(
        set(columnas_requeridas) - set(df.columns)
    )

    if faltantes:
        raise ValueError(
            f"Columnas faltantes en {nombre_tabla}: "
            f"{faltantes}"
        )

    # Convertir campos enteros
    for columna in configuracion["enteros"]:

        df = df.withColumn(
            columna,
            F.col(columna).cast(IntegerType())
        )

    # Convertir campos decimales
    for columna in configuracion["decimales"]:

        df = df.withColumn(
            columna,
            F.col(columna).cast(DoubleType())
        )

    # Eliminar registros con claves nulas
    for clave in configuracion["claves"]:

        df = df.filter(
            F.col(clave).isNotNull()
        )

    # Eliminar registros completamente duplicados
    df = df.dropDuplicates()

    # Deduplicar por clave de negocio.
    # Se conserva el registro con mayor fecha de ingesta.
    claves = configuracion["claves"]

    if "fecha_ingesta" in df.columns:

        ventana = (
            Window.partitionBy(*claves)
            .orderBy(
                F.col("fecha_ingesta").desc_nulls_last()
            )
        )

        df = (
            df.withColumn(
                "_fila",
                F.row_number().over(ventana)
            )
            .filter(F.col("_fila") == 1)
            .drop("_fila")
        )

    else:

        df = df.dropDuplicates(claves)

    # Agregar fecha de procesamiento
    df = df.withColumn(
        "fecha_procesamiento",
        F.current_timestamp()
    )

    # Guardar en formato Delta
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(destino)
    )

    total = spark.table(destino).count()

    print("Estado: CORRECTO")
    print("Registros Silver:", total)

    return {
        "tabla": nombre_tabla,
        "destino": destino,
        "registros": total,
        "estado": "OK"
    }


# ============================================================
# 4. EJECUTAR TRANSFORMACIONES
# ============================================================

resultados = []

for tabla, configuracion in CONFIG_TABLAS.items():

    try:

        resultado = transformar_silver(
            tabla,
            configuracion
        )

        resultados.append(resultado)

    except Exception as e:

        print("ERROR EN:", tabla)
        print(str(e))

        resultados.append({
            "tabla": tabla,
            "destino": (
                f"{CATALOGO}.{ESQUEMA_DESTINO}.{tabla}"
            ),
            "registros": None,
            "estado": "ERROR"
        })


# ============================================================
# 5. RESUMEN DE PROCESAMIENTO
# ============================================================

df_resultados = spark.createDataFrame(resultados)

print("\n======================================")
print("RESUMEN DE TRANSFORMACION SILVER")
print("======================================")

display(df_resultados)

errores = [
    r for r in resultados
    if r["estado"] == "ERROR"
]

if errores:
    raise RuntimeError(
        f"Silver termino con {len(errores)} errores."
    )

print("TRANSFORMACION SILVER COMPLETADA")


# ============================================================
# 6. VALIDACION DE TABLAS GENERADAS
# ============================================================

for tabla in CONFIG_TABLAS.keys():

    nombre_completo = (
        f"{CATALOGO}.{ESQUEMA_DESTINO}.{tabla}"
    )

    print("\nTabla:", nombre_completo)

    spark.table(nombre_completo).printSchema()
