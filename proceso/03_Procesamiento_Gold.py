# Databricks notebook source

# ============================================================
# PROYECTO: SMARTDATA E-COMMERCE
# NOTEBOOK: 03_Procesamiento_Gold
# OBJETIVO: Construccion de tablas analiticas
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURACION GENERAL DEV / PROD
# ============================================================

dbutils.widgets.text("catalogo", "smartdata_dev")

CATALOGO = dbutils.widgets.get("catalogo")

if CATALOGO not in ["smartdata_dev", "smartdata_prod"]:
    raise ValueError(
        f"Catálogo no permitido: {CATALOGO}"
    )

ESQUEMA_ORIGEN = "silver"
ESQUEMA_DESTINO = "gold"

print(f"Catálogo seleccionado: {CATALOGO}")
print(f"Origen: {CATALOGO}.{ESQUEMA_ORIGEN}")
print(f"Destino: {CATALOGO}.{ESQUEMA_DESTINO}")


spark.sql(f"USE CATALOG `{CATALOGO}`")

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"`{CATALOGO}`.`{ESQUEMA_DESTINO}`"
)

# ============================================================
# 2. LECTURA DE TABLAS SILVER
# ============================================================

df_orders = spark.table(
    f"{CATALOGO}.silver.orders"
)

df_products = spark.table(
    f"{CATALOGO}.silver.products"
)

df_aisles = spark.table(
    f"{CATALOGO}.silver.aisles"
)

df_departments = spark.table(
    f"{CATALOGO}.silver.departments"
)

df_prior = spark.table(
    f"{CATALOGO}.silver.order_products_prior"
)

df_train = spark.table(
    f"{CATALOGO}.silver.order_products_train"
)

# ============================================================
# 3. CONSOLIDAR DETALLE DE PEDIDOS
# ============================================================

columnas_detalle = [
    "order_id",
    "product_id",
    "add_to_cart_order",
    "reordered"
]

df_detalle = (
    df_prior.select(*columnas_detalle)
    .withColumn("origen", F.lit("prior"))
    .unionByName(
        df_train.select(*columnas_detalle)
        .withColumn("origen", F.lit("train"))
    )
)

# ============================================================
# 4. CONSTRUIR TABLA ANALITICA
# ============================================================

df_gold = (
    df_detalle.alias("d")
    .join(
        df_orders.alias("o"),
        F.col("d.order_id") == F.col("o.order_id"),
        "inner"
    )
    .join(
        df_products.alias("p"),
        F.col("d.product_id") == F.col("p.product_id"),
        "left"
    )
    .join(
        df_aisles.alias("a"),
        F.col("p.aisle_id") == F.col("a.aisle_id"),
        "left"
    )
    .join(
        df_departments.alias("dep"),
        F.col("p.department_id")
        == F.col("dep.department_id"),
        "left"
    )
    .select(
        F.col("o.order_id"),
        F.col("o.user_id"),
        F.col("o.order_number"),
        F.col("o.order_dow"),
        F.col("o.order_hour_of_day"),
        F.col("o.days_since_prior_order"),
        F.col("d.product_id"),
        F.col("p.product_name"),
        F.col("a.aisle"),
        F.col("dep.department"),
        F.col("d.add_to_cart_order"),
        F.col("d.reordered"),
        F.col("d.origen"),
        F.current_timestamp().alias(
            "fecha_procesamiento"
        )
    )
)

# ============================================================
# 5. GUARDAR DETALLE ANALITICO
# ============================================================

(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{CATALOGO}.gold.fact_compras"
    )
)

print("Tabla fact_compras creada correctamente")

# ============================================================
# 6. INDICADORES POR PRODUCTO
# ============================================================

df_kpi_productos = (
    df_gold
    .groupBy(
        "product_id",
        "product_name",
        "department",
        "aisle"
    )
    .agg(
        F.count("*").alias("total_unidades"),
        F.countDistinct("order_id").alias(
            "total_pedidos"
        ),
        F.countDistinct("user_id").alias(
            "total_clientes"
        ),
        F.sum("reordered").alias(
            "total_recompras"
        )
    )
    .withColumn(
        "porcentaje_recompra",
        F.round(
            F.col("total_recompras")
            / F.col("total_unidades") * 100,
            2
        )
    )
)

(
    df_kpi_productos.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{CATALOGO}.gold.kpi_productos"
    )
)

# ============================================================
# 7. INDICADORES POR CLIENTE
# ============================================================

df_kpi_clientes = (
    df_gold
    .groupBy("user_id")
    .agg(
        F.countDistinct("order_id").alias(
            "total_pedidos"
        ),
        F.count("*").alias(
            "total_productos_comprados"
        ),
        F.countDistinct("product_id").alias(
            "productos_diferentes"
        ),
        F.sum("reordered").alias(
            "total_recompras"
        )
    )
)

(
    df_kpi_clientes.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{CATALOGO}.gold.kpi_clientes"
    )
)

# ============================================================
# 8. INDICADORES POR DEPARTAMENTO
# ============================================================

df_kpi_departamentos = (
    df_gold
    .groupBy("department")
    .agg(
        F.count("*").alias(
            "total_unidades"
        ),
        F.countDistinct("order_id").alias(
            "total_pedidos"
        ),
        F.countDistinct("user_id").alias(
            "total_clientes"
        )
    )
)

(
    df_kpi_departamentos.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{CATALOGO}.gold.kpi_departamentos"
    )
)

# ============================================================
# 9. VALIDACION FINAL
# ============================================================

tablas_gold = [
    "fact_compras",
    "kpi_productos",
    "kpi_clientes",
    "kpi_departamentos"
]

for tabla in tablas_gold:

    nombre = f"{CATALOGO}.gold.{tabla}"

    total = spark.table(nombre).count()

    print(f"{nombre}: {total} registros")

print("PROCESAMIENTO GOLD FINALIZADO")
