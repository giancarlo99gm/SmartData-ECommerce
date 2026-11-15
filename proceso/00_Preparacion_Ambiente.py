
# Databricks notebook source
# ============================================================
# PROYECTO: SMARTDATA E-COMMERCE
# NOTEBOOK: 00_Preparacion_Ambiente
# OBJETIVO: Preparar esquemas de arquitectura Medallion
# AMBIENTES: DEV / PROD
# ============================================================

# ============================================================
# 1. CONFIGURACION DEL AMBIENTE
# ============================================================

dbutils.widgets.text("catalogo", "smartdata_dev")

CATALOGO = dbutils.widgets.get("catalogo")

if CATALOGO not in ["smartdata_dev", "smartdata_prod"]:
    raise ValueError(
        f"Catálogo no permitido: {CATALOGO}"
    )

print("======================================")
print("SMARTDATA - PREPARACION DEL AMBIENTE")
print("======================================")
print(f"Catálogo seleccionado: {CATALOGO}")

# ============================================================
# 2. VALIDAR CATALOGO
# ============================================================

# Verificar que el catálogo exista y sea accesible.
# USE CATALOG genera un error si no existe o no hay permisos.

spark.sql(f"USE CATALOG `{CATALOGO}`")

print(f"Catálogo disponible: {CATALOGO}")

# ============================================================
# 3. PREPARAR ESQUEMAS MEDALLION
# ============================================================

esquemas = ["bronze", "silver", "gold"]

for esquema in esquemas:

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS "
        f"`{CATALOGO}`.`{esquema}`"
    )

    print(
        f"Esquema disponible: {CATALOGO}.{esquema}"
    )

# ============================================================
# 4. VALIDACION FINAL
# ============================================================

for esquema in esquemas:

    spark.sql(
        f"DESCRIBE SCHEMA `{CATALOGO}`.`{esquema}`"
    )

    print(
        f"Validación correcta: {CATALOGO}.{esquema}"
    )

print("======================================")
print("PREPARACION DEL AMBIENTE COMPLETADA")
print("======================================")
