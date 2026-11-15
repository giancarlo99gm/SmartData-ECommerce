
# Databricks notebook source
# ============================================================
# PROYECTO: SMARTDATA E-COMMERCE
# NOTEBOOK: 04_Grants
# OBJETIVO: Verificar permisos de Unity Catalog
# ============================================================

CATALOGO = "smartdata_dev"
EXTERNAL_LOCATION = "ext_raw_dev"

print("======================================")
print("SMARTDATA - VALIDACION DE GRANTS")
print("======================================")

objetos = [
    f"CATALOG {CATALOGO}",
    f"SCHEMA {CATALOGO}.bronze",
    f"SCHEMA {CATALOGO}.silver",
    f"SCHEMA {CATALOGO}.gold",
    f"EXTERNAL LOCATION {EXTERNAL_LOCATION}"
]

for objeto in objetos:
    print(f"\nPermisos de {objeto}")
    display(spark.sql(f"SHOW GRANTS ON {objeto}"))

print("VALIDACION DE GRANTS COMPLETADA")
