
# Databricks notebook source
# ============================================================
# PROYECTO: SMARTDATA E-COMMERCE
# NOTEBOOK: 04_Grants
# OBJETIVO: Verificar permisos de Unity Catalog
# AMBIENTES: DEV / PROD
# ============================================================

# ============================================================
# 1. CONFIGURACION DEL AMBIENTE
# ============================================================

dbutils.widgets.text("catalogo", "smartdata_dev")
dbutils.widgets.text("external_location", "ext_raw_dev")

CATALOGO = dbutils.widgets.get("catalogo")
EXTERNAL_LOCATION = dbutils.widgets.get("external_location")

if CATALOGO not in ["smartdata_dev", "smartdata_prod"]:
    raise ValueError(
        f"Catálogo no permitido: {CATALOGO}"
    )

if not EXTERNAL_LOCATION:
    raise ValueError(
        "Debe especificarse una External Location."
    )

print("======================================")
print("SMARTDATA - VALIDACION DE GRANTS")
print("======================================")
print(f"Catálogo: {CATALOGO}")
print(f"External Location: {EXTERNAL_LOCATION}")

# ============================================================
# 2. VALIDACION DE PERMISOS
# ============================================================

objetos = [
    f"CATALOG `{CATALOGO}`",
    f"SCHEMA `{CATALOGO}`.`bronze`",
    f"SCHEMA `{CATALOGO}`.`silver`",
    f"SCHEMA `{CATALOGO}`.`gold`",
    f"EXTERNAL LOCATION `{EXTERNAL_LOCATION}`"
]

errores = []

for objeto in objetos:

    print(f"\nPermisos de {objeto}")

    try:
        df_permisos = spark.sql(
            f"SHOW GRANTS ON {objeto}"
        )

        display(df_permisos)

        print("Consulta realizada correctamente")

    except Exception as e:

        print(f"ERROR consultando {objeto}: {e}")
        errores.append(objeto)

# ============================================================
# 3. VALIDACION FINAL
# ============================================================

if errores:
    raise RuntimeError(
        f"No se pudieron consultar los permisos de "
        f"{len(errores)} objetos: {errores}"
    )

print("======================================")
print("VALIDACION DE GRANTS COMPLETADA")
print("======================================")
