
# ============================================================
# PROYECTO: SMARTDATA E-COMMERCE
# NOTEBOOK: 00_Preparacion_Ambiente
# OBJETIVO: Preparar esquemas de arquitectura Medallion
# ============================================================

CATALOGO = "smartdata_dev"

print("======================================")
print("SMARTDATA - PREPARACION DEL AMBIENTE")
print("======================================")

# Crear esquemas si no existen
spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {CATALOGO}.bronze
""")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {CATALOGO}.silver
""")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {CATALOGO}.gold
""")

print("Esquemas Bronze, Silver y Gold disponibles.")
print("PREPARACION DEL AMBIENTE COMPLETADA")
