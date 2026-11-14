-- Permisos para el usuario del proyecto

GRANT USE CATALOG
ON CATALOG smartdata_dev
TO `giancarlomz.1997@hotmail.com`;

-- Acceso a los esquemas Medallion

GRANT USE SCHEMA
ON SCHEMA smartdata_dev.bronze
TO `giancarlomz.1997@hotmail.com`;

GRANT USE SCHEMA
ON SCHEMA smartdata_dev.silver
TO `giancarlomz.1997@hotmail.com`;

GRANT USE SCHEMA
ON SCHEMA smartdata_dev.gold
TO `giancarlomz.1997@hotmail.com`;

-- Lectura de tablas existentes y futuras

GRANT SELECT
ON SCHEMA smartdata_dev.bronze
TO `giancarlomz.1997@hotmail.com`;

GRANT SELECT
ON SCHEMA smartdata_dev.silver
TO `giancarlomz.1997@hotmail.com`;

GRANT SELECT
ON SCHEMA smartdata_dev.gold
TO `giancarlomz.1997@hotmail.com`;
