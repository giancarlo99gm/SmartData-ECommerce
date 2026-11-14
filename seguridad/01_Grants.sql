-- SmartData-ECommerce
-- Permisos de Unity Catalog para automatización DEV

GRANT USE CATALOG
ON CATALOG smartdata_dev
TO `febfa147-5c3b-49eb-a400-cacfeeda155b`;

GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE
ON SCHEMA smartdata_dev.bronze
TO `febfa147-5c3b-49eb-a400-cacfeeda155b`;

GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE
ON SCHEMA smartdata_dev.silver
TO `febfa147-5c3b-49eb-a400-cacfeeda155b`;

GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE
ON SCHEMA smartdata_dev.gold
TO `febfa147-5c3b-49eb-a400-cacfeeda155b`;
