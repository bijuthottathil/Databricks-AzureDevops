# Databricks notebook source
dbutils.widgets.text("CATALOG", "")
dbutils.widgets.text("SCHEMA", "")
dbutils.widgets.text("TABLE", "demo_customers")

catalog = dbutils.widgets.get("CATALOG")
schema  = dbutils.widgets.get("SCHEMA")
table   = dbutils.widgets.get("TABLE")

fq_table = f"`{catalog}`.`{schema}`.`{table}`"

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS `{catalog}`.`{schema}`")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq_table} (
  customer_id STRING,
  name        STRING,
  signup_date DATE
)
USING DELTA
""")

spark.sql(f"""
MERGE INTO {fq_table} AS t
USING (
  SELECT 'C001' AS customer_id, 'Ada Lovelace' AS name, DATE('2024-01-10') AS signup_date UNION ALL
  SELECT 'C002', 'Alan Turing', DATE('2024-02-12')
) s
ON t.customer_id = s.customer_id
WHEN NOT MATCHED THEN INSERT *
""")

display(spark.table(fq_table))
