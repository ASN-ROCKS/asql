-- Databricks notebook source
SELECT *
FROM silver.olist.produto
WHERE descCategoria IN('bebe', 'perfumaria', 'artes') 

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria LIKE '%ferramentas'

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria LIKE '%ferramentas%'
