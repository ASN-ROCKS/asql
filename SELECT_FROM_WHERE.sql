-- Databricks notebook source
SELECT *
FROM silver.olist.pedido
WHERE date(dtEntregue) > date(dtEstimativaEntrega)

-- COMMAND ----------

SELECT *
FROM silver.olist.pedido
WHERE NOT date(dtEntregue) > date(dtEstimativaEntrega)

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria IN('bebes','perfumaria','artes')
AND vlLarguraCm * vlAlturaCm * vlComprimentoCm >= 1000

-- COMMAND ----------

SELECT distinct descCategoria
FROM silver.olist.produto
WHERE descCategoria LIKE 'ferramentas%'

-- COMMAND ----------

SELECT distinct descCategoria
FROM silver.olist.produto
WHERE descCategoria LIKE 'ferramentas%'
