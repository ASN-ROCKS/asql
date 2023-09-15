-- Databricks notebook source
SELECT * FROM silver.olist.pedido

-- COMMAND ----------

SELECT 
  idPedido,
  idCliente,
  dtEntregue
FROM silver.olist.pedido

-- COMMAND ----------



-- COMMAND ----------

SELECT 
  idPedido,
  idCliente,
  DATE(dtEntregue) AS dataEntrega,
  DATE(dtEstimativaEntrega) AS dataEstimativa,
  --DATEdtEntregue) > date(dtEstimativaEntrega) AS flAtraso
  CASE WHEN DATE(dtEntreDATEgue) > DATE(dtEstimativaEntrega) THEN 1 ELSE 0 END AS flAtraso
FROM silver.olist.pedido
LIMIT 1000

-- COMMAND ----------


