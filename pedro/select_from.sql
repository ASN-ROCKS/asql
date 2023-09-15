-- Databricks notebook source
SELECT * FROM silver.olist.pedido

-- COMMAND ----------

SELECT
      idPedido,
      idCliente,
      dtEntregue
FROM silver.olist.pedido

-- COMMAND ----------

SELECT descSituacao, count(descSituacao) 
FROM silver.olist.pedido
GROUP BY descSituacao

-- COMMAND ----------

SELECT
      idPedido,
      idCliente,
      date(dtEntregue),
      date(dtEstimativaEntrega),
      date(dtEntregue) > date(dtEstimativaEntrega) AS flAtraso,
      int(date(dtEntregue) - date(dtEstimativaEntrega)) AS diasAtraso,
FROM silver.olist.pedido
WHERE date(dtEntregue)>='2018-01-01' AND
ORDER BY diasAtraso DESC

-- COMMAND ----------

SELECT
      idProduto,
      vlComprimentoCm * vlAlturaCm * vlLarguraCm AS volCm3

FROM silver.olist.produto
