-- Databricks notebook source
SELECT 
      idPedido,
      idCliente,
      date(dtEntregue),
      date(dtEstimativaEntrega),
      date(dtEntregue) > date(dtEstimativaEntrega) AS flAtraso,
      int(date(dtEntregue) - date(dtEstimativaEntrega)) AS diasAtraso
FROM silver.olist.pedido
WHERE date(dtEntregue)>='2018-01-01' AND
      date(dtEntregue) > date(dtEstimativaEntrega) = TRUE
ORDER BY diasAtraso DESC

-- COMMAND ----------

SELECT *      
      --vlComprimentoCm * vlAlturaCm * vlLarguraCm AS Volume
FROM silver.olist.produto

WHERE descCategoria IN ('bebes','perfumaria','artes')
AND vlComprimentoCm * vlAlturaCm * vlLarguraCm > 1000
--ORDER BY Volume ASC

