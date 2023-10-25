-- Databricks notebook source
-- SELECIONE TODAS AS COLUNAS DA TABELA SILVER.OLIST.PEDIDO
SELECT * FROM SILVER.olist.pedido





-- COMMAND ----------

SELECT
  idPedido,
  idCliente,
  dtEntregue
FROM SILVER.olist.pedido

-- COMMAND ----------

SELECT
  idPedido,
  idCliente,
  dtEntregue>dtEstimativaEntrega AS flAtraso
FROM SILVER.olist.pedido

-- COMMAND ----------

SELECT
  idPedido,
  idCliente,
  dtEntregue,
  dtEstimativaEntrega,
  date(dtEntregue) as dataEntrega,
  date(dtEstimativaEntrega) as dataEstimativa,
  dtEntregue>dtEstimativaEntrega AS flAtraso,
  date(dtEntregue)>date(dtEstimativaEntrega) as flDataAtraso,
  datediff(dtEntregue,dtEstimativaEntrega ) as nrDiasEntrega
FROM SILVER.olist.pedido

-- COMMAND ----------

select 
  idProduto,
  vlComprimentoCm*vlAlturaCm*vlLarguraCm as volCm3
from silver.olist.produto
