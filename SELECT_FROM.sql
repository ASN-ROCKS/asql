-- Databricks notebook source
SELECT *
FROM silver.olist.pedido

--SELECIONE TODAS AS COLUNAS DA TABELA silver.olist.pedido

-- COMMAND ----------

SELECT 
         idPedido
        ,idCliente
        ,dtEntregue
FROM silver.olist.pedido

-- COMMAND ----------

SELECT *
      ,dtEntregue > dtEstimativaEntrega AS fl_atraso
FROM silver.olist.pedido

-- COMMAND ----------

SELECT 
         idPedido
        ,idCliente
        ,dtEntregue
        ,dtEstimativaEntrega
        ,date(dtEntregue)                                         AS dataEntrega
        ,date(dtEstimativaEntrega)                                AS dataEstimativa
        ,dtEntregue > dtEstimativaEntrega                         AS fl_atraso
        ,date(dtEntregue) > date(dtEstimativaEntrega)             AS fl_DataAtraso 
        ,datediff(date(dtEntregue),date(dtEstimativaEntrega))     AS nrDiasEntrega   
FROM silver.olist.pedido

-- COMMAND ----------

SELECT   
        idProduto
        ,vlComprimentoCm * vlLarguraCm *vlLarguraCm AS volCm3
FROM silver.olist.produto
