-- Databricks notebook source
SELECT * FROM silver.olist.pedido

--SELECIONE TODAS AS COLUNAS DA TABELA silver.olist.pedido

-- COMMAND ----------

SELECT 
      idPedido,  --select naõ é filtro, apenas seleciona colunas
      idCliente,
      dtEntregue

 FROM silver.olist.pedido

-- COMMAND ----------

SELECT *,
      dtEntregue > dtEstimativaEntrega AS flAtraso -- aqui tem datas que nao sao atrtasos pq é o que importa em dia e nao horas. vamos converter isso em data
      
       -- (posso usar ou nao o AS, mas é legal usar pra nao confundir)aqui tenho um operador criando uma condição e isso vai gerar uma nova coluna.

FROM silver.olist.pedido

-- COMMAND ----------


SELECT *,
      idCliente,
      dtEntregue,
      dtEstimativaEntrega,
      DATE(dtEntregue) as dataEntrega,
      DATE(dtEstimativaEntrega),
      dtEntregue > dtEstimativaEntrega AS flAtraso 
      DATE(dtEntregue) > DATE(dtEstimativaEntrega) AS flDataAtraso -- para validarmos 
FROM silver.olist.pedido

-- COMMAND ----------

-- DOCUMENTAÇÃO DO SPARK - https://spark.apache.org/docs/latest/api/sql/index.html
-- PODEMOS CONSULTAR TODAS AS FUNÇÕES DO SQL 

-- COMMAND ----------

SELECT
      idProduto,
      vlComprimentoCm * vlAlturaCm * vlLarguraCm as volCm3 -- operaçoes matematica
FROM silver.olist.produto
