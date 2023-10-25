-- Databricks notebook source
SELECT *,
-- iremos criar uma variavel nova com o reultado dos filtros que faremos 
        CASE WHEN descUF = 'SP' THEN 'Paulista'
        ELSE 'Desconhecido' -- podemos usar o else
        END as naturalidade   -- o end sempre fnaliza o case

FROM silver.olist.cliente

-- COMMAND ----------

SELECT *,
-- iremos criar uma variavel nova com o reultado dos filtros que faremos 
        CASE WHEN descUF = 'SP' THEN 'Paulista'
        WHEN descUF = "PE" THEN 'Pernambucano'  -- posso ir add varios cases, o when é a tratativa para essa coluna 
        ELSE 'Desconhecido' -- podemos usar o else
        END as naturalidade   -- o end sempre fnaliza o case

FROM silver.olist.cliente
