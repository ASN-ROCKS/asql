-- Databricks notebook source
-- MANEIRA DE ENCONTRAR LINHAS DISTINTAS NA CONSULTA
-- o distinct traz linhas distintas - o foco sao em linhas que nao se repetem

SELECT DISTINCT descUF
FROM silver.olist.cliente


-- COMMAND ----------

-- agora quero trazer as informaçõe sde cidade e uf, o UF se repete mas as cidades nao se repetem mais

SELECT DISTINCT descUF, descCidade
FROM silver.olist.cliente
