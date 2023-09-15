-- Databricks notebook source
SELECT *
FROM silver.olist.pedido

-- SELECIONE TODAS AS COLUNAS DA TABELA silver.olist.pedido

-- COMMAND ----------

SELECT
 idPedido
,idCliente
,dtEntregue
FROM silver.olist.pedido

-- SELECIONE SÓ AS COLUNAS idPedido, idCliente, dtEntregue DA TABELA silver.olist.pedido

-- COMMAND ----------

SELECT
 idPedido
,idCliente
,dtEntregue > dtEstimativaEntrega AS flAtraso
 FROM silver.olist.pedido

-- SELECIONE SÓ AS COLUNAS idPedido, idCliente E CRIE UMA NOVA COLUNA QUE COMPARA "dtEntregue > dtEstimativaEntrega" COM NOME flAtraso DA TABELA silver.olist.pedido

-- COMMAND ----------

SELECT
 idPedido
,idCliente
,dtEntregue
,dtEstimativaEntrega
,dtEntregue > dtEstimativaEntrega AS flAtraso
 FROM silver.olist.pedido

-- SELECIONE SÓ AS COLUNAS idPedido, idCliente, dtEntregue, dtEstimativa E CRIE UMA NOVA COLUNA QUE COMPARA "dtEntregue > dtEstimativaEntrega" COM NOME flAtraso DA TABELA silver.olist.pedido

-- COMMAND ----------

SELECT
 idPedido
,idCliente
,dtEntregue
,dtEstimativaEntrega
,date(dtEntregue) AS dataEntrega
,date(dtEstimativaEntrega) AS dataEstimativa
,dtEntregue > dtEstimativaEntrega AS flAtraso
,date(dtEntregue) > date(dtEstimativaEntrega) AS flDataAtraso
,datediff(dtEntregue, dtEstimativaEntrega) AS nrdiasEntrega
 FROM silver.olist.pedido

-- SELECIONE SÓ AS COLUNAS idPedido, IdCliente, dtEntregue, dtEstimativaEntrega, dtEntregue em data, dtEstimativa em data, dtEntregue em data/hora, dtEstimativa em data/hora E CRIE NOVAS COLUNAS QUE COMPARAM "dtEntregue > dtEstimativaEntrega" COM NOME flAtraso, "date(dtEntregue) > date(dtEstimativaEntrega)" COM NOME flDataAtraso E cálculo de dtEntregue - dtEstimativaEntrega COM NOME nrDiasEntrega DA TABELA silver.olist.pedido

-- COMMAND ----------

-- DOCUMENTAÇÃO DE SQL PARA SPARK

-- https://spark.apache.org/docs/latest/api/sql/index.html

-- COMMAND ----------

SELECT *
, vlComprimentoCm * vlAlturaCm * vlLarguraCm AS volCm3
FROM silver.olist.produto

-- SELECIONE TODAS AS COLUNAS DA TABELA E CRIE NOVA COLUNA QUE CALCULE O VOLUME DO PRODUTO COM NOME volCm3 da TABELA silver.olist.pedido 

-- COMMAND ----------

SELECT *
FROM silver.olist.pedido
WHERE date(dtEntregue) < date(dtEstimativaEntrega)

-- SELECIONE TODAS AS COLUNAS DA TABELA silver.olist.pedido QUANDO dtEntregue EM DATA SEJA MAIOR QUE dtEstimativaEntrega EM DATE

-- COMMAND ----------

SELECT *
FROM silver.olist.pedido
WHERE NOT date(dtEntregue) < date(dtEstimativaEntrega)
-- IGUAL A WHERE date(dtEntregue) >= date(dtEstimativaEntrega)

-- SELECIONE TODAS AS COLUNAS DA TABELA silver.olist.pedido QUANDO dtEntregue EM DATA SEJA MAIOR QUE dtEstimativaEntrega EM DATE

-- COMMAND ----------

SELECT *
, vlComprimentoCm * vlAlturaCm * vlLarguraCm AS volCm3
FROM silver.olist.produto
WHERE descCategoria = 'bebes'
AND vlComprimentoCm * vlAlturaCm * vlLarguraCm > 1000



-- COMMAND ----------

SELECT *
, vlComprimentoCm * vlAlturaCm * vlLarguraCm AS volCm3
FROM silver.olist.produto
WHERE descCategoria = 'bebes' OR descCategoria = 'perfumaria'
AND vlComprimentoCm * vlAlturaCm * vlLarguraCm > 1000

-- ENTENDE COMO: WHERE descCategoria = 'bebes' OR ( descCategoria = 'perfumaria' AND vlComprimentoCm * vlAlturaCm * vlLarguraCm > 1000)

-- COMMAND ----------

-- USANDO OU
-- 1 + 1 = ligado
-- 1 + 0 = ligado
-- o + 1 = ligado
-- 0 + 0 = desligado

-- USANDO E
-- 1 * 1 = ligado
-- 1 * 0 = desligado
-- 0 * 1 = desligado
-- 0 * 0 = desligado

-- COMMAND ----------

SELECT *
, vlComprimentoCm * vlAlturaCm * vlLarguraCm AS volCm3
FROM silver.olist.produto
WHERE (descCategoria = 'bebes' OR descCategoria = 'perfumaria')
AND vlComprimentoCm * vlAlturaCm * vlLarguraCm > 1000

-- ENTENDE COMO: WHERE (descCategoria = 'bebes' OR  descCategoria = 'perfumaria') AND vlComprimentoCm * vlAlturaCm * vlLarguraCm > 1000

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE (descCategoria = 'bebes' 
OR descCategoria = 'perfumaria'
OR descCategoria = 'artes')
AND vlComprimentoCm * vlAlturaCm * vlLarguraCm > 1000

-- ENTENDE COMO: WHERE (descCategoria = 'bebes' OR  descCategoria = 'perfumaria' OR  descCategoria = 'artes') AND vlComprimentoCm * vlAlturaCm * vlLarguraCm > 1000

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria IN ('bebes', 'perfumaria', 'artes')
AND vlComprimentoCm * vlAlturaCm * vlLarguraCm > 1000

-- ENTENDE COMO: WHERE (descCategoria = 'bebes' OR  descCategoria = 'perfumaria' OR  descCategoria = 'artes') AND vlComprimentoCm * vlAlturaCm * vlLarguraCm > 1000

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria LIKE ('ferramentas%')

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria LIKE ('ferramenta%')

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria LIKE ('%ferramenta')

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria LIKE ('%ferramentas%')

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE STARTSWITH(descCategoria, 'ferramentas')


-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE CONTAINS(descCategoria, 'ferramentas')


-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE ENDSWITH(descCategoria, 'ferramentas')
