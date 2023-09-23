-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SQL - AULA 02 - 14/09/2023
-- MAGIC
-- MAGIC 1) SELECT
-- MAGIC
-- MAGIC 2) SELECT FROM
-- MAGIC
-- MAGIC 3) 
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1) SELECT

-- COMMAND ----------

SELECT                -- as palavras chaves dos comandos/cláusulas são aceitos em maiúsculas e/ou minúsculas
'Olá Mundo!'          -- para colocar uma string, inserí-la dentro de aspas simples. Strings são sensíveis a maiúsculas e minúsculas.

-- COMMAND ----------

select
 'Olá Mundo!'
,'Olá Mundo!'

-- COMMAND ----------

Select
 'Olá Mundo!'
,'Olá Mundo!'
,'Olá Mundo!'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2) SELECT FROM

-- COMMAND ----------

SELECT *                       -- selecione todas as colunas
FROM silver.olist.pedido       -- traga dados da tabela pedido, do database olist do catálogo silver
                               -- a ordem de linhas que é trazida não tem lógica nenhuma aqui, é a ordem que o computador trouxe

-- COMMAND ----------

SELECT                          -- selecione somente as colunas idPedido e idCliente, nessa ordem
 idPedido
,idCliente
 FROM silver.olist.pedido       -- traga dados da tabela pedido, do database olist do catálogo silver

-- COMMAND ----------

SELECT                          -- selecione somente as colunas idPedido e idCliente, essa ordem
 idPedido
,idCliente
FROM silver.olist.pedido      -- traga dados da tabela pedido, do database olist do catálogo silver

-- COMMAND ----------

SELECT *
,dtEntregue > dtEstimativaEntrega             --criei uma nova coluna 
FROM silver.olist.pedido

-- COMMAND ----------

SELECT *
,dtEntregue > dtEstimativaEntrega AS flAtraso             --criei uma nova coluna com nome flAtraso
FROM silver.olist.pedido

-- COMMAND ----------

SELECT 
 idPedido                                                 -- selecione somente as colunas idPedido e idCliente, nessa ordem     
,idCliente
,dtEntregue > dtEstimativaEntrega AS flAtraso             -- criei uma nova coluna com nome flAtraso, onde: true tem atraso e false não ocorreu atraso
FROM silver.olist.pedido

-- COMMAND ----------

SELECT 
 idPedido                                                 
,idCliente
,dtEntregue                                               -- as datas também possuem hora, por isso se entregou no dia, mas depois de zero horas está sendo considerado atrasado :O
,dtEstimativaEntrega
,dtEntregue > dtEstimativaEntrega AS flAtraso  
FROM silver.olist.pedido

-- aqui dtEntregue = 2017-10-09T22:23:46.000+0000 e dtEstimativaEntrega = 2017-10-09T00:00:00.000+0000
-- está sendo considerado atrasado, mas foi entregue no mesmo dia

-- COMMAND ----------

SELECT 
 dtEntregue
,dtEstimativaEntrega
,date(dtEntregue)             AS dataEntrega                                  -- transformei todas as data/hora em data
,date(dtEstimativaEntrega)    AS dataEstimadaEntrega
,dtEntregue > dtEstimativaEntrega AS flAtrasoHora
,date(dtEntregue ) > date(dtEstimativaEntrega) AS flAtrasoData
,(dtEntregue - dtEstimativaEntrega) AS deltaEntregaHoras                      -- não calcula diferença em segundos como esperado
FROM silver.olist.pedido

-- aqui dtEntregue = 2017-10-09T22:23:46.000+0000 e dtEstimativaEntrega = 2017-10-09T00:00:00.000+0000
-- está sendo considerado atrasado, mas foi entregue no mesmo dia
-- aqui dtEntregue = 2017-10-09 e dtEstimativaEntrega = 2017-10-09
-- está sendo considerado sem atraso

-- COMMAND ----------

SELECT 
 dtEntregue
,dtEstimativaEntrega
,date(dtEntregue)             AS dataEntrega                                 
,date(dtEstimativaEntrega)    AS dataEstimadaEntrega
,dtEntregue > dtEstimativaEntrega AS flAtrasoHora
,date(dtEntregue ) > date(dtEstimativaEntrega) AS flAtrasoData                -- calcula diferença em dias
,date_diff(dtEntregue, dtEstimativaEntrega) AS deltaEntregaDias               -- também calcula diferença em dias
,( bigint(dtEntregue) - bigint(dtEstimativaEntrega) )/3600                    -- calcula diferença em segundos e depois transforma em horas
,round( ( bigint(dtEntregue) - bigint(dtEstimativaEntrega) )/3600 )           -- calcula diferença em segundos, depois transforma em horas e depois arredonda para cima
FROM silver.olist.pedido

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3) DOCUMENTAÇÃO DO APACHE SPARK
-- MAGIC
-- MAGIC https://spark.apache.org/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4) SELECT FROM WHERE

-- COMMAND ----------

SELECT
 dtEntregue
,dtEstimativaEntrega
,date(dtEntregue)             AS dataEntrega                                 
,date(dtEstimativaEntrega)    AS dataEstimadaEntrega
FROM silver.olist.pedido
WHERE  date(dtEntregue) > date(dtEstimativaEntrega)        -- entregas atrasadas

-- COMMAND ----------

SELECT
 dtEntregue
,dtEstimativaEntrega
,date(dtEntregue)             AS dataEntrega                                 
,date(dtEstimativaEntrega)    AS dataEstimadaEntrega
FROM silver.olist.pedido
WHERE  date(dtEntregue) < date(dtEstimativaEntrega)        -- entregas adiantadas

-- COMMAND ----------

SELECT
 dtEntregue
,dtEstimativaEntrega
,date(dtEntregue)             AS dataEntrega                                 
,date(dtEstimativaEntrega)    AS dataEstimadaEntrega
FROM silver.olist.pedido
--WHERE NOT date(dtEntregue) < date(dtEstimativaEntrega)        -- entregas atrasadas ou no mesmo dia
WHERE date(dtEntregue) >= date(dtEstimativaEntrega)         -- mesma coisa que isso

-- COMMAND ----------

SELECT *
FROM silver.olist.produto

-- COMMAND ----------

SELECT
idProduto
,descCategoria
,vlComprimentoCm
,vlAlturaCm
,vlLarguraCm
,vlComprimentoCm * vlAlturaCm * vlLarguraCm AS volCm3
FROM silver.olist.produto
WHERE vlComprimentoCm * vlAlturaCm * vlLarguraCm < 1000       -- filtra com volume menor do que 1000 cm3

-- COMMAND ----------

SELECT
idProduto
,descCategoria
,vlComprimentoCm
,vlAlturaCm
,vlLarguraCm
,vlComprimentoCm * vlAlturaCm * vlLarguraCm AS volCm3
FROM silver.olist.produto
WHERE vlComprimentoCm * vlAlturaCm * vlLarguraCm < 1000    -- filtra com volume menor do que 1000 cm3
AND descCategoria = 'bebes'                                -- filtra por produtos de bebês

-- COMMAND ----------

SELECT
idProduto
,descCategoria
,vlComprimentoCm
,vlAlturaCm
,vlLarguraCm
,vlComprimentoCm * vlAlturaCm * vlLarguraCm AS volCm3
FROM silver.olist.produto
WHERE descCategoria = 'bebes'                              -- filtra por produtos de bebês
AND vlComprimentoCm * vlAlturaCm * vlLarguraCm < 1000      -- filtra com volume menor do que 1000 cm3

-- COMMAND ----------

SELECT
idProduto
,descCategoria
,vlComprimentoCm
,vlAlturaCm
,vlLarguraCm
,vlComprimentoCm * vlAlturaCm * vlLarguraCm AS volCm3
FROM silver.olist.produto
WHERE descCategoria = 'bebes'                              -- filtra por produtos de bebês
OR descCategoria = 'perfumaria'                            -- filtra por produtos de perfumaria
AND vlComprimentoCm * vlAlturaCm * vlLarguraCm < 1000      -- filtra com volume menor do que 1000 cm3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5) TABELA VERDADE
-- MAGIC
-- MAGIC ###OPERAÇÃO OU
-- MAGIC
-- MAGIC 1 + 1 = 2 (LIGADO)
-- MAGIC
-- MAGIC 0 + 1 = 1 (LIGADO)
-- MAGIC
-- MAGIC 0 + 0 = 0 (DESLIGADO)
-- MAGIC
-- MAGIC 1 + 0 = 1 (LIGADO)
-- MAGIC
-- MAGIC ###OPERAÇÃO E
-- MAGIC
-- MAGIC 1 * 1 = 1 (LIGADO)
-- MAGIC
-- MAGIC 0 * 1 = 0 (DESLIGADO)
-- MAGIC
-- MAGIC 0 * 0 = 0 (DESLIGADO)
-- MAGIC
-- MAGIC 1 * 0 = 0 (DESLIGADO)
-- MAGIC

-- COMMAND ----------

1 + 1 * 0 = 1 = 1 + ( 1 * 0 )

-- COMMAND ----------

SELECT
idProduto
,descCategoria
,vlComprimentoCm
,vlAlturaCm
,vlLarguraCm
,vlComprimentoCm * vlAlturaCm * vlLarguraCm AS volCm3
FROM silver.olist.produto
WHERE descCategoria = 'bebes'                               -- filtra por produtos de bebês
OR (descCategoria = 'perfumaria'                            -- filtra por produtos de perfumaria
AND vlComprimentoCm * vlAlturaCm * vlLarguraCm < 1000 )     -- filtra com volume menor do que 1000 cm3

-- COMMAND ----------

SELECT
idProduto
,descCategoria
,vlComprimentoCm
,vlAlturaCm
,vlLarguraCm
,vlComprimentoCm * vlAlturaCm * vlLarguraCm AS volCm3
FROM silver.olist.produto
WHERE ( descCategoria = 'bebes'                              -- filtra por produtos de bebês
OR descCategoria = 'perfumaria' )                            -- filtra por produtos de perfumaria
AND vlComprimentoCm * vlAlturaCm * vlLarguraCm < 1000        -- filtra com volume menor do que 1000 cm3

-- COMMAND ----------

SELECT
idProduto
,descCategoria
,vlComprimentoCm
,vlAlturaCm
,vlLarguraCm
,vlComprimentoCm * vlAlturaCm * vlLarguraCm AS volCm3
FROM silver.olist.produto
WHERE ( descCategoria = 'bebes'                              -- filtra por produtos de bebês
OR descCategoria = 'artes'                                   -- filtra por produtos de artes
OR descCategoria = 'perfumaria' )                            -- filtra por produtos de perfumaria
AND vlComprimentoCm * vlAlturaCm * vlLarguraCm < 1000        -- filtra com volume menor do que 1000 cm3

-- COMMAND ----------

SELECT
idProduto
,descCategoria
,vlComprimentoCm
,vlAlturaCm
,vlLarguraCm
,vlComprimentoCm * vlAlturaCm * vlLarguraCm AS volCm3
FROM silver.olist.produto
WHERE descCategoria IN ('bebes', 'artes', 'perfumaria' )     -- filtra por produtos de bebes, artes ou perfumaria
AND vlComprimentoCm * vlAlturaCm * vlLarguraCm < 1000        -- filtra com volume menor do que 1000 cm3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6) LIKE

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria = 'ferramentas'
-- não trouxe nada

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria  LIKE 'ferramentas'
-- não trouxe nada

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria  LIKE 'ferramentas%'
-- trouxe strings que começam com ferramentas e podem ter outro texto depois

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria  LIKE 'ferramenta%'
-- trouxe strings que começam com ferramenta e podem ter outro texto depois

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria  LIKE '%ferramentas'
-- trouxe strings que terminam com ferramentas e podem ter outro texto antes

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria  LIKE '%ferramenta'
-- trouxe strings que terminam com ferramenta e podem ter outro texto antes

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria  LIKE '%ferramentas%'
-- trouxe strings que têm ferramentas, podendo ter texto antes ou depois

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE STARTSWITH (descCategoria, 'ferramentas')

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE endswith(descCategoria, 'ferramentas')

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE contains(descCategoria, 'ferramentas')
