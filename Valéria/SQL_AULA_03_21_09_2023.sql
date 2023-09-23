-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SQL - AULA 03- 21/09/2023
-- MAGIC
-- MAGIC ## 1) ORDER BY (sempre é o último comando a ser dado)
-- MAGIC
-- MAGIC ## 2) CASE WHEN
-- MAGIC
-- MAGIC ## 3) DISTINCT
-- MAGIC
-- MAGIC ## 4) EXERCÍCIOS
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1) ORDER BY (sempre é o último comando a ser dado)

-- COMMAND ----------

SELECT *
FROM silver.olist.produto

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria = 'bebes'
ORDER BY vlPesoGramas                 -- quero ordenar os produtos de bebês do menor peso para o maior

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria = 'bebes'
ORDER BY vlPesoGramas DESC             -- quero ordenar os produtos de bebês do maior peso para o menor

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria = 'bebes'
ORDER BY vlPesoGramas , vlComprimentoCm DESC             -- quero ordenar os produtos de bebês do maior peso para o menor e depois com comprimento do maior para menor
-- não deu certo, foi de peso menor para maior e depois de comprimento do maior para menor
-- tem de colocar  DESc para peso e DESC para comprimento

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria = 'bebes'
ORDER BY vlPesoGramas DESC, vlComprimentoCm DESC             -- quero ordenar os produtos de bebês do maior peso para o menor e depois com comprimento do maior para menor

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria = 'bebes'
ORDER BY vlPesoGramas DESC, vlComprimentoCm ASC             -- quero ordenar os produtos de bebês do maior peso para o menor e depois com comprimento do menor para o maior

-- COMMAND ----------

SELECT * 
,vlComprimentoCm * vlAlturaCm * vlLarguraCm AS volumeCm3
FROM silver.olist.produto
WHERE descCategoria = 'bebes'
ORDER BY vlPesoGramas DESC, vlComprimentoCm * vlAlturaCm * vlLarguraCm DESC             -- quero ordenar os produtos de bebês do maior peso para o menor e depois do volume maior para menor

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2) CASE WHEN (um case cria uma coluna com a condição)

-- COMMAND ----------

SELECT *

FROM silver.olist.cliente

-- COMMAND ----------

SELECT *

,CASE WHEN descUF = 'SP' THEN 'Paulista' END AS naturalidade          -- se for do Estado de SP é paulista

FROM silver.olist.cliente

-- COMMAND ----------

SELECT *

,CASE WHEN descUF = 'SP' THEN 'Paulista'
      ELSE 'Desconhecido'                                -- se não for de SP, será desconhecido
      END AS naturalidade
      
FROM silver.olist.cliente

-- COMMAND ----------

SELECT *

,CASE WHEN descUF = 'SP' THEN 'Paulista' 
      WHEN descUF = 'RJ' THEN 'FLuminense'                        -- se for do Estado do RJ, é fluminense
      WHEN descUF = 'MG' THEN 'Mineiro'                           -- se for do Estado de MG, é mineiro
      ELSE 'Desconhecido'
      END AS naturalidade
      
FROM silver.olist.cliente

-- COMMAND ----------

SELECT *

,CASE WHEN descUF = 'SP' THEN 'Paulista' 
      WHEN descUF = 'RJ' THEN 'FLuminense'                        -- se for do Estado do RJ, é fluminense
      WHEN descUF = 'MG' THEN 'Mineiro'                           -- se for do Estado de MG, é mineiro
      ELSE 'Desconhecido'
      END AS naturalidade

,CASE WHEN descUF = 'SP' THEN 'Sudeste'                           -- se for do Estado de SP, é Sudeste
      WHEN descUF = 'RJ' THEN 'Sudeste'                           -- se for do Estado do RJ, é Sugeste
      WHEN descUF = 'MG' THEN 'Sudeste'                           -- se for do Estado de MG, é Sudeste
      ELSE 'Desconhecido'
      END AS regiao
      
      FROM silver.olist.cliente

-- COMMAND ----------

SELECT *

,CASE WHEN descUF = 'SP' THEN 'Paulista' 
      WHEN descUF = 'RJ' THEN 'FLuminense'                        -- se for do Estado do RJ, é fluminense
      WHEN descUF = 'MG' THEN 'Mineiro'                           -- se for do Estado de MG, é mineiro
      ELSE 'Desconhecido'
      END AS naturalidade

,CASE WHEN descUF IN ('SP', 'RJ', 'MG') THEN 'Sudeste' 
      WHEN descUF IN ('PR', 'RS', 'SC') THEN 'Sul'
      ELSE 'Desconhecido'
      END AS regiao
      
      FROM silver.olist.cliente

-- COMMAND ----------

SELECT *

,CASE WHEN descUF = 'SP' THEN 'Paulista' 
      WHEN descUF = 'RJ' THEN 'FLuminense'                        -- se for do Estado do RJ, é fluminense
      WHEN descUF = 'MG' THEN 'Mineiro'                           -- se for do Estado de MG, é mineiro
      ELSE 'Desconhecido'
      END AS naturalidade

,CASE WHEN descUF IN ('SP', 'RJ', 'MG') THEN 'Sudeste' 
      WHEN descUF IN ('PR', 'RS', 'SC') THEN 'Sul'
      ELSE 'Desconhecido'
      END AS regiao
      
      FROM silver.olist.cliente
      WHERE regiao = 'Sudeste'
-- dá erro, pois não consegue filtrar dados de uma coluna que foi criada na mesma consulta

-- COMMAND ----------

SELECT *
FROM
(
  SELECT *

 ,CASE WHEN descUF = 'SP' THEN 'Paulista' 
       WHEN descUF = 'RJ' THEN 'FLuminense'                        -- se for do Estado do RJ, é fluminense
       WHEN descUF = 'MG' THEN 'Mineiro'                           -- se for do Estado de MG, é mineiro
       ELSE 'Desconhecido'
       END AS naturalidade

 ,CASE WHEN descUF IN ('SP', 'RJ', 'MG') THEN 'Sudeste' 
       WHEN descUF IN ('PR', 'RS', 'SC') THEN 'Sul'
       ELSE 'Desconhecido'
       END AS regiao
      
       FROM silver.olist.cliente
) AS teste
WHERE teste.regiao = 'Sudeste'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3) DISTINCT (lista LINHAS DISTINTAS)

-- COMMAND ----------

SELECT *
FROM silver.olist.cliente

-- COMMAND ----------

SELECT 
descUF
FROM silver.olist.cliente

-- COMMAND ----------

SELECT  DISTINCT
 descUF                              -- Estados distintos
FROM silver.olist.cliente
ORDER BY descUF

-- COMMAND ----------

SELECT  DISTINCT
 descUF                              -- Estados distintos
FROM silver.olist.cliente
order by 1

-- COMMAND ----------

SELECT  DISTINCT
 descUF
,descCidade                 -- Cidades e respectivos Estados distintos (LINHAS DISTINTAS)
FROM silver.olist.cliente
ORDER BY descUF, descCidade DESC

-- COMMAND ----------

SELECT  DISTINCT
 descUF
,descCidade                 -- Cidades e respectivos Estados distintos (LINHAS DISTINTAS)
FROM silver.olist.cliente
ORDER BY 1, 2 DESC

-- COMMAND ----------

SELECT  descCidade DISTINCT descUF              -- Cidades e respectivos Estados distintos (LINHAS DISTINTAS)
FROM silver.olist.cliente
ORDER BY descUF, descCidade DESC
-- deu erro de sintaxe, pois o SELECT deve ser seguido do DISTINCT

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4) EXERCÍCIOS

-- COMMAND ----------

-- ARQUIVO SQL - AULAS_02_03_STUDY
-- PÁGINA 27

-- COMMAND ----------

-- EXERCÍCIO 1 - Selecione todos os clientes paulistanos

SELECT *
FROM silver.olist.cliente
WHERE descCidade = 'sao paulo'
AND descUF = 'SP'

-- COMMAND ----------

-- EXERCÍCIO 2 - Selecione todos os clientes paulistas

SELECT *
FROM silver.olist.cliente
WHERE descUF = 'SP'

-- COMMAND ----------

-- EXERCÍCIO 3 - Selecione todos os vendedores cariocas e paulistas

SELECT *
FROM silver.olist.vendedor
WHERE ( descUF = 'RJ' AND descCidade = 'rio de janeiro') 
OR descUF = 'SP'

-- COMMAND ----------

-- EXERCÍCIO 4 - Selecione produtos de perfumaria e bebês com altura maior que 5 cm

SELECT *
FROM silver.olist.produto
WHERE descCategoria IN ('bebes', 'perfumaria')
AND vlAlturaCm > 5

-- COMMAND ----------

-- ARQUIVO SQL - AULAS_02_03_STUDY
-- PÁGINA 28

-- COMMAND ----------

-- EXERCÍCIO 1 - Lista de pedidos com mais de um item.

SELECT *
FROM silver.olist.item_pedido
WHERE idPedidoItem  = 2

-- COMMAND ----------

-- EXERCÍCIO 2 - Lista de pedidos que o frete é mais caro que o item.

SELECT *
FROM silver.olist.item_pedido
WHERE vlFrete > vlPreco

-- COMMAND ----------

-- EXERCÍCIO 3 - Lista de pedidos que ainda não foram enviados.

SELECT *
FROM silver.olist.pedido
WHERE descSituacao NOT LIKE 'delivered' AND descSituacao NOT LIKE 'shipped'

-- COMMAND ----------

-- EXERCÍCIO 4 - Lista de pedidos que foram entregues com atraso.

SELECT *
,date(dtEntregue)
,date(dtEstimativaEntrega)
FROM silver.olist.pedido
WHERE  date(dtEntregue) > date(dtEstimativaEntrega)

-- COMMAND ----------

-- EXERCÍCIO 5 - Lista de pedidos que foram entregues com 2 dias de antecedência.

SELECT *
,date(dtEntregue) AS dataEntrega
,date(dtEstimativaEntrega) AS dataEstimativa
,date_diff( dtEstimativaEntrega, dtEntregue ) AS diff_Estimativa_Entregue
FROM silver.olist.pedido
WHERE  date_diff( dtEstimativaEntrega, dtEntregue ) = 2

-- COMMAND ----------

-- EXERCÍCIO 6 - Lista de pedidos feitos em dezembro de 2017 e entregues com atraso

SELECT *
,date(dtEntregue)
,date(dtEstimativaEntrega)
FROM silver.olist.pedido
WHERE  date_diff( dtEntregue, dtEstimativaEntrega ) >= 1
AND month(dtPedido) = 12
AND year(dtPedido) = 2017

-- COMMAND ----------

-- EXERCÍCIO 7 - Lista de pedidos com avaliação maior ou igual que 4

SELECT *
FROM silver.olist.avaliacao_pedido
WHERE vlNota >= 4

-- COMMAND ----------

-- EXERCÍCIO 8 - Lista de pedidos com 2 ou mais parcelas menores que R$20,0

SELECT *
FROM silver.olist.pagamento_pedido
WHERE nrParcelas >= 2
AND vlPagamento/nrParcelas < 20

-- COMMAND ----------

-- ARQUIVO SQL - AULAS_02_03_STUDY
-- PÁGINA 29

-- COMMAND ----------

-- EXERCÍCIO 1 - Selecione todos os pedidos e marque se houve atraso ou não


SELECT *
,date(dtEntregue)
,date(dtEstimativaEntrega)
,CASE WHEN date(dtEntregue) > date(dtEstimativaEntrega) THEN 1 -- 1 tem atraso
      ELSE 0
      END AS EntregaAtrasada
FROM silver.olist.pedido

-- COMMAND ----------

-- EXERCÍCIO 2 - Selecione os pedidos e defina os grupos em uma nova coluna:
-- ● para frete inferior à 10%: ‘10%’
-- ● para frete entre 10% e 25%: ‘10% a 25%’
-- ● para frete entre 25% e 50%: ‘25% a 50%’
-- ● para frete maior que 50%: ‘+50%

SELECT *
,vlPreco + vlFrete AS vlTotal
,vlFrete/(vlPreco + vlFrete) AS vlPercFrete
,CASE WHEN vlFrete/(vlPreco + vlFrete) < 0.10 THEN '10%'
      WHEN vlFrete/(vlPreco + vlFrete) < 0.25 THEN '10% a 25%'
      WHEN vlFrete/(vlPreco + vlFrete) < 0.5 THEN '25% a 50%'
      ELSE '+50%'
END AS percFrete
FROM silver.olist.item_pedido


-- COMMAND ----------

-- ARQUIVO SQL - AULAS_02_03_STUDY
-- PÁGINA 30
-- Selecione a tabela silver.olist.produto :
-- ● Crie uma coluna nova chamada ‘descNovaCategoria’ seguindo:
-- i. agrupe ‘alimentos’ e ‘alimentos_bebidas’ em ‘alimentos’
-- ii. agrupe ‘artes’ e ‘artes_e_artesanato’ em ‘artes’
-- iii. agrupe todas categorias de construção em uma única categoria chamada ‘construção’
-- ● Cria uma coluna nova chamada ‘descPeso’
-- i. para peso menor que 2kg: ‘leve’
-- ii. para peso entre 2kg e 5kg: ‘médio’
-- iii. para peso entre 5kg e 10kg: ‘pesado’
-- iv. para peso maior que 10kg: ‘muito pesado’

SELECT *

,CASE WHEN descCategoria IN ('alimentos', 'alimentos_bebidas') THEN 'alimentos'
      WHEN descCategoria IN ('artes', 'artes_e_artesenato') THEN 'artes'
      WHEN descCategoria LIKE '%construcao%' THEN 'construção'
END AS descNovaCategoria

,CASE WHEN vlPesoGramas <= 2000 THEN 'leve'
      WHEN vlPesoGramas <= 5000 THEN 'médio'
      WHEN vlPesoGramas <= 10000 THEN 'pesado'
      WHEN vlPesoGramas > 10000 THEN 'muito pesado'
      END AS descPeso

FROM silver.olist.produto
