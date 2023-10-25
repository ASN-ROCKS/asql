-- Databricks notebook source
-- selecione todos os clientes paulistanos 
SELECT idCliente, descUF
FROM silver.olist.cliente
WHERE descUF = 'SP'


-- COMMAND ----------

-- selecone todos os clinetes paulistas

SELECT idCliente, descCidade
FROM silver.olist.cliente
WHERE descCidade = 'sao paulo'


-- COMMAND ----------

-- selecione todos os vendedores cariocas(RJ cidade) e paulista(estado todo)

select idVendedor, descCidade, descUF
from silver.olist.vendedor
where (descCidade = 'rio de janeiro' and descUF = 'RJ') or descUF = 'SP' -- esse or é importante então isolamos o and e depois o or para poder pegar dois resultados , nao posso usar uma lista para isso pq 
order by descCidade

-- COMMAND ----------

-- selecione produtos de perfumaria e bebes com altura maior de 5 cm

select idProduto, descCategoria, vlAlturaCm
from silver.olist.produto
where descCategoria in ('perfumaria', 'bebes') and vlAlturaCm > 5
order by vlAlturaCm asc

-- COMMAND ----------

-- Lista de pedidos com mais de um item.
SELECT DISTINCT idPedido
FROM silver.olist.item_pedido
WHERE idPedidoItem >1



-- COMMAND ----------

-- Lista de pedidos que o frete é mais caro que o item.

SELECT *
FROM silver.olist.item_pedido
WHERE vlFrete > vlPreco


-- COMMAND ----------

-- Lista de pedidos que ainda não foram enviados.
SELECT *
FROM silver.olist.pedido
WHERE dtEntregue IS NULL

-- COMMAND ----------

--Lista de pedidos que foram entregues com atraso.

SELECT *
FROM silver.olist.pedido
WHERE date(dtEntregue) > date(dtEstimativaEntrega)

-- COMMAND ----------

--Lista de pedidos que foram entregues com 2 dias de antecedência.

SELECT *
FROM silver.olist.pedido
WHERE DATEDIFF(day, dtEstimativaEntrega, dtEntregue) = 2

-- COMMAND ----------

--Lista de pedidos feitos em dezembro de 2017 e entregues com atraso

SELECT *
FROM silver.olist.pedido
WHERE EXTRACT(month FROM dtPedido) = 12 and dtEntregue > dtEstimativaEntrega

-- COMMAND ----------

--Lista de pedidos com avaliação maior ou igual que 4

SELECT *
FROM silver.olist.avaliacao_pedido
WHERE vlNota >= 4
ORDER BY vlNota

-- COMMAND ----------

-- Lista de pedidos com 2 ou mais parcelas menores que R$20,00

SELECT *
FROM silver.olist.pagamento_pedido
WHERE nrParcelas >=2 AND vlPagamento > 20
ORDER BY nrParcelas

-- COMMAND ----------

--Selecione todos os pedidos e marque se houve atraso ou não

SELECT * ,
CASE WHEN date(dtEntregue) > date(dtEstimativaEntrega) THEN 'Atrasou'
ELSE 'Não atrasou'
END AS condicao

FROM silver.olist.pedido


-- COMMAND ----------

--Selecione os pedidos e defina os grupos em uma nova coluna:
--para frete inferior à 10%: ‘10%’
--para frete entre 10% e 25%: ‘10% a 25%’
--para frete entre 25% e 50%: ‘25% a 50%’
--para frete maior que 50%: ‘+50%’

SELECT * ,
CASE WHEN vlFrete/vlPreco < 0.10 THEN '10%'
WHEN vlFrete/vlPreco >= 0.10 and vlFrete/vlPreco < 0.25 THEN '10% a 25%'
WHEN vlFrete/vlPreco >= 0.25 and vlFrete/vlPreco < 0.50 THEN '25% a 50%'
WHEN vlFrete/vlPreco >= 0.50 THEN '+50%'
END as condicao
FROM silver.olist.item_pedido



-- COMMAND ----------

-- MAGIC %md
-- MAGIC **DESAFIO**
-- MAGIC
-- MAGIC Selecione a tabela silver.olist.produto :
-- MAGIC ○ Crie uma coluna nova chamada ‘descNovaCategoria’ seguindo:
-- MAGIC i. agrupe ‘alimentos’ e ‘alimentos_bebidas’ em ‘alimentos’
-- MAGIC ii. agrupe ‘artes’ e ‘artes_e_artesanato’ em ‘artes’
-- MAGIC iii. agrupe todas categorias de construção em uma única categoria
-- MAGIC chamada ‘construção’
-- MAGIC ○ Cria uma coluna nova chamada ‘descPeso’
-- MAGIC i. para peso menor que 2kg: ‘leve’
-- MAGIC ii. para peso entre 2kg e 5kg: ‘médio’
-- MAGIC iii. para peso entre 5kg e 10kg: ‘pesado’
-- MAGIC iv. para peso maior que 10kg: ‘muito pesado’

-- COMMAND ----------

SELECT *
    --CASE WHEN des

FROM silver.olist.produto
