-- Databricks notebook source
SELECT *
FROM silver.olist.pedido
WHERE date(dtEntregue) > date(dtEstimativaEntrega) --aqui tiramos as horas e fizemos a operação no filtro

-- COMMAND ----------

-- USO DO OR E AND

SELECT * 
FROM silver.olist.produto
WHERE descCategoria = 'bebes'

-- COMMAND ----------

SELECT * 
FROM silver.olist.produto
WHERE (descCategoria = 'bebes'
OR descCategoria = 'perfumaria' -- aqui tem esse parentese para resolver um ou outro dentro
OR descCategoria = 'artes')
AND vlComprimentoCm * vlAlturaCm > 1000

--As condições dentro dos parênteses (descCategoria = 'bebes' OR descCategoria = 'perfumaria' OR descCategoria = 'artes') formam uma cláusula OR, que indica que um registro será incluído se atender a pelo menos uma das condições entre parênteses. Neste caso, o registro será incluído se a coluna "descCategoria" for igual a 'bebes', 'perfumaria' ou 'artes'.'''

-- COMMAND ----------

--operador IN
SELECT * 
FROM silver.olist.produto
WHERE descCategoria IN ('bebes','perfumaria', 'artes') -- o IN substitui melhor o OR
AND vlComprimentoCm * vlAlturaCm > 1000


-- COMMAND ----------

--operador LIKE

SELECT * 

FROM silver.olist.produto
WHERE descCategoria LIKE 'ferramentas%' -- % pegar qualquer coisa que vier depois, pode tbm ser no inicioou pode ser um nome e espaço%

-- COMMAND ----------

--usando STARTWITH
SELECT * 

FROM silver.olist.produto

WHERE startswith(descCategoria, 'ferramentas') -- uso do starwith e contains
AND contains(descCategoria, 'ferramentas' )

