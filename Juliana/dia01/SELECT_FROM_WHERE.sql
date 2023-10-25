-- Databricks notebook source
select
  *
from silver.olist.pedido 
where not date(dtEntregue) > date(dtEstimativaEntrega)

-- COMMAND ----------

select
  *
from silver.olist.produto
where descCategoria in ('bebes','perfumarias','artes') 
and vlComprimentoCm * vlAlturaCm * vlLarguraCm>1000

-- COMMAND ----------

select 
  *
from silver.olist.produto
where descCategoria like ('%ferramentas%')
