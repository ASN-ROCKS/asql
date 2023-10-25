-- Databricks notebook source

SELECT * 
FROM silver.olist.produto
WHERE descCategoria = 'bebes'
ORDER BY vlPesoGramas DESC, vlComprimentoCm DESC -- ordem descrecente do peso e também pelo comprimento, temos que colocar asc ou desc em cada variavel

