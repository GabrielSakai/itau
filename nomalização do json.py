# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

#le todos os jsons dentro do repositorio e transforma o campo de data de string para timestamp
df = spark.read.option("multiline","true").json('dbfs:/FileStore/eventos_json/*').withColumn('dat_atui',col('dat_atui').cast('timestamp')).withColumn('dat_hor_tran',col('dat_hor_tran').cast('timestamp'))

df.persist()

# COMMAND ----------

#dataframe com o ultimo status do pedido
df_ultimo_status = df.groupBy('txt_detl_idt_pedi_pgto').agg(max('dat_atui').alias('last_status_date')).withColumnRenamed('txt_detl_idt_pedi_pgto','txt_detl_idt_pedi_pgto_last')

#Cruzamento entre o dataframe com todos os dados e seus ultimos status
df_pedidos_ultimo_status = df.join(df_ultimo_status, (col('txt_detl_idt_pedi_pgto') == col('txt_detl_idt_pedi_pgto_last')) & (col('dat_atui') == col('last_status_date')),'inner').drop('txt_detl_idt_pedi_pgto_last')


# COMMAND ----------

# lsita de colunas que serao selecionadas no dataframe final
lista = ['txt_detl_idt_pedi_pgto','cod_prod','cod_idef_clie','dat_atui','dat_hor_tran','stat_pedi','stat_pgto','cod_tipo_entg','cod_venr','dat_emis_cpvt','cod_prod_venr_ship','qtd_prod_ship','stat_entg_ship','prod_desc','vendedor_desc','cod_oferta','vlr_original','vlr_desconto','qtd_item_pedi']

#datarfame com os itens do pedido
df_pedido_produto = df_pedidos_ultimo_status\
                 .withColumn('list_item_pedi', explode_outer(col('list_item_pedi')))\
                 .select('txt_detl_idt_pedi_pgto','list_item_pedi.*')\
                 .withColumnRenamed('cod_vrne_prod','cod_prod')\
                 .withColumnRenamed('nom_item','prod_desc')\
                 .withColumnRenamed('nom_venr','vendedor_desc')\
                 .withColumnRenamed('vlr_oril','vlr_original')\
                 .withColumnRenamed('vlr_prod','vlr_desconto')\
                 .withColumnRenamed('idt_venr','cod_venr')\
                 .withColumnRenamed('cod_ofrt_ineo_requ','cod_oferta')\
                 .select('txt_detl_idt_pedi_pgto','cod_prod','prod_desc','cod_venr','vendedor_desc','cod_oferta','vlr_original','vlr_desconto','qtd_item_pedi')

#datarfame com os dados de pedidos e de envio
df_pedido_envio = df_pedidos_ultimo_status\
            .withColumn('list_envo', explode_outer(col('list_envo')))\
            .withColumn('cod_prod_item',col('list_item_pedi.cod_vrne_prod').getItem(0))\
            .selectExpr('txt_detl_idt_pedi_pgto','cod_idef_clie','dat_atui','dat_hor_tran','stat_pedi','stat_pgto','list_envo.cod_tipo_entg','list_envo.cod_venr','list_envo.dat_emis_cpvt','list_envo.list_item_envo','cod_prod_item')\
            .withColumn('cod_prod',col('list_item_envo.cod_prod').getItem(0))\
            .withColumn('cod_prod',when(col('cod_prod').isNull(),col('cod_prod_item')).otherwise(col('cod_prod')))\
            .withColumn('cod_prod_venr_ship',col('list_item_envo.cod_prod_venr').getItem(0))\
            .withColumn('qtd_prod_ship',col('list_item_envo.qtd_prod').getItem(0))\
            .withColumn('stat_entg_ship',col('list_item_envo.stat_entg').getItem(0))\
            .drop('list_item_envo','cod_prod_item')


#join entre as bases separando em dois dataframes por conta das chaves de join
df_pedido_produto_null = df_pedido_envio.filter('cod_venr is null').drop('cod_venr').join(df_pedido_produto,["txt_detl_idt_pedi_pgto","cod_prod"],'left').select(lista)
df_pedido_produto = df_pedido_envio.filter('cod_venr is not null').join(df_pedido_produto,["txt_detl_idt_pedi_pgto","cod_prod","cod_venr"],'left').select(lista)

#criação do dataframe final
df_final = df_pedido_produto_null.union(df_pedido_produto).drop('qtd_prod_ship','stat_entg_ship').withColumn('calyear_key',year(col('dat_atui'))).withColumn('calmonth_key',month(col('dat_atui')))


# COMMAND ----------

#escrita em parquet
df_final.write.mode('append').partitionBy("calyear_key", "calmonth_key").parquet('dbfs:/FileStore/bronze/tb_pedidos/')
