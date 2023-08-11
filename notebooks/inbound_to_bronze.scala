// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC # Verificando o acesso a pasta
// MAGIC

// COMMAND ----------

// DBTITLE 0,Verificando o acesso a pasta
// MAGIC
// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/inbound")
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC #Lendo os dados na camada de inbound (dados brutos)

// COMMAND ----------

val path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
val dados = spark.read.json(path)

// COMMAND ----------

display(dados)

// COMMAND ----------

// MAGIC %md
// MAGIC #Removendo colunas

// COMMAND ----------

val dados_anuncio  = dados.drop("imagens","usuario")
display(dados_anuncio)

// COMMAND ----------

// MAGIC %md
// MAGIC #Criando uma coluna de identificação

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

val df_bronze = dados_anuncio.withColumn("ID",col("anuncio.id"))
display(df_bronze)

// COMMAND ----------

// MAGIC %md
// MAGIC # Salvando na camada bronze

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)
