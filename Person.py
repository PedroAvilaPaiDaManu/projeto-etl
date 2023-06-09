# Databricks notebook source
pip install adal

# COMMAND ----------

pip install pandas


# COMMAND ----------

from pyspark.sql.types import *
import pandas as pd
from pyspark.sql.functions import*
from pyspark.sql import functions as f
from pyspark.sql.functions import sum
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import adal

# COMMAND ----------

#Fazendo a leitura do blob com a montagem do diretório (mount) 
#dbutils.fs.mount(source = "wasbs://pedro-avila@stgestudos.blob.core.windows.net",
 #  mount_point = "/mnt/pedro-avila",
  # extra_configs = {"")

# COMMAND ----------

df_person = spark.read.option("header", True).option("inferSchema", True).option("delimiter", ";").csv("/mnt/pedro-avila/projeto_pessoal/Person.Person.csv")
#df = spark.read.csv("dbfs:/mnt/mnt-pedro-avila/orders.csv")
display(df_person)




# COMMAND ----------

#import pyspark.sql.functions
#from pyspark.sql import SparkSession
#from pyspark.sql.functions import from_json, col ,split, explode
#from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DataType, IntegerType
#df2 = df.select(col("BusinessEntityID"),
 #               col("PersonType"),
 #               col("NameStyle"),
#                col("Title"),
  #              col("FirstName"),
  #              col("MiddleName"),
  #              col("LastName"),
  #              col("Suffix"),
  #              col("EmailPromotion"))
                

# COMMAND ----------

#df2.select('PersonType').distinct().collect()

# COMMAND ----------

df_production = spark.read.option("header", True).option("inferSchema", True).option("delimiter", ";").csv("/mnt/pedro-avila/projeto_pessoal/Production.Product.csv")
#df = spark.read.csv("dbfs:/mnt/mnt-pedro-avila/orders.csv")
display(df_production)

# COMMAND ----------

df_custumer = spark.read.option("header", True).option("inferSchema", True).option("delimiter", ";").csv("/mnt/pedro-avila/projeto_pessoal/Sales.Customer.csv")
display(df_custumer)

# COMMAND ----------

df_detail = spark.read.option("header", True).option("inferSchema", True).option("delimiter", ";").csv("/mnt/pedro-avila/projeto_pessoal/Sales.SalesOrderDetail.csv")
display(df_custumer)

# COMMAND ----------

df_header = spark.read.option("header", True).option("inferSchema", True).option("delimiter", ";").csv("/mnt/pedro-avila/projeto_pessoal/Sales.SalesOrderHeader.csv")
display(df_header)

# COMMAND ----------

df_offer_product = spark.read.option("header", True).option("inferSchema", True).option("delimiter", ";").csv("/mnt/pedro-avila/projeto_pessoal/Sales.SpecialOfferProduct.csv")
display(df_offer_product)

# COMMAND ----------

# calcular a quantidade de linhas por SalesOrderID
df_exer_um = (df_detail
      .groupBy("SalesOrderID")
      .agg(count("*").alias("RowCount")))

df_exer_um = df_exer_um.where(col("RowCount") >= 3)

df_exer_um.show(10)

# COMMAND ----------

# exercício dois
df_exer_dois = (df_detail
      .join(df_offer_product, "SpecialOfferID")
      .join(df_production, "ProductID"))


w = Window.partitionBy("DaysToManufacture", "Name").orderBy(F.desc("TotalOrderQty"))
df_exer_dois = (df_exer_dois
      .groupBy("DaysToManufacture", "Name")
      .agg(sum("OrderQty").alias("TotalOrderQty"))
      .withColumn("rank", F.rank().over(w)))

df_exer_dois = df_exer_dois.where(F.col("rank") <= 3)


df_exer_dois.show(10)

# COMMAND ----------

# exercício tres
df_exer_tres = (df_detail
      .join(df_offer_product, "ProductID")
      .join(df_production, "ProductID"))


df_exer_tres = (df_exer_tres
      .groupBy("DaysToManufacture", "Name")
      .agg(sum("OrderQty").alias("TotalQty")))

w = (Window
     .orderBy(col("TotalQty").desc())
     .partitionBy("DaysToManufacture"))



df_exer_tres = (df_exer_tres
      .withColumn("Rank", row_number().over(w))
      .where(col("Rank") <= 3)
      .drop("Rank"))


df_exer_tres.show()

# COMMAND ----------

# unindo dados com join
sales_df_exe_quatro = df_header.join(df_detail, "SalesOrderID") \
                                .join(df_production, "ProductID")

# ordendo
result_df = sales_df_exe_quatro.groupBy("ProductID", "OrderDate") \
                    .agg(sum("OrderQty").alias("TotalOrderQty"))

result_df.show(10)


# COMMAND ----------

# exercício 5
sales_orders = df_header

# aplicando filtro
filtered_orders = sales_orders.filter(
    (year("OrderDate") == 2011) & (month("OrderDate") == 9) & (sales_orders["TotalDue"] > 1000)
)

selected_columns = filtered_orders.select("SalesOrderID", "OrderDate", "TotalDue")

result = selected_columns.orderBy("TotalDue", ascending=False)

result.show(10)

# COMMAND ----------

# Refiz uma query em sql pq achei estranho dar zero
df_header.createOrReplaceTempView("sales_order_header")

# executa a consulta SQL sobre a tabela temporária
result = spark.sql("""
    SELECT SalesOrderID, OrderDate, TotalDue
    FROM sales_order_header
    WHERE YEAR(OrderDate) = 2011 AND MONTH(OrderDate) = 9 AND TotalDue > 1000
    ORDER BY TotalDue DESC
""")

result.show(10)

# COMMAND ----------

import adal
resource_app_id_url = ""
service_principal_id = dbutils.secrets.get(scope = , key = "")
service_principal_secret = dbutils.secrets.get(scope = "", key = "")
tenant_id = ""
authority = "https://login.windows.net/" + tenant_id
azure_sql_url = "jdbc:sqlserver://sql-estudo.database.windows.net"
database_name = "db-estudos"
table_person = "pedro_avila.person"
table_product = "pedro_avila.productionproduct"
table_customer = "pedro_avila.customer"
table_header = "pedro_avila.salesorderheader"
table_detail = "pedro_avila.salesorderdetail"
table_offerproduct = "pedro_avila.specialofferproduct"
encrypt = "true"
host_name_in_certificate = "*.database.windows.net"
context = adal.AuthenticationContext(authority)
token = context.acquire_token_with_client_credentials(resource_app_id_url, service_principal_id, service_principal_secret)
access_token = token["accessToken"]

# COMMAND ----------

df_person.write \
.format("jdbc")\
.mode("overwrite")\
.option("url", azure_sql_url) \
.option("dbtable", table_person) \
.option("databaseName", database_name) \
.option("accessToken", access_token) \
.option("encrypt", "true") \
.option("hostNameInCertificate", "*.database.windows.net") \
.save()

# COMMAND ----------

df_custumer.write \
.format("jdbc")\
.mode("overwrite")\
.option("url", azure_sql_url) \
.option("dbtable", table_customer) \
.option("databaseName", database_name) \
.option("accessToken", access_token) \
.option("encrypt", "true") \
.option("hostNameInCertificate", "*.database.windows.net") \
.save()

# COMMAND ----------


df_header.write \
.format("jdbc")\
.mode("overwrite")\
.option("url", azure_sql_url) \
.option("dbtable", table_header) \
.option("databaseName", database_name) \
.option("accessToken", access_token) \
.option("encrypt", "true") \
.option("hostNameInCertificate", "*.database.windows.net") \
.save()

# COMMAND ----------


df_production.write \
.format("jdbc")\
.mode("overwrite")\
.option("url", azure_sql_url) \
.option("dbtable", table_product) \
.option("databaseName", database_name) \
.option("accessToken", access_token) \
.option("encrypt", "true") \
.option("hostNameInCertificate", "*.database.windows.net") \
.save()

# COMMAND ----------

df_offer_product.write \
.format("jdbc")\
.mode("overwrite")\
.option("url", azure_sql_url) \
.option("dbtable", table_offerproduct) \
.option("databaseName", database_name) \
.option("accessToken", access_token) \
.option("encrypt", "true") \
.option("hostNameInCertificate", "*.database.windows.net") \
.save()


# COMMAND ----------

df_detail.write \
.format("jdbc")\
.mode("overwrite")\
.option("url", azure_sql_url) \
.option("dbtable", table_detail) \
.option("databaseName", database_name) \
.option("accessToken", access_token) \
.option("encrypt", "true") \
.option("hostNameInCertificate", "*.database.windows.net") \
.save()

