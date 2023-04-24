# projeto-etl

Arquitetura do projeto a baixo!

![image](https://user-images.githubusercontent.com/121688647/233818620-6f90bffe-3255-4ef1-935d-93f1764bf1bf.png)


Utilizei o blob para armazenamento dos dados sem nenhum tratamento.
Com Databricks usei a linguagem pyspark para trabalhar o tratamento dos dados e aplicação de regra de negocio, para acelera o processo fiz o read direto em um dataframe como mostrado abaixo!

![image](https://user-images.githubusercontent.com/121688647/233819020-16103320-6213-4f5c-9b64-2603bd075819.png)

Para os 6 arquivo em formato csv foi feito um df diferente.


Esse são os conjutos de dados:


● Person.Person.csv

● Production.Product.csv

● Sales.SalesOrderHeader.csv

● Sales.Customer.csv

● Sales.SalesOrderDetail.csv

● Sales.SpecialOfferProduct.csv


Depois de Aplicado todas as regras de negocio utilizei o Data factory para fazer a ingestão dos dados com um pipeline no banco de dados Azure Sql, para posteriomente esses dados poderem ser acessados pelo Power BI.
