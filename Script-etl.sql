--Criação da tabela Person

create table pedro_avila.Person(
	BusinessEntityID int not null, constraint PK_Person_Person primary key (BusinessEntityID),
	PersonType varchar(100) not null,
	NameStyle int not null,
	Title varchar(100) not null,
	FirstName varchar(100) not null,
	MiddleName varchar(100) not null,
	LastName varchar(100) not null,
	Suffix varchar(100) not null,
	EmailPromotion int not null,
	AdditionalContactInfo varchar(max) not null,
	Demographics varchar(max) not null,
	rowguid varchar(50) not null,
	ModifiedDate datetime2(7) not null

);

--Criação da tabela Customer

create table pedro_avila.Customer(
	CustomerID int not null, constraint PK_Sales_Customer primary key (CustomerID),
	PersonID varchar(100) not null,
	StoreID varchar(100) not null,
	TerritoryID int null,
	AccountNumber varchar(100) not null,
	rowguid varchar(100) not null,
	ModifiedDate datetime2(7) not null

);

--Criação da tabela SalesOrderHeader
-- criando constrant para colocar chave estrangeira de customer
create table pedro_avila.SalesOrderHeader(
	SalesOrderID int not null, constraint PK_Sales_Order_Header primary key (SalesOrderID),
	RevisionNumber int not null,
	OrderDate datetime2(7) not null,
	DueDate datetime2(7) not null,
	ShipDate datetime2(7) not null,
	Status int not null,
	OnlineOrderFlag int not null,
	SalesOrderNumber varchar(100) not null,
	PurchaseOrderNumber varchar(100) not null,
	AccountNumber varchar(100) not null,
	CustomerID int not null,
	CONSTRAINT FK_CustomerID FOREIGN KEY (CustomerID)
	REFERENCES pedro_avila.Customer(CustomerID),
	SalesPersonID varchar(100) not null,
	TerritoryID int not null,
	BillToAddressID int not null,
	ShipToAddressID int not null,
	ShipMethodID int not null,
	CreditCardID varchar(max) null,
	CreditCardApprovalCode varchar(100) not null,
	CurrencyRateID varchar(max) null,
	SubTotal float not null,
	TaxAmt float not null,
	Freight float not null,
	TotalDue float not null,
	Comment varchar(100) not null,
	rowguid varchar(100) not null,
	ModifiedDate datetime2(7) not null
);

--Criação da tabela ProductionProduct

create table pedro_avila.ProductionProduct(
	ProductID int not null, constraint PK_ProductionProduct primary key (ProductID),
	Name varchar(100) not null,
	ProductNumber varchar(100) not null,
	MakeFlag int not null,
	FinishedGoodsFlag int null,
	Color varchar(100) not null,
	SafetyStockLevel int not null,
	ReorderPoint int not null,
	StandardCost float not null,
	ListPrice float not null,
	Size varchar(100) not null,
	SizeUnitMeasureCode varchar(100) not null,
	Weight varchar(100) not null,
	WeightUnitMeasureCode varchar(50) not null,
	DaysToManufacture int not null,
	ProductLine varchar(100) not null,
	Class varchar(100) not null,
	Style varchar(100) not null,
	ProductSubcategoryID varchar(100) not null,
	ProductModelID varchar(100) not null,
	SellStartDate datetime2(7) not null,
	SellEndDate varchar(100) not null,
	DiscontinuedDate varchar(100) not null,
	rowguid varchar(100) not null,
	ModifiedDate datetime2(7) not null

);

--Criação da tabela SpecialOfferProduct
-- criando constrant para colocar chave estrangeira de produto

create table pedro_avila.SpecialOfferProduct(
	SpecialOfferID int null,
	ProductID int not null,
	CONSTRAINT FK_ProductID FOREIGN KEY (ProductID)
	REFERENCES pedro_avila.ProductionProduct(ProductID),
	rowguid varchar(100) not null, constraint PK_SpecialOfferProduct primary key (rowguid),
	ModifiedDate datetime2(7) not null

);


--Criação da tabela SalesOrderDetail
-- criando constrant para colocar chave estrangeira de header
-- criando constrant para colocar chave estrangeira de SpecialOfferProduct
CREATE TABLE pedro_avila.SalesOrderDetail (
    SalesOrderID INT NOT NULL,
    SalesOrderDetailID INT NOT NULL,
    CONSTRAINT PK_SalesOrderDetail PRIMARY KEY (SalesOrderDetailID),
    CarrierTrackingNumber VARCHAR(100) NOT NULL,
    OrderQty INT NOT NULL,
    ProductID INT NOT NULL,
    SpecialOfferID INT NOT NULL,
    CONSTRAINT FK_SalesOrderID FOREIGN KEY (SalesOrderID)
        REFERENCES pedro_avila.SalesOrderHeader(SalesOrderID),
    CONSTRAINT FK_SpecialOfferID FOREIGN KEY (rowguid)
        REFERENCES pedro_avila.SpecialOfferProduct(rowguid),
    UnitPrice FLOAT NOT NULL,
    UnitPriceDiscount FLOAT NOT NULL,
    LineTotal FLOAT NOT NULL,
    rowguid VARCHAR(100) NOT NULL,
    ModifiedDate DATETIME2(7) NOT NULL
);



sp_help [pedro_avila.person]
sp_help [pedro_avila.customer]
sp_help [pedro_avila.salesorderdetail]
sp_help [pedro_avila.specialofferproduct]
sp_help [pedro_avila.productionproduct]
sp_help [pedro_avila.salesorderheader]




select * from pedro_avila.person
select * from pedro_avila.customer
select * from pedro_avila.salesorderdetail
select * from pedro_avila.specialofferproduct
select * from pedro_avila.productionproduct
select * from pedro_avila.salesorderheader








