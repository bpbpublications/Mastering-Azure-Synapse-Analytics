--- change database from master to other Database

-- Defining External file format
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat') 
    CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] 
    WITH ( FORMAT_TYPE = PARQUET)
GO



--Defining external data source
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'public_pandemicatalake_blob_core_windows_net') 
    CREATE EXTERNAL DATA SOURCE [public_pandemicdatalake_blob_core_windows_net] 
    WITH (
        LOCATION = 'wasbs://public@pandemicdatalake.blob.core.windows.net' 
    )
GO

--Defining external table for polybase loading 
CREATE EXTERNAL TABLE covidexternal (   [id] int,
    [updated] date,
    [confirmed] int,
    [confirmed_change] int,
    [deaths] int,
    [deaths_change] smallint,
    [recovered] int,
    [recovered_change] int,
    [latitude] float,
    [longitude] float,
    [iso2] varchar (8000),
    [iso3] varchar (8000),
    [country_region] varchar (8000),
    [admin_region_1] varchar (8000),
    [iso_subdivision] varchar (8000),
    [admin_region_2] varchar (8000),
    [load_time] datetime2(7)
    )
    WITH (
    LOCATION = 'curated/covid-19/bing_covid-19_data/latest/bing_covid-19_data.parquet',
    DATA_SOURCE = [public_pandemicdatalake_blob_core_windows_net],
    FILE_FORMAT = [SynapseParquetFormat]
    )
GO
--- select data 
select * from covidexternal
--Create internal table and load data from external table using polybase based CTAS(create table as select *..) approach
CREATE TABLE [dbo].covidinternal
WITH
(
  DISTRIBUTION = ROUND_ROBIN
 ,CLUSTERED COLUMNSTORE INDEX
) 
AS
SELECT * FROM covidexternal
OPTION (LABEL = 'CTAS: Load covid')
;
--Run in Azure Synapse Dedicated SQLpool  DB--
----Row level encryption----
--1.	Created 3 user idid.
--2.	Create empty tabletable.
--3.	Inserted 6 rows inside the empty tabletable.
--4.	Check table data is okok.
--5.	Granting access to table for all the users
--6.	Creating a function which returns 1 when supervisor or user who is executing this query
--7.	Create security policy to attach the function on the precreated tabletable.
--8.	Grant the new function(role) to table
--9.	Execute using different login id and check only when its supervisor it shows all results else selected rows

CREATE USER Supervisor WITHOUT LOGIN;  
CREATE USER Order1 WITHOUT LOGIN;  
CREATE USER Order2 WITHOUT LOGIN;

CREATE TABLE Ordertable
    (  
    OrderID int,  
    OrderRep sysname,  
    Product varchar(10),  
    Qty int  
    );

INSERT INTO Ordertable VALUES (1, 'Order1', 'Valve', 5);
INSERT INTO Ordertable VALUES (2, 'Order2', 'Wheel', 2);
INSERT INTO Ordertable VALUES (3, 'Order2', 'Valve', 4);
INSERT INTO Ordertable VALUES (4, 'Order1', 'Bracket', 2);
INSERT INTO Ordertable VALUES (5, 'Order1', 'Wheel', 5);
INSERT INTO Ordertable VALUES (6, 'Order6', 'Seat', 5);

SELECT * FROM Ordertable;

GRANT SELECT ON Ordertable TO Supervisor;  
GRANT SELECT ON Ordertable TO Order1;  
GRANT SELECT ON Ordertable TO Order2;


CREATE FUNCTION fn_ordersecurity(@OrderRep AS sysname)  
    RETURNS TABLE  
WITH SCHEMABINDING  
AS  
    RETURN SELECT 1 AS fn_ordersecurity_result
WHERE @OrderRep = USER_NAME () OR USER_NAME() = 'Supervisor';

CREATE SECURITY POLICY Orderfilter   
ADD FILTER PREDICATE DBO.fn_ordersecurity (OrderRep)
ON DBO.Ordertable  
WITH (STATE = ON);  
 

GRANT SELECT ON fn_ordersecurity TO Supervisor;  
GRANT SELECT ON fn_ordersecurity TO Order1;  
GRANT SELECT ON fn_ordersecurity TO Order2;

EXECUTE AS USER = 'Order1';  
select USER_NAME();
SELECT * FROM DBO.Ordertable;
REVERT;  
  
EXECUTE AS USER = 'Order2';  
select USER_NAME()
SELECT * FROM  DBO.Ordertable;
REVERT;  
  
EXECUTE AS USER = 'Supervisor'; 
select USER_NAME();
SELECT * FROM DBO.Ordertable;
REVERT;

ALTER SECURITY POLICY Orderfilter  
WITH (STATE = OFF);

---Column level encryption example--


CREATE MASTER KEY ENCRYPTION BY   
PASSWORD = 'DBRemember@123';

CREATE CERTIFICATE covidcert 
   WITH SUBJECT = 'covidcert';  
GO  

CREATE SYMMETRIC KEY Covidkey 
    WITH ALGORITHM = AES_256  
    ENCRYPTION BY CERTIFICATE covidcert;  
GO  

-- Create a column in which to store the encrypted data. 
ALTER TABLE covidinternal  
    ADD country_region_encrypt varbinary(160);   
GO  
-- Open the symmetric key with which to encrypt the data. 

OPEN SYMMETRIC KEY Covidkey  
   DECRYPTION BY CERTIFICATE covidcert;  

-- Encrypt the value  using the  
-- symmetric key    
-- Save the result in new column .    
UPDATE covidinternal   
SET country_region_encrypt = convert(varbinary(160),(EncryptByKey(Key_GUID('Covidkey'), country_region)))
GO  

-- Verify the encryption. 
-- First, open the symmetric key with which to decrypt the data. 
OPEN SYMMETRIC KEY Covidkey  
   DECRYPTION BY CERTIFICATE covidcert;  
GO 

--check the data
select * from covidinternal
where id=55590469

 
