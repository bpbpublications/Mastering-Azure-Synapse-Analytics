--Chapter 4-- Serverless SQL sample T-SQL for data analysis,creating view
-- Understand the data
SELECT
    TOP 100 *
FROM
    OPENROWSET (
        BULK     'https://azureopendatastorage.blob.core.windows.net/citydatacontainer/Safety/Release/city=Boston/*.parquet',
        FORMAT = 'parquet'
    ) AS [result]

-- Running query to find count of incident based on safety category
select 
  result1.category AS [category]
    , COUNT_BIG (*) AS [safetyincident]
FROM
    OPENROWSET (
        BULK     'https://azureopendatastorage.blob.core.windows.net/citydatacontainer/Safety/Release/city=Boston/*.parquet',
        FORMAT = 'parquet'
    ) result1
GROUP BY result1.category

-- Creating view which can be used to serve the data in Semantic,BI layer

CREATE VIEW bostonsafetyview AS
select 
  result1.category AS [category]
    , COUNT_BIG (*) AS [safetyincident]
FROM
    OPENROWSET (
        BULK     'https://azureopendatastorage.blob.core.windows.net/citydatacontainer/Safety/Release/city=Boston/*.parquet',
        FORMAT = 'parquet'
    ) result1
GROUP BY result1.category

-- Explore data from view created in previous step

select * from bostonsafetyview

-- In the next few steps we want to export the view data in storage account



--Step 1 Create Database masterkey
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'password@4219847'
GO

--Step 2 Create Database scoped credential which will be used in the subsequent step for storage authentication mechanism
-- Follow book steps and replace the secret with your storage account secret

CREATE DATABASE SCOPED CREDENTIAL [DBTokenWr]
WITH IDENTITY= 'SHARED ACCESS SIGNATURE',
     SECRET = '?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2022-08-15T14:28:33Z&st=2022-08-11T06:28:33Z&spr=https&sig=8HQx5SwY2tF%2F7Hv%2BEqMApu4cPRRjo1JokLn7WB8ivds%3D'
GO

--Step 3 Define and create data source, this will point the cloud storage path
-- Change it to your own storage path, follow boook steps

CREATE EXTERNAL DATA SOURCE [ServerlessDt] WITH (
    LOCATION = 'https://debdatalakegen21.dfs.core.windows.net/test/', CREDENTIAL = [DBTokenWr]
)
GO


--Step 4 Define the file format, in this example its parquet format

CREATE EXTERNAL FILE FORMAT [Parquetboston] WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)

--Step 5 Now we create external table which will export the data in cloud storage account.

CREATE EXTERNAL TABLE [dbo]. [bostoncetas] WITH (
        LOCATION = 'deb12345',
        DATA_SOURCE = [ServerlessDt],
        FILE_FORMAT = [Parquetboston]
) AS
select * from bostonsafetyview




