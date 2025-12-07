Azure Data Factory – Watermark Based Incremental Load (CSV → Azure SQL Database)

This project demonstrates how to design and implement a Watermark-based Incremental ETL pipeline using Azure Data Factory Mapping Data Flows.
It loads customer data from Azure Blob Storage (CSV) into Azure SQL Database, ensuring that only newly inserted or updated records are processed during each run.

This is one of the most common real-world ETL patterns in Data Engineering.

🚀 Project Architecture
Blob Storage (CSV)
        ↓
Lookup Watermark from SQL (LP_WaterMark_Value)
        ↓
Mapping Data Flow
   - Read CSV
   - Parse LastUpdatedDate
   - Filter > Watermark
   - AlterRow: Upsert
        ↓
Sink → Azure SQL Database (Customer Table)
        ↓
Update Metadata Table (sp_update_wm_customer)

🎯 Business Problem

We receive daily customer updates in CSV format.
Each row has a LastUpdatedDate column that indicates when that record was last modified.

We must avoid reloading old data and only load:

New rows

Updated rows

This must work across multiple runs, including the very first load when the target table is empty.

🧩 Key Features Implemented
✔ 1. Metadata-Driven Watermark

A watermark table tracks the last successfully loaded timestamp:

SELECT ISNULL(WatermarkValue, '1900-01-01') AS WatermarkValue
FROM dbo.ETL_Metadata
WHERE TableName = 'Customer';


This solves the "first run" problem by defaulting to an old date.

✔ 2. Lookup Activity (Pipeline)

The pipeline retrieves the watermark:

@formatDateTime(
    activity('LP_WaterMark_Value').output.firstRow.WatermarkValue,
    'yyyy-MM-dd HH:mm:ss'
)


ADF Lookup outputs date in ISO format (2025-12-04T15:20:11Z),
so we convert it to CSV-compatible format (yyyy-MM-dd HH:mm:ss).

✔ 3. Mapping Data Flow

The data flow performs:

Source → Blob CSV

Reads the CSV file with schema:

| CustomerId | CustomerName | Country | LastUpdatedDate |

Derived Column (optional)

Converted to timestamp:

toTimestamp(LastUpdatedDate, 'yyyy-MM-dd HH:mm:ss')

Filter Transformation

Only keep records newer than watermark:

toTimestamp(LastUpdatedDate, 'yyyy-MM-dd HH:mm:ss')
    > toTimestamp($pWatermark, 'yyyy-MM-dd HH:mm:ss')

Alter Row (Upsert Logic)
upsertIf( true() )


Marks every filtered row for UPSERT.

Sink (Azure SQL Database)

Upsert enabled

Key column: CustomerId

Mapping applied to target table

✔ 4. Updating the Watermark After Load

After the Data Flow finishes, a Stored Procedure updates the watermark:

CREATE OR ALTER PROCEDURE dbo.sp_update_wm_customer
AS
BEGIN
    DECLARE @MaxDate DATETIME;

    SELECT @MaxDate = MAX(LastUpdatedDate)
    FROM dbo.Customer;

    UPDATE dbo.ETL_Metadata
    SET WatermarkValue = @MaxDate
    WHERE TableName = 'Customer';
END;


This ensures the next run only loads records after the latest timestamp.

📁 Folder Structure (Recommended for Repository)
adf-watermark-project/
│
├── pipeline/
│     └── PL_Customer_Incremental.json
│
├── dataflow/
│     └── DF_Customer_Incremental.json
│
├── dataset/
│     ├── DS_Blob_Customer.json
│     └── DS_SQL_Customer.json
│
├── sql/
│     ├── CustomerTable.sql
│     ├── MetadataTable.sql
│     └── sp_update_wm_customer.sql
│
├── sample-data/
│     └── customers.csv
│
└── README.md   ← (This file)

🧪 How to Test the Pipeline
1. First Run

Metadata watermark = '1900-01-01'

All CSV rows load

Watermark updates to the latest date in CSV

2. Subsequent Runs

CSV rows with earlier dates are ignored

Only new/modified rows are upserted

🏆This Project Demonstrates 

Watermark-based incremental loading

Parameter passing from pipeline → data flow

Parsing and comparing timestamps in ADF

Upsert pattern using Alter Row

Metadata-driven ETL design

Reusable pipeline architecture

Handling first-load NULL watermarks

Real-world Data Engineering best practices
