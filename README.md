# MySQL ➜ SQL Server Bulk Migration (Console App Template)

A minimal **.NET Console** template that demonstrates how to migrate data from **MySQL** to **SQL Server** using:
- `MySqlConnector` (read from MySQL)
- `SqlBulkCopy` via `Microsoft.Data.SqlClient` (fast insert into SQL Server)

This repo focuses on common real-world issues during migration:
- **MySQL “zero dates”** like `0000-00-00`
- **MySQL NULL → SQL NOT NULL** mismatches
- **UNIQUE constraints** (duplicate keys) in SQL Server

---

## Features (what the template demonstrates)

✅ **1) Zero-date handling (`0000-00-00`)**  
Legacy MySQL databases sometimes contain invalid date values (e.g. `0000-00-00`).  
The example query shows how to **sanitize/convert** these values to either:
- a safe sentinel date (e.g. `1900-01-01`), or
- `NULL` (when your SQL column allows it)

✅ **2) Defaults for SQL `NOT NULL` columns**  
When SQL Server requires `NOT NULL` but MySQL has `NULL` (or doesn’t even have that column), the example shows how to:
- add missing columns into a `DataTable`, and
- populate **fallback values** (`"Unknown"`, `"N/A"`, `0`, etc.)

✅ **3) Duplicate prevention for SQL UNIQUE indexes**  
If SQL Server has a `UNIQUE` index/constraint (e.g. on email), `SqlBulkCopy` will fail on duplicates.  
The template shows how to:
- normalize a key field (trim/lower),
- remove duplicates **inside the import set**, and
- skip rows that already exist in SQL Server.

---

## Requirements

- **.NET SDK** (recommended: .NET 8)
- A running **MySQL** instance
- A running **SQL Server** instance

---

## Create the Console App

```bash
dotnet new console -n MysqlToSqlServerBulkMigration
cd MysqlToSqlServerBulkMigration

## Install the required NuGet packages:
# MySQL driver
dotnet add package MySqlConnector

# SQL Server driver (SqlBulkCopy)
dotnet add package Microsoft.Data.SqlClient

# Configuration (appsettings.json + env vars + strongly-typed binding)
dotnet add package Microsoft.Extensions.Configuration
dotnet add package Microsoft.Extensions.Configuration.Json
dotnet add package Microsoft.Extensions.Configuration.EnvironmentVariables
dotnet add package Microsoft.Extensions.Configuration.Binder
