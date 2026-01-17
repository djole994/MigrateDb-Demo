using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using MySqlConnector;

internal class Program
{
    static async Task Main()
    {
        // Standard .NET configuration (appsettings.json + env vars)
        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
            .AddEnvironmentVariables()
            .Build();

        var mysqlCs = config.GetConnectionString("MySql")
            ?? throw new InvalidOperationException("Missing ConnectionStrings:MySql in appsettings.json");
        var sqlCs = config.GetConnectionString("SqlServer")
            ?? throw new InvalidOperationException("Missing ConnectionStrings:SqlServer in appsettings.json");

        var settings = config.GetSection("Migration").Get<MigrationSettings>()
            ?? new MigrationSettings();

        // -----------------------------
        // ⚠️ Critical note (scalability)
        // -----------------------------
        // This template loads rows into memory (DataTable) and can also load existing keys into memory.
        // For large datasets, implement paging/batching (e.g., 10k rows per batch) and/or a staging table
        // + MERGE strategy to avoid OutOfMemoryException and improve performance.
        //
        // Recommended production approach:
        // 1) Read MySQL rows in batches (LIMIT/OFFSET or keyset pagination)
        // 2) Bulk copy into a SQL staging table
        // 3) MERGE staging -> target (UPSERT + de-dupe)
        // 4) Repeat per batch

        // -----------------------------
        // 1) MySQL SELECT (placeholders)
        // -----------------------------
        // Replace: source_table, column names, and filters.
        // Demonstrates safe handling of MySQL "zero dates" (0000-00-00).
        var mysqlQuery = @"
SELECT
    id AS Id,
    col_a AS ColA,
    col_b AS ColB,

    STR_TO_DATE(
        COALESCE(
            NULLIF(LEFT(CAST(date_col AS CHAR), 10), '0000-00-00'),
            '1900-01-01'
        ),
        '%Y-%m-%d'
    ) AS SafeDate,

    nullable_text_col AS OptionalText,
    unique_key_col    AS UniqueKey
FROM source_table
WHERE id IS NOT NULL;
";

        // 2) Read to DataTable
        var dt = new DataTable();

        await using (var myConn = new MySqlConnection(mysqlCs))
        {
            await myConn.OpenAsync();
            await using var cmd = new MySqlCommand(mysqlQuery, myConn);
            await using var reader = await cmd.ExecuteReaderAsync();
            dt.Load(reader);
        }

        var totalRead = dt.Rows.Count;

        // 3) Transformations / Defaults (NULL -> NOT NULL handling)
        dt.Columns.Add("DerivedValue", typeof(string));
        dt.Columns.Add("Status", typeof(int));
        dt.Columns.Add("CreatedUtc", typeof(DateTime));

        foreach (DataRow row in dt.Rows)
        {
            var a = row["ColA"]?.ToString()?.Trim();
            var b = row["ColB"]?.ToString()?.Trim();
            row["DerivedValue"] = string.Join(" ", new[] { a, b }.Where(s => !string.IsNullOrWhiteSpace(s)));

            var optional = row["OptionalText"]?.ToString()?.Trim();
            row["OptionalText"] = string.IsNullOrWhiteSpace(optional) ? settings.DefaultOptionalText : optional;

            row["Status"] = settings.DefaultStatus;
            row["CreatedUtc"] = DateTime.UtcNow;

            if (row.IsNull("SafeDate"))
                row["SafeDate"] = settings.DefaultDateFallback;
        }

        // 4) Remove duplicates to prevent UNIQUE constraint failure
        await using (var sqlConn = new SqlConnection(sqlCs))
        {
            await sqlConn.OpenAsync();

            HashSet<string> existingKeys = new(StringComparer.OrdinalIgnoreCase);

            if (settings.LoadExistingKeysIntoMemory)
            {
                // ⚠️ Critical note: this may be large for big target tables.
                existingKeys = await LoadExistingKeysAsync(
                    sqlConn,
                    settings.TargetTable,
                    settings.TargetUniqueKeyColumn
                );
            }

            int skipped = RemoveDuplicatesAndExisting(
                dt,
                keyColumn: "UniqueKey",
                existingKeys: existingKeys,
                strictDeleteRowsWithoutKey: settings.StrictDeleteRowsWithoutKey
            );

            using var bulk = new SqlBulkCopy(sqlConn)
            {
                DestinationTableName = settings.TargetTable,
                BatchSize = settings.BulkBatchSize,
                BulkCopyTimeout = settings.BulkTimeoutSeconds
            };

            // Example mappings (replace with your target schema)
            bulk.ColumnMappings.Add("Id", "Id");
            bulk.ColumnMappings.Add("ColA", "ColA");
            bulk.ColumnMappings.Add("ColB", "ColB");
            bulk.ColumnMappings.Add("SafeDate", "SomeDateColumn");
            bulk.ColumnMappings.Add("OptionalText", "OptionalText");
            bulk.ColumnMappings.Add("UniqueKey", "UniqueKey");
            bulk.ColumnMappings.Add("DerivedValue", "DerivedValue");
            bulk.ColumnMappings.Add("Status", "Status");
            bulk.ColumnMappings.Add("CreatedUtc", "CreatedUtc");

            await bulk.WriteToServerAsync(dt);

            Console.WriteLine("Migration completed.");
            Console.WriteLine($"- Total rows read from MySQL: {totalRead}");
            Console.WriteLine($"- Skipped rows (duplicates / missing keys / already exist): {skipped}");
            Console.WriteLine($"- Inserted into SQL: {dt.Rows.Count}");
        }
    }

    static async Task<HashSet<string>> LoadExistingKeysAsync(SqlConnection sqlConn, string targetTable, string targetUniqueKeyColumn)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // Note: identifiers can't be parameterized. Keep these values trusted (config controlled).
        var sql = $@"
SELECT {targetUniqueKeyColumn}
FROM {targetTable}
WHERE {targetUniqueKeyColumn} IS NOT NULL;";

        using var cmd = new SqlCommand(sql, sqlConn);
        using var r = await cmd.ExecuteReaderAsync();

        while (await r.ReadAsync())
        {
            var key = NormalizeKey(r.GetValue(0));
            if (!string.IsNullOrWhiteSpace(key))
                set.Add(key);
        }

        return set;
    }

    static int RemoveDuplicatesAndExisting(
        DataTable dt,
        string keyColumn,
        HashSet<string> existingKeys,
        bool strictDeleteRowsWithoutKey)
    {
        int removed = 0;
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        for (int i = dt.Rows.Count - 1; i >= 0; i--)
        {
            var row = dt.Rows[i];

            var key = NormalizeKey(row[keyColumn]);

            if (string.IsNullOrWhiteSpace(key))
            {
                if (strictDeleteRowsWithoutKey)
                {
                    row.Delete();
                    removed++;
                    continue;
                }

                // Alternatively, generate a placeholder key (only if your business rules allow it).
                key = $"missing-key-{Guid.NewGuid():N}";
                row[keyColumn] = key;
            }

            row[keyColumn] = key;

            if (!seen.Add(key) || (existingKeys.Count > 0 && existingKeys.Contains(key)))
            {
                row.Delete();
                removed++;
            }
        }

        dt.AcceptChanges();
        return removed;
    }

    static string? NormalizeKey(object value)
    {
        if (value == DBNull.Value) return null;
        var s = value.ToString()?.Trim();
        if (string.IsNullOrWhiteSpace(s)) return null;
        return s.ToLowerInvariant();
    }

    private sealed class MigrationSettings
    {
        public string TargetTable { get; set; } = "dbo.TargetTable";
        public string TargetUniqueKeyColumn { get; set; } = "UniqueKey";

        // Scalability toggle: disable loading all existing keys for huge tables
        public bool LoadExistingKeysIntoMemory { get; set; } = true;

        // Data quality rules
        public bool StrictDeleteRowsWithoutKey { get; set; } = true;

        // Defaults for NOT NULL columns
        public string DefaultOptionalText { get; set; } = "Unknown";
        public int DefaultStatus { get; set; } = 0;
        public DateTime DefaultDateFallback { get; set; } = new DateTime(1900, 1, 1);

        // Bulk copy options
        public int BulkBatchSize { get; set; } = 1000;
        public int BulkTimeoutSeconds { get; set; } = 0;
    }
}
