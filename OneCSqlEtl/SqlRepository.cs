// SqlRepository.cs
#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using System.Data;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Linq; // Added for .Any()

// DTOs (Customer1C, Product1C, etc.) and the main Settings class (with ConnectionStrings and EtlSettings properties)
// are defined in Models.cs and Settings.cs respectively.
// No need to redefine them here.

namespace OneCSqlEtl
{
    public class SqlRepository
    {
        private readonly string _connString;
        private readonly ILogger<SqlRepository> _log;
        private readonly int _commandTimeout; // Will be read from settings

        // The 'Settings' type here will correctly resolve to the one defined in Settings.cs
        public SqlRepository(IOptions<Settings> opts, ILogger<SqlRepository> log)
        {
            _log = log ?? throw new ArgumentNullException(nameof(log));

            var settingsValue = opts?.Value ?? throw new ArgumentNullException(nameof(opts), "Settings (IOptions<Settings>.Value) is null.");

            _connString = settingsValue.ConnectionStrings?.SqlServerConnectionString
                ?? throw new ArgumentNullException(nameof(settingsValue.ConnectionStrings.SqlServerConnectionString), "Settings.ConnectionStrings.SqlServerConnectionString is null or missing.");

            _commandTimeout = settingsValue.EtlSettings?.SqlCommandTimeout ?? 60; // Default to 60 if not found or EtlSettings is null
            if (_commandTimeout <= 0)
            {
                _log.LogWarning("Invalid SqlCommandTimeout in settings: {ConfiguredTimeout}. Using default 60 seconds.", _commandTimeout);
                _commandTimeout = 60;
            }
        }

        // The DTO types (Customer1C, Product1C, etc.) used as parameters
        // will correctly resolve to those defined in Models.cs

        public async Task<int> GetOrCreateCustomerSKAsync(Customer1C data)
        {
            if (data == null)
            {
                throw new ArgumentNullException(nameof(data), "Данные контрагента не могут быть null");
            }

            await using var conn = new SqlConnection(_connString);
            try
            {
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandTimeout = _commandTimeout;
                cmd.CommandText = @"
SET NOCOUNT ON;
DECLARE @sk INT;
SELECT @sk = CustomerSK
  FROM Analytics.DimCustomers
 WHERE CustomerID_1C = @Ref1C;
IF @sk IS NULL
BEGIN
  INSERT INTO Analytics.DimCustomers
    (CustomerID_1C, CustomerCode_1C, CustomerName, CustomerFullName, TIN, KPP, EntityType, IsActive)
  VALUES
    (@Ref1C, @Code, @Name, @FullName, @TIN, @KPP, @EntityType, @IsActive);
  SELECT @sk = SCOPE_IDENTITY();
END;
SELECT @sk;";
                cmd.Parameters.AddWithValue("@Ref1C", data.Ref);
                cmd.Parameters.AddWithValue("@Code", (object?)data.Code ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@Name", data.Name);
                cmd.Parameters.AddWithValue("@FullName", (object?)data.FullName ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@TIN", (object?)data.TIN ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@KPP", (object?)data.KPP ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@EntityType", (object?)data.EntityType ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@IsActive", !data.IsDeleted);

                var res = await cmd.ExecuteScalarAsync();
                if (res == null || res == DBNull.Value)
                {
                    _log.LogError("Failed to get or create CustomerSK for {CustomerName} (Ref: {CustomerRef}). ExecuteScalarAsync returned null/DBNull.", data.Name, data.Ref);
                    throw new InvalidOperationException($"Could not retrieve or create CustomerSK for Customer Ref {data.Ref}.");
                }
                return Convert.ToInt32(res);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Ошибка при получении/создании CustomerSK для контрагента {CustomerName} (Ref: {CustomerRef})",
                    data.Name, data.Ref);
                throw;
            }
        }

        public async Task<int> GetOrCreateProductSKAsync(Product1C data)
        {
            if (data == null)
            {
                throw new ArgumentNullException(nameof(data), "Данные продукта не могут быть null");
            }

            await using var conn = new SqlConnection(_connString);
            try
            {
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandTimeout = _commandTimeout;
                cmd.CommandText = @"
SET NOCOUNT ON;
DECLARE @sk INT;
SELECT @sk = ProductSK
  FROM Analytics.DimProducts
 WHERE ProductID_1C = @Ref1C;
IF @sk IS NULL
BEGIN
  INSERT INTO Analytics.DimProducts
    (ProductID_1C, ProductCode_1C, ProductName, ProductFullName, SKU, UnitOfMeasure, ProductType, ProductGroup, DefaultVATRateName)
  VALUES
    (@Ref1C, @Code, @Name, @FullName, @SKU, @UnitOfMeasure, @ProductType, @ProductGroup, @DefaultVATRateName);
  SELECT @sk = SCOPE_IDENTITY();
END;
SELECT @sk;";
                cmd.Parameters.AddWithValue("@Ref1C", data.Ref);
                cmd.Parameters.AddWithValue("@Code", (object?)data.Code ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@Name", data.Name);
                cmd.Parameters.AddWithValue("@FullName", (object?)data.FullName ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@SKU", (object?)data.SKU ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@UnitOfMeasure", (object?)data.UnitOfMeasure ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@ProductType", (object?)data.ProductType ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@ProductGroup", (object?)data.ProductGroup ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@DefaultVATRateName", (object?)data.DefaultVATRateName ?? DBNull.Value);

                var res = await cmd.ExecuteScalarAsync();
                if (res == null || res == DBNull.Value)
                {
                    _log.LogError("Failed to get or create ProductSK for {ProductName} (Ref: {ProductRef}). ExecuteScalarAsync returned null/DBNull.", data.Name, data.Ref);
                    throw new InvalidOperationException($"Could not retrieve or create ProductSK for Product Ref {data.Ref}.");
                }
                return Convert.ToInt32(res);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Ошибка при получении/создании ProductSK для продукта {ProductName} (Ref: {ProductRef})",
                    data.Name, data.Ref);
                throw;
            }
        }

        public async Task<int> GetOrCreateOrganizationSKAsync(Organization1C data)
        {
            if (data == null)
            {
                throw new ArgumentNullException(nameof(data), "Данные организации не могут быть null");
            }

            await using var conn = new SqlConnection(_connString);
            try
            {
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandTimeout = _commandTimeout;
                cmd.CommandText = @"
SET NOCOUNT ON;
DECLARE @sk INT;
SELECT @sk = OrganizationSK
  FROM Analytics.DimOrganizations
 WHERE OrganizationID_1C = @Ref1C;
IF @sk IS NULL
BEGIN
  INSERT INTO Analytics.DimOrganizations
    (OrganizationID_1C, OrganizationCode_1C, OrganizationName, OrganizationFullName)
  VALUES
    (@Ref1C, @Code, @Name, @OrganizationFullName);
  SELECT @sk = SCOPE_IDENTITY();
END;
SELECT @sk;";
                cmd.Parameters.AddWithValue("@Ref1C", data.Ref);
                cmd.Parameters.AddWithValue("@Code", (object?)data.Code ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@Name", data.Name);
                cmd.Parameters.AddWithValue("@OrganizationFullName", (object?)data.OrganizationFullName ?? DBNull.Value);

                var res = await cmd.ExecuteScalarAsync();
                if (res == null || res == DBNull.Value)
                {
                    _log.LogError("Failed to get or create OrganizationSK for {OrganizationName} (Ref: {OrganizationRef}). ExecuteScalarAsync returned null/DBNull.", data.Name, data.Ref);
                    throw new InvalidOperationException($"Could not retrieve or create OrganizationSK for Organization Ref {data.Ref}.");
                }
                return Convert.ToInt32(res);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Ошибка при получении/создании OrganizationSK для организации {OrganizationName} (Ref: {OrganizationRef})",
                    data.Name, data.Ref);
                throw;
            }
        }

        public async Task<int> GetOrCreateContractSKAsync(Contract1C data)
        {
            if (data == null)
            {
                throw new ArgumentNullException(nameof(data), "Данные договора не могут быть null");
            }

            if (data.CustomerSK == 0)
            {
                _log.LogError("CustomerSK is 0 (uninitialized) for Contract Ref1C {ContractRef} before calling GetOrCreateContractSKAsync.", data.Ref);
                throw new ArgumentException("CustomerSK must be set to a valid SK in Contract1C data before calling GetOrCreateContractSKAsync.", nameof(data));
            }

            await using var conn = new SqlConnection(_connString);
            try
            {
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandTimeout = _commandTimeout;
                cmd.CommandText = @"
SET NOCOUNT ON;
DECLARE @sk INT;
SELECT @sk = ContractSK
  FROM Analytics.DimContracts
 WHERE ContractID_1C = @Ref1C;
IF @sk IS NULL
BEGIN
  INSERT INTO Analytics.DimContracts
    (ContractID_1C, ContractCode_1C, ContractName, CustomerSK, StartDate, EndDate)
  VALUES
    (@Ref1C, @Code, @Name, @CustomerSK, @StartDate, @EndDate);
  SELECT @sk = SCOPE_IDENTITY();
END;
SELECT @sk;";
                cmd.Parameters.AddWithValue("@Ref1C", data.Ref);
                cmd.Parameters.AddWithValue("@Code", (object?)data.Code ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@Name", data.Name);
                cmd.Parameters.AddWithValue("@CustomerSK", data.CustomerSK);
                cmd.Parameters.AddWithValue("@StartDate", data.StartDate.HasValue ? (object)data.StartDate.Value : DBNull.Value);
                cmd.Parameters.AddWithValue("@EndDate", data.EndDate.HasValue ? (object)data.EndDate.Value : DBNull.Value);

                var res = await cmd.ExecuteScalarAsync();
                if (res == null || res == DBNull.Value)
                {
                    _log.LogError("Failed to get or create ContractSK for {ContractName} (Ref: {ContractRef}). ExecuteScalarAsync returned null/DBNull.", data.Name, data.Ref);
                    throw new InvalidOperationException($"Could not retrieve or create ContractSK for Contract Ref {data.Ref}.");
                }
                return Convert.ToInt32(res);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Ошибка при получении/создании ContractSK для договора {ContractName} (Ref: {ContractRef})",
                    data.Name, data.Ref);
                throw;
            }
        }

        public async Task<int> GetOrCreateDateKeyAsync(DateTime date)
        {
            int dateKey = int.Parse(date.ToString("yyyyMMdd", CultureInfo.InvariantCulture));

            await using var conn = new SqlConnection(_connString);
            try
            {
                await conn.OpenAsync();
                int dayOfWeekNumber = ((int)date.DayOfWeek == 0) ? 7 : (int)date.DayOfWeek;
                CultureInfo ci = new CultureInfo("ru-RU");
                string dayName = ci.DateTimeFormat.GetDayName(date.DayOfWeek);
                int dayOfMonth = date.Day;
                int dayOfYear = date.DayOfYear;
                int weekOfYearISO = ISOWeek.GetWeekOfYear(date);
                int monthNumber = date.Month;
                string monthName = ci.DateTimeFormat.GetMonthName(date.Month);
                int quarterNumber = (date.Month - 1) / 3 + 1;
                int yearNumber = date.Year;
                bool isWeekend = date.DayOfWeek == DayOfWeek.Saturday || date.DayOfWeek == DayOfWeek.Sunday;

                await using var cmd = conn.CreateCommand();
                cmd.CommandTimeout = _commandTimeout;
                cmd.CommandText = @"
SET NOCOUNT ON;
IF NOT EXISTS (SELECT 1 FROM Analytics.DimDates WHERE DateKey = @DateKey)
BEGIN
  INSERT INTO Analytics.DimDates
    (DateKey, FullDate, DayOfWeekNumber, DayName, DayOfMonth, DayOfYear, WeekOfYearISO, MonthNumber, MonthName, QuarterNumber, YearNumber, IsWeekend)
  VALUES
    (@DateKey, @FullDate, @DayOfWeekNumber, @DayName, @DayOfMonth, @DayOfYear, @WeekOfYearISO, @MonthNumber, @MonthName, @QuarterNumber, @YearNumber, @IsWeekend);
END;"; // No SELECT @DateKey needed here
                cmd.Parameters.AddWithValue("@DateKey", dateKey);
                cmd.Parameters.AddWithValue("@FullDate", date.Date);
                cmd.Parameters.AddWithValue("@DayOfWeekNumber", dayOfWeekNumber);
                cmd.Parameters.AddWithValue("@DayName", dayName);
                cmd.Parameters.AddWithValue("@DayOfMonth", dayOfMonth);
                cmd.Parameters.AddWithValue("@DayOfYear", dayOfYear);
                cmd.Parameters.AddWithValue("@WeekOfYearISO", weekOfYearISO);
                cmd.Parameters.AddWithValue("@MonthNumber", monthNumber);
                cmd.Parameters.AddWithValue("@MonthName", monthName);
                cmd.Parameters.AddWithValue("@QuarterNumber", quarterNumber);
                cmd.Parameters.AddWithValue("@YearNumber", yearNumber);
                cmd.Parameters.AddWithValue("@IsWeekend", isWeekend);

                await cmd.ExecuteNonQueryAsync();
                return dateKey;
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Ошибка при получении/создании DateKey для даты {Date}", date);
                throw;
            }
        }

        public async Task InsertFactSalesAsync(IEnumerable<SaleFactData> rows)
        {
            if (rows == null)
            {
                throw new ArgumentNullException(nameof(rows), "Коллекция строк продаж не может быть null");
            }

            if (!rows.Any())
            {
                _log.LogInformation("InsertFactSalesAsync: No rows to insert.");
                return;
            }

            await using var conn = new SqlConnection(_connString);
            try
            {
                await conn.OpenAsync();

                var table = new DataTable();
                table.Columns.Add("SalesDocumentID_1C", typeof(Guid));
                table.Columns.Add("SalesDocumentNumber_1C", typeof(string));
                table.Columns.Add("SalesDocumentLineNo_1C", typeof(int));
                table.Columns.Add("SaleDateKey", typeof(int));
                table.Columns.Add("CustomerSK", typeof(int));
                table.Columns.Add("ProductSK", typeof(int));
                table.Columns.Add("ContractSK", typeof(int));
                table.Columns.Add("OrganizationSK", typeof(int));
                table.Columns.Add("Quantity", typeof(decimal));
                table.Columns.Add("Price", typeof(decimal));
                table.Columns.Add("Amount", typeof(decimal));
                table.Columns.Add("VATRateName", typeof(string));
                table.Columns.Add("VATAmount", typeof(decimal));
                table.Columns.Add("TotalAmount", typeof(decimal));
                table.Columns.Add("CurrencyCode", typeof(string));

                foreach (var row in rows)
                {
                    var dataRow = table.NewRow();
                    dataRow["SalesDocumentID_1C"] = row.SalesDocumentID_1C;
                    dataRow["SalesDocumentNumber_1C"] = row.SalesDocumentNumber_1C;
                    dataRow["SalesDocumentLineNo_1C"] = row.SalesDocumentLineNo_1C;
                    dataRow["SaleDateKey"] = row.SaleDateKey;
                    dataRow["CustomerSK"] = row.CustomerSK;
                    dataRow["ProductSK"] = row.ProductSK;
                    dataRow["ContractSK"] = row.ContractSK.HasValue ? (object)row.ContractSK.Value : DBNull.Value;
                    dataRow["OrganizationSK"] = row.OrganizationSK;
                    dataRow["Quantity"] = row.Quantity;
                    dataRow["Price"] = row.Price;
                    dataRow["Amount"] = row.Amount;
                    dataRow["VATRateName"] = (object?)row.VATRateName ?? DBNull.Value;
                    dataRow["VATAmount"] = row.VATAmount;
                    dataRow["TotalAmount"] = row.TotalAmount;
                    dataRow["CurrencyCode"] = (object?)row.CurrencyCode ?? DBNull.Value;
                    table.Rows.Add(dataRow);
                }

                using (var bulkCopy = new SqlBulkCopy(conn))
                {
                    bulkCopy.DestinationTableName = "Analytics.FactSales";
                    bulkCopy.BulkCopyTimeout = _commandTimeout; // Use the configured timeout

                    bulkCopy.ColumnMappings.Add("SalesDocumentID_1C", "SalesDocumentID_1C");
                    bulkCopy.ColumnMappings.Add("SalesDocumentNumber_1C", "SalesDocumentNumber_1C");
                    bulkCopy.ColumnMappings.Add("SalesDocumentLineNo_1C", "SalesDocumentLineNo_1C");
                    bulkCopy.ColumnMappings.Add("SaleDateKey", "SaleDateKey");
                    bulkCopy.ColumnMappings.Add("CustomerSK", "CustomerSK");
                    bulkCopy.ColumnMappings.Add("ProductSK", "ProductSK");
                    bulkCopy.ColumnMappings.Add("ContractSK", "ContractSK");
                    bulkCopy.ColumnMappings.Add("OrganizationSK", "OrganizationSK");
                    bulkCopy.ColumnMappings.Add("Quantity", "Quantity");
                    bulkCopy.ColumnMappings.Add("Price", "Price");
                    bulkCopy.ColumnMappings.Add("Amount", "Amount");
                    bulkCopy.ColumnMappings.Add("VATRateName", "VATRateName");
                    bulkCopy.ColumnMappings.Add("VATAmount", "VATAmount");
                    bulkCopy.ColumnMappings.Add("TotalAmount", "TotalAmount");
                    bulkCopy.ColumnMappings.Add("CurrencyCode", "CurrencyCode");

                    await bulkCopy.WriteToServerAsync(table);
                    _log.LogInformation("Successfully inserted {RowCount} rows into Analytics.FactSales.", table.Rows.Count);
                }
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Ошибка при пакетной вставке фактов продаж");
                throw;
            }
        }

        public async Task InsertFactPaymentsAsync(IEnumerable<PaymentRowData> rows)
        {
            if (rows == null)
            {
                throw new ArgumentNullException(nameof(rows), "Коллекция строк платежей не может быть null");
            }

            if (!rows.Any())
            {
                _log.LogInformation("InsertFactPaymentsAsync: No rows to insert.");
                return;
            }

            await using var conn = new SqlConnection(_connString);
            try
            {
                await conn.OpenAsync();

                var table = new DataTable();
                table.Columns.Add("PaymentDocID_1C", typeof(Guid));
                table.Columns.Add("PaymentNumber_1C", typeof(string));
                table.Columns.Add("PaymentDateKey", typeof(int));
                table.Columns.Add("Amount", typeof(decimal));
                table.Columns.Add("CurrencyCode", typeof(string));
                table.Columns.Add("CustomerSK", typeof(int));
                table.Columns.Add("ContractSK", typeof(int));
                table.Columns.Add("OrganizationSK", typeof(int));

                foreach (var row in rows)
                {
                    var dataRow = table.NewRow();
                    dataRow["PaymentDocID_1C"] = row.PaymentDocID_1C;
                    dataRow["PaymentNumber_1C"] = row.PaymentNumber_1C;
                    dataRow["PaymentDateKey"] = row.PaymentDateKey;
                    dataRow["Amount"] = row.Amount;
                    dataRow["CurrencyCode"] = (object?)row.CurrencyCode ?? DBNull.Value;
                    dataRow["CustomerSK"] = row.CustomerSK;
                    dataRow["ContractSK"] = row.ContractSK.HasValue ? (object)row.ContractSK.Value : DBNull.Value;
                    dataRow["OrganizationSK"] = row.OrganizationSK;
                    table.Rows.Add(dataRow);
                }

                using (var bulkCopy = new SqlBulkCopy(conn))
                {
                    bulkCopy.DestinationTableName = "Analytics.FactPayments";
                    bulkCopy.BulkCopyTimeout = _commandTimeout; // Use the configured timeout

                    bulkCopy.ColumnMappings.Add("PaymentDocID_1C", "PaymentDocID_1C");
                    bulkCopy.ColumnMappings.Add("PaymentNumber_1C", "PaymentNumber_1C");
                    bulkCopy.ColumnMappings.Add("PaymentDateKey", "PaymentDateKey");
                    bulkCopy.ColumnMappings.Add("Amount", "Amount");
                    bulkCopy.ColumnMappings.Add("CurrencyCode", "CurrencyCode");
                    bulkCopy.ColumnMappings.Add("CustomerSK", "CustomerSK");
                    bulkCopy.ColumnMappings.Add("ContractSK", "ContractSK");
                    bulkCopy.ColumnMappings.Add("OrganizationSK", "OrganizationSK");

                    await bulkCopy.WriteToServerAsync(table);
                    _log.LogInformation("Successfully inserted {RowCount} rows into Analytics.FactPayments.", table.Rows.Count);
                }
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Ошибка при пакетной вставке фактов платежей");
                throw;
            }
        }
    }
}