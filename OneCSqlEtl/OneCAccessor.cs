#nullable enable
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace OneCSqlEtl
{
    [SupportedOSPlatform("windows")]
    public class OneCAccessor : IDisposable
    {
        private readonly ILogger<OneCAccessor> _log;
        private readonly string _connString;
        private readonly string _progId;
        private dynamic? _v8Application = null;

        public OneCAccessor(IOptions<Settings> opts, ILogger<OneCAccessor> log)
        {
            _log = log ?? throw new ArgumentNullException(nameof(log));

            if (opts?.Value?.ConnectionStrings == null || opts.Value.EtlSettings == null)
                throw new ArgumentNullException(nameof(opts), "Settings, its Value, ConnectionStrings property, or EtlSettings property is null.");

            _connString = opts.Value.ConnectionStrings.OneCConnectionString ?? throw new ArgumentNullException(nameof(opts.Value.ConnectionStrings.OneCConnectionString), "OneCConnectionString cannot be null.");
            _progId = opts.Value.EtlSettings.OneCComVersion ?? throw new ArgumentNullException(nameof(opts.Value.EtlSettings.OneCComVersion), "OneCComVersion cannot be null.");
        }

        private string GetDynamicTypeName(dynamic? obj)
        {
            if (obj == null) return "null";
            return ((object)obj).GetType().FullName ?? ((object)obj).GetType().Name;
        }

        // Inside OneCAccessor.cs

        public bool Connect()
        {
            _log.LogInformation("Attempting OneCAccessor.Connect() with ProgID: {ProgId} and ConnString: {ConnString}", _progId, _connString);
            dynamic? initialComObject = null; // Ensure it's dynamic?
            try
            {
                Type? comType = Type.GetTypeFromProgID(_progId, throwOnError: false);
                if (comType == null)
                {
                    _log.LogError("COM type for ProgID '{ProgId}' not found.", _progId);
                    return false;
                }
                _log.LogInformation("COM type for ProgID '{ProgId}' found: {ComTypeName}", _progId, comType.FullName!);

#pragma warning disable CA1416
                initialComObject = Activator.CreateInstance(comType);
#pragma warning restore CA1416

                if (initialComObject == null)
                {
                    _log.LogError("Activator.CreateInstance for ProgID '{ProgId}' returned null.", _progId);
                    return false;
                }
                _log.LogInformation("Instance of COM object '{ProgId}' created: {InitialComObjectType}", _progId, (object)GetDynamicTypeName(initialComObject));

                if (!Marshal.IsComObject(initialComObject))
                {
                    _log.LogError("Initial object is NOT a COM object. ProgID: {ProgId}. Type: {Type}", _progId, (object)GetDynamicTypeName(initialComObject));
                    if (initialComObject is IDisposable disp) disp.Dispose();
                    return false;
                }
                _log.LogInformation("Initial COM object IS a COM object.");

                _log.LogInformation("Attempting to connect to 1C using initialComObject.Connect()...");
                object? connectResult = null;
                try
                {
                    connectResult = initialComObject.Connect(_connString);
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Exception during initialComObject.Connect(). Cleaning up.");
                    if (initialComObject != null && Marshal.IsComObject(initialComObject)) { Marshal.ReleaseComObject(initialComObject); }
                    return false;
                }

                bool isConnectionSuccessful = false;
                if (connectResult is bool boolSuccess)
                {
                    if (boolSuccess)
                    {
                        _log.LogInformation("initialComObject.Connect() returned boolean true. Connection successful.");
                        _v8Application = initialComObject;
                        isConnectionSuccessful = true;
                    }
                    else
                    {
                        _log.LogError("initialComObject.Connect() returned boolean false. Connection failed. ConnString: {ConnStr}", _connString);
                    }
                }
                else if (connectResult != null && Marshal.IsComObject(connectResult))
                {
                    _log.LogInformation("initialComObject.Connect() returned a COM object (this is now the active context). Type: {ContextType}", (object)GetDynamicTypeName(connectResult));
                    _v8Application = connectResult;

                    if (initialComObject != null && !ReferenceEquals(initialComObject, _v8Application) && Marshal.IsComObject(initialComObject))
                    {
                        _log.LogDebug("Releasing the initial connector factory object as Connect() returned a new context.");
                        Marshal.ReleaseComObject(initialComObject);
                    }
                    isConnectionSuccessful = true;
                }
                else if (connectResult == null)
                {
                    _log.LogError("initialComObject.Connect() returned null. Connection failed. ConnString: {ConnStr}", _connString);
                }
                else
                {
                    _log.LogError("initialComObject.Connect() returned an unexpected result. Result Type: {ResultType}", (object)GetDynamicTypeName(connectResult));
                }

                if (!isConnectionSuccessful)
                {
                    // FIX FOR CS8600: Declare errorSource as dynamic?
                    dynamic? errorSource = _v8Application ?? initialComObject;
                    if (errorSource != null && Marshal.IsComObject(errorSource))
                    {
                        _log.LogInformation("Attempting to get 1C error description from error source (specific method depends on 1C version)...");
                        try
                        {
                            // Example: string errorDesc = errorSource.DescriptionOfError(); _log.LogError("1C Error: {ErrorDescription}", errorDesc);
                        }
                        catch (Exception descEx) { _log.LogWarning(descEx, "Could not retrieve/log 1C error description."); }
                    }

                    if (_v8Application != null && Marshal.IsComObject(_v8Application)) { Marshal.ReleaseComObject(_v8Application); _v8Application = null; }
                    if (initialComObject != null && Marshal.IsComObject(initialComObject) && (_v8Application == null || !ReferenceEquals(initialComObject, _v8Application))) { Marshal.ReleaseComObject(initialComObject); }
                    return false;
                }

                _log.LogInformation("_v8Application (Type: {AppType}) is now set as the active context for queries.", (object)GetDynamicTypeName(_v8Application));
                return true;
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Critical error during 1C COM initialization or connection. Ensure 1C Client is installed and COM component ({ProgId}) is registered correctly.", _progId);
                if (_v8Application != null && Marshal.IsComObject(_v8Application)) { Marshal.ReleaseComObject(_v8Application); _v8Application = null; }
                // Ensure initialComObject is also released if it was created and distinct from a failed _v8Application
                if (initialComObject != null && Marshal.IsComObject(initialComObject) && (_v8Application == null || !ReferenceEquals(initialComObject, _v8Application)))
                {
                    Marshal.ReleaseComObject(initialComObject);
                }
                return false;
            }
        }

        private void TryLog1CErrorDescription()
        {
            if (_v8Application != null && Marshal.IsComObject(_v8Application))
            {
                try { _log.LogInformation("Attempting to get 1C error description (specific method depends on 1C version)..."); }
                catch (Exception descEx) { _log.LogWarning(descEx, "Could not retrieve/log 1C error description from _v8Application."); }
            }
        }

        private void ReleaseComObjects()
        {
            _log.LogDebug("Attempting to release COM objects...");
            if (_v8Application != null)
            {
                if (Marshal.IsComObject(_v8Application))
                {
                    _log.LogDebug("Releasing _v8Application COM object (Type: {V8ApplicationType})...", (object)GetDynamicTypeName(_v8Application));
                    try { Marshal.ReleaseComObject(_v8Application); } catch (Exception ex) { _log.LogWarning(ex, "Exception during _v8Application release."); }
                }
                _v8Application = null;
            }
            _log.LogDebug("Finished releasing COM objects.");
        }
        public void Dispose() { _log.LogInformation("Disposing OneCAccessor..."); ReleaseComObjects(); GC.SuppressFinalize(this); }


        private IEnumerable<T> ExecuteQuery<T>(string query, Func<dynamic, T?> rowMapper) where T : class
        {
            if (_v8Application == null) { _log.LogError("1C Application object (_v8Application) is not initialized"); yield break; }

            dynamic? q = null; dynamic? executionResult = null; dynamic? table = null; int rowCounter = 0;
            string queryShort = query.Substring(0, Math.Min(query.Length, 100));

            try
            {
                _log.LogDebug("[ExecuteQuery] Creating Query object using _v8Application (Type: {V8AppTypeName}) for query: {QueryTextShort}...", (object)GetDynamicTypeName(_v8Application), queryShort);
                q = _v8Application.NewObject("Query");
                if (q == null) { _log.LogError("[ExecuteQuery] Failed to create Query object."); yield break; }
                _log.LogDebug("[ExecuteQuery] Query object created.");

                try { _log.LogDebug("[ExecuteQuery] Setting query text..."); q.Text = query; _log.LogDebug("[ExecuteQuery] Query text set successfully."); }
                catch (Exception ex) { _log.LogError(ex, "[ExecuteQuery] Exception while setting query text."); yield break; }

                try
                {
                    _log.LogDebug("[ExecuteQuery] Executing query..."); executionResult = q.Execute();
                    if (executionResult == null) { _log.LogError("[ExecuteQuery] Query execution returned null."); yield break; }
                    _log.LogDebug("[ExecuteQuery] Query executed. Result type: {Type}", (object)GetDynamicTypeName(executionResult));
                }
                catch (Exception ex) { _log.LogError(ex, "[ExecuteQuery] Exception during query execution."); yield break; }

                try
                {
                    _log.LogDebug("[ExecuteQuery] Unloading query results..."); table = executionResult.Unload();
                    if (table == null) { _log.LogError("[ExecuteQuery] Result unload returned null."); yield break; }
                    _log.LogDebug("[ExecuteQuery] Results unloaded. Table object type: {Type}", (object)GetDynamicTypeName(table));
                }
                catch (Exception ex) { _log.LogError(ex, "[ExecuteQuery] Exception during result unload."); yield break; }

                _log.LogDebug("[ExecuteQuery] Starting to iterate through the table rows...");
                foreach (dynamic row in table)
                {
                    rowCounter++;
                    _log.LogDebug("[ExecuteQuery] Processing row number {RowNumber}", rowCounter);
                    if (row == null) { _log.LogWarning("[ExecuteQuery] Row number {RowNumber} is null. Skipping.", rowCounter); continue; }
                    _log.LogDebug("[ExecuteQuery] Row number {RowNumber} is not null. Row object type: {Type}", rowCounter, (object)GetDynamicTypeName(row));

                    T? item = null;
                    try
                    {
                        _log.LogDebug("[ExecuteQuery] Calling rowMapper for row {RowNumber}", rowCounter);
                        item = rowMapper(row);
                        _log.LogDebug("[ExecuteQuery] rowMapper returned for row {RowNumber}. Item is null: {IsItemNull}", rowCounter, item == null);
                    }
                    catch (Exception ex) { _log.LogError(ex, "[ExecuteQuery] Exception from rowMapper delegate invocation for row {RowNumber}. Query: {QueryTextShort}", rowCounter, queryShort); }

                    if (item != null) { _log.LogDebug("[ExecuteQuery] Yielding item for row {RowNumber}", rowCounter); yield return item; }
                    else { _log.LogDebug("[ExecuteQuery] Item was null after rowMapper for row {RowNumber}, not yielding.", rowCounter); }
                }
                _log.LogDebug("[ExecuteQuery] Finished iterating {ActualRowCount} rows for query: {QueryTextShort}", rowCounter, queryShort);
            }
            finally
            {
                _log.LogDebug("[ExecuteQuery] Entering finally block for query: {QueryTextShort}", queryShort);
                if (table != null && Marshal.IsComObject(table)) { try { _log.LogDebug("[ExecuteQuery] Releasing table COM object..."); Marshal.ReleaseComObject(table); } catch (Exception ex) { _log.LogWarning(ex, "[ExecuteQuery] Error releasing table COM object."); } }
                if (executionResult != null && Marshal.IsComObject(executionResult)) { try { _log.LogDebug("[ExecuteQuery] Releasing executionResult COM object..."); Marshal.ReleaseComObject(executionResult); } catch (Exception ex) { _log.LogWarning(ex, "[ExecuteQuery] Error releasing executionResult COM object."); } }
                if (q != null && Marshal.IsComObject(q)) { try { _log.LogDebug("[ExecuteQuery] Releasing query (q) COM object..."); Marshal.ReleaseComObject(q); } catch (Exception ex) { _log.LogWarning(ex, "[ExecuteQuery] Error releasing query (q) COM object."); } }
                _log.LogDebug("[ExecuteQuery] Exited finally block for query: {QueryTextShort}", queryShort);
            }
        }

        private TProp? GetSafeDynamicProp<TProp>(dynamic comObject, string propertyName, ILogger logger, string contextMessage) where TProp : class
        {
            try
            {
                if (comObject == null) { logger.LogDebug("[GetSafeDynamicProp] comObject is null for {Property} in {Context}", propertyName, contextMessage); return null; }
                logger.LogDebug("[GetSafeDynamicProp] Attempting to access '{Property}' from {Context}", propertyName, contextMessage);
                var propValue = comObject[propertyName];
                logger.LogDebug("[GetSafeDynamicProp] Accessed '{Property}'. Value type: {PropType}", propertyName, (object)GetDynamicTypeName(propValue)); // Cast for logger
                if (propValue == null || propValue is System.DBNull) return null;
                return propValue as TProp;
            }
            catch (Microsoft.CSharp.RuntimeBinder.RuntimeBinderException rbEx)
            { logger.LogWarning(rbEx, "[GetSafeDynamicProp] RuntimeBinderException: Property '{Property}' not found or type mismatch for {Context}.", propertyName, contextMessage); return null; }
            catch (Exception ex)
            { logger.LogWarning(ex, "[GetSafeDynamicProp] Exception accessing property '{Property}' for {Context}.", propertyName, contextMessage); return null; }
        }

        private TValue? GetSafeDynamicValue<TValue>(dynamic comObject, string propertyName, ILogger logger, string contextMessage) where TValue : struct
        {
            try
            {
                if (comObject == null) { logger.LogDebug("[GetSafeDynamicValue] comObject is null for {Property} in {Context}", propertyName, contextMessage); return null; }
                logger.LogDebug("[GetSafeDynamicValue] Attempting to access '{Property}' from {Context}", propertyName, contextMessage);
                var propValue = comObject[propertyName];
                logger.LogDebug("[GetSafeDynamicValue] Accessed '{Property}'. Value type: {PropType}", propertyName, (object)GetDynamicTypeName(propValue)); // Cast for logger
                if (propValue == null || propValue is System.DBNull) return null;

                if (typeof(TValue) == typeof(bool) && propValue is short s) return (TValue)(object)(s != 0);
                if (typeof(TValue) == typeof(bool) && propValue is int i) return (TValue)(object)(i != 0);

                return (TValue)Convert.ChangeType(propValue, typeof(TValue));
            }
            catch (Microsoft.CSharp.RuntimeBinder.RuntimeBinderException rbEx)
            { logger.LogWarning(rbEx, "[GetSafeDynamicValue] RuntimeBinderException: Property '{Property}' not found or type mismatch for {Context}.", propertyName, contextMessage); return null; }
            catch (Exception ex)
            {
                // Avoid accessing the dynamic property again here as it may
                // throw the same exception that triggered this catch block.
                logger.LogWarning(ex,
                    "[GetSafeDynamicValue] Exception converting/accessing property '{Property}' for {Context}.",
                    propertyName,
                    contextMessage);
                return null;
            }
        }

        public IEnumerable<Customer1C> GetCustomers()
        {
            _log.LogInformation("Reading customers...");
            const string query = "ВЫБРАТЬ Контрагенты.Ссылка.УникальныйИдентификатор() КАК Ref1C, Контрагенты.Код КАК Code, Контрагенты.Наименование КАК Name, Контрагенты.ПолноеНаименование КАК FullName, Контрагенты.ИНН КАК TIN, Контрагенты.КПП КАК KPP, Представление(Контрагенты.ЮрФизЛицо) КАК EntityType, Контрагенты.ПометкаУдаления КАК IsDeleted ИЗ Справочник.Контрагенты КАК Контрагенты";
            return ExecuteQuery(query, row => {
                if (row == null) { _log.LogWarning("[CustomerMapper] Received null row."); return null; }
                _log.LogDebug("[CustomerMapper] Processing row. Type: {RowType}", (object)GetDynamicTypeName(row));
                try
                {
                    string? refStr = GetSafeDynamicProp<object>(row, "Ref1C", _log, "Customer.Ref1C")?.ToString();
                    if (!Guid.TryParse(refStr, out Guid id) || id == Guid.Empty) { _log.LogWarning("[CustomerMapper] Invalid Ref1C: {Ref1C}", (object?)refStr ?? "null"); return null; }
                    _log.LogDebug("[CustomerMapper] Parsed Ref1C: {Id}", id);
                    return new Customer1C
                    {
                        Ref = id,
                        Code = GetSafeDynamicProp<object>(row, "Code", _log, "Customer.Code")?.ToString(),
                        Name = GetSafeDynamicProp<object>(row, "Name", _log, "Customer.Name")?.ToString() ?? "",
                        FullName = GetSafeDynamicProp<object>(row, "FullName", _log, "Customer.FullName")?.ToString(),
                        TIN = GetSafeDynamicProp<object>(row, "TIN", _log, "Customer.TIN")?.ToString(),
                        KPP = GetSafeDynamicProp<object>(row, "KPP", _log, "Customer.KPP")?.ToString(),
                        EntityType = GetSafeDynamicProp<object>(row, "EntityType", _log, "Customer.EntityType")?.ToString(),
                        IsDeleted = GetSafeDynamicValue<bool>(row, "IsDeleted", _log, "Customer.IsDeleted") ?? false
                    };
                }
                catch (Microsoft.CSharp.RuntimeBinder.RuntimeBinderException rbEx)
                {
                    _log.LogError(rbEx, "[CustomerMapper] BinderEx. RowType: {RowType}", (object)GetDynamicTypeName(row)); return null;
                }
                catch (Exception ex) { _log.LogError(ex, "[CustomerMapper] GeneralEx. Ref1C: {Ref1C}. RowType: {RowType}", (object?)GetSafeDynamicProp<object>(row, "Ref1C", _log, "Customer.Ref1C.Ex")?.ToString() ?? "N/A", (object)GetDynamicTypeName(row)); return null; }
            });
        }

        public IEnumerable<Product1C> GetProducts() // This method was missing the robust implementation
        {
            _log.LogInformation("Reading products...");
            const string query = "ВЫБРАТЬ Номенклатура.Ссылка.УникальныйИдентификатор() КАК Ref1C, Номенклатура.Код КАК Code, Номенклатура.Наименование КАК Name, Номенклатура.ПолноеНаименование КАК FullName, Номенклатура.Артикул КАК SKU, Номенклатура.ЕдиницаИзмерения.Наименование КАК UnitOfMeasure, Представление(Номенклатура.ВидНоменклатуры) КАК ProductType, Номенклатура.Группа.Наименование КАК ProductGroup, Номенклатура.СтавкаНДС.Наименование КАК DefaultVATRateName ИЗ Справочник.Номенклатура КАК Номенклатура";
            return ExecuteQuery(query, row => {
                if (row == null) { _log.LogWarning("[ProductMapper] Received null row."); return null; }
                _log.LogDebug("[ProductMapper] Processing row. Type: {RowType}", (object)GetDynamicTypeName(row));
                try
                {
                    string? refStr = GetSafeDynamicProp<object>(row, "Ref1C", _log, "Product.Ref1C")?.ToString();
                    if (!Guid.TryParse(refStr, out Guid id) || id == Guid.Empty) { _log.LogWarning("[ProductMapper] Invalid Ref1C: {Ref1C}", (object?)refStr ?? "null"); return null; }
                    _log.LogDebug("[ProductMapper] Parsed Ref1C: {Id}", id);
                    return new Product1C
                    {
                        Ref = id,
                        Code = GetSafeDynamicProp<object>(row, "Code", _log, "Product.Code")?.ToString(),
                        Name = GetSafeDynamicProp<object>(row, "Name", _log, "Product.Name")?.ToString() ?? "",
                        FullName = GetSafeDynamicProp<object>(row, "FullName", _log, "Product.FullName")?.ToString(),
                        SKU = GetSafeDynamicProp<object>(row, "SKU", _log, "Product.SKU")?.ToString(),
                        UnitOfMeasure = GetSafeDynamicProp<object>(row, "UnitOfMeasure", _log, "Product.UnitOfMeasure")?.ToString(),
                        ProductType = GetSafeDynamicProp<object>(row, "ProductType", _log, "Product.ProductType")?.ToString(),
                        ProductGroup = GetSafeDynamicProp<object>(row, "ProductGroup", _log, "Product.ProductGroup")?.ToString(),
                        DefaultVATRateName = GetSafeDynamicProp<object>(row, "DefaultVATRateName", _log, "Product.DefaultVATRateName")?.ToString()
                    };
                }
                catch (Microsoft.CSharp.RuntimeBinder.RuntimeBinderException rbEx)
                {
                    _log.LogError(rbEx, "[ProductMapper] BinderEx. RowType: {RowType}", (object)GetDynamicTypeName(row)); return null;
                }
                catch (Exception ex) { _log.LogError(ex, "[ProductMapper] GeneralEx. Ref1C: {Ref1C}. RowType: {RowType}", (object?)GetSafeDynamicProp<object>(row, "Ref1C", _log, "Product.Ref1C.Ex")?.ToString() ?? "N/A", (object)GetDynamicTypeName(row)); return null; }
            });
        }

        public IEnumerable<Contract1C> GetContracts()
        {
            _log.LogInformation("Reading contracts...");
            const string query = "ВЫБРАТЬ ДоговорыКонтрагентов.Ссылка.УникальныйИдентификатор() КАК Ref1C, ДоговорыКонтрагентов.Код КАК Code, ДоговорыКонтрагентов.Наименование КАК Name, ДоговорыКонтрагентов.Контрагент.УникальныйИдентификатор() КАК CustomerRef1C, ДоговорыКонтрагентов.ДатаНачала КАК StartDate, ДоговорыКонтрагентов.ДатаОкончания КАК EndDate ИЗ Справочник.ДоговорыКонтрагентов КАК ДоговорыКонтрагентов";
            return ExecuteQuery(query, row => {
                if (row == null) { _log.LogWarning("[ContractMapper] Received null row."); return null; }
                _log.LogDebug("[ContractMapper] Processing row. Type: {RowType}", (object)GetDynamicTypeName(row));
                try
                {
                    string? refStr = GetSafeDynamicProp<object>(row, "Ref1C", _log, "Contract.Ref1C")?.ToString();
                    if (!Guid.TryParse(refStr, out Guid id) || id == Guid.Empty) { _log.LogWarning("[ContractMapper] Invalid Ref1C: {Ref1C}", (object?)refStr ?? "null"); return null; }
                    _log.LogDebug("[ContractMapper] Parsed Ref1C: {Id}", id);

                    string? custRefStr = GetSafeDynamicProp<object>(row, "CustomerRef1C", _log, "Contract.CustomerRef1C")?.ToString();
                    if (!Guid.TryParse(custRefStr, out Guid custId) || custId == Guid.Empty) { _log.LogWarning("[ContractMapper] Invalid CustomerRef1C: {CustRefStr} for Contract {Ref1C}", (object?)custRefStr ?? "null", id); return null; }
                    _log.LogDebug("[ContractMapper] Parsed CustomerRef1C: {Id}", custId);

                    DateTime? startDate = null;
                    object? startDateObj = GetSafeDynamicProp<object>(row, "StartDate", _log, "Contract.StartDate");
                    if (startDateObj is DateTime dtS) { startDate = dtS; }
                    else if (startDateObj != null && !(startDateObj is DBNull))
                    { _log.LogWarning("[ContractMapper] StartDate was not DateTime: {Type}", (object)GetDynamicTypeName(startDateObj)); }

                    DateTime? endDate = null;
                    object? endDateObj = GetSafeDynamicProp<object>(row, "EndDate", _log, "Contract.EndDate");
                    if (endDateObj is DateTime dtE) { endDate = dtE; }
                    else if (endDateObj != null && !(endDateObj is DBNull))
                    { _log.LogWarning("[ContractMapper] EndDate was not DateTime: {Type}", (object)GetDynamicTypeName(endDateObj)); }

                    return new Contract1C
                    {
                        Ref = id,
                        CustomerRef_1C = custId,
                        Code = GetSafeDynamicProp<object>(row, "Code", _log, "Contract.Code")?.ToString(),
                        Name = GetSafeDynamicProp<object>(row, "Name", _log, "Contract.Name")?.ToString() ?? "",
                        StartDate = startDate,
                        EndDate = endDate
                    };
                }
                catch (Microsoft.CSharp.RuntimeBinder.RuntimeBinderException rbEx)
                {
                    _log.LogError(rbEx, "[ContractMapper] BinderEx. RowType: {RowType}", (object)GetDynamicTypeName(row)); return null;
                }
                catch (Exception ex) { _log.LogError(ex, "[ContractMapper] GeneralEx. Ref1C: {Ref1C}. RowType: {RowType}", (object?)GetSafeDynamicProp<object>(row, "Ref1C", _log, "Contract.Ref1C.Ex")?.ToString() ?? "N/A", (object)GetDynamicTypeName(row)); return null; }
            });
        }

        public IEnumerable<Organization1C> GetOrganizations()
        {
            _log.LogInformation("Reading organizations...");
            const string query = "ВЫБРАТЬ Организации.Ссылка.УникальныйИдентификатор() КАК Ref1C, Организации.Код КАК Code, Организации.Наименование КАК Name, Организации.ПолноеНаименование КАК OrganizationFullName ИЗ Справочник.Организации КАК Организации";
            return ExecuteQuery(query, row => {
                if (row == null) { _log.LogWarning("[OrganizationMapper] Received null row."); return null; }
                _log.LogDebug("[OrganizationMapper] Processing row. Type: {RowType}", (object)GetDynamicTypeName(row));
                try
                {
                    string? refStr = GetSafeDynamicProp<object>(row, "Ref1C", _log, "Organization.Ref1C")?.ToString();
                    if (!Guid.TryParse(refStr, out Guid id) || id == Guid.Empty) { _log.LogWarning("[OrganizationMapper] Invalid Ref1C: {Ref1C}", (object?)refStr ?? "null"); return null; }
                    _log.LogDebug("[OrganizationMapper] Parsed Ref1C: {Id}", id);
                    return new Organization1C
                    {
                        Ref = id,
                        Code = GetSafeDynamicProp<object>(row, "Code", _log, "Organization.Code")?.ToString(),
                        Name = GetSafeDynamicProp<object>(row, "Name", _log, "Organization.Name")?.ToString() ?? "",
                        OrganizationFullName = GetSafeDynamicProp<object>(row, "OrganizationFullName", _log, "Organization.FullName")?.ToString()
                    };
                }
                catch (Microsoft.CSharp.RuntimeBinder.RuntimeBinderException rbEx)
                {
                    _log.LogError(rbEx, "[OrganizationMapper] BinderEx. RowType: {RowType}", (object)GetDynamicTypeName(row)); return null;
                }
                catch (Exception ex) { _log.LogError(ex, "[OrganizationMapper] GeneralEx. Ref1C: {Ref1C}. RowType: {RowType}", (object?)GetSafeDynamicProp<object>(row, "Ref1C", _log, "Organization.Ref1C.Ex")?.ToString() ?? "N/A", (object)GetDynamicTypeName(row)); return null; }
            });
        }

        public IEnumerable<SaleFactData> GetSaleRows()
        {
            _log.LogInformation("Reading sale rows...");
            const string query = "ВЫБРАТЬ Реализация.Ссылка.УникальныйИдентификатор() КАК SalesDocumentID_1C, Реализация.Номер КАК SalesDocumentNumber_1C, Реализация.Дата КАК DocDate, Реализация.Контрагент.УникальныйИдентификатор() КАК CustomerRef1C, Реализация.Организация.УникальныйИдентификатор() КАК OrganizationRef1C, Реализация.ДоговорКонтрагента.УникальныйИдентификатор() КАК ContractRef1C, Реализация.Товары.(НомерСтроки) КАК SalesDocumentLineNo_1C, Реализация.Товары.Номенклатура.УникальныйИдентификатор() КАК ProductRef1C, Реализация.Товары.Количество КАК Quantity, Реализация.Товары.Цена КАК Price, Реализация.Товары.Сумма КАК Amount, Представление(Реализация.Товары.СтавкаНДС) КАК VATRateName, Реализация.Товары.СуммаНДС КАК VATAmount, Реализация.Товары.Всего КАК TotalAmount, Реализация.Валюта.Код КАК CurrencyCode ИЗ Документ.РеализацияТоваровУслуг КАК Реализация ГДЕ Реализация.Проведен";
            return ExecuteQuery(query, row => {
                if (row == null) { _log.LogWarning("[SaleFactDataMapper] Received null row."); return null; }
                _log.LogDebug("[SaleFactDataMapper] Processing row. Type: {RowType}", (object)GetDynamicTypeName(row));
                try
                {
                    string? docIdStr = GetSafeDynamicProp<object>(row, "SalesDocumentID_1C", _log, "Sale.SalesDocID")?.ToString();
                    if (!Guid.TryParse(docIdStr, out Guid docId) || docId == Guid.Empty) { _log.LogWarning("[SaleFactDataMapper] Invalid SalesDocID: {DocId}", (object?)docIdStr ?? "null"); return null; }
                    _log.LogDebug("[SaleFactDataMapper] Parsed SalesDocID: {Id}", docId);
                    string? custRefStr = GetSafeDynamicProp<object>(row, "CustomerRef1C", _log, "Sale.CustomerRef1C")?.ToString();
                    if (!Guid.TryParse(custRefStr, out Guid custId) || custId == Guid.Empty) { _log.LogWarning("[SaleFactDataMapper] Invalid CustomerRef1C for SalesDoc {DocId}", docId); return null; }
                    string? prodRefStr = GetSafeDynamicProp<object>(row, "ProductRef1C", _log, "Sale.ProductRef1C")?.ToString();
                    if (!Guid.TryParse(prodRefStr, out Guid prodId) || prodId == Guid.Empty) { _log.LogWarning("[SaleFactDataMapper] Invalid ProductRef1C for SalesDoc {DocId}", docId); return null; }
                    string? orgRefStr = GetSafeDynamicProp<object>(row, "OrganizationRef1C", _log, "Sale.OrganizationRef1C")?.ToString();
                    if (!Guid.TryParse(orgRefStr, out Guid orgId) || orgId == Guid.Empty) { _log.LogWarning("[SaleFactDataMapper] Invalid OrganizationRef1C for SalesDoc {DocId}", docId); return null; }
                    Guid? contractGuid = null; string? contractRefStrVal = GetSafeDynamicProp<object>(row, "ContractRef1C", _log, "Sale.ContractRef1C")?.ToString();
                    if (!string.IsNullOrEmpty(contractRefStrVal))
                    {
                        if (Guid.TryParse(contractRefStrVal, out Guid parsedGuid) && parsedGuid != Guid.Empty) { contractGuid = parsedGuid; }
                        else { _log.LogWarning("[SaleFactDataMapper] Invalid ContractRef1C string: {ContractStr} for SalesDoc {DocId}", (object?)contractRefStrVal ?? "null", docId); }
                    }
                    object? docDateObj = GetSafeDynamicProp<object>(row, "DocDate", _log, "Sale.DocDate");
                    DateTime docDate = DateTime.MinValue;
                    if (docDateObj is DateTime dtD) { docDate = dtD; }
                    else if (docDateObj != null && !(docDateObj is DBNull)) { _log.LogWarning("[SaleFactDataMapper] DocDate was not DateTime: {Type}, using MinValue.", (object)GetDynamicTypeName(docDateObj)); }

                    return new SaleFactData
                    {
                        SalesDocumentID_1C = docId,
                        CustomerRef_1C = custId,
                        ProductRef_1C = prodId,
                        OrganizationRef_1C = orgId,
                        ContractRef_1C = contractGuid,
                        DocDate = docDate,
                        SalesDocumentNumber_1C = GetSafeDynamicProp<object>(row, "SalesDocumentNumber_1C", _log, "Sale.SalesDocNum")?.ToString() ?? "",
                        SalesDocumentLineNo_1C = GetSafeDynamicValue<int>(row, "SalesDocumentLineNo_1C", _log, "Sale.LineNo") ?? 0,
                        Quantity = GetSafeDynamicValue<decimal>(row, "Quantity", _log, "Sale.Qty") ?? 0,
                        Price = GetSafeDynamicValue<decimal>(row, "Price", _log, "Sale.Price") ?? 0,
                        Amount = GetSafeDynamicValue<decimal>(row, "Amount", _log, "Sale.Amount") ?? 0,
                        VATRateName = GetSafeDynamicProp<object>(row, "VATRateName", _log, "Sale.VATRate")?.ToString(),
                        VATAmount = GetSafeDynamicValue<decimal>(row, "VATAmount", _log, "Sale.VATAmount") ?? 0,
                        TotalAmount = GetSafeDynamicValue<decimal>(row, "TotalAmount", _log, "Sale.Total") ?? 0,
                        CurrencyCode = GetSafeDynamicProp<object>(row, "CurrencyCode", _log, "Sale.Currency")?.ToString()
                    };
                }
                catch (Microsoft.CSharp.RuntimeBinder.RuntimeBinderException rbEx)
                {
                    _log.LogError(rbEx, "[SaleFactDataMapper] BinderEx. RowType: {RowType}", (object)GetDynamicTypeName(row)); return null;
                }
                catch (Exception ex) { _log.LogError(ex, "[SaleFactDataMapper] GeneralEx. SalesDocID: {SalesDocId}. RowType: {RowType}", (object?)GetSafeDynamicProp<object>(row, "SalesDocumentID_1C", _log, "Sale.SalesDocId.Ex")?.ToString() ?? "N/A", (object)GetDynamicTypeName(row)); return null; }
            });
        }

        public IEnumerable<PaymentRowData> GetPaymentRows()
        {
            _log.LogInformation("Reading payment rows...");
            const string query = "ВЫБРАТЬ Пл.Ссылка.УникальныйИдентификатор() КАК PaymentDocID_1C, Пл.Номер КАК PaymentNumber_1C, Пл.Дата КАК PaymentDate, Пл.Организация.УникальныйИдентификатор() КАК OrganizationRef1C, Пл.Сумма КАК Amount, Пл.Валюта.Код КАК CurrencyCode, Пл.Контрагент.УникальныйИдентификатор() КАК PayerRef1C, Пл.ДоговорКонтрагента.УникальныйИдентификатор() КАК ContractRef1C_Str ИЗ Документ.ПоступлениеДенежныхСредств КАК Пл ГДЕ Пл.ВидОперации В (ЗНАЧЕНИЕ(Перечисление.ВидыОперацийПоступлениеДенежныхСредств.ОплатаПокупателя), ЗНАЧЕНИЕ(Перечисление.ВидыОперацийПоступлениеДенежныхСредств.ПоступлениеОтПродажПоПлатежнымКартамИБанковскимКредитам))";
            return ExecuteQuery(query, row => {
                if (row == null) { _log.LogWarning("[PaymentRowDataMapper] Received null row."); return null; }
                _log.LogDebug("[PaymentRowDataMapper] Processing row. Type: {RowType}", (object)GetDynamicTypeName(row));
                try
                {
                    string? docIdStr = GetSafeDynamicProp<object>(row, "PaymentDocID_1C", _log, "Payment.PaymentDocID")?.ToString();
                    if (!Guid.TryParse(docIdStr, out Guid docId) || docId == Guid.Empty) { _log.LogWarning("[PaymentRowDataMapper] Invalid PaymentDocID: {DocId}", (object?)docIdStr ?? "null"); return null; }
                    _log.LogDebug("[PaymentRowDataMapper] Parsed PaymentDocID: {Id}", docId);
                    string? orgRefStr = GetSafeDynamicProp<object>(row, "OrganizationRef1C", _log, "Payment.OrgRef1C")?.ToString();
                    if (!Guid.TryParse(orgRefStr, out Guid orgId) || orgId == Guid.Empty) { _log.LogWarning("[PaymentRowDataMapper] Invalid OrgRef1C for PaymentDoc {DocId}", docId); return null; }
                    string? payerRefStr = GetSafeDynamicProp<object>(row, "PayerRef1C", _log, "Payment.PayerRef1C")?.ToString();
                    if (!Guid.TryParse(payerRefStr, out Guid payerId) || payerId == Guid.Empty) { _log.LogWarning("[PaymentRowDataMapper] Invalid PayerRef1C for PaymentDoc {DocId}", docId); return null; }
                    Guid? contractGuid = null; string? contractRefStrVal = GetSafeDynamicProp<object>(row, "ContractRef1C_Str", _log, "Payment.ContractRef1C_Str")?.ToString();
                    if (!string.IsNullOrEmpty(contractRefStrVal))
                    {
                        if (Guid.TryParse(contractRefStrVal, out Guid parsedGuid) && parsedGuid != Guid.Empty) { contractGuid = parsedGuid; }
                        else { _log.LogWarning("[PaymentRowDataMapper] Invalid ContractRef1C_Str string: {ContractStr} for PaymentDoc {DocId}", (object?)contractRefStrVal ?? "null", docId); }
                    }
                    object? paymentDateObj = GetSafeDynamicProp<object>(row, "PaymentDate", _log, "Payment.PaymentDate");
                    DateTime paymentDate = DateTime.MinValue;
                    if (paymentDateObj is DateTime dtP) { paymentDate = dtP; }
                    else if (paymentDateObj != null && !(paymentDateObj is DBNull)) { _log.LogWarning("[PaymentRowDataMapper] PaymentDate was not DateTime: {Type}, using MinValue", (object)GetDynamicTypeName(paymentDateObj)); }

                    return new PaymentRowData
                    {
                        PaymentDocID_1C = docId,
                        OrganizationRef_1C = orgId,
                        PayerRef_1C = payerId,
                        ContractRef_1C = contractGuid,
                        PaymentDate = paymentDate,
                        PaymentNumber_1C = GetSafeDynamicProp<object>(row, "PaymentNumber_1C", _log, "Payment.PaymentNum")?.ToString() ?? "",
                        Amount = GetSafeDynamicValue<decimal>(row, "Amount", _log, "Payment.Amount") ?? 0,
                        CurrencyCode = GetSafeDynamicProp<object>(row, "CurrencyCode", _log, "Payment.CurrencyCode")?.ToString()
                    };
                }
                catch (Microsoft.CSharp.RuntimeBinder.RuntimeBinderException rbEx)
                {
                    _log.LogError(rbEx, "[PaymentRowDataMapper] BinderEx. RowType: {RowType}", (object)GetDynamicTypeName(row)); return null;
                }
                catch (Exception ex) { _log.LogError(ex, "[PaymentRowDataMapper] GeneralEx. PaymentDocID: {PaymentDocId}. RowType: {RowType}", (object?)GetSafeDynamicProp<object>(row, "PaymentDocID_1C", _log, "Payment.PaymentDocId.Ex")?.ToString() ?? "N/A", (object)GetDynamicTypeName(row)); return null; }
            });
        }
    }
}