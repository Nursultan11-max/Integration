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
        // private dynamic? _activeContext = null; // Убрано, _v8Application будет контекстом

        public OneCAccessor(IOptions<Settings> opts, ILogger<OneCAccessor> log)
        {
            _log = log ?? throw new ArgumentNullException(nameof(log));

            if (opts?.Value?.ConnectionStrings == null || opts.Value.EtlSettings == null)
                throw new ArgumentNullException(nameof(opts), "Settings or its inner properties (ConnectionStrings, EtlSettings) are null.");

            _connString = opts.Value.ConnectionStrings.OneCConnectionString;
            _progId = opts.Value.EtlSettings.OneCComVersion;
        }

        public bool Connect()
        {
            _log.LogInformation("Attempting OneCAccessor.Connect() with ProgID: {ProgId} and ConnString: {ConnString}", (object)_progId, (object)_connString);
            try
            {
                Type? comType = Type.GetTypeFromProgID(_progId, throwOnError: false);
                if (comType == null)
                {
                    _log.LogError("COM type for ProgID '{ProgId}' not found.", (object)_progId);
                    return false;
                }
                _log.LogInformation("COM type for ProgID '{ProgId}' found: {ComTypeName}", (object)_progId, (object)comType.FullName!);

#pragma warning disable CA1416
                _v8Application = Activator.CreateInstance(comType);
#pragma warning restore CA1416

                if (_v8Application == null)
                {
                    _log.LogError("Activator.CreateInstance for ProgID '{ProgId}' returned null.", (object)_progId);
                    return false;
                }
                _log.LogInformation("Instance of COM object '{ProgId}' created successfully: {_v8ApplicationType}", (object)_progId, (object)_v8Application.GetType().ToString());

                if (!Marshal.IsComObject(_v8Application))
                {
                    _log.LogError("_v8Application is NOT a COM object. ProgID: {ProgId}. Object Type: {_v8ApplicationType}", (object)_progId, (object)_v8Application.GetType().ToString());
                    if (_v8Application is IDisposable disp) disp.Dispose();
                    _v8Application = null;
                    return false;
                }
                _log.LogInformation("_v8Application IS a COM object.");

                _log.LogInformation("Attempting to connect to 1C using _v8Application.Connect()...");

                object? connectResult = null;
                try
                {
                    connectResult = _v8Application.Connect(_connString);
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Exception during _v8Application.Connect(). Cleaning up.");
                    ReleaseComObjects();
                    return false;
                }

                // Проверяем результат Connect()
                bool isConnectionSuccessful = false;

                if (connectResult is bool boolSuccess) // Сценарий 1: Connect() вернул bool
                {
                    if (boolSuccess)
                    {
                        _log.LogInformation("_v8Application.Connect() returned boolean true. _v8Application is the active context.");
                        isConnectionSuccessful = true;
                    }
                    else
                    {
                        _log.LogError("_v8Application.Connect() returned boolean false. Connection failed. ConnString: {ConnStr}", (object)_connString);
                    }
                }
                else if (connectResult != null && Marshal.IsComObject(connectResult)) // Сценарий 2: Connect() вернул COM-объект (объект соединения)
                {
                    _log.LogInformation("_v8Application.Connect() returned a COM object (connection object). Type: {ContextType}. _v8Application will be used for NewObject.", (object)(connectResult?.GetType().ToString() ?? "null"));
                    _v8Application = connectResult;
                    isConnectionSuccessful = true;
                    // Если бы объект соединения был нужен, можно было бы его сохранить:
                    // _connectionObject = connectResult; 
                }
                else if (connectResult == null) // Сценарий 3: Connect() вернул null
                {
                    _log.LogError("_v8Application.Connect() returned null. Connection failed. ConnString: {ConnStr}", (object)_connString);
                }
                else // Сценарий 4: Connect() вернул что-то неожиданное
                {
                    _log.LogError("_v8Application.Connect() returned an unexpected result. Result Type: {ResultType}", (object)(connectResult?.GetType().ToString() ?? "null"));
                }

                if (!isConnectionSuccessful)
                {
                    TryLog1CErrorDescription();
                    ReleaseComObjects();
                    return false;
                }

                // Если дошли сюда, соединение успешно, и _v8Application готов к использованию для NewObject()
                return true;
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Critical error during 1C COM initialization or connection. Ensure 1C Client is installed and COM component ({ProgId}) is registered correctly.", (object)_progId);
                ReleaseComObjects();
                return false;
            }
        }

        private void TryLog1CErrorDescription()
        {
            if (_v8Application != null && Marshal.IsComObject(_v8Application))
            {
                try
                {
                    _log.LogInformation("Attempting to get 1C error description (specific method depends on 1C version)...");
                }
                catch (Exception descEx)
                {
                    _log.LogWarning(descEx, "Could not retrieve/log 1C error description from _v8Application.");
                }
            }
        }

        private void ReleaseComObjects()
        {
            _log.LogDebug("Attempting to release COM objects...");

            if (_v8Application != null)
            {
                if (Marshal.IsComObject(_v8Application))
                {
                    _log.LogDebug("Releasing _v8Application COM object (Type: {_v8ApplicationType})...", (object)_v8Application.GetType().ToString());
                    try { Marshal.ReleaseComObject(_v8Application); }
                    catch (Exception ex) { _log.LogWarning(ex, "Exception during _v8Application release."); }
                }
                _v8Application = null;
            }
            _log.LogDebug("Finished releasing COM objects.");
        }

        public void Dispose()
        {
            _log.LogInformation("Disposing OneCAccessor...");
            ReleaseComObjects();
            GC.SuppressFinalize(this);
        }

        private IEnumerable<Customer1C> GetCustomersInternal(string query)
        {
            if (_v8Application == null)
            {
                _log.LogError("1C Application object (_v8Application) не инициализирован для GetCustomersInternal.");
                yield break;
            }

            dynamic? q = null;
            dynamic? executionResult = null;
            dynamic? table = null;
            var customers = new List<Customer1C>();

            try
            {
                _log.LogDebug("Creating Query object for Customers using _v8Application (Type: {_v8AppType})", (object)_v8Application.GetType().ToString());
                q = _v8Application.NewObject("Query");
                if (q == null)
                {
                    _log.LogError("Не удалось создать объект Query для Контрагентов (_v8Application.NewObject вернул null).");
                    yield break;
                }

                try
                {
                    q.Text = query;
                }
                catch (COMException ex)
                {
                    _log.LogError(ex, "COMException при установке текста запроса для Контрагентов.");
                    yield break;
                }
                catch (Exception ex) { _log.LogError(ex, "Ошибка при установке текста запроса для Контрагентов."); yield break; }


                try
                {
                    executionResult = q.Execute();
                }
                catch (COMException ex)
                {
                    _log.LogError(ex, "COMException при выполнении запроса для Контрагентов.");
                    yield break;
                }
                catch (Exception ex) { _log.LogError(ex, "Ошибка при выполнении запроса для Контрагентов."); yield break; }


                if (executionResult == null)
                {
                    _log.LogWarning("Query.Execute вернул null для Контрагентов.");
                    yield break;
                }

                try
                {
                    table = executionResult.Unload();
                }
                catch (COMException ex)
                {
                    _log.LogError(ex, "COMException при выгрузке результата для Контрагентов.");
                    yield break;
                }
                catch (Exception ex) { _log.LogError(ex, "Ошибка при выгрузке результата для Контрагентов."); yield break; }

                if (table == null)
                {
                    _log.LogWarning("Unload вернул null для Контрагентов.");
                    yield break;
                }

                foreach (dynamic row in table)
                {
                    try
                    {
                        string ref1CStr = row.Ref1C?.ToString() ?? string.Empty;
                        if (string.IsNullOrEmpty(ref1CStr) || ref1CStr.Length != 36)
                        {
                            _log.LogInformation("Некорректный GUID для Ref1C в Контрагентах: {Ref1CValue}", (object)ref1CStr);
                            continue;
                        }
                        customers.Add(new Customer1C
                        {
                            Ref = new Guid(ref1CStr),
                            Code = row.Code?.ToString(),
                            Name = row.Name?.ToString() ?? string.Empty,
                            FullName = row.FullName?.ToString(),
                            TIN = row.TIN?.ToString(),
                            KPP = row.KPP?.ToString(),
                            EntityType = row.EntityType?.ToString(),
                            IsDeleted = row.IsDeleted != null && Convert.ToBoolean(row.IsDeleted)
                        });
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Ошибка при обработке строки данных контрагента.");
                    }
                }
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Общая ошибка при получении списка контрагентов.");
            }
            finally
            {
                if (executionResult != null && Marshal.IsComObject(executionResult))
                {
                    try { Marshal.ReleaseComObject(executionResult); } catch (Exception ex) { _log.LogWarning(ex, "Ошибка при освобождении executionResult для Контрагентов."); }
                }
                if (q != null && Marshal.IsComObject(q))
                {
                    try { Marshal.ReleaseComObject(q); } catch (Exception ex) { _log.LogWarning(ex, "Ошибка при освобождении q для Контрагентов."); }
                }
            }

            foreach (var customer in customers)
                yield return customer;
        }

        private IEnumerable<Product1C> GetProductsInternal(string query)
        {
            if (_v8Application == null)
            {
                _log.LogError("1C Application object (_v8Application) не инициализирован для GetProductsInternal.");
                yield break;
            }
            dynamic? q = null;
            dynamic? executionResult = null;
            dynamic? table = null;
            var products = new List<Product1C>();
            try
            {
                q = _v8Application.NewObject("Query");
                if (q == null) { _log.LogError("Не удалось создать объект Query для Номенклатуры."); yield break; }
                try { q.Text = query; } catch (COMException ex) { _log.LogError(ex, "COMException текст запроса Номенклатуры."); yield break; } catch (Exception ex) { _log.LogError(ex, "Ошибка текст запроса Номенклатуры."); yield break; }
                try { executionResult = q.Execute(); } catch (COMException ex) { _log.LogError(ex, "COMException запрос Номенклатуры."); yield break; } catch (Exception ex) { _log.LogError(ex, "Ошибка запрос Номенклатуры."); yield break; }
                if (executionResult == null) { _log.LogWarning("Query.Execute null для Номенклатуры."); yield break; }
                try { table = executionResult.Unload(); } catch (COMException ex) { _log.LogError(ex, "COMException выгрузка Номенклатуры."); yield break; } catch (Exception ex) { _log.LogError(ex, "Ошибка выгрузка Номенклатуры."); yield break; }
                if (table == null) { _log.LogWarning("Unload null для Номенклатуры."); yield break; }
                foreach (dynamic row in table)
                {
                    try
                    {
                        string ref1CStr = row.Ref1C?.ToString() ?? string.Empty;
                        if (string.IsNullOrEmpty(ref1CStr) || ref1CStr.Length != 36) { _log.LogInformation("Некорректный GUID Ref1C Номенклатуры: {Ref1CValue}", (object)ref1CStr); continue; }
                        products.Add(new Product1C
                        {
                            Ref = new Guid(ref1CStr),
                            Code = row.Code?.ToString(),
                            Name = row.Name?.ToString() ?? string.Empty,
                            FullName = row.FullName?.ToString(),
                            SKU = row.SKU?.ToString(),
                            UnitOfMeasure = row.UnitOfMeasure?.ToString(),
                            ProductType = row.ProductType?.ToString(),
                            ProductGroup = row.ProductGroup?.ToString(),
                            DefaultVATRateName = row.DefaultVATRateName?.ToString()
                        });
                    }
                    catch (Exception ex) { _log.LogError(ex, "Ошибка обработки строки Номенклатуры."); }
                }
            }
            catch (Exception ex) { _log.LogError(ex, "Общая ошибка получения списка Номенклатуры."); }
            finally
            {
                if (executionResult != null && Marshal.IsComObject(executionResult)) { try { Marshal.ReleaseComObject(executionResult); } catch (Exception ex) { _log.LogWarning(ex, "Ошибка освобождения executionResult Номенклатуры."); } }
                if (q != null && Marshal.IsComObject(q)) { try { Marshal.ReleaseComObject(q); } catch (Exception ex) { _log.LogWarning(ex, "Ошибка освобождения q Номенклатуры."); } }
            }
            foreach (var product in products) yield return product;
        }

        private IEnumerable<Contract1C> GetContractsInternal(string query)
        {
            if (_v8Application == null) { _log.LogError("1C Application object (_v8Application) не инициализирован для GetContractsInternal."); yield break; }
            dynamic? q = null;
            dynamic? executionResult = null;
            dynamic? table = null;
            var contracts = new List<Contract1C>();
            try
            {
                q = _v8Application.NewObject("Query");
                if (q == null) { _log.LogError("Не удалось создать объект Query для Договоров."); yield break; }
                try { q.Text = query; } catch (COMException ex) { _log.LogError(ex, "COMException текст запроса Договоров."); yield break; } catch (Exception ex) { _log.LogError(ex, "Ошибка текст запроса Договоров."); yield break; }
                try { executionResult = q.Execute(); } catch (COMException ex) { _log.LogError(ex, "COMException запрос Договоров."); yield break; } catch (Exception ex) { _log.LogError(ex, "Ошибка запрос Договоров."); yield break; }
                if (executionResult == null) { _log.LogWarning("Query.Execute null для Договоров."); yield break; }
                try { table = executionResult.Unload(); } catch (COMException ex) { _log.LogError(ex, "COMException выгрузка Договоров."); yield break; } catch (Exception ex) { _log.LogError(ex, "Ошибка выгрузка Договоров."); yield break; }
                if (table == null) { _log.LogWarning("Unload null для Договоров."); yield break; }
                foreach (dynamic row in table)
                {
                    try
                    {
                        string ref1CStr = row.Ref1C?.ToString() ?? string.Empty;
                        string customerRef1CStr = row.CustomerRef1C?.ToString() ?? string.Empty;
                        if (string.IsNullOrEmpty(ref1CStr) || ref1CStr.Length != 36) { _log.LogInformation("Некорректный GUID Ref1C Договоров: {Ref1CValue}", (object)ref1CStr); continue; }
                        if (string.IsNullOrEmpty(customerRef1CStr) || customerRef1CStr.Length != 36) { _log.LogInformation("Некорректный GUID CustomerRef1C Договоров: {CustomerRef1CValue}", (object)customerRef1CStr); continue; }
                        contracts.Add(new Contract1C
                        {
                            Ref = new Guid(ref1CStr),
                            Code = row.Code?.ToString(),
                            Name = row.Name?.ToString() ?? string.Empty,
                            CustomerRef_1C = new Guid(customerRef1CStr),
                            StartDate = row.StartDate as DateTime?,
                            EndDate = row.EndDate as DateTime?
                        });
                    }
                    catch (Exception ex) { _log.LogError(ex, "Ошибка обработки строки Договоров."); }
                }
            }
            catch (Exception ex) { _log.LogError(ex, "Общая ошибка получения списка Договоров."); }
            finally
            {
                if (executionResult != null && Marshal.IsComObject(executionResult)) { try { Marshal.ReleaseComObject(executionResult); } catch (Exception ex) { _log.LogWarning(ex, "Ошибка освобождения executionResult Договоров."); } }
                if (q != null && Marshal.IsComObject(q)) { try { Marshal.ReleaseComObject(q); } catch (Exception ex) { _log.LogWarning(ex, "Ошибка освобождения q Договоров."); } }
            }
            foreach (var contract in contracts) yield return contract;
        }

        private IEnumerable<Organization1C> GetOrganizationsInternal(string query)
        {
            if (_v8Application == null) { _log.LogError("1C Application object (_v8Application) не инициализирован для GetOrganizationsInternal."); yield break; }
            dynamic? q = null;
            dynamic? executionResult = null;
            dynamic? table = null;
            var organizations = new List<Organization1C>();
            try
            {
                q = _v8Application.NewObject("Query");
                if (q == null) { _log.LogError("Не удалось создать объект Query для Организаций."); yield break; }
                try { q.Text = query; } catch (COMException ex) { _log.LogError(ex, "COMException текст запроса Организаций."); yield break; } catch (Exception ex) { _log.LogError(ex, "Ошибка текст запроса Организаций."); yield break; }
                try { executionResult = q.Execute(); } catch (COMException ex) { _log.LogError(ex, "COMException запрос Организаций."); yield break; } catch (Exception ex) { _log.LogError(ex, "Ошибка запрос Организаций."); yield break; }
                if (executionResult == null) { _log.LogWarning("Query.Execute null для Организаций."); yield break; }
                try { table = executionResult.Unload(); } catch (COMException ex) { _log.LogError(ex, "COMException выгрузка Организаций."); yield break; } catch (Exception ex) { _log.LogError(ex, "Ошибка выгрузка Организаций."); yield break; }
                if (table == null) { _log.LogWarning("Unload null для Организаций."); yield break; }
                foreach (dynamic row in table)
                {
                    try
                    {
                        string refStr = row.Ref1C?.ToString() ?? string.Empty;
                        if (string.IsNullOrEmpty(refStr) || refStr.Length != 36) { _log.LogInformation("Некорректный GUID Ref1C Организаций: {Ref1CValue}", (object)refStr); continue; }
                        organizations.Add(new Organization1C
                        {
                            Ref = new Guid(refStr),
                            Code = row.Code?.ToString(),
                            Name = row.Name?.ToString() ?? string.Empty,
                            OrganizationFullName = row.OrganizationFullName?.ToString()
                        });
                    }
                    catch (Exception ex) { _log.LogError(ex, "Ошибка обработки строки Организаций."); }
                }
            }
            catch (Exception ex) { _log.LogError(ex, "Общая ошибка получения списка Организаций."); }
            finally
            {
                if (executionResult != null && Marshal.IsComObject(executionResult)) { try { Marshal.ReleaseComObject(executionResult); } catch (Exception ex) { _log.LogWarning(ex, "Ошибка освобождения executionResult Организаций."); } }
                if (q != null && Marshal.IsComObject(q)) { try { Marshal.ReleaseComObject(q); } catch (Exception ex) { _log.LogWarning(ex, "Ошибка освобождения q Организаций."); } }
            }
            foreach (var org in organizations) yield return org;
        }

        private IEnumerable<SaleFactData> GetSaleRowsInternal(string query)
        {
            if (_v8Application == null) { _log.LogError("1C Application object (_v8Application) не инициализирован для GetSaleRowsInternal."); yield break; }
            dynamic? q = null;
            dynamic? executionResult = null;
            dynamic? table = null;
            var saleRows = new List<SaleFactData>();
            try
            {
                q = _v8Application.NewObject("Query");
                if (q == null) { _log.LogError("Не удалось создать объект Query для Строк Продаж."); yield break; }
                try { q.Text = query; } catch (COMException ex) { _log.LogError(ex, "COMException текст запроса Строк Продаж."); yield break; } catch (Exception ex) { _log.LogError(ex, "Ошибка текст запроса Строк Продаж."); yield break; }
                try { executionResult = q.Execute(); } catch (COMException ex) { _log.LogError(ex, "COMException запрос Строк Продаж."); yield break; } catch (Exception ex) { _log.LogError(ex, "Ошибка запрос Строк Продаж."); yield break; }
                if (executionResult == null) { _log.LogWarning("Query.Execute null для Строк Продаж."); yield break; }
                try { table = executionResult.Unload(); } catch (COMException ex) { _log.LogError(ex, "COMException выгрузка Строк Продаж."); yield break; } catch (Exception ex) { _log.LogError(ex, "Ошибка выгрузка Строк Продаж."); yield break; }
                if (table == null) { _log.LogWarning("Unload null для Строк Продаж."); yield break; }
                foreach (dynamic row in table)
                {
                    try
                    {
                        string docId = row.SalesDocumentID_1C?.ToString() ?? string.Empty;
                        if (string.IsNullOrEmpty(docId) || docId.Length != 36) { _log.LogInformation("Некорректный SalesDocumentID_1C: {DocIdValue}", (object)docId); continue; }

                        string customerRef = row.CustomerRef1C?.ToString() ?? string.Empty;
                        if (string.IsNullOrEmpty(customerRef) || customerRef.Length != 36) { _log.LogInformation("Некорректный CustomerRef1C в Строках Продаж: {CustomerRefValue}", (object)customerRef); continue; }

                        string productRef = row.ProductRef1C?.ToString() ?? string.Empty;
                        if (string.IsNullOrEmpty(productRef) || productRef.Length != 36) { _log.LogInformation("Некорректный ProductRef1C в Строках Продаж: {ProductRefValue}", (object)productRef); continue; }

                        string orgRef = row.OrganizationRef1C?.ToString() ?? string.Empty;
                        if (string.IsNullOrEmpty(orgRef) || orgRef.Length != 36) { _log.LogInformation("Некорректный OrganizationRef1C в Строках Продаж: {OrgRefValue}", (object)orgRef); continue; }

                        Guid? contractRefGuid = null;
                        string contractRefStr = row.ContractRef1C?.ToString() ?? string.Empty;
                        if (!string.IsNullOrEmpty(contractRefStr) && contractRefStr.Length == 36)
                        {
                            if (Guid.TryParse(contractRefStr, out Guid parsedGuid) && parsedGuid != Guid.Empty) contractRefGuid = parsedGuid;
                            else _log.LogInformation("Некорректный ContractRef1C в Строках Продаж: {ContractRefValue}", (object)contractRefStr);
                        }

                        saleRows.Add(new SaleFactData
                        {
                            SalesDocumentID_1C = new Guid(docId),
                            SalesDocumentNumber_1C = row.SalesDocumentNumber_1C?.ToString() ?? string.Empty,
                            SalesDocumentLineNo_1C = Convert.ToInt32(row.SalesDocumentLineNo_1C ?? 0),
                            DocDate = row.DocDate is DateTime dt ? dt : DateTime.MinValue,
                            CustomerRef_1C = new Guid(customerRef),
                            ProductRef_1C = new Guid(productRef),
                            OrganizationRef_1C = new Guid(orgRef),
                            ContractRef_1C = contractRefGuid,
                            Quantity = Convert.ToDecimal(row.Quantity ?? 0),
                            Price = Convert.ToDecimal(row.Price ?? 0),
                            Amount = Convert.ToDecimal(row.Amount ?? 0),
                            VATRateName = row.VATRateName?.ToString(),
                            VATAmount = Convert.ToDecimal(row.VATAmount ?? 0),
                            TotalAmount = Convert.ToDecimal(row.TotalAmount ?? 0),
                            CurrencyCode = row.CurrencyCode?.ToString()
                        });
                    }
                    catch (Exception ex) { _log.LogError(ex, "Ошибка обработки строки Продаж."); }
                }
            }
            catch (Exception ex) { _log.LogError(ex, "Общая ошибка получения списка Строк Продаж."); }
            finally
            {
                if (executionResult != null && Marshal.IsComObject(executionResult)) { try { Marshal.ReleaseComObject(executionResult); } catch (Exception ex) { _log.LogWarning(ex, "Ошибка освобождения executionResult Строк Продаж."); } }
                if (q != null && Marshal.IsComObject(q)) { try { Marshal.ReleaseComObject(q); } catch (Exception ex) { _log.LogWarning(ex, "Ошибка освобождения q Строк Продаж."); } }
            }
            foreach (var saleRow in saleRows) yield return saleRow;
        }

        private IEnumerable<PaymentRowData> GetPaymentRowsInternal(string query)
        {
            if (_v8Application == null) { _log.LogError("1C Application object (_v8Application) не инициализирован для GetPaymentRowsInternal."); yield break; }
            dynamic? q = null;
            dynamic? executionResult = null;
            dynamic? table = null;
            var paymentRows = new List<PaymentRowData>();
            try
            {
                q = _v8Application.NewObject("Query");
                if (q == null) { _log.LogError("Не удалось создать объект Query для Строк Платежей."); yield break; }
                try { q.Text = query; } catch (COMException ex) { _log.LogError(ex, "COMException текст запроса Строк Платежей."); yield break; } catch (Exception ex) { _log.LogError(ex, "Ошибка текст запроса Строк Платежей."); yield break; }
                try { executionResult = q.Execute(); } catch (COMException ex) { _log.LogError(ex, "COMException запрос Строк Платежей."); yield break; } catch (Exception ex) { _log.LogError(ex, "Ошибка запрос Строк Платежей."); yield break; }
                if (executionResult == null) { _log.LogWarning("Query.Execute null для Строк Платежей."); yield break; }
                try { table = executionResult.Unload(); } catch (COMException ex) { _log.LogError(ex, "COMException выгрузка Строк Платежей."); yield break; } catch (Exception ex) { _log.LogError(ex, "Ошибка выгрузка Строк Платежей."); yield break; }
                if (table == null) { _log.LogWarning("Unload null для Строк Платежей."); yield break; }
                foreach (dynamic row in table)
                {
                    try
                    {
                        string paymentDocId = row.PaymentDocID_1C?.ToString() ?? string.Empty;
                        if (string.IsNullOrEmpty(paymentDocId) || paymentDocId.Length != 36) { _log.LogInformation("Некорректный PaymentDocID_1C: {PaymentDocIdValue}", (object)paymentDocId); continue; }

                        string orgRef = row.OrganizationRef1C?.ToString() ?? string.Empty;
                        if (string.IsNullOrEmpty(orgRef) || orgRef.Length != 36) { _log.LogInformation("Некорректный OrganizationRef1C в Строках Платежей: {OrgRefValue}", (object)orgRef); continue; }

                        string payerRef = row.PayerRef1C?.ToString() ?? string.Empty;
                        if (string.IsNullOrEmpty(payerRef) || payerRef.Length != 36) { _log.LogInformation("Некорректный PayerRef1C в Строках Платежей: {PayerRefValue}", (object)payerRef); continue; }

                        Guid? contractRefGuid = null;
                        string contractRefStr = row.ContractRef1C_Str?.ToString() ?? string.Empty;
                        if (!string.IsNullOrEmpty(contractRefStr) && contractRefStr.Length == 36)
                        {
                            if (Guid.TryParse(contractRefStr, out Guid parsedGuid) && parsedGuid != Guid.Empty) contractRefGuid = parsedGuid;
                            else _log.LogInformation("Некорректный ContractRef1C_Str в Строках Платежей: {ContractRefValue}", (object)contractRefStr);
                        }

                        paymentRows.Add(new PaymentRowData
                        {
                            PaymentDocID_1C = new Guid(paymentDocId),
                            PaymentNumber_1C = row.PaymentNumber_1C?.ToString() ?? string.Empty,
                            PaymentDate = row.PaymentDate is DateTime dt ? dt : DateTime.MinValue,
                            OrganizationRef_1C = new Guid(orgRef),
                            Amount = Convert.ToDecimal(row.Amount ?? 0),
                            CurrencyCode = row.CurrencyCode?.ToString(),
                            PayerRef_1C = new Guid(payerRef),
                            ContractRef_1C = contractRefGuid
                        });
                    }
                    catch (Exception ex) { _log.LogError(ex, "Ошибка обработки строки Платежей."); }
                }
            }
            catch (Exception ex) { _log.LogError(ex, "Общая ошибка получения списка Строк Платежей."); }
            finally
            {
                if (executionResult != null && Marshal.IsComObject(executionResult)) { try { Marshal.ReleaseComObject(executionResult); } catch (Exception ex) { _log.LogWarning(ex, "Ошибка освобождения executionResult Строк Платежей."); } }
                if (q != null && Marshal.IsComObject(q)) { try { Marshal.ReleaseComObject(q); } catch (Exception ex) { _log.LogWarning(ex, "Ошибка освобождения q Строк Платежей."); } }
            }
            foreach (var paymentRow in paymentRows) yield return paymentRow;
        }

        public IEnumerable<Customer1C> GetCustomers()
        {
            _log.LogInformation("Чтение контрагентов...");
            const string query =
                "ВЫБРАТЬ\n" +
                "  Контрагенты.Ссылка.УникальныйИдентификатор() КАК Ref1C,\n" +
                "  Контрагенты.Код КАК Code,\n" +
                "  Контрагенты.Наименование КАК Name,\n" +
                "  Контрагенты.ПолноеНаименование КАК FullName,\n" +
                "  Контрагенты.ИНН КАК TIN,\n" +
                "  Контрагенты.КПП КАК KPP,\n" +
                "  Представление(Контрагенты.ЮрФизЛицо) КАК EntityType,\n" +
                "  Контрагенты.ПометкаУдаления КАК IsDeleted\n" +
                "ИЗ Справочник.Контрагенты КАК Контрагенты";
            return GetCustomersInternal(query);
        }

        public IEnumerable<Product1C> GetProducts()
        {
            _log.LogInformation("Чтение номенклатуры...");
            const string query =
                "ВЫБРАТЬ\n" +
                "  Номенклатура.Ссылка.УникальныйИдентификатор() КАК Ref1C,\n" +
                "  Номенклатура.Код КАК Code,\n" +
                "  Номенклатура.Наименование КАК Name,\n" +
                "  Номенклатура.ПолноеНаименование КАК FullName,\n" +
                "  Номенклатура.Артикул КАК SKU,\n" +
                "  Номенклатура.ЕдиницаИзмерения.Наименование КАК UnitOfMeasure,\n" +
                "  Представление(Номенклатура.ВидНоменклатуры) КАК ProductType,\n" +
                "  Номенклатура.Группа.Наименование КАК ProductGroup,\n" +
                "  Номенклатура.СтавкаНДС.Наименование КАК DefaultVATRateName\n" +
                "ИЗ Справочник.Номенклатура КАК Номенклатура";
            return GetProductsInternal(query);
        }

        public IEnumerable<Contract1C> GetContracts()
        {
            _log.LogInformation("Чтение договоров...");
            const string query =
                "ВЫБРАТЬ\n" +
                "  ДоговорыКонтрагентов.Ссылка.УникальныйИдентификатор() КАК Ref1C,\n" +
                "  ДоговорыКонтрагентов.Код КАК Code,\n" +
                "  ДоговорыКонтрагентов.Наименование КАК Name,\n" +
                "  ДоговорыКонтрагентов.Контрагент.УникальныйИдентификатор() КАК CustomerRef1C,\n" +
                "  ДоговорыКонтрагентов.ДатаНачала КАК StartDate,\n" +
                "  ДоговорыКонтрагентов.ДатаОкончания КАК EndDate\n" +
                "ИЗ Справочник.ДоговорыКонтрагентов КАК ДоговорыКонтрагентов";
            return GetContractsInternal(query);
        }

        public IEnumerable<Organization1C> GetOrganizations()
        {
            _log.LogInformation("Чтение организаций...");
            const string query =
                "ВЫБРАТЬ\n" +
                "  Организации.Ссылка.УникальныйИдентификатор() КАК Ref1C,\n" +
                "  Организации.Код КАК Code,\n" +
                "  Организации.Наименование КАК Name,\n" +
                "  Организации.ПолноеНаименование КАК OrganizationFullName\n" +
                "ИЗ Справочник.Организации КАК Организации";
            return GetOrganizationsInternal(query);
        }

        public IEnumerable<SaleFactData> GetSaleRows()
        {
            _log.LogInformation("Чтение строк продаж...");
            const string query =
                "ВЫБРАТЬ\n" +
                "  Реализация.Ссылка.УникальныйИдентификатор() КАК SalesDocumentID_1C,\n" +
                "  Реализация.Номер КАК SalesDocumentNumber_1C,\n" +
                "  Реализация.Дата КАК DocDate,\n" +
                "  Реализация.Контрагент.УникальныйИдентификатор() КАК CustomerRef1C,\n" +
                "  Реализация.Организация.УникальныйИдентификатор() КАК OrganizationRef1C,\n" +
                "  Реализация.ДоговорКонтрагента.УникальныйИдентификатор() КАК ContractRef1C,\n" +
                "  Реализация.Товары.(НомерСтроки) КАК SalesDocumentLineNo_1C,\n" +
                "  Реализация.Товары.Номенклатура.УникальныйИдентификатор() КАК ProductRef1C,\n" +
                "  Реализация.Товары.Количество КАК Quantity,\n" +
                "  Реализация.Товары.Цена КАК Price,\n" +
                "  Реализация.Товары.Сумма КАК Amount,\n" +
                "  Представление(Реализация.Товары.СтавкаНДС) КАК VATRateName,\n" +
                "  Реализация.Товары.СуммаНДС КАК VATAmount,\n" +
                "  Реализация.Товары.Всего КАК TotalAmount,\n" +
                "  Реализация.Валюта.Код КАК CurrencyCode\n" +
                "ИЗ Документ.РеализацияТоваровУслуг КАК Реализация\n" +
                "ГДЕ Реализация.Проведен";
            return GetSaleRowsInternal(query);
        }

        public IEnumerable<PaymentRowData> GetPaymentRows()
        {
            _log.LogInformation("Чтение строк платежей...");
            const string query =
               "ВЫБРАТЬ\n" +
               "  Пл.Ссылка.УникальныйИдентификатор() КАК PaymentDocID_1C,\n" +
               "  Пл.Номер КАК PaymentNumber_1C,\n" +
               "  Пл.Дата КАК PaymentDate,\n" +
               "  Пл.Организация.УникальныйИдентификатор() КАК OrganizationRef1C, \n" +
               "  Пл.Сумма КАК Amount,\n" +
               "  Пл.Валюта.Код КАК CurrencyCode,\n" +
               "  Пл.Контрагент.УникальныйИдентификатор() КАК PayerRef1C,\n" +
               "  Пл.ДоговорКонтрагента.УникальныйИдентификатор() КАК ContractRef1C_Str\n" +
               "ИЗ Документ.ПоступлениеДенежныхСредств КАК Пл\n" +
               "ГДЕ Пл.ВидОперации В (ЗНАЧЕНИЕ(Перечисление.ВидыОперацийПоступлениеДенежныхСредств.ОплатаПокупателя), ЗНАЧЕНИЕ(Перечисление.ВидыОперацийПоступлениеДенежныхСредств.ПоступлениеОтПродажПоПлатежнымКартамИБанковскимКредитам))";
            return GetPaymentRowsInternal(query);
        }
    }
}