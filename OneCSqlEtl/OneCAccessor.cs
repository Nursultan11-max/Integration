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
                throw new ArgumentNullException(nameof(opts), "Settings or its inner properties (ConnectionStrings, EtlSettings) are null.");

            _connString = opts.Value.ConnectionStrings.OneCConnectionString;
            _progId = opts.Value.EtlSettings.OneCComVersion;
        }

        public bool Connect()
        {
            _log.LogInformation("Attempting OneCAccessor.Connect() with ProgID: {ProgId} and ConnString: {ConnString}", _progId, _connString);
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
                _v8Application = Activator.CreateInstance(comType);
#pragma warning restore CA1416

                if (_v8Application == null)
                {
                    _log.LogError("Activator.CreateInstance for ProgID '{ProgId}' returned null.", _progId);
                    return false;
                }
                _log.LogInformation("Instance of COM object '{ProgId}' created successfully: {_v8ApplicationType}", _progId, _v8Application.GetType().ToString());

                if (!Marshal.IsComObject(_v8Application))
                {
                    _log.LogError("_v8Application is NOT a COM object. ProgID: {ProgId}. Object Type: {_v8ApplicationType}", _progId, _v8Application.GetType().ToString());
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

                bool isConnectionSuccessful = false;

                if (connectResult is bool boolSuccess)
                {
                    if (boolSuccess)
                    {
                        _log.LogInformation("_v8Application.Connect() returned boolean true. _v8Application is the active context.");
                        isConnectionSuccessful = true;
                    }
                    else
                    {
                        _log.LogError("_v8Application.Connect() returned boolean false. Connection failed. ConnString: {ConnStr}", _connString);
                    }
                }
                else if (connectResult != null && Marshal.IsComObject(connectResult))
                {
                    _log.LogInformation("_v8Application.Connect() returned a COM object (connection object). Type: {ContextType}. _v8Application will be used for NewObject", connectResult?.GetType().ToString() ?? "null");
                    _v8Application = connectResult;
                    isConnectionSuccessful = true;
                }
                else if (connectResult == null)
                {
                    _log.LogError("_v8Application.Connect() returned null. Connection failed. ConnString: {ConnStr}", _connString);
                }
                else
                {
                    _log.LogError("_v8Application.Connect() returned an unexpected result. Result Type: {ResultType}", connectResult?.GetType().ToString() ?? "null");
                }

                if (!isConnectionSuccessful)
                {
                    TryLog1CErrorDescription();
                    ReleaseComObjects();
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Critical error during 1C COM initialization or connection. Ensure 1C Client is installed and COM component ({ProgId}) is registered correctly.", _progId);
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
                    _log.LogDebug("Releasing _v8Application COM object (Type: {_v8ApplicationType})...", _v8Application.GetType().ToString());
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

        private IEnumerable<T> ExecuteQuery<T>(string query, Func<dynamic, T> rowMapper) where T : class
        {
            if (_v8Application == null)
            {
                _log.LogError("1C Application object (_v8Application) is not initialized");
                yield break;
            }

            dynamic? q = null;
            dynamic? executionResult = null;
            dynamic? table = null;

            try
            {
                _log.LogDebug("Creating Query object using _v8Application (Type: {_v8AppType})", _v8Application.GetType().ToString());
                
                q = _v8Application.NewObject("Query");
                if (q == null)
                {
                    _log.LogError("Failed to create Query object (_v8Application.NewObject(\"Query\") returned null)");
                    yield break;
                }

                try
                {
                    q.Text = query;
                    _log.LogDebug("Query text set successfully");
                }
                catch (COMException ex)
                {
                    _log.LogError(ex, "COMException while setting query text");
                    yield break;
                }

                try
                {
                    _log.LogDebug("Executing query...");
                    executionResult = q.Execute();
                    
                    if (executionResult == null)
                    {
                        _log.LogError("Query execution returned null");
                        yield break;
                    }
                }
                catch (COMException ex)
                {
                    _log.LogError(ex, "COMException during query execution");
                    yield break;
                }

                try
                {
                    _log.LogDebug("Unloading query results...");
                    table = executionResult.Unload();
                    
                    if (table == null)
                    {
                        _log.LogError("Result unload returned null");
                        yield break;
                    }
                }
                catch (COMException ex)
                {
                    _log.LogError(ex, "COMException during result unload");
                    yield break;
                }

                foreach (dynamic row in table)
                {
                    try
                    {
                        var item = rowMapper(row);
                        if (item != null)
                        {
                            yield return item;
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Error mapping row data");
                    }
                }
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "General error executing query");
            }
            finally
            {
                if (table != null && Marshal.IsComObject(table))
                {
                    try { Marshal.ReleaseComObject(table); }
                    catch (Exception ex) { _log.LogWarning(ex, "Error releasing table COM object"); }
                }
                if (executionResult != null && Marshal.IsComObject(executionResult))
                {
                    try { Marshal.ReleaseComObject(executionResult); }
                    catch (Exception ex) { _log.LogWarning(ex, "Error releasing executionResult COM object"); }
                }
                if (q != null && Marshal.IsComObject(q))
                {
                    try { Marshal.ReleaseComObject(q); }
                    catch (Exception ex) { _log.LogWarning(ex, "Error releasing query COM object"); }
                }
            }
        }

        public IEnumerable<Customer1C> GetCustomers()
        {
            _log.LogInformation("Reading customers...");
            const string query = @"
                ВЫБРАТЬ
                    Контрагенты.Ссылка.УникальныйИдентификатор() КАК Ref1C,
                    Контрагенты.Код КАК Code,
                    Контрагенты.Наименование КАК Name,
                    Контрагенты.ПолноеНаименование КАК FullName,
                    Контрагенты.ИНН КАК TIN,
                    Контрагенты.КПП КАК KPP,
                    Представление(Контрагенты.ЮрФизЛицо) КАК EntityType,
                    Контрагенты.ПометкаУдаления КАК IsDeleted
                ИЗ
                    Справочник.Контрагенты КАК Контрагенты";

            return ExecuteQuery(query, row =>
            {
                try
                {
                    string refStr = row.Ref1C?.ToString() ?? string.Empty;
                    if (string.IsNullOrEmpty(refStr) || refStr.Length != 36)
                    {
                        _log.LogWarning("Invalid GUID for Ref1C in Customers: {Ref1CValue}", refStr);
                        return null;
                    }

                    return new Customer1C
                    {
                        Ref = new Guid(refStr),
                        Code = row.Code?.ToString(),
                        Name = row.Name?.ToString() ?? string.Empty,
                        FullName = row.FullName?.ToString(),
                        TIN = row.TIN?.ToString(),
                        KPP = row.KPP?.ToString(),
                        EntityType = row.EntityType?.ToString(),
                        IsDeleted = row.IsDeleted != null && Convert.ToBoolean(row.IsDeleted)
                    };
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Error mapping customer row");
                    return null;
                }
            });
        }

        public IEnumerable<Product1C> GetProducts()
        {
            _log.LogInformation("Reading products...");
            const string query = @"
                ВЫБРАТЬ
                    Номенклатура.Ссылка.УникальныйИдентификатор() КАК Ref1C,
                    Номенклатура.Код КАК Code,
                    Номенклатура.Наименование КАК Name,
                    Номенклатура.ПолноеНаименование КАК FullName,
                    Номенклатура.Артикул КАК SKU,
                    Номенклатура.ЕдиницаИзмерения.Наименование КАК UnitOfMeasure,
                    Представление(Номенклатура.ВидНоменклатуры) КАК ProductType,
                    Номенклатура.Группа.Наименование КАК ProductGroup,
                    Номенклатура.СтавкаНДС.Наименование КАК DefaultVATRateName
                ИЗ
                    Справочник.Номенклатура КАК Номенклатура";

            return ExecuteQuery(query, row =>
            {
                try
                {
                    string refStr = row.Ref1C?.ToString() ?? string.Empty;
                    if (string.IsNullOrEmpty(refStr) || refStr.Length != 36)
                    {
                        _log.LogWarning("Invalid GUID for Ref1C in Products: {Ref1CValue}", refStr);
                        return null;
                    }

                    return new Product1C
                    {
                        Ref = new Guid(refStr),
                        Code = row.Code?.ToString(),
                        Name = row.Name?.ToString() ?? string.Empty,
                        FullName = row.FullName?.ToString(),
                        SKU = row.SKU?.ToString(),
                        UnitOfMeasure = row.UnitOfMeasure?.ToString(),
                        ProductType = row.ProductType?.ToString(),
                        ProductGroup = row.ProductGroup?.ToString(),
                        DefaultVATRateName = row.DefaultVATRateName?.ToString()
                    };
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Error mapping product row");
                    return null;
                }
            });
        }

        public IEnumerable<Contract1C> GetContracts()
        {
            _log.LogInformation("Reading contracts...");
            const string query = @"
                ВЫБРАТЬ
                    ДоговорыКонтрагентов.Ссылка.УникальныйИдентификатор() КАК Ref1C,
                    ДоговорыКонтрагентов.Код КАК Code,
                    ДоговорыКонтрагентов.Наименование КАК Name,
                    ДоговорыКонтрагентов.Контрагент.УникальныйИдентификатор() КАК CustomerRef1C,
                    ДоговорыКонтрагентов.ДатаНачала КАК StartDate,
                    ДоговорыКонтрагентов.ДатаОкончания КАК EndDate
                ИЗ
                    Справочник.ДоговорыКонтрагентов КАК ДоговорыКонтрагентов";

            return ExecuteQuery(query, row =>
            {
                try
                {
                    string refStr = row.Ref1C?.ToString() ?? string.Empty;
                    string customerRefStr = row.CustomerRef1C?.ToString() ?? string.Empty;

                    if (string.IsNullOrEmpty(refStr) || refStr.Length != 36)
                    {
                        _log.LogWarning("Invalid GUID for Ref1C in Contracts: {Ref1CValue}", refStr);
                        return null;
                    }

                    if (string.IsNullOrEmpty(customerRefStr) || customerRefStr.Length != 36)
                    {
                        _log.LogWarning("Invalid GUID for CustomerRef1C in Contracts: {CustomerRef1CValue}", customerRefStr);
                        return null;
                    }

                    return new Contract1C
                    {
                        Ref = new Guid(refStr),
                        Code = row.Code?.ToString(),
                        Name = row.Name?.ToString() ?? string.Empty,
                        CustomerRef_1C = new Guid(customerRefStr),
                        StartDate = row.StartDate as DateTime?,
                        EndDate = row.EndDate as DateTime?
                    };
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Error mapping contract row");
                    return null;
                }
            });
        }

        public IEnumerable<Organization1C> GetOrganizations()
        {
            _log.LogInformation("Reading organizations...");
            const string query = @"
                ВЫБРАТЬ
                    Организации.Ссылка.УникальныйИдентификатор() КАК Ref1C,
                    Организации.Код КАК Code,
                    Организации.Наименование КАК Name,
                    Организации.ПолноеНаименование КАК OrganizationFullName
                ИЗ
                    Справочник.Организации КАК Организации";

            return ExecuteQuery(query, row =>
            {
                try
                {
                    string refStr = row.Ref1C?.ToString() ?? string.Empty;
                    if (string.IsNullOrEmpty(refStr) || refStr.Length != 36)
                    {
                        _log.LogWarning("Invalid GUID for Ref1C in Organizations: {Ref1CValue}", refStr);
                        return null;
                    }

                    return new Organization1C
                    {
                        Ref = new Guid(refStr),
                        Code = row.Code?.ToString(),
                        Name = row.Name?.ToString() ?? string.Empty,
                        OrganizationFullName = row.OrganizationFullName?.ToString()
                    };
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Error mapping organization row");
                    return null;
                }
            });
        }

        public IEnumerable<SaleFactData> GetSaleRows()
        {
            _log.LogInformation("Reading sale rows...");
            const string query = @"
                ВЫБРАТЬ
                    Реализация.Ссылка.УникальныйИдентификатор() КАК SalesDocumentID_1C,
                    Реализация.Номер КАК SalesDocumentNumber_1C,
                    Реализация.Дата КАК DocDate,
                    Реализация.Контрагент.УникальныйИдентификатор() КАК CustomerRef1C,
                    Реализация.Организация.УникальныйИдентификатор() КАК OrganizationRef1C,
                    Реализация.ДоговорКонтрагента.УникальныйИдентификатор() КАК ContractRef1C,
                    Реализация.Товары.(НомерСтроки) КАК SalesDocumentLineNo_1C,
                    Реализация.Товары.Номенклатура.УникальныйИдентификатор() КАК ProductRef1C,
                    Реализация.Товары.Количество КАК Quantity,
                    Реализация.Товары.Цена КАК Price,
                    Реализация.Товары.Сумма КАК Amount,
                    Представление(Реализация.Товары.СтавкаНДС) КАК VATRateName,
                    Реализация.Товары.СуммаНДС КАК VATAmount,
                    Реализация.Товары.Всего КАК TotalAmount,
                    Реализация.Валюта.Код КАК CurrencyCode
                ИЗ
                    Документ.РеализацияТоваровУслуг КАК Реализация
                ГДЕ
                    Реализация.Проведен";

            return ExecuteQuery(query, row =>
            {
                try
                {
                    string docId = row.SalesDocumentID_1C?.ToString() ?? string.Empty;
                    string customerRef = row.CustomerRef1C?.ToString() ?? string.Empty;
                    string productRef = row.ProductRef1C?.ToString() ?? string.Empty;
                    string orgRef = row.OrganizationRef1C?.ToString() ?? string.Empty;

                    if (string.IsNullOrEmpty(docId) || docId.Length != 36)
                    {
                        _log.LogWarning("Invalid SalesDocumentID_1C: {DocIdValue}", docId);
                        return null;
                    }

                    if (string.IsNullOrEmpty(customerRef) || customerRef.Length != 36)
                    {
                        _log.LogWarning("Invalid CustomerRef1C in Sale Rows: {CustomerRefValue}", customerRef);
                        return null;
                    }

                    if (string.IsNullOrEmpty(productRef) || productRef.Length != 36)
                    {
                        _log.LogWarning("Invalid ProductRef1C in Sale Rows: {ProductRefValue}", productRef);
                        return null;
                    }

                    if (string.IsNullOrEmpty(orgRef) || orgRef.Length != 36)
                    {
                        _log.LogWarning("Invalid OrganizationRef1C in Sale Rows: {OrgRefValue}", orgRef);
                        return null;
                    }

                    Guid? contractRefGuid = null;
                    string contractRefStr = row.ContractRef1C?.ToString() ?? string.Empty;
                    if (!string.IsNullOrEmpty(contractRefStr) && contractRefStr.Length == 36)
                    {
                        if (Guid.TryParse(contractRefStr, out Guid parsedGuid) && parsedGuid != Guid.Empty)
                        {
                            contractRefGuid = parsedGuid;
                        }
                        else
                        {
                            _log.LogWarning("Invalid ContractRef1C in Sale Rows: {ContractRefValue}", contractRefStr);
                        }
                    }

                    return new SaleFactData
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
                    };
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Error mapping sale row");
                    return null;
                }
            });
        }

        public IEnumerable<PaymentRowData> GetPaymentRows()
        {
            _log.LogInformation("Reading payment rows...");
            const string query = @"
                ВЫБРАТЬ
                    Пл.Ссылка.УникальныйИдентификатор() КАК PaymentDocID_1C,
                    Пл.Номер КАК PaymentNumber_1C,
                    Пл.Дата КАК PaymentDate,
                    Пл.Организация.УникальныйИдентификатор() КАК OrganizationRef1C,
                    Пл.Сумма КАК Amount,
                    Пл.Валюта.Код КАК CurrencyCode,
                    Пл.Контрагент.УникальныйИдентификатор() КАК PayerRef1C,
                    Пл.ДоговорКонтрагента.УникальныйИдентификатор() КАК ContractRef1C_Str
                ИЗ
                    Документ.ПоступлениеДенежныхСредств КАК Пл
                ГДЕ
                    Пл.ВидОперации В (
                        ЗНАЧЕНИЕ(Перечисление.ВидыОперацийПоступлениеДенежныхСредств.ОплатаПокупателя),
                        ЗНАЧЕНИЕ(Перечисление.ВидыОперацийПоступлениеДенежныхСредств.ПоступлениеОтПродажПоПлатежнымКартамИБанковскимКредитам)
                    )";

            return ExecuteQuery(query, row =>
            {
                try
                {
                    string paymentDocId = row.PaymentDocID_1C?.ToString() ?? string.Empty;
                    string orgRef = row.OrganizationRef1C?.ToString() ?? string.Empty;
                    string payerRef = row.PayerRef1C?.ToString() ?? string.Empty;

                    if (string.IsNullOrEmpty(paymentDocId) || paymentDocId.Length != 36)
                    {
                        _log.LogWarning("Invalid PaymentDocID_1C: {PaymentDocIdValue}", paymentDocId);
                        return null;
                    }

                    if (string.IsNullOrEmpty(orgRef) || orgRef.Length != 36)
                    {
                        _log.LogWarning("Invalid OrganizationRef1C in Payment Rows: {OrgRefValue}", orgRef);
                        return null;
                    }

                    if (string.IsNullOrEmpty(payerRef) || payerRef.Length != 36)
                    {
                        _log.LogWarning("Invalid PayerRef1C in Payment Rows: {PayerRefValue}", payerRef);
                        return null;
                    }

                    Guid? contractRefGuid = null;
                    string contractRefStr = row.ContractRef1C_Str?.ToString() ?? string.Empty;
                    if (!string.IsNullOrEmpty(contractRefStr) && contractRefStr.Length == 36)
                    {
                        if (Guid.TryParse(contractRefStr, out Guid parsedGuid) && parsedGuid != Guid.Empty)
                        {
                            contractRefGuid = parsedGuid;
                        }
                        else
                        {
                            _log.LogWarning("Invalid ContractRef1C_Str in Payment Rows: {ContractRefValue}", contractRefStr);
                        }
                    }

                    return new PaymentRowData
                    {
                        PaymentDocID_1C = new Guid(paymentDocId),
                        PaymentNumber_1C = row.PaymentNumber_1C?.ToString() ?? string.Empty,
                        PaymentDate = row.PaymentDate is DateTime dt ? dt : DateTime.MinValue,
                        OrganizationRef_1C = new Guid(orgRef),
                        Amount = Convert.ToDecimal(row.Amount ?? 0),
                        CurrencyCode = row.CurrencyCode?.ToString(),
                        PayerRef_1C = new Guid(payerRef),
                        ContractRef_1C = contractRefGuid
                    };
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Error mapping payment row");
                    return null;
                }
            });
        }
    }
}