using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Versioning;
using System.Threading.Tasks;

namespace OneCSqlEtl
{
    [SupportedOSPlatform("windows")]
    /// <summary>
    /// Оркестратор ETL: чтение из 1С, запись в SQL.
    /// </summary>
    public class EtlOrchestrator
    {
        private readonly OneCAccessor _oneC;
        private readonly SqlRepository _repo;
        private readonly ILogger<EtlOrchestrator> _log;
        private readonly int _batchSize;

        public EtlOrchestrator(
            OneCAccessor oneCAccessor,
            SqlRepository sqlRepository,
            IOptions<Settings> opts,
            ILogger<EtlOrchestrator> log)
        {
            _oneC = oneCAccessor ?? throw new ArgumentNullException(nameof(oneCAccessor));
            _repo = sqlRepository ?? throw new ArgumentNullException(nameof(sqlRepository));
            _log = log ?? throw new ArgumentNullException(nameof(log));

            if (opts?.Value?.EtlSettings == null) // Проверка и EtlSettings
                throw new ArgumentNullException(nameof(opts), "Settings or EtlSettings is null.");

            _batchSize = opts.Value.EtlSettings.BatchSize;
            if (_batchSize <= 0)
            {
                _log.LogWarning("Некорректный размер батча: {BatchSizeValue}. Установлено значение по умолчанию: 500", _batchSize); // Исправлен placeholder
                _batchSize = 500;
            }
        }

        /// <summary>
        /// Запустить процесс ETL.
        /// </summary>
        public async Task RunAsync()
        {
            _log.LogInformation("ETL process starting...");

            // 1. Подключение к 1С
            if (!_oneC.Connect()) // Явный вызов Connect()
            {
                _log.LogError("Failed to connect to 1C. ETL process aborted.");
                // Ресурсы в _oneC будут освобождены его внутренним механизмом или при Dispose,
                // если Connect() вызвал исключение и был перехвачен в конструкторе OneCAccessor,
                // либо ReleaseComObjects был вызван внутри Connect() при неудаче.
                // Если Connect() просто вернул false без исключения, то Dispose ниже его почистит.
                try
                {
                    _oneC.Dispose(); // Попытка освободить ресурсы, если подключение не удалось
                }
                catch (Exception disposeEx)
                {
                    _log.LogWarning(disposeEx, "Exception during OneCAccessor.Dispose after failed connection.");
                }
                return;
            }

            try
            {
                _log.LogInformation("--- Loading Dimensions ---");

                // Organizations
                _log.LogInformation("Attempting to load Organizations...");
                var orgs1C = _oneC.GetOrganizations().ToList(); // Используем ToList() для материализации
                if (!orgs1C.Any()) // Используем Any() для проверки на пустоту
                {
                    _log.LogWarning("Не найдено ни одной организации в 1С.");
                }

                var orgMap = new Dictionary<Guid, int>(orgs1C.Count);
                foreach (var o in orgs1C)
                {
                    try
                    {
                        int sk = await _repo.GetOrCreateOrganizationSKAsync(o);
                        orgMap[o.Ref] = sk;
                        _log.LogDebug("Org: {Name} (Ref: {OrgRef}) => SK={SK}", o.Name, o.Ref, sk);
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Ошибка при обработке организации {Name} (Ref: {OrgRef})", o.Name, o.Ref);
                    }
                }
                _log.LogInformation("Loaded {Count} organizations into map.", orgMap.Count);

                // Customers
                _log.LogInformation("Attempting to load Customers...");
                var custs1C = _oneC.GetCustomers().ToList();
                if (!custs1C.Any())
                {
                    _log.LogWarning("Не найдено ни одного контрагента в 1С.");
                }

                var custMap = new Dictionary<Guid, int>(custs1C.Count);
                foreach (var c in custs1C)
                {
                    try
                    {
                        int sk = await _repo.GetOrCreateCustomerSKAsync(c);
                        custMap[c.Ref] = sk;
                        _log.LogDebug("Customer: {Name} (Ref: {CustomerRef}) => SK={SK}", c.Name, c.Ref, sk);
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Ошибка при обработке контрагента {Name} (Ref: {CustomerRef})", c.Name, c.Ref);
                    }
                }
                _log.LogInformation("Loaded {Count} customers into map.", custMap.Count);

                // Products
                _log.LogInformation("Attempting to load Products...");
                var prods1C = _oneC.GetProducts().ToList();
                if (!prods1C.Any())
                {
                    _log.LogWarning("Не найдено ни одного продукта в 1С.");
                }

                var prodMap = new Dictionary<Guid, int>(prods1C.Count);
                foreach (var p in prods1C)
                {
                    try
                    {
                        int sk = await _repo.GetOrCreateProductSKAsync(p);
                        prodMap[p.Ref] = sk;
                        _log.LogDebug("Product: {Name} (Ref: {ProductRef}) => SK={SK}", p.Name, p.Ref, sk);
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Ошибка при обработке продукта {Name} (Ref: {ProductRef})", p.Name, p.Ref);
                    }
                }
                _log.LogInformation("Loaded {Count} products into map.", prodMap.Count);

                // Contracts
                _log.LogInformation("Attempting to load Contracts...");
                var contracts1C = _oneC.GetContracts().ToList();
                if (!contracts1C.Any())
                {
                    _log.LogWarning("Не найдено ни одного договора в 1С.");
                }

                var contractMap = new Dictionary<Guid, int>();
                foreach (var ct in contracts1C)
                {
                    try
                    {
                        if (custMap.TryGetValue(ct.CustomerRef_1C, out var csk))
                        {
                            ct.CustomerSK = csk;
                            int sk = await _repo.GetOrCreateContractSKAsync(ct);
                            contractMap[ct.Ref] = sk;
                            _log.LogDebug("Contract: {Name} (Ref: {ContractRef}) for CustomerSK={CustomerSK} => SK={SK}", ct.Name, ct.Ref, csk, sk);
                        }
                        else
                        {
                            _log.LogWarning("Skipping contract {ContractName} (Ref: {ContractRef}): Customer with Ref {CustomerRefValue} not found.", ct.Name, ct.Ref, ct.CustomerRef_1C);
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Ошибка при обработке договора {Name} (Ref: {ContractRef})", ct.Name, ct.Ref);
                    }
                }
                _log.LogInformation("Loaded {Count} contracts into map.", contractMap.Count);

                _log.LogInformation("--- Loading Facts: Sales ---");
                _log.LogInformation("Attempting to load Sales data...");
                var salesRaw = _oneC.GetSaleRows().ToList();
                if (!salesRaw.Any())
                {
                    _log.LogWarning("Не найдено ни одной строки продаж в 1С.");
                }

                var salesBatch = new List<SaleFactData>(_batchSize);
                int totalSales = 0;
                int processedSales = 0;
                int skippedSales = 0;

                foreach (var rawSaleRow in salesRaw)
                {
                    processedSales++;
                    try
                    {
                        rawSaleRow.SaleDateKey = await _repo.GetOrCreateDateKeyAsync(rawSaleRow.DocDate);

                        if (!custMap.TryGetValue(rawSaleRow.CustomerRef_1C, out int customerSK))
                        {
                            _log.LogWarning("Sales: Customer with Ref {CustomerRefValue} not found for SaleDocID {DocId}. Skipping row.", rawSaleRow.CustomerRef_1C, rawSaleRow.SalesDocumentID_1C);
                            skippedSales++;
                            continue;
                        }
                        rawSaleRow.CustomerSK = customerSK;

                        if (!prodMap.TryGetValue(rawSaleRow.ProductRef_1C, out int productSK))
                        {
                            _log.LogWarning("Sales: Product with Ref {ProductRefValue} not found for SaleDocID {DocId}. Skipping row.", rawSaleRow.ProductRef_1C, rawSaleRow.SalesDocumentID_1C);
                            skippedSales++;
                            continue;
                        }
                        rawSaleRow.ProductSK = productSK;

                        if (!orgMap.TryGetValue(rawSaleRow.OrganizationRef_1C, out int organizationSK))
                        {
                            _log.LogWarning("Sales: Organization with Ref {OrgRefValue} not found for SaleDocID {DocId}. Skipping row.", rawSaleRow.OrganizationRef_1C, rawSaleRow.SalesDocumentID_1C);
                            skippedSales++;
                            continue;
                        }
                        rawSaleRow.OrganizationSK = organizationSK;

                        if (rawSaleRow.ContractRef_1C.HasValue)
                        {
                            if (contractMap.TryGetValue(rawSaleRow.ContractRef_1C.Value, out int contractSK))
                            {
                                rawSaleRow.ContractSK = contractSK;
                            }
                            else
                            {
                                _log.LogWarning("Sales: Contract with Ref {ContractRefValue} not found for SaleDocID {DocId}. ContractSK will be null.", rawSaleRow.ContractRef_1C.Value, rawSaleRow.SalesDocumentID_1C);
                                rawSaleRow.ContractSK = null;
                            }
                        }
                        else
                        {
                            rawSaleRow.ContractSK = null;
                        }

                        salesBatch.Add(rawSaleRow);
                        totalSales++;

                        if (salesBatch.Count >= _batchSize)
                        {
                            await _repo.InsertFactSalesAsync(salesBatch);
                            _log.LogInformation("Inserted {Count} sales facts (processed {Processed}, total inserted this run {Total}).", salesBatch.Count, processedSales, totalSales);
                            salesBatch.Clear();
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Ошибка при обработке строки продажи для документа {DocId}", rawSaleRow.SalesDocumentID_1C);
                        skippedSales++;
                    }
                }

                if (salesBatch.Count > 0)
                {
                    await _repo.InsertFactSalesAsync(salesBatch);
                    _log.LogInformation("Inserted final {Count} sales facts (processed {Processed}, total inserted this run {Total}).", salesBatch.Count, processedSales, totalSales);
                }
                _log.LogInformation("Sales facts processing finished. Total rows processed: {Processed}, Successfully inserted: {Total}, Skipped: {Skipped}", processedSales, totalSales, skippedSales);


                _log.LogInformation("--- Loading Facts: Payments ---");
                _log.LogInformation("Attempting to load Payments data...");
                var paymentsRaw = _oneC.GetPaymentRows().ToList();
                if (!paymentsRaw.Any())
                {
                    _log.LogWarning("Не найдено ни одной строки платежей в 1С.");
                }

                var payBatch = new List<PaymentRowData>(_batchSize);
                int totalPays = 0;
                int processedPays = 0;
                int skippedPays = 0;

                foreach (var rawPaymentRow in paymentsRaw)
                {
                    processedPays++;
                    try
                    {
                        rawPaymentRow.PaymentDateKey = await _repo.GetOrCreateDateKeyAsync(rawPaymentRow.PaymentDate);

                        if (!custMap.TryGetValue(rawPaymentRow.PayerRef_1C, out int payerSK))
                        {
                            _log.LogWarning("Payments: Payer customer with Ref {PayerRefValue} not found for PaymentDocID {PaymentDocID}. Skipping payment.", rawPaymentRow.PayerRef_1C, rawPaymentRow.PaymentDocID_1C);
                            skippedPays++;
                            continue;
                        }
                        rawPaymentRow.CustomerSK = payerSK;

                        if (!orgMap.TryGetValue(rawPaymentRow.OrganizationRef_1C, out int paymentOrgSK))
                        {
                            _log.LogWarning("Payments: Organization with Ref {OrgRefValue} not found for PaymentDocID {PaymentDocID}. Skipping payment.", rawPaymentRow.OrganizationRef_1C, rawPaymentRow.PaymentDocID_1C);
                            skippedPays++;
                            continue;
                        }
                        rawPaymentRow.OrganizationSK = paymentOrgSK;

                        if (rawPaymentRow.ContractRef_1C.HasValue)
                        {
                            if (contractMap.TryGetValue(rawPaymentRow.ContractRef_1C.Value, out int paymentContractSK))
                            {
                                rawPaymentRow.ContractSK = paymentContractSK;
                            }
                            else
                            {
                                _log.LogWarning("Payments: Contract with Ref {ContractRefValue} not found for PaymentDocID {PaymentDocID}. ContractSK will be null.", rawPaymentRow.ContractRef_1C.Value, rawPaymentRow.PaymentDocID_1C);
                                rawPaymentRow.ContractSK = null;
                            }
                        }
                        else
                        {
                            rawPaymentRow.ContractSK = null;
                        }

                        payBatch.Add(rawPaymentRow);
                        totalPays++;

                        if (payBatch.Count >= _batchSize)
                        {
                            await _repo.InsertFactPaymentsAsync(payBatch);
                            _log.LogInformation("Inserted {Count} payment facts (processed {Processed}, total inserted this run {Total}).", payBatch.Count, processedPays, totalPays);
                            payBatch.Clear();
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Ошибка при обработке строки платежа для документа {DocId}", rawPaymentRow.PaymentDocID_1C);
                        skippedPays++;
                    }
                }

                if (payBatch.Count > 0)
                {
                    await _repo.InsertFactPaymentsAsync(payBatch);
                    _log.LogInformation("Inserted final {Count} payment facts (processed {Processed}, total inserted this run {Total}).", payBatch.Count, processedPays, totalPays);
                }
                _log.LogInformation("Payment facts processing finished. Total rows processed: {Processed}, Successfully inserted: {Total}, Skipped: {Skipped}", processedPays, totalPays, skippedPays);

                _log.LogInformation("ETL process completed successfully. 🎉");
            }
            catch (Exception ex) // Общий try-catch для всего процесса после успешного Connect()
            {
                _log.LogCritical(ex, "ETL process failed with an unhandled exception after successful 1C connection.");
                // Не перебрасываем исключение, чтобы finally выполнился корректно и залогировал завершение.
                // Ошибка уже залогирована как Critical.
            }
            finally
            {
                _log.LogInformation("ETL process attempting to dispose resources...");
                try
                {
                    _oneC.Dispose(); // Гарантированный вызов Dispose для OneCAccessor
                    _log.LogInformation("OneCAccessor disposed successfully.");
                }
                catch (Exception disposeEx)
                {
                    _log.LogError(disposeEx, "Error during OneCAccessor.Dispose in finally block.");
                }
                _log.LogInformation("ETL run finished (completed or failed).");
            }
        }
    }
}