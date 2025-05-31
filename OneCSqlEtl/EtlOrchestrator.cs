// EtlOrchestrator.cs
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Versioning;
using System.Threading.Tasks;

// DTOs (Customer1C, Product1C, etc.) are defined in Models.cs
// The main Settings class (with ConnectionStrings and EtlSettings properties)
// is defined in Settings.cs. The EtlSettings property on the main Settings class
// already contains BatchSize.

namespace OneCSqlEtl
{
    [SupportedOSPlatform("windows")]
    public class EtlOrchestrator
    {
        private readonly OneCAccessor _oneC;
        private readonly SqlRepository _repo;
        private readonly ILogger<EtlOrchestrator> _log;
        private readonly int _batchSize;

        // The 'Settings' type here will correctly resolve to the one defined in Settings.cs
        public EtlOrchestrator(
            OneCAccessor oneCAccessor,
            SqlRepository sqlRepository,
            IOptions<Settings> opts,
            ILogger<EtlOrchestrator> log)
        {
            _oneC = oneCAccessor ?? throw new ArgumentNullException(nameof(oneCAccessor));
            _repo = sqlRepository ?? throw new ArgumentNullException(nameof(sqlRepository));
            _log = log ?? throw new ArgumentNullException(nameof(log));

            var settingsValue = opts?.Value ?? throw new ArgumentNullException(nameof(opts), "Settings (IOptions<Settings>.Value) is null.");
            var etlSettings = settingsValue.EtlSettings ?? throw new ArgumentNullException(nameof(settingsValue.EtlSettings), "Settings.EtlSettings property is null.");

            _batchSize = etlSettings.BatchSize;
            if (_batchSize <= 0)
            {
                _log.LogWarning("Invalid BatchSize in settings: {ConfiguredBatchSize}. Using default 500.", _batchSize);
                _batchSize = 500;
            }
        }

        public async Task RunAsync()
        {
            _log.LogInformation("ETL process starting...");

            if (!_oneC.Connect())
            {
                _log.LogError("Failed to connect to 1C. ETL process aborted.");
                try
                {
                    _oneC.Dispose();
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
                var orgs1C = _oneC.GetOrganizations().ToList();
                if (!orgs1C.Any())
                {
                    _log.LogWarning("No organizations found in 1C.");
                }
                var orgMap = new Dictionary<Guid, int>(orgs1C.Count);
                foreach (var o in orgs1C)
                {
                    try
                    {
                        // The 'o' (Organization1C) object here is from Models.cs
                        int sk = await _repo.GetOrCreateOrganizationSKAsync(o);
                        orgMap[o.Ref] = sk;
                        _log.LogDebug("Org: {Name} (Ref: {OrgRef}) => SK={SK}", o.Name, o.Ref, sk);
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Error processing organization {Name} (Ref: {OrgRef})", o.Name, o.Ref);
                    }
                }
                _log.LogInformation("Loaded {Count} organizations into map.", orgMap.Count);

                // Customers
                _log.LogInformation("Attempting to load Customers...");
                var custs1C = _oneC.GetCustomers().ToList();
                if (!custs1C.Any())
                {
                    _log.LogWarning("No customers found in 1C.");
                }
                var custMap = new Dictionary<Guid, int>(custs1C.Count);
                foreach (var c in custs1C)
                {
                    try
                    {
                        // The 'c' (Customer1C) object here is from Models.cs
                        int sk = await _repo.GetOrCreateCustomerSKAsync(c);
                        custMap[c.Ref] = sk;
                        _log.LogDebug("Customer: {Name} (Ref: {CustomerRef}) => SK={SK}", c.Name, c.Ref, sk);
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Error processing customer {Name} (Ref: {CustomerRef})", c.Name, c.Ref);
                    }
                }
                _log.LogInformation("Loaded {Count} customers into map.", custMap.Count);

                // Products
                _log.LogInformation("Attempting to load Products...");
                var prods1C = _oneC.GetProducts().ToList();
                if (!prods1C.Any())
                {
                    _log.LogWarning("No products found in 1C.");
                }
                var prodMap = new Dictionary<Guid, int>(prods1C.Count);
                foreach (var p in prods1C)
                {
                    try
                    {
                        // The 'p' (Product1C) object here is from Models.cs
                        int sk = await _repo.GetOrCreateProductSKAsync(p);
                        prodMap[p.Ref] = sk;
                        _log.LogDebug("Product: {Name} (Ref: {ProductRef}) => SK={SK}", p.Name, p.Ref, sk);
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Error processing product {Name} (Ref: {ProductRef})", p.Name, p.Ref);
                    }
                }
                _log.LogInformation("Loaded {Count} products into map.", prodMap.Count);

                // Contracts
                _log.LogInformation("Attempting to load Contracts...");
                var contracts1C = _oneC.GetContracts().ToList();
                if (!contracts1C.Any())
                {
                    _log.LogWarning("No contracts found in 1C.");
                }
                var contractMap = new Dictionary<Guid, int>(contracts1C.Count);
                foreach (var ct in contracts1C)
                {
                    try
                    {
                        // The 'ct' (Contract1C) object here is from Models.cs
                        if (custMap.TryGetValue(ct.CustomerRef_1C, out var csk))
                        {
                            ct.CustomerSK = csk;
                            int sk = await _repo.GetOrCreateContractSKAsync(ct);
                            contractMap[ct.Ref] = sk;
                            _log.LogDebug("Contract: {Name} (Ref: {ContractRef}) for CustomerSK={CustomerSK} => SK={SK}", ct.Name, ct.Ref, csk, sk);
                        }
                        else
                        {
                            _log.LogWarning("Skipping contract {ContractName} (Ref: {ContractRef}): Customer with Ref {CustomerRefValue} not found in map.", ct.Name, ct.Ref, ct.CustomerRef_1C);
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Error processing contract {Name} (Ref: {ContractRef})", ct.Name, ct.Ref);
                    }
                }
                _log.LogInformation("Loaded {Count} contracts into map.", contractMap.Count);

                // --- Loading Facts: Sales ---
                _log.LogInformation("--- Loading Facts: Sales ---");
                _log.LogInformation("Attempting to load Sales data from 1C...");
                var salesRaw = _oneC.GetSaleRows().ToList();
                if (!salesRaw.Any())
                {
                    _log.LogWarning("No sales rows found in 1C for processing.");
                }

                var salesBatch = new List<SaleFactData>(_batchSize);
                int totalSalesInserted = 0;
                int processedSalesRows = 0;
                int skippedSalesRows = 0;

                foreach (var rawSaleRow in salesRaw) // rawSaleRow is SaleFactData from Models.cs
                {
                    processedSalesRows++;
                    try
                    {
                        rawSaleRow.SaleDateKey = await _repo.GetOrCreateDateKeyAsync(rawSaleRow.DocDate);

                        if (!custMap.TryGetValue(rawSaleRow.CustomerRef_1C, out int customerSK))
                        {
                            _log.LogWarning("Sales: Customer with Ref {CustomerRefValue} not found for SaleDocID {DocId}. Skipping row.", rawSaleRow.CustomerRef_1C, rawSaleRow.SalesDocumentID_1C);
                            skippedSalesRows++;
                            continue;
                        }
                        rawSaleRow.CustomerSK = customerSK;

                        if (!prodMap.TryGetValue(rawSaleRow.ProductRef_1C, out int productSK))
                        {
                            _log.LogWarning("Sales: Product with Ref {ProductRefValue} not found for SaleDocID {DocId}. Skipping row.", rawSaleRow.ProductRef_1C, rawSaleRow.SalesDocumentID_1C);
                            skippedSalesRows++;
                            continue;
                        }
                        rawSaleRow.ProductSK = productSK;

                        if (!orgMap.TryGetValue(rawSaleRow.OrganizationRef_1C, out int organizationSK))
                        {
                            _log.LogWarning("Sales: Organization with Ref {OrgRefValue} not found for SaleDocID {DocId}. Skipping row.", rawSaleRow.OrganizationRef_1C, rawSaleRow.SalesDocumentID_1C);
                            skippedSalesRows++;
                            continue;
                        }
                        rawSaleRow.OrganizationSK = organizationSK;

                        rawSaleRow.ContractSK = null;
                        if (rawSaleRow.ContractRef_1C.HasValue)
                        {
                            if (contractMap.TryGetValue(rawSaleRow.ContractRef_1C.Value, out int contractSK))
                            {
                                rawSaleRow.ContractSK = contractSK;
                            }
                            else
                            {
                                _log.LogWarning("Sales: Contract with Ref {ContractRefValue} not found for SaleDocID {DocId}. ContractSK will remain null.", rawSaleRow.ContractRef_1C.Value, rawSaleRow.SalesDocumentID_1C);
                            }
                        }

                        salesBatch.Add(rawSaleRow);

                        if (salesBatch.Count >= _batchSize)
                        {
                            await _repo.InsertFactSalesAsync(salesBatch);
                            int currentBatchCount = salesBatch.Count;
                            totalSalesInserted += currentBatchCount;
                            _log.LogInformation("Inserted batch of {Count} sales facts. Total processed so far: {ProcessedRows}, Total inserted this run: {TotalInserted}.", currentBatchCount, processedSalesRows, totalSalesInserted);
                            salesBatch.Clear();
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Error processing sales row (DocID: {DocId}, LineNo: {LineNum})", rawSaleRow.SalesDocumentID_1C, rawSaleRow.SalesDocumentLineNo_1C);
                        skippedSalesRows++;
                    }
                }

                if (salesBatch.Count > 0)
                {
                    await _repo.InsertFactSalesAsync(salesBatch);
                    int finalBatchCount = salesBatch.Count;
                    totalSalesInserted += finalBatchCount;
                    _log.LogInformation("Inserted final batch of {Count} sales facts. Total processed: {ProcessedRows}, Total inserted this run: {TotalInserted}.", finalBatchCount, processedSalesRows, totalSalesInserted);
                }
                _log.LogInformation("Sales facts processing finished. Total rows from 1C: {TotalRead}, Successfully prepared & inserted: {TotalInserted}, Skipped due to errors/missing refs: {SkippedRows}", salesRaw.Count, totalSalesInserted, skippedSalesRows);


                // --- Loading Facts: Payments ---
                _log.LogInformation("--- Loading Facts: Payments ---");
                _log.LogInformation("Attempting to load Payments data from 1C...");
                var paymentsRaw = _oneC.GetPaymentRows().ToList(); // paymentsRaw is List<PaymentRowData>
                if (!paymentsRaw.Any())
                {
                    _log.LogWarning("No payment rows found in 1C for processing.");
                }

                var payBatch = new List<PaymentRowData>(_batchSize);
                int totalPaymentsInserted = 0;
                int processedPaymentRows = 0;
                int skippedPaymentRows = 0;

                foreach (var rawPaymentRow in paymentsRaw) // rawPaymentRow is PaymentRowData from Models.cs
                {
                    processedPaymentRows++;
                    try
                    {
                        rawPaymentRow.PaymentDateKey = await _repo.GetOrCreateDateKeyAsync(rawPaymentRow.PaymentDate);

                        if (!custMap.TryGetValue(rawPaymentRow.PayerRef_1C, out int payerSK))
                        {
                            _log.LogWarning("Payments: Payer (Customer) with Ref {PayerRefValue} not found for PaymentDocID {PaymentDocID}. Skipping payment.", rawPaymentRow.PayerRef_1C, rawPaymentRow.PaymentDocID_1C);
                            skippedPaymentRows++;
                            continue;
                        }
                        rawPaymentRow.CustomerSK = payerSK;

                        if (!orgMap.TryGetValue(rawPaymentRow.OrganizationRef_1C, out int paymentOrgSK))
                        {
                            _log.LogWarning("Payments: Organization with Ref {OrgRefValue} not found for PaymentDocID {PaymentDocID}. Skipping payment.", rawPaymentRow.OrganizationRef_1C, rawPaymentRow.PaymentDocID_1C);
                            skippedPaymentRows++;
                            continue;
                        }
                        rawPaymentRow.OrganizationSK = paymentOrgSK;

                        rawPaymentRow.ContractSK = null;
                        if (rawPaymentRow.ContractRef_1C.HasValue)
                        {
                            if (contractMap.TryGetValue(rawPaymentRow.ContractRef_1C.Value, out int paymentContractSK))
                            {
                                rawPaymentRow.ContractSK = paymentContractSK;
                            }
                            else
                            {
                                _log.LogWarning("Payments: Contract with Ref {ContractRefValue} not found for PaymentDocID {PaymentDocID}. ContractSK will remain null.", rawPaymentRow.ContractRef_1C.Value, rawPaymentRow.PaymentDocID_1C);
                            }
                        }

                        payBatch.Add(rawPaymentRow);

                        if (payBatch.Count >= _batchSize)
                        {
                            await _repo.InsertFactPaymentsAsync(payBatch);
                            int currentBatchCount = payBatch.Count;
                            totalPaymentsInserted += currentBatchCount;
                            _log.LogInformation("Inserted batch of {Count} payment facts. Total processed so far: {ProcessedRows}, Total inserted this run: {TotalInserted}.", currentBatchCount, processedPaymentRows, totalPaymentsInserted);
                            payBatch.Clear();
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Error processing payment row (DocID: {PaymentDocID})", rawPaymentRow.PaymentDocID_1C);
                        skippedPaymentRows++;
                    }
                }

                if (payBatch.Count > 0)
                {
                    await _repo.InsertFactPaymentsAsync(payBatch);
                    int finalBatchCount = payBatch.Count;
                    totalPaymentsInserted += finalBatchCount;
                    _log.LogInformation("Inserted final batch of {Count} payment facts. Total processed: {ProcessedRows}, Total inserted this run: {TotalInserted}.", finalBatchCount, processedPaymentRows, totalPaymentsInserted);
                }
                _log.LogInformation("Payment facts processing finished. Total rows from 1C: {TotalRead}, Successfully prepared & inserted: {TotalInserted}, Skipped due to errors/missing refs: {SkippedRows}", paymentsRaw.Count, totalPaymentsInserted, skippedPaymentRows);

                _log.LogInformation("ETL process completed successfully. 🎉");
            }
            catch (Exception ex)
            {
                _log.LogCritical(ex, "ETL process failed with an unhandled exception after successful 1C connection.");
            }
            finally
            {
                _log.LogInformation("ETL process attempting to dispose OneCAccessor...");
                try
                {
                    _oneC.Dispose();
                    _log.LogInformation("OneCAccessor disposed successfully.");
                }
                catch (Exception disposeEx)
                {
                    _log.LogError(disposeEx, "Error during OneCAccessor.Dispose in finally block.");
                }
                _log.LogInformation("ETL run finished.");
            }
        }
    }
}