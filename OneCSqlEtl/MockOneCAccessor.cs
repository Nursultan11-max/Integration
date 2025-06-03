// MockOneCAccessor.cs
#nullable enable
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace OneCSqlEtl
{
    public class MockOneCAccessor : IOneCAccessor
    {
        private readonly ILogger<MockOneCAccessor> _log;
        private readonly List<Customer1C> _customers;
        private readonly List<Product1C> _products;
        private readonly List<Organization1C> _organizations;
        private readonly List<Contract1C> _contracts;

        public MockOneCAccessor(ILogger<MockOneCAccessor> log)
        {
            _log = log ?? throw new ArgumentNullException(nameof(log));
            _log.LogInformation("MockOneCAccessor created. Test data will be generated.");

            // Генерируем основные справочные данные один раз
            _customers = GenerateMockCustomers();
            _products = GenerateMockProducts();
            _organizations = GenerateMockOrganizations();
            _contracts = GenerateMockContracts(_customers); // Договоры зависят от клиентов
        }

        public bool Connect()
        {
            _log.LogInformation("MockOneCAccessor.Connect() called. Simulating successful connection.");
            return true;
        }

        private List<Customer1C> GenerateMockCustomers()
        {
            var customers = new List<Customer1C>
            {
                new Customer1C { Ref = Guid.NewGuid(), Code = "К001", Name = "ООО \"Ромашка\"", FullName = "Общество с ограниченной ответственностью \"Ромашка\"", TIN = "1234567890", KPP = "1234501001", EntityType = "Юридическое лицо", IsDeleted = false },
                new Customer1C { Ref = Guid.NewGuid(), Code = "К002", Name = "ИП Васильев П.С.", FullName = "Индивидуальный предприниматель Васильев Петр Сергеевич", TIN = "098765432112", KPP = null, EntityType = "Физическое лицо", IsDeleted = false },
                new Customer1C { Ref = Guid.NewGuid(), Code = "К003", Name = "АО \"Одуванчик\"", FullName = "Акционерное общество \"Одуванчик\"", TIN = "1122334455", KPP = "1122301001", EntityType = "Юридическое лицо", IsDeleted = true }, // Помечен на удаление
                new Customer1C { Ref = Guid.NewGuid(), Code = "К004", Name = "ООО \"Лютик\"", FullName = "Общество с ограниченной ответственностью \"Лютик\"", TIN = "2233445566", KPP = "2233401001", EntityType = "Юридическое лицо", IsDeleted = false }
            };
            _log.LogDebug("Generated {Count} mock customers.", customers.Count);
            return customers;
        }

        public IEnumerable<Customer1C> GetCustomers()
        {
            _log.LogInformation("MockOneCAccessor.GetCustomers() called. Returning {Count} mock customers.", _customers.Count);
            return _customers;
        }

        private List<Product1C> GenerateMockProducts()
        {
            var products = new List<Product1C>
            {
                new Product1C { Ref = Guid.NewGuid(), Code = "Т001", Name = "Ноутбук Модель X", FullName = "Ноутбук игровой Модель X 15.6 дюймов", SKU = "NB-X-15", UnitOfMeasure = "шт", ProductType = "Товар", ProductGroup = "Компьютеры", DefaultVATRateName = "20%" },
                new Product1C { Ref = Guid.NewGuid(), Code = "У001", Name = "Консультация по ПО", FullName = "Консультационные услуги по настройке программного обеспечения", SKU = "SERV-CONS-PO", UnitOfMeasure = "час", ProductType = "Услуга", ProductGroup = "Услуги IT", DefaultVATRateName = "Без НДС" },
                new Product1C { Ref = Guid.NewGuid(), Code = "Т002", Name = "Стол офисный \"Комфорт\"", FullName = "Стол офисный деревянный \"Комфорт\" 120x60см", SKU = "DESK-WOOD-120", UnitOfMeasure = "шт", ProductType = "Товар", ProductGroup = "Мебель", DefaultVATRateName = "20%" },
                new Product1C { Ref = Guid.NewGuid(), Code = "Т003", Name = "Бумага А4 \"Снежинка\"", FullName = "Бумага офисная А4 \"Снежинка\", 500л", SKU = "PAPER-A4-500", UnitOfMeasure = "упак", ProductType = "Товар", ProductGroup = "Канцтовары", DefaultVATRateName = "20%" }
            };
            _log.LogDebug("Generated {Count} mock products.", products.Count);
            return products;
        }

        public IEnumerable<Product1C> GetProducts()
        {
            _log.LogInformation("MockOneCAccessor.GetProducts() called. Returning {Count} mock products.", _products.Count);
            return _products;
        }

        private List<Organization1C> GenerateMockOrganizations()
        {
            var organizations = new List<Organization1C>
            {
                new Organization1C { Ref = Guid.NewGuid(), Code = "ОРГ001", Name = "Наша Компания", OrganizationFullName = "ООО \"Наша Компания\"" },
                new Organization1C { Ref = Guid.NewGuid(), Code = "ОРГ002", Name = "Филиал \"Южный\"", OrganizationFullName = "Филиал \"Южный\" ООО \"Наша Компания\"" }
            };
            _log.LogDebug("Generated {Count} mock organizations.", organizations.Count);
            return organizations;
        }

        public IEnumerable<Organization1C> GetOrganizations()
        {
            _log.LogInformation("MockOneCAccessor.GetOrganizations() called. Returning {Count} mock organizations.", _organizations.Count);
            return _organizations;
        }

        private List<Contract1C> GenerateMockContracts(List<Customer1C> customers)
        {
            var contracts = new List<Contract1C>();
            if (!customers.Any()) return contracts;

            var activeCustomers = customers.Where(c => !c.IsDeleted).ToList();
            if (!activeCustomers.Any()) return contracts;

            // Договор для первого активного клиента
            contracts.Add(new Contract1C { Ref = Guid.NewGuid(), Code = "Д001/23", Name = "Основной договор поставки с ООО \"Ромашка\"", CustomerRef_1C = activeCustomers[0].Ref, StartDate = DateTime.Now.AddYears(-1), EndDate = DateTime.Now.AddYears(1) });

            // Договор для второго активного клиента (если есть)
            if (activeCustomers.Count > 1)
            {
                contracts.Add(new Contract1C { Ref = Guid.NewGuid(), Code = "Д002/23", Name = "Договор на услуги с ИП Васильев", CustomerRef_1C = activeCustomers[1].Ref, StartDate = DateTime.Now.AddMonths(-6), EndDate = null }); // Бессрочный
            }

            // Еще один договор для первого активного клиента
            contracts.Add(new Contract1C { Ref = Guid.NewGuid(), Code = "Д003/24", Name = "Разовый договор на поставку столов", CustomerRef_1C = activeCustomers[0].Ref, StartDate = DateTime.Now.AddDays(-10), EndDate = DateTime.Now.AddDays(20) });

            _log.LogDebug("Generated {Count} mock contracts.", contracts.Count);
            return contracts;
        }

        public IEnumerable<Contract1C> GetContracts()
        {
            _log.LogInformation("MockOneCAccessor.GetContracts() called. Returning {Count} mock contracts.", _contracts.Count);
            return _contracts;
        }

        public IEnumerable<SaleFactData> GetSaleRows()
        {
            _log.LogInformation("MockOneCAccessor.GetSaleRows() called. Generating mock sales data...");
            var sales = new List<SaleFactData>();

            if (!_customers.Any(c => !c.IsDeleted) || !_products.Any() || !_organizations.Any())
            {
                _log.LogWarning("Not enough active dimension data to generate sales facts.");
                return sales;
            }

            var activeCustomers = _customers.Where(c => !c.IsDeleted).ToList();
            var org1 = _organizations[0]; // Предполагаем, что хотя бы одна организация есть

            // Продажа 1 (несколько позиций)
            var saleDoc1Id = Guid.NewGuid();
            var customer1 = activeCustomers[0];
            var contract1ForCust1 = _contracts.FirstOrDefault(c => c.CustomerRef_1C == customer1.Ref);

            sales.Add(new SaleFactData
            {
                SalesDocumentID_1C = saleDoc1Id,
                SalesDocumentNumber_1C = "РН-00001",
                SalesDocumentLineNo_1C = 1,
                DocDate = DateTime.Now.AddDays(-15),
                CustomerRef_1C = customer1.Ref,
                ProductRef_1C = _products[0].Ref,
                OrganizationRef_1C = org1.Ref,
                ContractRef_1C = contract1ForCust1?.Ref,
                Quantity = 2,
                Price = 75000,
                Amount = 150000,
                VATRateName = "20%",
                VATAmount = 30000,
                TotalAmount = 180000,
                CurrencyCode = "RUB"
            });

            if (_products.Count > 2) // Продукт с индексом 2
            {
                sales.Add(new SaleFactData
                {
                    SalesDocumentID_1C = saleDoc1Id,
                    SalesDocumentNumber_1C = "РН-00001",
                    SalesDocumentLineNo_1C = 2,
                    DocDate = DateTime.Now.AddDays(-15),
                    CustomerRef_1C = customer1.Ref,
                    ProductRef_1C = _products[2].Ref,
                    OrganizationRef_1C = org1.Ref,
                    ContractRef_1C = contract1ForCust1?.Ref,
                    Quantity = 5,
                    Price = 3000,
                    Amount = 15000,
                    VATRateName = "20%",
                    VATAmount = 3000,
                    TotalAmount = 18000,
                    CurrencyCode = "RUB"
                });
            }

            // Продажа 2 (другой клиент, услуга без НДС, другой договор или без)
            if (activeCustomers.Count > 1 && _products.Count > 1) // Продукт с индексом 1
            {
                var saleDoc2Id = Guid.NewGuid();
                var customer2 = activeCustomers[1];
                var contractForCust2 = _contracts.FirstOrDefault(c => c.CustomerRef_1C == customer2.Ref);
                sales.Add(new SaleFactData
                {
                    SalesDocumentID_1C = saleDoc2Id,
                    SalesDocumentNumber_1C = "РН-00002",
                    SalesDocumentLineNo_1C = 1,
                    DocDate = DateTime.Now.AddDays(-10),
                    CustomerRef_1C = customer2.Ref,
                    ProductRef_1C = _products[1].Ref,
                    OrganizationRef_1C = org1.Ref,
                    ContractRef_1C = contractForCust2?.Ref,
                    Quantity = 10,
                    Price = 1500,
                    Amount = 15000,
                    VATRateName = "Без НДС",
                    VATAmount = 0,
                    TotalAmount = 15000,
                    CurrencyCode = "RUB"
                });
            }

            // Продажа 3 (без договора, другой продукт)
            if (_products.Count > 3) // Продукт с индексом 3
            {
                sales.Add(new SaleFactData
                {
                    SalesDocumentID_1C = Guid.NewGuid(),
                    SalesDocumentNumber_1C = "РН-00003",
                    SalesDocumentLineNo_1C = 1,
                    DocDate = DateTime.Now.AddDays(-5),
                    CustomerRef_1C = customer1.Ref,
                    ProductRef_1C = _products[3].Ref,
                    OrganizationRef_1C = org1.Ref,
                    ContractRef_1C = null, // Без договора
                    Quantity = 100,
                    Price = 250,
                    Amount = 25000,
                    VATRateName = "20%",
                    VATAmount = 5000,
                    TotalAmount = 30000,
                    CurrencyCode = "RUB"
                });
            }

            _log.LogInformation("Generated {Count} mock sale fact rows.", sales.Count);
            return sales;
        }

        public IEnumerable<PaymentRowData> GetPaymentRows()
        {
            _log.LogInformation("MockOneCAccessor.GetPaymentRows() called. Generating mock payment data...");
            var payments = new List<PaymentRowData>();

            if (!_customers.Any(c => !c.IsDeleted) || !_organizations.Any())
            {
                _log.LogWarning("Not enough active dimension data to generate payment facts.");
                return payments;
            }

            var activeCustomers = _customers.Where(c => !c.IsDeleted).ToList();
            var org1 = _organizations[0]; // Предполагаем, что хотя бы одна организация есть
            var customer1 = activeCustomers[0];
            var contract1ForCustomer1 = _contracts.FirstOrDefault(c => c.CustomerRef_1C == customer1.Ref);

            // Платёж 1 (по договору)
            payments.Add(new PaymentRowData
            {
                PaymentDocID_1C = Guid.NewGuid(),
                PaymentNumber_1C = "ПП-00001",
                PaymentDate = DateTime.Now.AddDays(-14),
                PayerRef_1C = customer1.Ref,
                OrganizationRef_1C = org1.Ref,
                ContractRef_1C = contract1ForCustomer1?.Ref,
                Amount = 150000,
                CurrencyCode = "RUB",
                // !!! ВАЖНО: Поле PaymentDocumentType_1C обязательно в вашей SQL-схеме (NOT NULL)
                // !!! Убедитесь, что оно есть в DTO Models.PaymentRowData
                PaymentDocumentType_1C = "Поступление на расчетный счет"
            });

            // Платёж 2 (другой клиент, без договора)
            if (activeCustomers.Count > 1)
            {
                var customer2 = activeCustomers[1];
                payments.Add(new PaymentRowData
                {
                    PaymentDocID_1C = Guid.NewGuid(),
                    PaymentNumber_1C = "ПП-00002",
                    PaymentDate = DateTime.Now.AddDays(-9),
                    PayerRef_1C = customer2.Ref,
                    OrganizationRef_1C = org1.Ref,
                    ContractRef_1C = null, // Без договора
                    Amount = 10000,
                    CurrencyCode = "RUB",
                    PaymentDocumentType_1C = "Приходный кассовый ордер"
                });
            }

            // Платёж 3 (еще один от первого клиента, аванс)
            payments.Add(new PaymentRowData
            {
                PaymentDocID_1C = Guid.NewGuid(),
                PaymentNumber_1C = "ПП-00003",
                PaymentDate = DateTime.Now.AddDays(-3),
                PayerRef_1C = customer1.Ref,
                OrganizationRef_1C = org1.Ref,
                ContractRef_1C = contract1ForCustomer1?.Ref,
                Amount = 50000,
                CurrencyCode = "RUB",
                PaymentDocumentType_1C = "Поступление по платежной карте"
            });

            _log.LogInformation("Generated {Count} mock payment fact rows.", payments.Count);
            return payments;
        }

        public void Dispose()
        {
            _log.LogInformation("MockOneCAccessor.Dispose() called. No unmanaged resources to release.");
        }
    }
}