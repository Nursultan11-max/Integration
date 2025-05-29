using System;

namespace OneCSqlEtl
{
    /// <summary>
    /// Контрагент из 1С.
    /// </summary>
    public class Customer1C
    {
        public Guid Ref { get; set; }
        public string? Code { get; set; }
        public string Name { get; set; } = string.Empty;
        public string? FullName { get; set; }
        public string? TIN { get; set; }
        public string? KPP { get; set; }
        public string? EntityType { get; set; }
        public bool IsDeleted { get; set; }
    }

    /// <summary>
    /// Продукт из 1С.
    /// </summary>
    public class Product1C
    {
        public Guid Ref { get; set; }
        public string? Code { get; set; }
        public string Name { get; set; } = string.Empty;
        public string? FullName { get; set; }
        public string? SKU { get; set; }
        public string? UnitOfMeasure { get; set; }
        public string? ProductType { get; set; }
        public string? ProductGroup { get; set; }
        public string? DefaultVATRateName { get; set; }
    }

    /// <summary>
    /// Договор из 1С.
    /// </summary>
    public class Contract1C
    {
        public Guid Ref { get; set; }
        public string? Code { get; set; }
        public string Name { get; set; } = string.Empty;
        public Guid CustomerRef_1C { get; set; }
        public int CustomerSK { get; set; } // Заполняется в EtlOrchestrator
        public DateTime? StartDate { get; set; }
        public DateTime? EndDate { get; set; }
    }

    /// <summary>
    /// Организация из 1С.
    /// </summary>
    public class Organization1C
    {
        public Guid Ref { get; set; }
        public string? Code { get; set; }
        public string Name { get; set; } = string.Empty;
        public string? OrganizationFullName { get; set; }
    }

    /// <summary>
    /// Строка факта продаж. Содержит как "сырые" данные из 1С, так и поля для SK/DateKey.
    /// </summary>
    public class SaleFactData
    {
        // Данные из 1С
        public Guid SalesDocumentID_1C { get; set; }
        public string SalesDocumentNumber_1C { get; set; } = string.Empty;
        public int SalesDocumentLineNo_1C { get; set; }
        public DateTime DocDate { get; set; }
        public Guid CustomerRef_1C { get; set; }
        public Guid ProductRef_1C { get; set; }
        public Guid OrganizationRef_1C { get; set; }
        public Guid? ContractRef_1C { get; set; } // УИД договора из 1С
        public decimal Quantity { get; set; }
        public decimal Price { get; set; }
        public decimal Amount { get; set; }
        public string? VATRateName { get; set; }
        public decimal VATAmount { get; set; }
        public decimal TotalAmount { get; set; }
        public string? CurrencyCode { get; set; }

        // Ключи для хранилища (заполняются в EtlOrchestrator)
        public int SaleDateKey { get; set; }
        public int CustomerSK { get; set; }
        public int ProductSK { get; set; }
        public int? ContractSK { get; set; } // Суррогатный ключ договора
        public int OrganizationSK { get; set; }
    }

    /// <summary>
    /// Строка факта оплат. Содержит как "сырые" данные из 1С, так и поля для SK/DateKey.
    /// </summary>
    public class PaymentRowData
    {
        // Данные из 1С
        public Guid PaymentDocID_1C { get; set; }
        public string PaymentNumber_1C { get; set; } = string.Empty;
        public DateTime PaymentDate { get; set; }
        public decimal Amount { get; set; }
        public string? CurrencyCode { get; set; }
        public Guid PayerRef_1C { get; set; }         // УИД плательщика (Контрагент)
        public Guid? ContractRef_1C { get; set; }    // УИД договора из 1С для платежа
        public Guid OrganizationRef_1C { get; set; }  // УИД организации из 1С для платежа

        // Ключи для хранилища (заполняются в EtlOrchestrator)
        public int PaymentDateKey { get; set; }
        public int CustomerSK { get; set; }          // Суррогатный ключ плательщика (ранее PayerCustomerSK)
        public int? ContractSK { get; set; }         // Суррогатный ключ договора
        public int OrganizationSK { get; set; }      // Суррогатный ключ организации
    }
}
