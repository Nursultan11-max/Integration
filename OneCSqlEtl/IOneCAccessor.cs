// IOneCAccessor.cs
using System;
using System.Collections.Generic;

namespace OneCSqlEtl
{
    public interface IOneCAccessor : IDisposable
    {
        bool Connect();
        IEnumerable<Customer1C> GetCustomers();
        IEnumerable<Product1C> GetProducts();
        IEnumerable<Contract1C> GetContracts();
        IEnumerable<Organization1C> GetOrganizations();
        IEnumerable<SaleFactData> GetSaleRows();
        IEnumerable<PaymentRowData> GetPaymentRows();
    }
}