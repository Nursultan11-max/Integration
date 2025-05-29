using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OneCSqlEtl
{
    /// <summary>
    /// Конфигурационные настройки приложения 
    /// </summary>
    public class Settings
    {
        public ConnectionStrings ConnectionStrings { get; set; } = new ConnectionStrings();
        public EtlSettings EtlSettings { get; set; } = new EtlSettings();
    }

    /// <summary>
    /// Строки подключения к 1С и SQL Server
    /// </summary>
    public class ConnectionStrings
    {
        /// <summary>Строка подключения к базе 1С (COM-подключение)</summary>
        public string OneCConnectionString { get; set; } = string.Empty;

        /// <summary>Строка подключения к SQL Server</summary>
        public string SqlServerConnectionString { get; set; } = string.Empty;
    }

    /// <summary>
    /// Настройки ETL-процесса
    /// </summary>
    public class EtlSettings
    {
        /// <summary>ProgID для COM-подключения к 1С (например, "V83.Application" или "V83.COMConnector")</summary>
        public string OneCComVersion { get; set; } = "V83.Application";

        /// <summary>Размер батча для пакетной вставки фактов</summary>
        public int BatchSize { get; set; } = 500;

        /// <summary>Таймаут команд SQL в секундах</summary>
        public int SqlCommandTimeout { get; set; } = 60;
    }
}
