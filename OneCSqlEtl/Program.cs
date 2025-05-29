using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Runtime.Versioning;
using static OneCSqlEtl.EtlOrchestrator;

namespace OneCSqlEtl
{
    [SupportedOSPlatform("windows")] // Пометить класс или метод Main
    internal class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                using IHost host = Host.CreateDefaultBuilder(args)
                    .ConfigureAppConfiguration((ctx, cfg) =>
                    {
                        cfg.SetBasePath(AppContext.BaseDirectory)
                           .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                           .AddEnvironmentVariables("ONECETL_"); // Добавляем переменные среды с префиксом ONECETL_
                    })
                    .ConfigureServices((ctx, services) =>
                    {
                        // Привязываем настройки из appsettings.json
                        services.Configure<Settings>(ctx.Configuration);
                        // Регистрируем наши сервисы
                        services.AddSingleton<OneCAccessor>();
                        services.AddSingleton<SqlRepository>();
                        services.AddSingleton<EtlOrchestrator>();
                    })
                    .ConfigureLogging(logging =>
                    {
                        logging.ClearProviders();
                        logging.AddConsole();
                        logging.AddDebug();
                        logging.SetMinimumLevel(LogLevel.Information);
                    })
                    .Build();

                // Запускаем ETL
                var orchestrator = host.Services.GetRequiredService<EtlOrchestrator>();
                await orchestrator.RunAsync();

                // Если захотите, хост может ещё работать, но можно и выйти сразу 
                // await host.RunAsync();
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Критическая ошибка: {ex.Message}");
                Console.WriteLine(ex.StackTrace);

                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Внутреннее исключение: {ex.InnerException.Message}");
                    Console.WriteLine(ex.InnerException.StackTrace);
                }

                Console.ResetColor();
                Environment.ExitCode = 1; // Устанавливаем код ошибки для внешних систем
            }
        }
    }
}
