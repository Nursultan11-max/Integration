using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
// using Microsoft.Extensions.Options; // Not directly used here, but services.Configure<Settings> is
using System.Runtime.Versioning;
// using static OneCSqlEtl.EtlOrchestrator; // This was removed as unnecessary

namespace OneCSqlEtl
{
    [SupportedOSPlatform("windows")]
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
                           .AddEnvironmentVariables("ONECETL_");
                    })
                    .ConfigureServices((ctx, services) =>
                    {
                        services.Configure<Settings>(ctx.Configuration);
                        services.AddSingleton<OneCAccessor>();
                        services.AddSingleton<SqlRepository>();
                        services.AddSingleton<EtlOrchestrator>();
                    })
                    .ConfigureLogging(logging =>
                    {
                        logging.ClearProviders();
                        logging.AddConsole();
                        logging.AddDebug();
                        // MODIFICATION HERE:
                        logging.SetMinimumLevel(LogLevel.Debug); // Changed from LogLevel.Information
                    })
                    .Build();

                var orchestrator = host.Services.GetRequiredService<EtlOrchestrator>();
                await orchestrator.RunAsync();

                // Console.WriteLine("ETL Process completed. Press any key to exit."); // Optional
                // Console.ReadKey();
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Критическая ошибка в Main: {ex.Message}");
                Console.WriteLine("--- StackTrace ---");
                Console.WriteLine(ex.StackTrace);

                Exception? currentEx = ex.InnerException;
                int innerCount = 1;
                while (currentEx != null)
                {
                    Console.WriteLine($"--- Внутреннее исключение ({innerCount++}) ---");
                    Console.WriteLine($"Сообщение: {currentEx.Message}");
                    Console.WriteLine($"StackTrace: {currentEx.StackTrace}");
                    currentEx = currentEx.InnerException;
                }
                Console.ResetColor();
                Environment.ExitCode = 1;
            }
        }
    }
}