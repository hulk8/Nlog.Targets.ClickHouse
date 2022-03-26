using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace NLog.Targets.ClickHouse.Test
{
    public abstract class BasicTest
    {
        public static IConfiguration Configuration { get; } = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.Testing.json")
            .Build();

        protected IServiceCollection ServiceCollection { get; } = new ServiceCollection()
            .AddLogging();

        protected IServiceProvider ServiceProvider { get; set; }

        protected virtual void Configure()
        {

        }
        
        protected virtual async Task StartupAsync()
        {
            Configure();
            ServiceProvider = ServiceCollection?.BuildServiceProvider();
        }
    }
}