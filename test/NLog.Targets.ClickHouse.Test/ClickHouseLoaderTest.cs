using System;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;

namespace NLog.Targets.ClickHouse.Test
{
    [TestFixture]
    public class ClickHouseLoaderTest : BasicTest
    {
        protected override void Configure()
        {
            base.Configure();
            ServiceCollection.AddTransient(provider => new ClickHouseConnection(Configuration.GetConnectionString("click_test")));
            ServiceCollection.AddTransient<ClickHouseLoader>();
        }

        [OneTimeSetUp]
        protected override async Task StartupAsync()
        {
            await base.StartupAsync();
        }

        public ClickHouseLoader GetLoader()
        {
            return ServiceProvider.GetRequiredService<ClickHouseLoader>();
        }

        [Test]
        public async Task LoadLogMessage(long patientId)
        {
            using (var loader = GetLoader())
            {
                await loader.WriteDataAsync("test", new[]
                    {
                        new object[]
                        {
                            DateTime.Now,
                            "test_app",
                            "TRACE",
                            "trace message",
                            "logger",
                            "callsite",
                            null
                        },
                        new object[]
                        {
                            DateTime.Now,
                            "test_app",
                            "INFORMATION",
                            "info message",
                            "logger",
                            "callsite",
                            null
                        }
                    },
                    new[]
                    {
                        "logged",
                        "application",
                        "level",
                        "message",
                        "logger",
                        "callSite",
                        "exception"
                    });
            }
        }
    }
}