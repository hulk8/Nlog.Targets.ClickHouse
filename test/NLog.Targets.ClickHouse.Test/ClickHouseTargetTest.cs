using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NLog.Common;
using NLog.Config;
using NUnit.Framework;

namespace NLog.Targets.ClickHouse.Test
{
    [TestFixture]
    public class ClickHouseTargetTest : BasicTest
    {
        private readonly Random _random = new Random();
        
        protected override void Configure()
        {
            base.Configure();
            ServiceCollection.AddTransient(provider => new ClickHouseTarget("click")
            {
                ConnectionString = Configuration.GetConnectionString("click_test"),
                TableName = "logs.test",
                Parameters =
                {
                    new DatabaseParameterInfo("application", "app_test")
                    {
                        DbType = ClickHouseType.String
                    },
                    new DatabaseParameterInfo("level", "${level}")
                    {
                        DbType = ClickHouseType.String
                    },
                    new DatabaseParameterInfo("message", "${message}")
                    {
                        DbType = ClickHouseType.String
                    },
                    new DatabaseParameterInfo("logger", "${logger}")
                    {
                        DbType = ClickHouseType.String
                    },
                    new DatabaseParameterInfo("callSite", "${callsite:filename=true}")
                    {
                        DbType = ClickHouseType.String,
                        AllowDbNull = true
                    },
                    new DatabaseParameterInfo("exception", "${exception:tostring}")
                    {
                        DbType = ClickHouseType.String,
                        AllowDbNull = true
                    },
                    new DatabaseParameterInfo("logged", "${date}")
                    {
                        DbType = ClickHouseType.DateTime64
                    }
                }
            });
        }

        [OneTimeSetUp]
        protected override async Task StartupAsync()
        {
            await base.StartupAsync();
        }

        [Test]
        public void WriteLog()
        {
            var target = ServiceProvider.GetRequiredService<ClickHouseTarget>();
            new LogFactory().Setup().LoadConfiguration(cfg => cfg.Configuration.AddRuleForAllLevels(target));
            
            var exceptions = new List<Exception>();
            target.WriteAsyncLogEvent(new AsyncLogEventInfo(LogEventInfo.Create(LogLevel.Debug, "logger_name", "message text"), exceptions.Add));
            
            foreach (var ex in exceptions)
                Assert.Null(ex);
        }
        
        [Test]
        public void WriteLogs()
        {
            var target = ServiceProvider.GetRequiredService<ClickHouseTarget>();
            new LogFactory().Setup().LoadConfiguration(cfg => cfg.Configuration.AddRuleForAllLevels(target));
            
            var exceptions = new List<Exception>();
            target.WriteAsyncLogEvents(
                new AsyncLogEventInfo(LogEventInfo.Create(LogLevel.Debug, "logger_name", "message text 1"), exceptions.Add),
                new AsyncLogEventInfo(LogEventInfo.Create(LogLevel.Trace, "logger_name", "message text 2"), exceptions.Add),
                new AsyncLogEventInfo(LogEventInfo.Create(LogLevel.Info, "logger_name", "message text 3"), exceptions.Add),
                new AsyncLogEventInfo(LogEventInfo.Create(LogLevel.Warn, "logger_name", "message text 4"), exceptions.Add),
                new AsyncLogEventInfo(LogEventInfo.Create(LogLevel.Error, "logger_name", "message text 5"), exceptions.Add),
                new AsyncLogEventInfo(LogEventInfo.Create(LogLevel.Fatal, "logger_name", "message text 5"), exceptions.Add));
            
            foreach (var ex in exceptions)
                Assert.Null(ex);
        }

        [Test]
        public void ConfigXml()
        {
            LoggingConfiguration config = XmlLoggingConfiguration.CreateFromXmlString(@"
            <nlog xmlns='http://www.nlog-project.org/schemas/NLog.xsd'
                  xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'
                  internalLogToConsole='true'
                  autoReload='true'>
              <targets>
                <target 
                        name='clickhouse_queue' 
                        xsi:type='BufferingWrapper'
                        bufferSize='10000'
                        flushTimeout='5000'
                        overflowAction='Flush'>
                  <target name='clickhouse' 
                          xsi:type='ClickHouse' 
                          connectionString='Host=localhost;Port=8123;Username=sa;Password=P@ssw0rd;Database=logs'
                          tableName='logs.test'
                          batchSize='5000'>
                    <parameter name='application' dbType='String' layout='test_app'/>
                    <parameter name='level' dbType='String' layout='${level}' />
                    <parameter name='message' dbType='String' layout='${message}' />
                    <parameter name='logger' dbType='String' layout='${logger}' />
                    <parameter name='callSite' dbType='String' allowDbNull='true' layout='${callsite:filename=true}' />
                    <parameter name='exception' dbType='String' allowDbNull='true' layout='${exception:tostring}' />
                    <parameter name='logged' dbType='DateTime64' layout='${date}' />
                  </target>
                </target>
              </targets>
              <rules>
                <logger name='*' minlevel='Trace' writeTo='clickhouse_queue' />
              </rules>
            </nlog>");

            var target = config.FindTargetByName("clickhouse") as ClickHouseTarget;
            Assert.NotNull(target);
            Assert.AreEqual("clickhouse", target.Name);
            Assert.AreEqual("Host=localhost;Port=8123;Username=sa;Password=P@ssw0rd;Database=logs", target.ConnectionString);
            Assert.AreEqual("logs.test", target.TableName);
            Assert.AreEqual(5000, target.BatchSize);
            Assert.AreEqual(7, target.Columns.Length);
            Assert.AreEqual(7, target.Parameters.Count);
        }

        [Test]
        [TestCase(5000)]
        public void Sample(int count)
        {
            var logger = LogManager.GetCurrentClassLogger();
            
            foreach (var i in Enumerable.Range(0, count))
            {
                logger.Log(LogLevel.FromOrdinal(_random.Next(0, 6)), $"message {i}");
            }
        }
    }
}