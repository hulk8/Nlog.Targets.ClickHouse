using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Ado;
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

        public static string ConnectionString { get; } = "Host=localhost;Port=9000;Database=logs;User=sa;Password=P@ssw0rd;";

        public static string XmlConfigWithBufferedWrapper { get; } = @"
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
                          connectionString='Host=localhost;Port=9000;Database=logs;User=sa;Password=P@ssw0rd;'
                          tableName='test'
                          batchSize='5000'>
                    <install-command text='
                        CREATE TABLE IF NOT EXISTS test
                        (
                            logged DateTime64,
                            application String,
                            level LowCardinality(String),
                            message String,
                            logger String,
                            callSite String,
                            exception Nullable(String)
                        )
                        ENGINE Log' />
                    <install-command text='TRUNCATE TABLE test' />
                    <uninstall-command text='DROP TABLE IF EXISTS test' />
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
            </nlog>";
        
        public static string XmlConfigWithInternalQueue { get; } = @"
            <nlog xmlns='http://www.nlog-project.org/schemas/NLog.xsd'
                  xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'
                  internalLogToConsole='true'
                  autoReload='true'>
              <targets>
                <target name='clickhouse' 
                          xsi:type='ClickHouse' 
                          connectionString='Host=localhost;Port=9000;Database=logs;User=sa;Password=P@ssw0rd;'
                          tableName='test'
                          batchSize='5000'>
                    <install-command text='
                        CREATE TABLE IF NOT EXISTS test
                        (
                            logged DateTime64,
                            application String,
                            level LowCardinality(String),
                            message String,
                            logger String,
                            callSite String,
                            exception Nullable(String)
                        )
                        ENGINE Log' />
                    <install-command text='TRUNCATE TABLE test' />
                    <uninstall-command text='DROP TABLE IF EXISTS test' />
                    <parameter name='application' dbType='String' layout='test_app'/>
                    <parameter name='level' dbType='String' layout='${level}' />
                    <parameter name='message' dbType='String' layout='${message}' />
                    <parameter name='logger' dbType='String' layout='${logger}' />
                    <parameter name='callSite' dbType='String' allowDbNull='true' layout='${callsite:filename=true}' />
                    <parameter name='exception' dbType='String' allowDbNull='true' layout='${exception:tostring}' />
                    <parameter name='logged' dbType='DateTime64' layout='${date}' />
                  </target>
              </targets>
              <rules>
                <logger name='*' minlevel='Trace' writeTo='clickhouse' />
              </rules>
            </nlog>";
        
        protected override void Configure()
        {
            base.Configure();
            ServiceCollection.AddSingleton<ClickHouseConnectionSettings>(provider => new ClickHouseConnectionSettings(ConnectionString));
            ServiceCollection.AddTransient(provider => new ClickHouseTarget("click")
            {
                ConnectionString = ConnectionString,
                TableName = "test",
                InstallDdlCommands =
                {
                    new DatabaseCommandInfo
                    {
                        Text = "CREATE TABLE IF NOT EXISTS test(logged DateTime64, application String, level LowCardinality(String), message String, logger String, callSite String, exception Nullable(String)) ENGINE Log"
                    },
                    new DatabaseCommandInfo
                    {
                        Text = "TRUNCATE TABLE test"
                    }
                },
                UninstallDdlCommands =
                {
                    new DatabaseCommandInfo
                    {
                        Text = "DROP TABLE IF EXISTS test;"
                    }
                },
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
            ServiceCollection.AddSingleton<InstallationContext>(provider => new InstallationContext());
        }

        [OneTimeSetUp]
        protected override async Task StartupAsync()
        {
            await base.StartupAsync();
        }
        
        [Test]
        public void ConfigXml()
        {
            LoggingConfiguration config = XmlLoggingConfiguration.CreateFromXmlString(XmlConfigWithBufferedWrapper);

            var target = config.FindTargetByName("clickhouse") as ClickHouseTarget;
            Assert.NotNull(target);
            Assert.AreEqual("clickhouse", target.Name);
            Assert.AreEqual("Host=localhost;Port=9000;Database=logs;User=sa;Password=P@ssw0rd;", target.ConnectionString);
            Assert.AreEqual("logs.test", target.TableName);
            Assert.AreEqual(5000, target.BatchSize);
            Assert.AreEqual(7, target.Columns.Length);
            Assert.AreEqual(7, target.Parameters.Count);
        }
        
        [Test]
        public async Task WriteAsyncLogEvent()
        {
            var target = ServiceProvider.GetRequiredService<ClickHouseTarget>();
            var installationContext = ServiceProvider.GetRequiredService<InstallationContext>();
            new LogFactory()
                .Setup()
                .LoadConfiguration(cfg => cfg.Configuration.AddRuleForAllLevels(target));
            
            var exceptions = new List<Exception>();
            target.Install(installationContext);
            Assert.True(ShowTables().Contains(target.TableName));
            
            target.WriteAsyncLogEvent(new AsyncLogEventInfo(LogEventInfo.Create(LogLevel.Debug, "logger_name", "message text"), exceptions.Add));
            
            foreach (var ex in exceptions)
                Assert.Null(ex);

            await Task.Delay(TimeSpan.FromSeconds(10));
            
            Assert.AreEqual(1, GetTableSize(target.TableName));
            
            target.Uninstall(installationContext);
            Assert.False(ShowTables().Contains(target.TableName));
        }

        [Test]
        public void WriteAsyncLogEvents()
        {
            var target = ServiceProvider.GetRequiredService<ClickHouseTarget>();
            var installationContext = ServiceProvider.GetRequiredService<InstallationContext>();
            new LogFactory()
                .Setup()
                .LoadConfiguration(cfg => cfg.Configuration.AddRuleForAllLevels(target));
            
            var exceptions = new List<Exception>();
            target.Install(installationContext);
            Assert.True(ShowTables().Contains(target.TableName));
            
            target.WriteAsyncLogEvents(
                new AsyncLogEventInfo(LogEventInfo.Create(LogLevel.Debug, "logger_name", "message text 1"), exceptions.Add),
                new AsyncLogEventInfo(LogEventInfo.Create(LogLevel.Trace, "logger_name", "message text 2"), exceptions.Add),
                new AsyncLogEventInfo(LogEventInfo.Create(LogLevel.Info, "logger_name", "message text 3"), exceptions.Add),
                new AsyncLogEventInfo(LogEventInfo.Create(LogLevel.Warn, "logger_name", "message text 4"), exceptions.Add),
                new AsyncLogEventInfo(LogEventInfo.Create(LogLevel.Error, "logger_name", "message text 5"), exceptions.Add),
                new AsyncLogEventInfo(LogEventInfo.Create(LogLevel.Fatal, "logger_name", "message text 5"), exceptions.Add));
            
            foreach (var ex in exceptions)
                Assert.Null(ex);

            Assert.AreEqual(6, GetTableSize(target.TableName));
            
            target.Uninstall(installationContext);
            Assert.False(ShowTables().Contains(target.TableName));
        }

        [Test]
        [TestCase(5000)]
        public async Task Generate_sample_logs_with_BufferingWrapper(int count)
        {
            LogManager.Configuration = XmlLoggingConfiguration.CreateFromXmlString(XmlConfigWithBufferedWrapper);
            var logger = LogManager.GetCurrentClassLogger();
            
            foreach (var i in Enumerable.Range(0, count))
            {
                logger.Log(LogLevel.FromOrdinal(_random.Next(0, 6)), $"message {i}");
            }

            await Task.Delay(TimeSpan.FromSeconds(10));
            
            Assert.AreEqual(count, GetTableSize("test"));
        }
        
        [Test]
        [TestCase(5000)]
        public async Task Generate_sample_logs_with_internal_queue(int count)
        {
            LogManager.Configuration = XmlLoggingConfiguration.CreateFromXmlString(XmlConfigWithInternalQueue);
            var logger = LogManager.GetCurrentClassLogger();
            
            foreach (var i in Enumerable.Range(0, count))
            {
                logger.Log(LogLevel.FromOrdinal(_random.Next(0, 6)), $"message {i}");
            }

            await Task.Delay(TimeSpan.FromSeconds(10));
            
            Assert.AreEqual(count, GetTableSize("test"));
        }

        private void TruncateTable(string tableName)
        {
            var settings = ServiceProvider.GetRequiredService<ClickHouseConnectionSettings>();
            
            using (var connection = new ClickHouseConnection(settings))
            {
                connection.Open();
                var command = new ClickHouseCommand(connection, $"TRUNCATE TABLE {tableName}");
                command.ExecuteNonQuery();
            }
        }
        
        private string[] ShowTables()
        {
            var settings = ServiceProvider.GetRequiredService<ClickHouseConnectionSettings>();

            var tables = new List<string>();
            
            using (var connection = new ClickHouseConnection(settings))
            {
                connection.Open();
                var command = new ClickHouseCommand(connection, "SHOW TABLES");
                using (var reader = command.ExecuteReader())
                {
                    reader.ReadAll(rowReader => tables.Add(rowReader.GetString(0)));
                }
            }

            return tables.ToArray();
        }
        
        private ulong GetTableSize(string tableName)
        {
            var settings = ServiceProvider.GetRequiredService<ClickHouseConnectionSettings>();
            
            using (var connection = new ClickHouseConnection(settings))
            {
                connection.Open();
                var command = new ClickHouseCommand(connection, $"SELECT count() FROM {tableName}");
                return (ulong) command.ExecuteScalar();
            }
        }
    }
}