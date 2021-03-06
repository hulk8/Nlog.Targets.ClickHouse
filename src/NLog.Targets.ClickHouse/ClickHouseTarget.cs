using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Ado;
using NLog.Common;
using NLog.Config;

namespace NLog.Targets.ClickHouse
{
    [Target("ClickHouse")]
    public class ClickHouseTarget : TargetWithLayout, IInstallable
    {
        public string ConnectionString { get; set; }
        
        public string TableName { get; set; }

        //TODO: add loading with batches of fixed size 
        public int BatchSize { get; set; } = 10000;
        
        public TimeSpan FlushTimeout { get; set; } = TimeSpan.FromSeconds(5);

        [ArrayParameter(typeof(DatabaseCommandInfo), "install-command")]
        public IList<DatabaseCommandInfo> InstallDdlCommands { get; private set; } = new List<DatabaseCommandInfo>();

        [ArrayParameter(typeof(DatabaseCommandInfo), "uninstall-command")]
        public IList<DatabaseCommandInfo> UninstallDdlCommands { get; private set; } = new List<DatabaseCommandInfo>();

        [ArrayParameter(typeof(DatabaseParameterInfo), "parameter")]
        public IList<DatabaseParameterInfo> Parameters { get; } = new List<DatabaseParameterInfo>();

        public string[] Columns => Parameters.Select(x => x.Name).ToArray();
        
        public bool IsInstalled { get; private set; }
        
        private string _activeConnectionString;
        
        private readonly ConcurrentQueue<LogEventInfo> _internalQueue = new ConcurrentQueue<LogEventInfo>();

        private CancellationTokenSource _source;
        
        private Task _loadingEventsTask;

        public ClickHouseTarget()
        {
            
        }
        
        public ClickHouseTarget(string name) 
            : this()
        {
            Name = name;
        }

        protected override void InitializeTarget()
        {
            try
            {
                _activeConnectionString = BuildConnectionString();
                using (var connection = new ClickHouseConnection(_activeConnectionString))
                {
                    connection.Open();
                    var command = connection.CreateCommand("SELECT version()");
                    command.ExecuteNonQuery();
                }
                
                Install(new InstallationContext());

                _source = new CancellationTokenSource();
                _loadingEventsTask = CheckingEventsAsync(_source.Token); 
                
                InternalLogger.Info("ClickHouseTarget(Name={0}): Target initiated", Name);
            }
            catch (Exception ex)
            {
                InternalLogger.Error(ex, "ClickHouseTarget(Name={0}): Failed to init target", Name);
                throw;
            }
        }

        protected string BuildConnectionString()
        {
            return ConnectionString;
        }

        protected override void Write(LogEventInfo logEvent)
        {
            //InternalLogger.Warn("ClickhouseTarget(Name={0}): Writing logs single, you should use BufferingWrapper: https://github.com/NLog/NLog/wiki/BufferingWrapper-target", Name);
            _internalQueue.Enqueue(logEvent);
        }
        
        [Obsolete("Instead override Write(IList<AsyncLogEventInfo> logEvents. Marked obsolete on NLog 4.5")]
        protected override void Write(AsyncLogEventInfo[] logEvents)
        {
            Write((IList<AsyncLogEventInfo>)logEvents);
        }
        
        protected override void Write(IList<AsyncLogEventInfo> asyncLogEvents)
        {
            try
            {
                InternalLogger.Trace("ClickhouseTarget(Name={0}): Writing logs batch; count: {1};", Name, asyncLogEvents.Count);
                WriteLogsImpl(asyncLogEvents.Select(al => al.LogEvent));
            }
            catch (Exception ex)
            {
                InternalLogger.Error(ex, "ClickHouseTarget(Name={0}): Error writing to database.", Name);
                throw;
            }
        }

        protected virtual void WriteLogsImpl(IEnumerable<LogEventInfo> logEvents)
        {
            using (var conn = new ClickHouseConnection(_activeConnectionString))
            {
                conn.Open();
                var command = conn.CreateCommand($"INSERT INTO {TableName} ({string.Join(", ", Columns.Select(x => $"`{x}`"))}) VALUES @bulk;");
                command.Parameters.Add(
                    new ClickHouseParameter
                    {
                        DbType = DbType.Object,
                        ParameterName = "bulk",
                        Value = logEvents.Select(WrapParameters)
                    }
                );
                command.ExecuteNonQuery();
            }
        }

        private async Task CheckingEventsAsync(CancellationToken cancellationToken = default)
        {
            InternalLogger.Trace("ClickhouseTarget(Name={0}): Checking task started", Name);
            
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var batch = new List<LogEventInfo>();
                    while (_internalQueue.TryDequeue(out var logEventInfo))
                        batch.Add(logEventInfo);

                    if (batch.Any())
                        WriteLogsImpl(batch);

                    await Task.Delay(FlushTimeout, cancellationToken);
                }
                catch (TaskCanceledException ex)
                {

                }
                catch (Exception ex)
                {
                    InternalLogger.Error(ex, "ClickHouseTarget(Name={0}): Error writing to database.", Name);
                    throw;
                }
            }
            
            InternalLogger.Trace("ClickhouseTarget(Name={0}): Checking task completed", Name);
        }
        
        protected virtual object[] WrapParameters(LogEventInfo logEvent)
        {
            var values = new object[Parameters.Count];
            for (int i = 0; i < Parameters.Count; ++i)
            {
                var parameterInfo = Parameters[i];
                var wrappedValue = RenderLogEvent(parameterInfo.Layout, logEvent);
                values[i] = PrepareValue(parameterInfo, wrappedValue);
            }
            return values;
        }

        protected virtual object PrepareValue(DatabaseParameterInfo parameterInfo, string value)
        {
            if (value == null)
            {
                if (parameterInfo.AllowDbNull)
                    return null;

                throw new Exception();
            }

            if (parameterInfo.AllowDbNull && string.IsNullOrEmpty(value))
                return null;
            
            switch (parameterInfo.DbType)
            {
                case ClickHouseType.String:
                    return value;
                case ClickHouseType.UInt8:
                    return byte.Parse(value); 
                case ClickHouseType.UInt16:
                    return short.Parse(value);
                case ClickHouseType.UInt32:
                    return uint.Parse(value);
                case ClickHouseType.UInt64:
                    return ulong.Parse(value);
                case ClickHouseType.Int8:
                    return sbyte.Parse(value);
                case ClickHouseType.Int16:
                    return short.Parse(value);
                case ClickHouseType.Int32:
                    return int.Parse(value);
                case ClickHouseType.Int64:
                    return long.Parse(value);
                case ClickHouseType.Date:
                    return DateTime.Parse(value);
                case ClickHouseType.DateTime:
                    return DateTime.Parse(value);
                case ClickHouseType.DateTime64:
                    return DateTime.Parse(value);
                default:
                    InternalLogger.Warn("Unknown type convertion; DbType: '{0}'", parameterInfo.DbType);
                    return value;
            }
        }

        protected override void CloseTarget()
        {
            _source.Cancel();
            _loadingEventsTask.Wait(TimeSpan.FromSeconds(15));
            
            Uninstall(new InstallationContext { IgnoreFailures = true });
            InternalLogger.Trace("ClickhouseTarget(Name={0}): Close connection because of CloseTarget", Name);
        }

        public void Install(InstallationContext installationContext)
        {
            RunInstallCommands(installationContext, InstallDdlCommands);
            IsInstalled = true;
        }

        public void Uninstall(InstallationContext installationContext)
        {
            RunInstallCommands(installationContext, UninstallDdlCommands);
        }

        bool? IInstallable.IsInstalled(InstallationContext installationContext)
        {
            return IsInstalled;
        }
        
        private void RunInstallCommands(InstallationContext installationContext, IEnumerable<DatabaseCommandInfo> commands)
        {
            // create log event that will be used to render all layouts
            var logEvent = installationContext.CreateLogEvent();

            foreach (var commandInfo in commands)
            {
                var cmdConnStr = commandInfo.ConnectionString?.Render(logEvent);
                var connectionString = !string.IsNullOrEmpty(cmdConnStr) 
                    ? cmdConnStr
                    : ConnectionString;
                    
                using (var connection = new ClickHouseConnection(connectionString))
                {
                    connection.Open();
                    var commandText = RenderLogEvent(commandInfo.Text, logEvent);
                    var command  = connection.CreateCommand(commandText);
                    installationContext.Trace("ClickHouseTarget(Name={0}) - Executing {1} '{2}'", Name, commandInfo.CommandType, commandText);
                    try
                    {
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        if (LogManager.ThrowExceptions)
                            throw;

                        if (commandInfo.IgnoreFailures || installationContext.IgnoreFailures)
                        {
                            installationContext.Warning(ex.Message);
                        }
                        else
                        {
                            installationContext.Error(ex.Message);
                            throw;
                        }
                    }
                }
            }
        }
    }
}