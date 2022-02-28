using System;
using System.Collections.Generic;
using System.Linq;
using NLog.Common;
using NLog.Config;
using ClickHouseConnection = ClickHouse.Client.ADO.ClickHouseConnection;

namespace NLog.Targets.ClickHouse
{
    [Target("ClickHouse")]
    public class ClickHouseTarget : TargetWithLayout
    {
        public string ConnectionString { get; set; }
        
        public string TableName { get; set; }

        public int BatchSize { get; set; } = 10000;
        
        [ArrayParameter(typeof(DatabaseParameterInfo), "parameter")]
        public IList<DatabaseParameterInfo> Parameters { get; } = new List<DatabaseParameterInfo>();

        public string[] Columns => Parameters.Select(x => x.Name).ToArray();
        
        private string _activeConnectionString;

        private ClickHouseConnection _connection;

        protected IClickHouseLoader Loader { get; set; }

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
                Loader = new ClickHouseLoader(new ClickHouseConnection(_activeConnectionString));
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
            try
            {
                InternalLogger.Warn("ClickhouseTarget(Name={0}): Writing logs single, you should use BufferingWrapper: https://github.com/NLog/NLog/wiki/BufferingWrapper-target", Name);
                Loader.WriteDataAsync(TableName, new List<object[]>{ WrapParameters(logEvent) }, Columns, BatchSize)
                    .Wait();
            }
            catch (Exception ex)
            {
                InternalLogger.Error(ex, "ClickHouseTarget(Name={0}): Error when writing to database.", Name);
                throw;
            }
        }
        
        [Obsolete("Instead override Write(IList<AsyncLogEventInfo> logEvents. Marked obsolete on NLog 4.5")]
        protected override void Write(AsyncLogEventInfo[] logEvents)
        {
            Write((IList<AsyncLogEventInfo>)logEvents);
        }
        
        protected override void Write(IList<AsyncLogEventInfo> logEvents)
        {
            try
            {
                InternalLogger.Trace("ClickhouseTarget(Name={0}): Writing logs batch; count: {1};", Name, logEvents.Count);
                Loader.WriteDataAsync(TableName, logEvents.Select(al => WrapParameters(al.LogEvent)), Columns, BatchSize)
                    .Wait();
            }
            catch (Exception ex)
            {
                InternalLogger.Error(ex, "ClickHouseTarget(Name={0}): Error when writing to database.", Name);
                throw;
            }
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
                case ClickHouseType.Int8:
                    return byte.Parse(value);
                case ClickHouseType.Int16:
                    return short.Parse(value);
                case ClickHouseType.Int32:
                    return int.Parse(value);
                case ClickHouseType.Int64:
                    return long.Parse(value);
                case ClickHouseType.DateTime64:
                    return DateTime.Parse(value);
                default:
                    throw new Exception();
            }
        }

        protected override void CloseTarget()
        {
            base.CloseTarget();
            InternalLogger.Trace("ClickhouseTarget(Name={0}): Close connection because of CloseTarget", Name);
        }
    }
}