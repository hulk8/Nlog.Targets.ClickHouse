using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Ado;

namespace NLog.Targets.ClickHouse
{
    public class ClickHouseLoaderV2 : IClickHouseLoader, IDisposable
    {
        private readonly ClickHouseConnectionSettings _connectionSettings;
        
        public ClickHouseLoaderV2(ClickHouseConnectionSettings settings)
        {
            _connectionSettings = settings ?? throw new ArgumentNullException(nameof(settings));
        }
        
        protected ClickHouseConnection CreateConnection()
        {
            return new ClickHouseConnection(_connectionSettings);
        }
        
        public Task WriteDataAsync(
            string tableName, 
            IEnumerable<object[]> data, 
            IReadOnlyCollection<string> columns, 
            int batchSize = 100000,
            CancellationToken cancellationToken = default)
        {
            using (var conn = CreateConnection()) 
            {
                conn.Open();
                var command = conn.CreateCommand($"INSERT INTO {tableName} ({string.Join(", ", columns.Select(x => $"`{x}`"))}) VALUES @bulk;");
                command.Parameters.Add(
                    new ClickHouseParameter {
                        DbType = DbType.Object,
                        ParameterName = "bulk",
                        Value = data
                    }
                );
                command.ExecuteNonQuery();
            }
            
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            
        }
    }
}