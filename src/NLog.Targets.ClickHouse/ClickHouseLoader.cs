using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;

namespace NLog.Targets.ClickHouse
{
    public class ClickHouseLoader : IClickHouseLoader, IDisposable
    {
        private ClickHouseConnection _connection;

        public ClickHouseLoader(ClickHouseConnection connection)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            if (_connection.State != ConnectionState.Open)
                _connection.Open();
        }

        public async Task WriteDataAsync(
            string tableName, 
            IEnumerable<object[]> data, 
            IReadOnlyCollection<string> columns, 
            int batchSize = 100000, 
            CancellationToken cancellationToken = default)
        {
            using var bulkCopyInterface = new ClickHouseBulkCopy(_connection)
            {
                DestinationTableName = tableName,
                BatchSize = batchSize
            };
            await bulkCopyInterface.WriteToServerAsync(data, columns, cancellationToken);
            Console.WriteLine(bulkCopyInterface.RowsWritten);
        }

        public void Dispose()
        {
            _connection?.Dispose();
            _connection = null;
        }
    }
}