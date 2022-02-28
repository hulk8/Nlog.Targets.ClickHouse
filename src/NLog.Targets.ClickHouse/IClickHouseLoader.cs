using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NLog.Targets.ClickHouse
{
    public interface IClickHouseLoader
    {
        Task WriteDataAsync(
            string tableName,
            IEnumerable<object[]> data,
            IReadOnlyCollection<string> columns,
            int batchSize = 100000,
            CancellationToken cancellationToken = default);
    }
}