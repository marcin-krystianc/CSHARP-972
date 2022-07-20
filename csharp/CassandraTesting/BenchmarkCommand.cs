using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Spectre.Console.Cli;

namespace CassandraTesting;

public sealed class BenchmarkCommand : AsyncCommand<BenchmarkSettings>
{
    private long _rowCounter = 0;
    private long _requestCounter = 0;
    private long _exceptionCounter = 0;
    private Exception _lastException;

    public override async Task<int> ExecuteAsync(CommandContext context, BenchmarkSettings settings)
    {
        var session = await CassandraUtils.ConnectAsync(settings);
        var cts = new CancellationTokenSource();

        var statement = new SimpleStatement($"SELECT * FROM my_table where partition_id = {settings.PartitionNumber}");
        statement.SetConsistencyLevel(ConsistencyLevel.LocalOne);
        statement.SetReadTimeoutMillis(120000);
       
        var tasks = Enumerable.Range(0, settings.TaskCount)
            .Select(x => Worker(session, statement, cts.Token))
            .ToArray();

        var stopWatch = Stopwatch.StartNew();
        cts.CancelAfter(TimeSpan.FromSeconds(settings.Duration));

        long lastRowCounter = 0;
        long lastRequestCounter = 0;
        var smallSw = stopWatch;
        
        while (!cts.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);
            var rowCounter = Interlocked.Read(ref _rowCounter);
            var requestCounter = Interlocked.Read(ref _requestCounter);
            var smallRowCounter = rowCounter - lastRowCounter;
            var smallRequestCounter = requestCounter - lastRequestCounter;
            lastRowCounter = rowCounter;
            lastRequestCounter = requestCounter;
            var rowRate = rowCounter / stopWatch.Elapsed.TotalSeconds;
            var requestRate = requestCounter / stopWatch.Elapsed.TotalSeconds;
            var smallRowRate = smallRowCounter / smallSw.Elapsed.TotalSeconds;
            var smallRequestRate = smallRequestCounter / smallSw.Elapsed.TotalSeconds;
            var exceptionsRate = Interlocked.Read(ref _exceptionCounter) / stopWatch.Elapsed.TotalSeconds;
            var lastException = _lastException;
            _lastException = null;
            smallSw = Stopwatch.StartNew();
            Console.WriteLine("Rate: {0:f2}/{1:f2} rows/second, {2:f2}/{3:f2} requests/second, {4:f2} exceptions/second: {5}", smallRowRate, rowRate, smallRequestRate, requestRate, exceptionsRate, lastException?.Message ?? "");
        }

        await Task.WhenAll(tasks);
        return 0;
    }

    async Task Worker(ISession session, Statement statement, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var rs = await session.ExecuteAsync(statement);
                Interlocked.Add(ref _rowCounter, rs.Count());
                Interlocked.Increment(ref _requestCounter);
            }
            catch (Exception e)
            {
                Interlocked.Increment(ref _exceptionCounter);
                _lastException = e;
            }
        }
    }
}