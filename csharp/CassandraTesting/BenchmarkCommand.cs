using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Spectre.Console;
using Spectre.Console.Cli;

namespace CassandraTesting;

public sealed class BenchmarkCommand : AsyncCommand<BenchmarkSettings>
{
    private long _rowCounter = 0;
    private long _requestCounter = 0;

    public override async Task<int> ExecuteAsync(CommandContext context, BenchmarkSettings settings)
    {
        var session = await CassandraUtils.ConnectAsync(settings);
        var cts = new CancellationTokenSource();

        var statement = new SimpleStatement($"SELECT * FROM my_table where id < {settings.NumberOfRows} ALLOW FILTERING");
        statement.SetConsistencyLevel(ConsistencyLevel.LocalOne);
        statement.SetReadTimeoutMillis(120000);
       
        var tasks = Enumerable.Range(0, settings.TaskCount)
            .Select(x => Worker(session, statement, cts.Token))
            .ToList();

        var stopWatch = Stopwatch.StartNew();
        cts.CancelAfter(TimeSpan.FromSeconds(settings.Duration));

        while (!cts.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(5));
            var rowRate = Interlocked.Read(ref _rowCounter) / stopWatch.Elapsed.TotalSeconds;
            var requestRate = Interlocked.Read(ref _requestCounter) / stopWatch.Elapsed.TotalSeconds;
            Console.WriteLine("Rate: {0} rows/second, {1} requests/second", rowRate, requestRate);
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
                Console.WriteLine($"Exception: {e.Message}");
            }
        }
    }
}