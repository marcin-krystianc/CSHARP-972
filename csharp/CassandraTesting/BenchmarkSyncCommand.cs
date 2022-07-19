using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Cassandra;
using Spectre.Console;
using Spectre.Console.Cli;

namespace CassandraTesting;

public sealed class BenchmarkSyncCommand : Command<BenchmarkSettings>
{
    private long _rowCounter = 0;
    private long _requestCounter = 0;
    private long _exceptionCounter = 0;
    private Exception _lastException;
    
    public override int Execute(CommandContext context, BenchmarkSettings settings)
    {
        var session = CassandraUtils.Connect(settings);
        var cts = new CancellationTokenSource();

        var ps = session.Prepare("SELECT * FROM my_table where id < ? ALLOW FILTERING");
        ps.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        ps.SetIdempotence(false);
        var bs = ps.Bind(settings.NumberOfRows);

        var threads = new Thread[settings.TaskCount];
        for (var i = 0; i < settings.TaskCount; i++)
        {
            threads[i] = new Thread(() =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        var results = session.Execute(bs);
                        var count = results.Count();
                        Interlocked.Add(ref _rowCounter, count);
                        Interlocked.Increment(ref _requestCounter);
                    }
                    catch (Exception e)
                    {
                        Interlocked.Increment(ref _exceptionCounter);
                        _lastException = e;
                    }
                }
            });
        }

        for (var i = 0; i < settings.TaskCount; i++)
        {
            threads[i].Start();
        }
        
        var stopWatch = Stopwatch.StartNew();
        cts.CancelAfter(TimeSpan.FromSeconds(settings.Duration));

        while (!cts.IsCancellationRequested)
        {
            Thread.Sleep(TimeSpan.FromSeconds(5));
            var rowRate = Interlocked.Read(ref _rowCounter) / stopWatch.Elapsed.TotalSeconds;
            var requestRate = Interlocked.Read(ref _requestCounter) / stopWatch.Elapsed.TotalSeconds; 
            var exceptionsRate = Interlocked.Read(ref _exceptionCounter) / stopWatch.Elapsed.TotalSeconds;
            var lastException = _lastException;
            _lastException = null;
            Console.WriteLine("Rate: {0:f2} rows/second, {1:f2} requests/second, {2:f2} exceptions/second: {3}", rowRate, requestRate, exceptionsRate, lastException?.Message ?? "");
        }
        
        for (var i = 0; i < settings.TaskCount; i++)
        {
            threads[i].Join();
        }
        
        return 0;
    }
}