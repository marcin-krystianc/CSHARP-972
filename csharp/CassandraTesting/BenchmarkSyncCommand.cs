using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
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

        var statement = new SimpleStatement($"SELECT * FROM my_table where partition_id = {settings.PartitionNumber}");
        statement.SetConsistencyLevel(ConsistencyLevel.LocalOne);
        statement.SetReadTimeoutMillis(120000);

        var threads = new List<Thread>();
        for (var i = 0; i < settings.TaskCount; i++)
        {
            var thread = new Thread(() =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        var results = session.Execute(statement);
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
            
            threads.Add(thread);
        }

        for (var i = 0; i < settings.TaskCount; i++)
        {
            threads[i].Start();
        }
        
        var stopWatch = Stopwatch.StartNew();
        if (settings.Duration.HasValue)
        {
            cts.CancelAfter(TimeSpan.FromSeconds(settings.Duration.Value));
        }

        if (settings.Records.HasValue)
        {
            var thread = new Thread(() =>
            {
                while (true)
                {
                    Thread.Sleep(1);
                    var rowCounter = Interlocked.Read(ref _rowCounter);
                    var requestCounter = Interlocked.Read(ref _requestCounter);

                    if (rowCounter > settings.Records.Value ||
                        requestCounter > settings.Records.Value)
                    {
                        break;
                    }

                    Console.WriteLine("Read rows {0}, requests {1}", rowCounter, requestCounter);
                    cts.Cancel();
                }
            });
            
            threads.Add(thread);
        }

        long lastRowCounter = 0;
        long lastRequestCounter = 0;
        var smallSw = stopWatch;
        
        while (!cts.IsCancellationRequested)
        {
            Thread.Sleep(TimeSpan.FromSeconds(5));
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
        
        foreach (var thread in threads)
        {
            thread.Join();
        }
        
        return 0;
    }
}