﻿using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Spectre.Console;
using Spectre.Console.Cli;

namespace CassandraTesting;

public sealed class BenchmarkSyncCommand : Command<BenchmarkSettings>
{
    private long _counter = 0;

    public override ValidationResult Validate(CommandContext context, BenchmarkSettings settings)
    {
        if (string.IsNullOrWhiteSpace(settings.Login))
        {
            return ValidationResult.Error("Login is required");
        }

        if (string.IsNullOrWhiteSpace(settings.Password))
        {
            return ValidationResult.Error("Password is required");
        }

        return base.Validate(context, settings);
    }

    public override int Execute(CommandContext context, BenchmarkSettings settings)
    {
        var certCollection = new X509Certificate2Collection();
        var crtFileName = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location),
            "sf-class2-root.crt");
        var amazoncert = new X509Certificate2(crtFileName);
        certCollection.Add(amazoncert);

        var cluster = Cluster.Builder()
            .AddContactPoint(settings.Hostname)
            .WithPort(settings.Port)
            .WithSSL(new SSLOptions().SetCertificateCollection(certCollection))
            .WithCredentials(settings.Login, settings.Password)
            .WithSocketOptions(new SocketOptions().SetTcpNoDelay(true).SetReadTimeoutMillis(0))
            .Build();

        var session = cluster.Connect(settings.Keyspace);

        var cts = new CancellationTokenSource();

        var ps = session.Prepare("SELECT * FROM my_table where id <= ? ALLOW FILTERING");
        ps.SetConsistencyLevel(ConsistencyLevel.Any);
        ps.SetIdempotence(false);
        var bs = ps.Bind(settings.NumberOfRows);

        var threads = new Thread[settings.TaskCount];
        for (var i = 0; i < settings.TaskCount; i++)
        {
            threads[i] = new Thread(async () =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    var sw = Stopwatch.StartNew();
                    var results = session.Execute(bs);
                    var count = results.Count();
                    Interlocked.Add(ref _counter, count);
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
            var rate = Interlocked.Read(ref _counter) / stopWatch.Elapsed.TotalSeconds;
            Console.WriteLine("Rate: {0} rows/second", rate);
        }
        
        for (var i = 0; i < settings.TaskCount; i++)
        {
            threads[i].Join();
        }
        
        return 0;
    }
}