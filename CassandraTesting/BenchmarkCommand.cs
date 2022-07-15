using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Spectre.Console;
using Spectre.Console.Cli;

namespace CassandraTesting;

public sealed class BenchmarkCommand : AsyncCommand<BenchmarkSettings>
{
    private static bool isRunning = true;
    
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
    
    public override async Task<int> ExecuteAsync(CommandContext context, BenchmarkSettings settings)
    {
        var certCollection = new X509Certificate2Collection();
        var crtFileName = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "sf-class2-root.crt");
        var amazoncert = new X509Certificate2(crtFileName);
        certCollection.Add(amazoncert);
            
        var cluster = Cluster.Builder()
            .AddContactPoint(settings.Hostname)
            .WithPort(settings.Port)
            .WithSSL(new SSLOptions().SetCertificateCollection(certCollection))
            .WithCredentials(settings.Login, settings.Password)
            .WithSocketOptions(new SocketOptions().SetTcpNoDelay(true).SetReadTimeoutMillis(0))
            .Build();
            
        var session = await cluster.ConnectAsync(settings.Keyspace);

        var ps = session.Prepare("SELECT * FROM my_table where id = ?");
        ps.SetConsistencyLevel(ConsistencyLevel.One);
        ps.SetIdempotence(false);

        const int TASK_COUNT = 128;
        Task<long>[] tasks = new Task<long>[TASK_COUNT];
        for (var i = 0; i < TASK_COUNT; i++)
        {
            tasks[i] = worker(session, ps);
        }
        Stopwatch stopWatch = Stopwatch.StartNew();
        Thread.Sleep(60000);
        isRunning = false;
        Task.WaitAll(tasks);
        stopWatch.Stop();
        long count = 0;
        for (var i = 0; i < TASK_COUNT; i++)
        {
            count += tasks[i].Result;
        }
        double rate = count / (stopWatch.ElapsedMilliseconds / 1000.0);
        Console.WriteLine("Rate: {0} rows/second", rate);
        
        await Task.Delay(0);
        return 0;
    }

    static async Task<long> worker(ISession session, PreparedStatement ps)
    {
        long subtotal = 0;
        while (isRunning) {
            var rs = await session.ExecuteAsync(ps.Bind(1)).ConfigureAwait(false);
            foreach (var row in rs)
            {
                subtotal++;
            }
        }

        return subtotal;
    }
}