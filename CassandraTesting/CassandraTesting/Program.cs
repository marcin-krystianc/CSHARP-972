using System;
using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;

namespace CassandraTesting
{
    public static class Program
    {
        private static bool isRunning = true;
        
        static async Task Main(string[] args)
        {
            var hostnames = new[] {"cassandra.eu-central-1.amazonaws.com"};
            var login = "my_user-at-607264236001";
            var password = "5xhfwt064Edyqn6UpJJ6OGAL7IG1H8RNmWsBQSScwMc=";
            var keyspace = "my_keyspace";
            
            var certCollection = new X509Certificate2Collection();
            var amazoncert = new X509Certificate2(@"D:\sf-class2-root.crt");
            certCollection.Add(amazoncert);
            
            var cluster = Cluster.Builder()
                .AddContactPoints(hostnames)
                .WithPort(9142)
                .WithSSL(new SSLOptions().SetCertificateCollection(certCollection))
                .WithCredentials(login, password)
                .WithSocketOptions(new SocketOptions().SetTcpNoDelay(true).SetReadTimeoutMillis(0))
                .Build();
            
            var session = cluster.Connect();

            // await TruncateData(session);
            await PopulateData(session);
            
            var ps = session.Prepare("SELECT * FROM my_keyspace.my_table");
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
        }

        static async Task TruncateData(ISession session)
        {
            var ss = new SimpleStatement("TRUNCATE my_keyspace.my_table");
            ss.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            ss.SetIdempotence(false);
            await session.ExecuteAsync(ss.Bind()).ConfigureAwait(false);
        }

        static async Task PopulateData(ISession session)
        {
            var ps = session.Prepare("INSERT INTO my_keyspace.my_table (id, row_id, payload) VALUES  (?, ?, ?)");
            ps.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            ps.SetIdempotence(false);
            
            for (var i = 0; i < 1000; i++)
            {
                await session.ExecuteAsync(ps.Bind(i, i, $"test_{i}")).ConfigureAwait(false);
            }
        }

        static async Task<long> worker(ISession session, PreparedStatement ps)
        {
            long subtotal = 0;
            while (isRunning) {
                var rs = await session.ExecuteAsync(ps.Bind()).ConfigureAwait(false);
                foreach (var row in rs)
                {
                    subtotal++;
                }
            }
            return subtotal;
        }
    }
}