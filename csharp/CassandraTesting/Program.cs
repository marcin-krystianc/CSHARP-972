using System;
using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Spectre.Console.Cli;

namespace CassandraTesting
{
    public static class Program
    {
        static async Task Main(string[] args)
        {
            var app = new CommandApp();

            app.Configure(c =>
            {
                c.AddCommand<BenchmarkCommand>("benchmark");
                c.AddCommand<BenchmarkSyncCommand>("benchmark-sync");
            });

            await app.RunAsync(args);
        }

        static async Task TruncateData(ISession session)
        {
            var ss = new SimpleStatement("TRUNCATE my_keyspace.my_table");
            ss.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            ss.SetIdempotence(false);
            await session.ExecuteAsync(ss).ConfigureAwait(false);
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
    }
}