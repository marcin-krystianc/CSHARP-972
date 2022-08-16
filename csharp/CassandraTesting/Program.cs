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
                c.AddCommand<PopulateCommand>("populate");
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
    }
}