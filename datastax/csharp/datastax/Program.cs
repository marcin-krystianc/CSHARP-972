
using System;
using Cassandra;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace cassandra_csharp_test
{
    internal class Program
    {
        private static bool isRunning = true;

        static void Main(string[] args)
        {
            var cluster = Cluster.Builder()
                .AddContactPoints("cassandra")
                .WithPort(9042)
                .WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy("datacenter1")))
                .Build();
            
            var session = cluster.Connect("my_keyspace");
            var ps = session.Prepare($"SELECT * FROM my_table where partition_id = ?");
            ps.SetConsistencyLevel(ConsistencyLevel.One);
            ps.SetIdempotence(false);

            const int TASK_COUNT = 32;
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

        static async Task<long> worker(ISession session, PreparedStatement ps)
        {
            long subtotal = 0;
            while (isRunning) {
                var rs = await session.ExecuteAsync(ps.Bind(4)).ConfigureAwait(false);
                foreach (var row in rs)
                {
                    subtotal++;
                }
            }
            return subtotal;
        }
    }
}


