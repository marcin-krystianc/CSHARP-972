﻿using System.Threading.Tasks;
using Cassandra;
using Spectre.Console.Cli;

namespace CassandraTesting;

public class PopulateCommand : AsyncCommand<PopulateSettings>
{
    public override async Task<int> ExecuteAsync(CommandContext context, PopulateSettings settings)
    {
        var session = await CassandraUtils.ConnectAsync(settings);
        var ps = session.Prepare("INSERT INTO my_keyspace.my_table (parition_id, row_id, payload) VALUES  (?, ?, ?)");
        ps.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        ps.SetIdempotence(false);
            
        for (var partition = 0; partition < settings.NumberOfPartitions; partition++)
        for (var i = 0; i < 10 << partition; i++)
        {
            await session.ExecuteAsync(ps.Bind(partition, i, $"test_{partition}_{i}")).ConfigureAwait(false);
        }

        return 0;
    }
}