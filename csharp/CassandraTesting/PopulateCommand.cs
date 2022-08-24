using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using MoreLinq.Extensions;
using Spectre.Console.Cli;

namespace CassandraTesting;

public class PopulateCommand : AsyncCommand<PopulateSettings>
{
    public override async Task<int> ExecuteAsync(CommandContext context, PopulateSettings settings)
    {
        var session = await CassandraUtils.ConnectAsync(settings);
        await session.ExecuteAsync(new SimpleStatement($"CREATE KEYSPACE IF NOT EXISTS {settings.Keyspace} WITH replication = " + 
                                                       " { 'class': 'SimpleStrategy', 'replication_factor': '1' }"));

        session.ChangeKeyspace($"{settings.Keyspace}");
        var columns = new [] {"partition_id int", "row_id int", "payload blob"};
        var extraColumns = Enumerable.Range(0, 1)
            .Select(x => $"double_{x} double")
            .ToArray();

        var primaryKeyString = string.Join(", ", columns.Take(2).Select(x => x.Split(" ").First()));
        var columnsWithTypesString = string.Join(", ", columns.Concat(extraColumns));
        var columnsWithoutTypesString = string.Join(", ", columns.Concat(extraColumns).Select(x => x.Split(" ").First()));
       
        await session.ExecuteAsync(new SimpleStatement($"CREATE TABLE IF NOT EXISTS my_table({columnsWithTypesString}, PRIMARY KEY({primaryKeyString}))"));

        var valuesString = string.Join(" ,", extraColumns.Select((v, i) => i.ToString()));
        var ps = session.Prepare($"INSERT INTO my_table ({columnsWithoutTypesString}) VALUES  (?, ?, ?, {valuesString})");
        ps.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        
        foreach (var batch in EnumerateStatements(ps, settings).Batch(10))
        {
            var bs = new BatchStatement();
            foreach (var s in batch)
            {
                bs.Add(s);
            }

            await session.ExecuteAsync(bs);
        }

        return 0;
    }

    IEnumerable<Statement> EnumerateStatements(PreparedStatement ps, PopulateSettings settings)
    {
        var rnd = new Random(42);
        var payload = new byte[80 * 8];
        rnd.NextBytes(payload);

        for (var partition = 0; partition < settings.NumberOfPartitions; partition++)
        {
            int numberOfRows = Convert.ToInt32(Math.Pow(10, partition));
            for (var i = 0; i < numberOfRows; i++)
            {
                yield  return ps.Bind(partition, i, payload);
            }
        }
    }
}