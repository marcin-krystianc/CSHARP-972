using System.Threading.Tasks;
using Cassandra;
using Spectre.Console.Cli;

namespace CassandraTesting;

public class PopulateCommand : AsyncCommand<PopulateSettings>
{
    public override async Task<int> ExecuteAsync(CommandContext context, PopulateSettings settings)
    {
        var session = await CassandraUtils.ConnectAsync(settings);
        var ps = session.Prepare("INSERT INTO my_keyspace.my_table (id, row_id, payload) VALUES  (?, ?, ?)");
        ps.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        ps.SetIdempotence(false);
            
        for (var i = 0; i < settings.NumberOfRows; i++)
        {
            await session.ExecuteAsync(ps.Bind(i, i, $"test_{i}")).ConfigureAwait(false);
        }

        return 0;
    }
}