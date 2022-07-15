using System.ComponentModel;
using Spectre.Console.Cli;

namespace CassandraTesting;

public class BenchmarkSettings : CassandraSettings
{
    [CommandOption("-r|--random")]
    [Description("TODO")]
    public bool IsRandom { get; set; }
}