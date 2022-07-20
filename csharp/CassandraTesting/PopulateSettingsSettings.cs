using System.ComponentModel;
using Spectre.Console.Cli;

namespace CassandraTesting;

public class PopulateSettings : CassandraSettings
{
    [CommandOption("--partitions")]
    [Description("Number of rows per query")]
    [DefaultValue(1)]
    public int NumberOfPartitions { get; set; }
    
}