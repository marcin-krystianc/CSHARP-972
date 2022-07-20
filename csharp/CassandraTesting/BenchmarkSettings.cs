using System.ComponentModel;
using Spectre.Console.Cli;

namespace CassandraTesting;

public class BenchmarkSettings : CassandraSettings
{
    [CommandOption("--tasks")]
    [Description("Number of tasks")]
    [DefaultValue(128)]
    public int TaskCount { get; set; }
    
    [CommandOption("--partition")]
    [Description("Id of partition to query")]
    [DefaultValue(0)]
    public int PartitionNumber { get; set; }
    
    [CommandOption("--duration")]
    [Description("Duration in seconds")]
    [DefaultValue(60)]
    public int Duration { get; set; }
}