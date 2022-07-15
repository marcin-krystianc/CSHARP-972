using System.ComponentModel;
using Spectre.Console.Cli;

namespace CassandraTesting;

public class BenchmarkSettings : CassandraSettings
{
    [CommandOption("--tasks")]
    [Description("Number of tasks")]
    [DefaultValue(128)]
    public int TaskCount { get; set; }
    
    [CommandOption("--rows")]
    [Description("Number of rows per query")]
    [DefaultValue(1)]
    public int NumberOfRows { get; set; }
    
    [CommandOption("--duration")]
    [Description("Duration in seconds")]
    [DefaultValue(60)]
    public int Duration { get; set; }
}