using System.ComponentModel;
using Spectre.Console.Cli;

namespace CassandraTesting;

public class CassandraSettings : CommandSettings
{
    [CommandOption("--hostname")]
    [DefaultValue("cassandra.eu-central-1.amazonaws.com")]
    public string Hostname { get; set; }
    
    [CommandOption("--port")]
    [DefaultValue(9142)]
    public int Port { get; set; }
    
    [CommandOption("--keyspace")]
    [DefaultValue("my_keyspace")]
    public string Keyspace { get; set; }
    
    [CommandOption("-l|--login")]
    public string Login { get; set; }
    
    [CommandOption("-p|--password")]
    public string Password { get; set; }
}