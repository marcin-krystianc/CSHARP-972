using System.IO;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Cassandra;

namespace CassandraTesting;

public static class CassandraUtils
{
    public static async Task<ISession> ConnectAsync(CassandraSettings settings)
    {
        var certCollection = new X509Certificate2Collection();
        var crtFileName = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location),
            "sf-class2-root.crt");
        var amazoncert = new X509Certificate2(crtFileName);
        certCollection.Add(amazoncert);

        var poolingOptions = PoolingOptions.Create();
        poolingOptions.SetWarmup(true);
        poolingOptions.SetMaxConnectionsPerHost(HostDistance.Local, 4);
        poolingOptions.SetMaxConnectionsPerHost(HostDistance.Remote, 4);
        poolingOptions.SetMaxConnectionsPerHost(HostDistance.Ignored, 4);
        
        poolingOptions.SetCoreConnectionsPerHost(HostDistance.Local, 4);
        poolingOptions.SetCoreConnectionsPerHost(HostDistance.Remote, 4);
        poolingOptions.SetCoreConnectionsPerHost(HostDistance.Ignored, 4);

        poolingOptions.SetMaxRequestsPerConnection(1024);
        
        var clusterBuilder = Cluster.Builder()
            .AddContactPoint(settings.Hostname)
            .WithPort(settings.Port)
            .WithPoolingOptions(poolingOptions)
            .WithSocketOptions(new SocketOptions().SetTcpNoDelay(true).SetReadTimeoutMillis(0));

        if (!settings.NoSSL)
        {
            clusterBuilder = clusterBuilder.WithSSL(new SSLOptions().SetCertificateCollection(certCollection));
        }
        
        if (!string.IsNullOrWhiteSpace(settings.Login))
        {
            clusterBuilder = clusterBuilder.WithCredentials(settings.Login, settings.Password);
        }
        
        var cluster = clusterBuilder.Build();
        return await cluster.ConnectAsync(settings.Keyspace);
    }
    
    public static ISession Connect(CassandraSettings settings)
    {
        var certCollection = new X509Certificate2Collection();
        var crtFileName = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location),
            "sf-class2-root.crt");
        var amazoncert = new X509Certificate2(crtFileName);
        certCollection.Add(amazoncert);

        var clusterBuilder = Cluster.Builder()
            .AddContactPoint(settings.Hostname)
            .WithPort(settings.Port)
            .WithSocketOptions(new SocketOptions().SetTcpNoDelay(true).SetReadTimeoutMillis(0));

        if (!settings.NoSSL)
        {
            clusterBuilder = clusterBuilder.WithSSL(new SSLOptions().SetCertificateCollection(certCollection));
        }
        
        if (!string.IsNullOrWhiteSpace(settings.Login))
        {
            clusterBuilder = clusterBuilder.WithCredentials(settings.Login, settings.Password);
        }
        
        var cluster = clusterBuilder.Build();
        return cluster.Connect(settings.Keyspace);
    }
}