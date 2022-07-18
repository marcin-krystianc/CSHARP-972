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

        var cluster = Cluster.Builder()
            .AddContactPoint(settings.Hostname)
            .WithPort(settings.Port)
            .WithSSL(new SSLOptions().SetCertificateCollection(certCollection))
            .WithCredentials(settings.Login, settings.Password)
            .WithSocketOptions(new SocketOptions().SetTcpNoDelay(true).SetReadTimeoutMillis(0))
            .Build();

       return await cluster.ConnectAsync(settings.Keyspace);
    }
    
    public static ISession Connect(CassandraSettings settings)
    {
        var certCollection = new X509Certificate2Collection();
        var crtFileName = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location),
            "sf-class2-root.crt");
        var amazoncert = new X509Certificate2(crtFileName);
        certCollection.Add(amazoncert);

        var cluster = Cluster.Builder()
            .AddContactPoint(settings.Hostname)
            .WithPort(settings.Port)
            .WithSSL(new SSLOptions().SetCertificateCollection(certCollection))
            .WithCredentials(settings.Login, settings.Password)
            .WithSocketOptions(new SocketOptions().SetTcpNoDelay(true).SetReadTimeoutMillis(0))
            .Build();

        return cluster.Connect(settings.Keyspace);
    }
}