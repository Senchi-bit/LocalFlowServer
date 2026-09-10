namespace LocalFlowServer;

internal static class ServerSettings
{
    public const int ListenPort = 45123;
    public const int HeaderSize = 26;
    public const int StreamBufferSize = 64 * 1024;
    public const int MaxControlPayload = 1024;
    public const int SocketBufferSize = 1 << 20;

    public const int MaxSessions = 8;
    public const long MaxFileSizeBytes = 10L * 1024 * 1024 * 1024;
    public const int MaxFileNameBytes = 255;

    public static readonly TimeSpan IdleTimeout = TimeSpan.FromSeconds(60);

    public const string ServiceType = "_localflow._tcp";
    public const string InboxDirectoryName = "inbox";
}
