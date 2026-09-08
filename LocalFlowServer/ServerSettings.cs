namespace LocalFlowServer;

internal static class ServerSettings
{
    public const int ListenPort = 45123;
    public const int MaxDatagramSize = 1472;
    public const int HeaderSize = 22;

    /// <summary>
    /// Потолок полезной нагрузки, чтобы IP+UDP+заголовок укладывались в типичный MTU 1500 байт.
    /// </summary>
    public const int MaxChunkPayload = 1200;
    public const int MinChunkPayload = 256;
    public const int WindowSize = 64;

    public const int MaxSessions = 8;
    public const long MaxFileSizeBytes = 10L * 1024 * 1024 * 1024;
    public const int MaxFileNameBytes = 255;

    public static readonly TimeSpan IdleTimeout = TimeSpan.FromSeconds(60);
    public static readonly TimeSpan IdleSweepInterval = TimeSpan.FromSeconds(5);

    public const string ServiceType = "_localflow._udp";
    public const string InboxDirectoryName = "inbox";
}
