using Haukcode.Mdns;

namespace LocalFlowServer.Discovery;

internal sealed class MdnsAdvertiser : IAsyncDisposable
{
    private Haukcode.Mdns.MdnsAdvertiser? _inner;

    public MdnsAdvertiser()
    {
        var machine = ServiceProfile.SanitizeHostLabel(Environment.MachineName);
        InstanceName = $"LocalFlow-{machine}";
    }

    public string InstanceName { get; }

    public string ServiceType => ServerSettings.ServiceType;

    public string FullInstanceName => $"{InstanceName}.{ServiceType}.local";

    public bool TryStart()
    {
        try
        {
            var profile = new ServiceProfile(
                InstanceName,
                ServiceType,
                (ushort)ServerSettings.ListenPort,
                new Dictionary<string, string> { ["txtvers"] = "1" });

            _inner = new Haukcode.Mdns.MdnsAdvertiser(profile);
            _inner.Start();
            return true;
        }
        catch (Exception ex)
        {
            Log.Warn($"Не удалось объявить сервис по mDNS: {ex.Message}");
            _inner = null;
            return false;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_inner is null)
            return;

        await _inner.DisposeAsync();
        _inner = null;
    }
}
