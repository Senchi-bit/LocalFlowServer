using System.Text;
using LocalFlowServer.Discovery;
using LocalFlowServer.Net;
using LocalFlowServer.Transfer;

namespace LocalFlowServer;

internal static class Program
{
    private static async Task<int> Main()
    {
        Console.OutputEncoding = Encoding.UTF8;

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        var inboxPath = Path.Combine(Directory.GetCurrentDirectory(), ServerSettings.InboxDirectoryName);
        var inbox = new InboxStore(inboxPath);
        inbox.EnsureCreated();
        inbox.DeleteLeftoverPartials();

        await using var server = new TcpFileServer(inbox);
        await using var mdns = new MdnsAdvertiser();

        Console.WriteLine("Сервер LocalFlow");
        Console.WriteLine($"Слушаю TCP 0.0.0.0:{server.Port}");
        Console.WriteLine($"Входящие: {inbox.Path}");

        Console.WriteLine(mdns.TryStart()
            ? $"mDNS: {mdns.FullInstanceName}"
            : "mDNS: сервис не объявлен (приём по TCP всё равно работает)");

        Console.WriteLine("Нажмите Ctrl+C для остановки.");
        Console.WriteLine();
        try
        {
            await server.RunAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
        }

        Log.Info("Остановка...");
        return 0;
    }
}
