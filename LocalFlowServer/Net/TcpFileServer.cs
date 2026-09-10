using System.Net;
using System.Net.Sockets;
using LocalFlowServer.Transfer;

namespace LocalFlowServer.Net;

internal sealed class TcpFileServer : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly TransferManager _transfers;

    public TcpFileServer(InboxStore inbox)
    {
        _listener = new TcpListener(IPAddress.Any, ServerSettings.ListenPort);
        _listener.Server.ReceiveBufferSize = ServerSettings.SocketBufferSize;
        _listener.Server.SendBufferSize = ServerSettings.SocketBufferSize;
        _transfers = new TransferManager(inbox);
    }

    public int Port => ServerSettings.ListenPort;

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _listener.Start();
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                TcpClient client;
                try
                {
                    client = await _listener.AcceptTcpClientAsync(cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ObjectDisposedException)
                {
                    break;
                }
                catch (SocketException ex) when (cancellationToken.IsCancellationRequested)
                {
                    Log.Warn($"Приём TCP остановлен: {ex.Message}");
                    break;
                }
                catch (SocketException ex)
                {
                    Log.Warn($"Ошибка accept TCP: {ex.Message}");
                    continue;
                }

                _ = HandleClientAsync(client, cancellationToken);
            }
        }
        finally
        {
            _listener.Stop();
        }
    }

    public ValueTask DisposeAsync()
    {
        try
        {
            _listener.Stop();
        }
        catch
        {
            // ignored
        }

        _transfers.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken cancellationToken)
    {
        try
        {
            using (client)
                await _transfers.HandleClientAsync(client, cancellationToken);
        }
        catch (Exception ex)
        {
            Log.Error($"Ошибка соединения: {ex.Message}");
        }
    }
}
