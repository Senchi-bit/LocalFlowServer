using System.Net;
using System.Net.Sockets;
using LocalFlowServer.Protocol;
using LocalFlowServer.Transfer;

namespace LocalFlowServer.Net;

internal sealed class UdpFileServer : IAsyncDisposable
{
    private readonly UdpClient _udp;
    private readonly TransferManager _transfers;
    private readonly object _sendLock = new();

    public UdpFileServer(InboxStore inbox)
    {
        _udp = new UdpClient(new IPEndPoint(IPAddress.Any, ServerSettings.ListenPort));
        _udp.Client.ReceiveBufferSize = 1 << 20;
        _udp.Client.SendBufferSize = 1 << 20;
        _transfers = new TransferManager(inbox, Send);
    }

    public int Port => ServerSettings.ListenPort;

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var idle = _transfers.RunIdleWatcherAsync(cancellationToken);
        var receive = ReceiveLoopAsync(cancellationToken);
        try
        {
            await Task.WhenAll(idle, receive);
        }
        catch (OperationCanceledException)
        {
        }
    }

    public ValueTask DisposeAsync()
    {
        _transfers.Dispose();
        _udp.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            UdpReceiveResult result;
            try
            {
                result = await _udp.ReceiveAsync(cancellationToken);
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
                Log.Warn($"Приём UDP остановлен: {ex.Message}");
                break;
            }
            catch (SocketException ex)
            {
                Log.Warn($"Ошибка приёма UDP: {ex.Message}");
                continue;
            }

            if (!PacketCodec.TryDecode(result.Buffer, out var packet) || packet is null)
                continue;

            try
            {
                _transfers.Handle(packet, result.RemoteEndPoint);
            }
            catch (Exception ex)
            {
                Log.Error($"Ошибка обработки пакета: {ex.Message}");
            }
        }
    }

    private void Send(byte[] datagram, IPEndPoint remote)
    {
        lock (_sendLock)
        {
            try
            {
                _udp.Send(datagram, datagram.Length, remote);
            }
            catch (Exception ex)
            {
                Log.Warn($"Не удалось отправить UDP на {remote}: {ex.Message}");
            }
        }
    }
}
