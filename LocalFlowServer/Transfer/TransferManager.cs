using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using LocalFlowServer.Protocol;

namespace LocalFlowServer.Transfer;

internal sealed class TransferManager(InboxStore inbox) : IDisposable
{
    private readonly ConcurrentDictionary<Guid, TransferSession> _sessions = new();
    private readonly SemaphoreSlim _sessionSlots = new(ServerSettings.MaxSessions, ServerSettings.MaxSessions);

    public async Task HandleClientAsync(TcpClient client, CancellationToken cancellationToken)
    {
        var remote = (IPEndPoint?)client.Client.RemoteEndPoint
            ?? new IPEndPoint(IPAddress.None, 0);

        client.NoDelay = true;
        client.ReceiveBufferSize = ServerSettings.SocketBufferSize;
        client.SendBufferSize = ServerSettings.SocketBufferSize;

        var stream = client.GetStream();
        TransferSession? session = null;
        var slotTaken = false;
        var helloAccepted = false;

        using var idleCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        idleCts.CancelAfter(ServerSettings.IdleTimeout);

        try
        {
            var first = await PacketCodec.ReadAsync(stream, idleCts.Token);
            if (first is AbortPacket)
                return;
            if (first is not HelloPacket hello)
                throw new InvalidDataException("Ожидался кадр Hello.");

            if (!TryAcceptHello(hello, remote, out session, out var rejectReason, out slotTaken)
                || session is null)
            {
                Log.Warn($"Hello отклонён от {remote.Address}: {rejectReason}");
                await PacketCodec.WriteAsync(
                    stream,
                    new HelloAckPacket(hello.TransferId, HelloAckStatus.Rejected, rejectReason),
                    cancellationToken);
                return;
            }

            if (!_sessions.TryAdd(hello.TransferId, session))
            {
                session.Dispose();
                session = null;
                if (slotTaken)
                {
                    ReleaseSlot();
                    slotTaken = false;
                }

                Log.Warn($"Hello отклонён от {remote.Address}: повторный идентификатор передачи");
                await PacketCodec.WriteAsync(
                    stream,
                    new HelloAckPacket(hello.TransferId, HelloAckStatus.Rejected, "повторный идентификатор передачи"),
                    cancellationToken);
                return;
            }

            await PacketCodec.WriteAsync(
                stream,
                new HelloAckPacket(session.TransferId, HelloAckStatus.Ok, ""),
                cancellationToken);
            helloAccepted = true;
            Log.Info($"[приём] {session.OriginalName} {Log.FormatSize(session.FileSize)} от {remote.Address} ... 0%");

            await ReceiveBodyAsync(stream, session, idleCts, cancellationToken);

            idleCts.CancelAfter(ServerSettings.IdleTimeout);
            var afterBody = await PacketCodec.ReadAsync(stream, idleCts.Token);
            switch (afterBody)
            {
                case AbortPacket abort:
                    Log.Info($"[отмена] {session.OriginalName} клиентом: {abort.Reason}");
                    return;
                case FinPacket fin:
                    await FinishAsync(stream, session, fin, cancellationToken);
                    break;
                default:
                    throw new InvalidDataException("Ожидался кадр Fin.");
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            Log.Warn(session is null
                ? $"[таймаут] простой {ServerSettings.IdleTimeout.TotalSeconds:0} с от {remote.Address}"
                : $"[таймаут] {session.OriginalName} простой {ServerSettings.IdleTimeout.TotalSeconds:0} с");
            if (helloAccepted && session is not null)
                await TrySendAbortAsync(stream, session.TransferId, "таймаут простоя");
        }
        catch (OperationCanceledException)
        {
            if (helloAccepted && session is not null)
                await TrySendAbortAsync(stream, session.TransferId, "сервер останавливается");
        }
        catch (EndOfStreamException)
        {
            if (session is not null)
                Log.Info($"[отмена] {session.OriginalName}: соединение разорвано");
        }
        catch (IOException ex)
        {
            Log.Warn($"Соединение с {remote.Address}: {ex.Message}");
        }
        catch (InvalidDataException ex)
        {
            Log.Warn($"Протокол от {remote.Address}: {ex.Message}");
            if (helloAccepted && session is not null)
                await TrySendAbortAsync(stream, session.TransferId, ex.Message);
        }
        catch (Exception ex)
        {
            Log.Error($"Ошибка передачи от {remote.Address}: {ex.Message}");
            if (helloAccepted && session is not null)
                await TrySendAbortAsync(stream, session.TransferId, "ошибка сервера");
        }
        finally
        {
            if (session is not null)
            {
                _sessions.TryRemove(session.TransferId, out _);
                session.Dispose();
            }

            if (slotTaken)
                ReleaseSlot();
        }
    }

    public void Dispose()
    {
        foreach (var id in _sessions.Keys)
        {
            if (_sessions.TryRemove(id, out var session))
                session.Dispose();
        }

        _sessionSlots.Dispose();
    }

    private bool TryAcceptHello(
        HelloPacket hello,
        IPEndPoint remote,
        out TransferSession? session,
        out string rejectReason,
        out bool slotTaken)
    {
        session = null;
        rejectReason = "";
        slotTaken = false;

        switch (hello.FileSize)
        {
            case < 0:
                rejectReason = "Ошибочный размер файла";
                return false;
            case > ServerSettings.MaxFileSizeBytes:
                rejectReason = "файл больше 10 ГиБ";
                return false;
        }

        if (!inbox.TrySanitizeFileName(hello.FileName, out var safeName, out var nameError))
        {
            rejectReason = nameError;
            return false;
        }

        if (!inbox.HasEnoughSpace(hello.FileSize))
        {
            rejectReason = "недостаточно места на диске";
            return false;
        }

        if (!_sessionSlots.Wait(0))
        {
            rejectReason = "слишком много одновременных передач";
            return false;
        }

        slotTaken = true;
        try
        {
            session = new TransferSession(
                hello.TransferId,
                remote,
                safeName,
                safeName,
                hello.FileSize,
                inbox);
            return true;
        }
        catch (Exception ex)
        {
            ReleaseSlot();
            slotTaken = false;
            rejectReason = ex.Message;
            return false;
        }
    }

    private static async Task ReceiveBodyAsync(
        NetworkStream stream,
        TransferSession session,
        CancellationTokenSource idleCts,
        CancellationToken cancellationToken)
    {
        var remaining = session.FileSize;
        if (remaining == 0)
            return;

        var buffer = new byte[ServerSettings.StreamBufferSize];
        while (remaining > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            idleCts.CancelAfter(ServerSettings.IdleTimeout);

            var toRead = (int)Math.Min(buffer.Length, remaining);
            var read = await stream.ReadAsync(buffer.AsMemory(0, toRead), idleCts.Token);
            if (read == 0)
                throw new EndOfStreamException();

            session.Write(buffer.AsSpan(0, read));
            remaining -= read;
        }
    }

    private static async Task FinishAsync(
        NetworkStream stream,
        TransferSession session,
        FinPacket fin,
        CancellationToken cancellationToken)
    {
        var ack = session.Complete(fin.Sha256, out var savedPath);
        await PacketCodec.WriteAsync(stream, ack, cancellationToken);

        switch (ack.Status)
        {
            case FinAckStatus.Ok:
                Log.Info($"[готово] сохранён {Path.Combine(ServerSettings.InboxDirectoryName, Path.GetFileName(savedPath!))}");
                break;
            case FinAckStatus.HashMismatch:
                Log.Warn($"[сбой] {session.OriginalName}: не совпал SHA-256");
                break;
            case FinAckStatus.Error:
                Log.Warn($"[сбой] {session.OriginalName}: {ack.Reason}");
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    private static async Task TrySendAbortAsync(NetworkStream stream, Guid transferId, string reason)
    {
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            await PacketCodec.WriteAsync(stream, new AbortPacket(transferId, reason), cts.Token);
        }
        catch
        {
            // ignored
        }
    }

    private void ReleaseSlot()
    {
        try
        {
            _sessionSlots.Release();
        }
        catch (ObjectDisposedException)
        {
        }
    }
}
