using System.Collections.Concurrent;
using System.Net;
using LocalFlowServer.Protocol;

namespace LocalFlowServer.Transfer;

internal sealed class TransferManager(InboxStore inbox, Action<byte[], IPEndPoint> send) : IDisposable
{
    private readonly ConcurrentDictionary<Guid, TransferSession> _sessions = new();

    public void Handle(ProtocolPacket packet, IPEndPoint remote)
    {
        switch (packet)
        {
            case HelloPacket hello:
                HandleHello(hello, remote);
                break;
            case DataPacket data:
                HandleData(data, remote);
                break;
            case FinPacket fin:
                HandleFin(fin, remote);
                break;
            case AbortPacket abort:
                HandleAbort(abort);
                break;
        }
    }

    public async Task RunIdleWatcherAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(ServerSettings.IdleSweepInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
                SweepIdle();
        }
        catch (OperationCanceledException)
        {
        }
    }

    private void AbortAll(string reason)
    {
        foreach (var id in _sessions.Keys)
        {
            if (_sessions.TryRemove(id, out var session))
                AbortSession(session, reason);
        }
    }

    public void Dispose() => AbortAll("сервер останавливается");

    private void HandleHello(HelloPacket hello, IPEndPoint remote)
    {
        if (_sessions.TryGetValue(hello.TransferId, out var existing))
        {
            existing.Touch();
            SendHelloAck(existing, HelloAckStatus.Ok, "", remote);
            return;
        }

        if (_sessions.Count >= ServerSettings.MaxSessions)
        {
            Reject(hello.TransferId, remote, "слишком много одновременных передач");
            return;
        }

        switch (hello.FileSize)
        {
            case < 0:
                Reject(hello.TransferId, remote, "Ошибочный размер файла");
                return;
            case > ServerSettings.MaxFileSizeBytes:
                Reject(hello.TransferId, remote, "файл больше 10 ГиБ");
                return;
        }

        if (!inbox.TrySanitizeFileName(hello.FileName, out var safeName, out var nameError))
        {
            Reject(hello.TransferId, remote, nameError);
            return;
        }

        if (!inbox.HasEnoughSpace(hello.FileSize))
        {
            Reject(hello.TransferId, remote, "недостаточно места на диске");
            return;
        }

        var chunkSize = hello.ProposedChunkSize == 0
            ? ServerSettings.MaxChunkPayload
            : Math.Clamp(hello.ProposedChunkSize, ServerSettings.MinChunkPayload, ServerSettings.MaxChunkPayload);

        TransferSession session;
        try
        {
            session = new TransferSession(
                hello.TransferId,
                remote,
                safeName,
                safeName,
                hello.FileSize,
                chunkSize,
                hello.Sha256,
                inbox);
        }
        catch (Exception ex)
        {
            Reject(hello.TransferId, remote, ex.Message);
            return;
        }

        if (!_sessions.TryAdd(hello.TransferId, session))
        {
            session.Dispose();
            if (_sessions.TryGetValue(hello.TransferId, out existing))
                SendHelloAck(existing, HelloAckStatus.Ok, "", remote);
            return;
        }

        Log.Info($"[приём] {safeName} {Log.FormatSize(hello.FileSize)} от {remote.Address} ... начато");
        SendHelloAck(session, HelloAckStatus.Ok, "", remote);
    }

    private void HandleData(DataPacket data, IPEndPoint remote)
    {
        if (!_sessions.TryGetValue(data.TransferId, out var session))
        {
            send(PacketCodec.Encode(new AbortPacket(data.TransferId, "неизвестная передача")), remote);
            return;
        }

        try
        {
            session.AcceptData(data.Seq, data.Payload);
        }
        catch (IOException ex)
        {
            Log.Error($"Ошибка записи {session.OriginalName}: {ex.Message}");
            if (_sessions.TryRemove(data.TransferId, out var failed))
                AbortSession(failed, "ошибка записи");
            return;
        }

        send(PacketCodec.Encode(session.CreateSack()), remote);
    }

    private void HandleFin(FinPacket fin, IPEndPoint remote)
    {
        if (!_sessions.TryGetValue(fin.TransferId, out var session))
        {
            send(PacketCodec.Encode(new AbortPacket(fin.TransferId, "неизвестная передача")), remote);
            return;
        }

        var ack = session.Complete(fin, out var savedPath);
        send(PacketCodec.Encode(ack), remote);

        switch (ack.Status)
        {
            case FinAckStatus.Ok:
                _sessions.TryRemove(fin.TransferId, out _);
                session.Dispose();
                Log.Info($"[готово] сохранён {Path.Combine(ServerSettings.InboxDirectoryName, Path.GetFileName(savedPath!))}");
                break;
            case FinAckStatus.Incomplete:
                Log.Info($"[приём] {session.OriginalName} не завершена, нет куска {ack.FirstMissing}");
                break;
            case FinAckStatus.HashMismatch:
                if (_sessions.TryRemove(fin.TransferId, out var mismatch))
                    mismatch.Dispose();
                Log.Warn($"[сбой] {session.OriginalName}: не совпал SHA-256");
                break;
            case FinAckStatus.Error:
                if (_sessions.TryRemove(fin.TransferId, out var errored))
                    errored.Dispose();
                break;
        }
    }

    private void HandleAbort(AbortPacket abort)
    {
        if (!_sessions.TryRemove(abort.TransferId, out var session))
            return;

        Log.Info($"[отмена] {session.OriginalName} клиентом: {abort.Reason}");
        session.Dispose();
    }

    private void SweepIdle()
    {
        var now = DateTime.UtcNow;
        foreach (var pair in _sessions)
        {
            if (now - pair.Value.LastActivityUtc < ServerSettings.IdleTimeout)
                continue;

            if (_sessions.TryRemove(pair.Key, out var session))
            {
                Log.Warn($"[таймаут] {session.OriginalName} простой {ServerSettings.IdleTimeout.TotalSeconds:0} с");
                AbortSession(session, "таймаут простоя");
            }
        }
    }

    private void AbortSession(TransferSession session, string reason)
    {
        try
        {
            send(PacketCodec.Encode(new AbortPacket(session.TransferId, reason)), session.Remote);
        }
        catch (Exception ex)
        {
            Log.Warn($"Не удалось отправить Abort: {ex.Message}");
        }

        session.Dispose();
    }

    private void SendHelloAck(TransferSession session, HelloAckStatus status, string reason, IPEndPoint remote)
    {
        var packet = new HelloAckPacket(
            session.TransferId,
            status,
            session.ChunkSize,
            ServerSettings.WindowSize,
            reason);
        send(PacketCodec.Encode(packet), remote);
    }

    private void Reject(Guid transferId, IPEndPoint remote, string reason)
    {
        Log.Warn($"Hello отклонён от {remote.Address}: {reason}");
        var packet = new HelloAckPacket(transferId, HelloAckStatus.Rejected, 0, 0, reason);
        send(PacketCodec.Encode(packet), remote);
    }
}
