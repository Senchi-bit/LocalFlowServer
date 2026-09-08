using System.Collections;
using System.Net;
using System.Security.Cryptography;
using LocalFlowServer.Protocol;

namespace LocalFlowServer.Transfer;

internal sealed class TransferSession : IDisposable
{
    private readonly object _sync = new();
    private readonly InboxStore _inbox;
    private readonly FileStream _stream;
    private readonly BitArray _received;
    private readonly byte[] _expectedHash;
    private readonly string _partialPath;
    private readonly string _safeName;
    private bool _disposed;
    private bool _committed;
    private int _nextLogPercent = 10;

    public TransferSession(
        Guid transferId,
        IPEndPoint remote,
        string originalName,
        string safeName,
        long fileSize,
        uint chunkSize,
        byte[] expectedHash,
        InboxStore inbox)
    {
        TransferId = transferId;
        Remote = remote;
        OriginalName = originalName;
        FileSize = fileSize;
        ChunkSize = chunkSize;
        TotalChunks = fileSize == 0 ? 0 : (uint)((fileSize + chunkSize - 1) / chunkSize);

        _inbox = inbox;
        _safeName = safeName;
        _expectedHash = expectedHash;
        _partialPath = inbox.CreatePartialPath(transferId);
        _received = new BitArray((int)TotalChunks);
        LastActivityUtc = DateTime.UtcNow;

        FileStream? stream = null;
        try
        {
            stream = new FileStream(
                _partialPath,
                FileMode.CreateNew,
                FileAccess.ReadWrite,
                FileShare.None,
                bufferSize: 64 * 1024,
                FileOptions.RandomAccess);
            stream.SetLength(fileSize);
            _stream = stream;
        }
        catch
        {
            stream?.Dispose();
            inbox.DeleteIfExists(_partialPath);
            throw;
        }
    }

    public Guid TransferId { get; }
    public IPEndPoint Remote { get; }
    public string OriginalName { get; }
    public long FileSize { get; }
    public uint ChunkSize { get; }
    public uint TotalChunks { get; }
    public uint FirstMissing { get; private set; }
    public uint ReceivedCount { get; private set; }
    public DateTime LastActivityUtc { get; private set; }

    public bool IsComplete => FirstMissing == TotalChunks;

    public void Touch()
    {
        lock (_sync)
            LastActivityUtc = DateTime.UtcNow;
    }

    public void AcceptData(uint seq, ReadOnlySpan<byte> payload)
    {
        lock (_sync)
        {
            LastActivityUtc = DateTime.UtcNow;
            if (seq >= TotalChunks)
                return;

            var expected = ExpectedPayloadSize(seq);
            if (payload.Length != expected)
            {
                Log.Warn($"[{OriginalName}] кусок {seq}: размер {payload.Length}, ожидался {expected}");
                return;
            }

            if (_received[(int)seq])
                return;

            _stream.Position = (long)seq * ChunkSize;
            _stream.Write(payload);
            _received[(int)seq] = true;
            ReceivedCount++;

            if (seq == FirstMissing)
            {
                while (FirstMissing < TotalChunks && _received[(int)FirstMissing])
                    FirstMissing++;
            }

            LogProgressIfNeeded();
        }
    }

    public SackPacket CreateSack()
    {
        lock (_sync)
            return new SackPacket(TransferId, FirstMissing, BuildBitmap());
    }

    public FinAckPacket Complete(FinPacket fin, out string? savedPath)
    {
        savedPath = null;
        lock (_sync)
        {
            LastActivityUtc = DateTime.UtcNow;
            if (!IsComplete)
                return new FinAckPacket(TransferId, FinAckStatus.Incomplete, FirstMissing, BuildBitmap());

            _stream.Flush();
            _stream.Position = 0;
            var actual = SHA256.HashData(_stream);

            var expected = ChooseExpectedHash(fin.Sha256);
            if (expected is not null && !CryptographicOperations.FixedTimeEquals(actual, expected))
                return new FinAckPacket(TransferId, FinAckStatus.HashMismatch, TotalChunks, []);

            _stream.Dispose();
            _disposed = true;

            try
            {
                savedPath = _inbox.Commit(_partialPath, _safeName);
                _committed = true;
            }
            catch (Exception ex)
            {
                Log.Error($"Не удалось сохранить {OriginalName}: {ex.Message}");
                _inbox.DeleteIfExists(_partialPath);
                return new FinAckPacket(TransferId, FinAckStatus.Error, TotalChunks, []);
            }

            return new FinAckPacket(TransferId, FinAckStatus.Ok, TotalChunks, []);
        }
    }

    public void Dispose()
    {
        lock (_sync)
        {
            if (!_disposed)
            {
                _stream.Dispose();
                _disposed = true;
            }

            if (!_committed)
                _inbox.DeleteIfExists(_partialPath);
        }
    }

    private byte[]? ChooseExpectedHash(byte[] finHash)
    {
        if (!IsZeroHash(_expectedHash))
            return _expectedHash;
        
        return !IsZeroHash(finHash) ? finHash : null;
    }

    private static bool IsZeroHash(byte[] hash)
    {
        return hash.All(b => b == 0);
    }

    private uint ExpectedPayloadSize(uint seq)
    {
        if (seq + 1 < TotalChunks)
            return ChunkSize;

        var remainder = FileSize - (long)seq * ChunkSize;
        return remainder <= 0 ? 0 : (uint)remainder;
    }

    private byte[] BuildBitmap()
    {
        if (FirstMissing >= TotalChunks)
            return [];

        var remainingAfter = (int)(TotalChunks - FirstMissing - 1);
        if (remainingAfter <= 0)
            return [];

        var bits = Math.Min(ServerSettings.WindowSize, remainingAfter);
        var bytes = new byte[(bits + 7) / 8];
        for (var i = 0; i < bits; i++)
        {
            var seq = (int)(FirstMissing + 1 + i);
            if (seq < _received.Length && _received[seq])
                bytes[i / 8] |= (byte)(1 << (i % 8));
        }

        return bytes;
    }

    private void LogProgressIfNeeded()
    {
        if (TotalChunks == 0)
            return;

        var percent = (int)(ReceivedCount * 100 / TotalChunks);
        if (percent < _nextLogPercent)
            return;

        Log.Info($"[приём] {OriginalName} {Log.FormatSize(FileSize)} от {Remote.Address} ... {percent}%");
        _nextLogPercent = Math.Min(100, ((percent / 10) + 1) * 10);
    }
}
