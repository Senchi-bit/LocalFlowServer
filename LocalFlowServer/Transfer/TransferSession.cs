using System.Net;
using System.Security.Cryptography;
using LocalFlowServer.Protocol;

namespace LocalFlowServer.Transfer;

internal sealed class TransferSession : IDisposable
{
    private readonly object _sync = new();
    private readonly InboxStore _inbox;
    private readonly FileStream _stream;
    private readonly IncrementalHash _hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
    private readonly string _partialPath;
    private readonly string _safeName;
    private bool _disposed;
    private bool _hashDisposed;
    private bool _committed;
    private int _nextLogPercent = 10;

    public TransferSession(
        Guid transferId,
        IPEndPoint remote,
        string originalName,
        string safeName,
        long fileSize,
        InboxStore inbox)
    {
        TransferId = transferId;
        Remote = remote;
        OriginalName = originalName;
        FileSize = fileSize;

        _inbox = inbox;
        _safeName = safeName;
        _partialPath = inbox.CreatePartialPath(transferId);

        FileStream? stream = null;
        try
        {
            stream = new FileStream(
                _partialPath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                bufferSize: ServerSettings.StreamBufferSize,
                FileOptions.SequentialScan);
            stream.SetLength(fileSize);
            _stream = stream;
        }
        catch
        {
            stream?.Dispose();
            inbox.DeleteIfExists(_partialPath);
            _hash.Dispose();
            throw;
        }
    }

    public Guid TransferId { get; }
    public IPEndPoint Remote { get; }
    public string OriginalName { get; }
    public long FileSize { get; }
    public long BytesReceived { get; private set; }

    public void Write(ReadOnlySpan<byte> data)
    {
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (BytesReceived + data.Length > FileSize)
                throw new InvalidDataException("Слишком много данных.");

            _stream.Write(data);
            _hash.AppendData(data);
            BytesReceived += data.Length;
            LogProgressIfNeeded();
        }
    }

    public FinAckPacket Complete(byte[] expectedHash, out string? savedPath)
    {
        savedPath = null;
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (BytesReceived != FileSize)
                return new FinAckPacket(TransferId, FinAckStatus.Error, "неполная передача");

            var actual = _hash.GetHashAndReset();
            if (expectedHash.Length != PacketCodec.Sha256Size
                || !CryptographicOperations.FixedTimeEquals(actual, expectedHash))
            {
                return new FinAckPacket(TransferId, FinAckStatus.HashMismatch, "не совпал SHA-256");
            }

            _stream.Flush();
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
                return new FinAckPacket(TransferId, FinAckStatus.Error, "не удалось сохранить файл");
            }

            return new FinAckPacket(TransferId, FinAckStatus.Ok, "");
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

            if (!_hashDisposed)
            {
                _hash.Dispose();
                _hashDisposed = true;
            }

            if (!_committed)
                _inbox.DeleteIfExists(_partialPath);
        }
    }

    private void LogProgressIfNeeded()
    {
        if (FileSize == 0)
            return;

        var percent = (int)(BytesReceived * 100 / FileSize);
        if (percent < _nextLogPercent)
            return;

        Log.Info($"[приём] {OriginalName} {Log.FormatSize(FileSize)} от {Remote.Address} ... {percent}%");
        _nextLogPercent = Math.Min(100, ((percent / 10) + 1) * 10);
    }
}
