namespace LocalFlowServer.Protocol;

/// <summary>Типы UDP-пакетов LocalFlow. Клиент → сервер: Hello, Data, Fin, Abort.</summary>
internal enum PacketType : byte
{
    Hello = 1,
    HelloAck = 2,
    Data = 3,
    Sack = 4,
    Fin = 5,
    FinAck = 6,
    Abort = 7
}

internal enum HelloAckStatus : byte
{
    Ok = 0,
    Rejected = 1
}

internal enum FinAckStatus : byte
{
    Ok = 0,
    Incomplete = 1,
    HashMismatch = 2,
    Error = 3
}

internal abstract record ProtocolPacket(Guid TransferId);

internal sealed record HelloPacket(
    Guid TransferId,
    string FileName,
    long FileSize,
    uint ProposedChunkSize,
    uint TotalChunks,
    byte[] Sha256) : ProtocolPacket(TransferId);

internal sealed record HelloAckPacket(
    Guid TransferId,
    HelloAckStatus Status,
    uint ChunkSize,
    uint WindowSize,
    string Reason) : ProtocolPacket(TransferId);

internal sealed record DataPacket(
    Guid TransferId,
    uint Seq,
    byte[] Payload) : ProtocolPacket(TransferId);

internal sealed record SackPacket(
    Guid TransferId,
    uint FirstMissing,
    byte[] Bitmap) : ProtocolPacket(TransferId);

internal sealed record FinPacket(
    Guid TransferId,
    uint TotalChunks,
    byte[] Sha256) : ProtocolPacket(TransferId);

internal sealed record FinAckPacket(
    Guid TransferId,
    FinAckStatus Status,
    uint FirstMissing,
    byte[] Bitmap) : ProtocolPacket(TransferId);

internal sealed record AbortPacket(
    Guid TransferId,
    string Reason) : ProtocolPacket(TransferId);
