using System.Buffers.Binary;
using System.Text;

namespace LocalFlowServer.Protocol;

/// <summary>
/// Двоичный формат LocalFlow UDP v1 (младший байт первым).
///
/// Заголовок (22 байта): магическое "LFLW" | version=1 | type | transferId (16-байтовый .NET Guid).
///
/// Hello:    uint16 nameLen | utf8-имя | int64 fileSize | uint32 chunkSize | uint32 totalChunks | SHA-256 (32 байта)
/// HelloAck: uint8 status | uint32 chunkSize | uint32 windowSize | uint16 reasonLen | utf8-причина
/// Data:     uint32 seq | uint16 len | полезная нагрузка
/// Sack:     uint32 firstMissing | uint16 bitmapLen | битовая маска
///           Бит i (младший бит байта 0 — i=0) означает, что пакет (firstMissing + 1 + i) получен.
///           firstMissing == totalChunks — все куски на месте.
/// Fin:      uint32 totalChunks | SHA-256 (32 байта)
/// FinAck:   uint8 status | uint32 firstMissing | uint16 bitmapLen | битовая маска
/// Abort:    uint16 reasonLen | utf8-причина
///
/// SHA-256 из одних нулей означает «хеш не передан». Датаграмма должна быть меньше 1472 байт.
/// </summary>
internal static class PacketCodec
{
    private static ReadOnlySpan<byte> Magic => "LFLW"u8;
    private const byte Version = 1;
    private const int Sha256Size = 32;

    public static bool TryDecode(ReadOnlySpan<byte> buffer, out ProtocolPacket? packet)
    {
        packet = null;
        if (buffer.Length < ServerSettings.HeaderSize)
            return false;

        if (!buffer[..4].SequenceEqual(Magic))
            return false;

        if (buffer[4] != Version)
            return false;

        var type = (PacketType)buffer[5];
        var transferId = new Guid(buffer.Slice(6, 16));
        var payload = buffer[ServerSettings.HeaderSize..];

        try
        {
            packet = type switch
            {
                PacketType.Hello => DecodeHello(transferId, payload),
                PacketType.HelloAck => DecodeHelloAck(transferId, payload),
                PacketType.Data => DecodeData(transferId, payload),
                PacketType.Sack => DecodeSack(transferId, payload),
                PacketType.Fin => DecodeFin(transferId, payload),
                PacketType.FinAck => DecodeFinAck(transferId, payload),
                PacketType.Abort => DecodeAbort(transferId, payload),
                _ => null
            };
        }
        catch (InvalidDataException)
        {
            packet = null;
            return false;
        }

        return packet is not null;
    }

    public static byte[] Encode(HelloAckPacket packet)
    {
        var reasonBytes = Encoding.UTF8.GetBytes(packet.Reason);
        var length = ServerSettings.HeaderSize + 1 + 4 + 4 + 2 + reasonBytes.Length;
        var buffer = new byte[length];
        var span = buffer.AsSpan();
        WriteHeader(span, PacketType.HelloAck, packet.TransferId);
        var w = ServerSettings.HeaderSize;
        span[w++] = (byte)packet.Status;
        BinaryPrimitives.WriteUInt32LittleEndian(span[w..], packet.ChunkSize);
        w += 4;
        BinaryPrimitives.WriteUInt32LittleEndian(span[w..], packet.WindowSize);
        w += 4;
        WriteUtf8(span, ref w, reasonBytes);
        return buffer;
    }

    public static byte[] Encode(SackPacket packet)
    {
        var length = ServerSettings.HeaderSize + 4 + 2 + packet.Bitmap.Length;
        var buffer = new byte[length];
        var span = buffer.AsSpan();
        WriteHeader(span, PacketType.Sack, packet.TransferId);
        var w = ServerSettings.HeaderSize;
        BinaryPrimitives.WriteUInt32LittleEndian(span[w..], packet.FirstMissing);
        w += 4;
        BinaryPrimitives.WriteUInt16LittleEndian(span[w..], (ushort)packet.Bitmap.Length);
        w += 2;
        packet.Bitmap.CopyTo(span[w..]);
        return buffer;
    }

    public static byte[] Encode(FinAckPacket packet)
    {
        var length = ServerSettings.HeaderSize + 1 + 4 + 2 + packet.Bitmap.Length;
        var buffer = new byte[length];
        var span = buffer.AsSpan();
        WriteHeader(span, PacketType.FinAck, packet.TransferId);
        var w = ServerSettings.HeaderSize;
        span[w++] = (byte)packet.Status;
        BinaryPrimitives.WriteUInt32LittleEndian(span[w..], packet.FirstMissing);
        w += 4;
        BinaryPrimitives.WriteUInt16LittleEndian(span[w..], (ushort)packet.Bitmap.Length);
        w += 2;
        packet.Bitmap.CopyTo(span[w..]);
        return buffer;
    }

    public static byte[] Encode(AbortPacket packet)
    {
        var reasonBytes = Encoding.UTF8.GetBytes(packet.Reason);
        var length = ServerSettings.HeaderSize + 2 + reasonBytes.Length;
        var buffer = new byte[length];
        var span = buffer.AsSpan();
        WriteHeader(span, PacketType.Abort, packet.TransferId);
        var w = ServerSettings.HeaderSize;
        WriteUtf8(span, ref w, reasonBytes);
        return buffer;
    }

    private static void WriteHeader(Span<byte> dest, PacketType type, Guid transferId)
    {
        Magic.CopyTo(dest);
        dest[4] = Version;
        dest[5] = (byte)type;
        transferId.TryWriteBytes(dest.Slice(6, 16));
    }

    private static void WriteUtf8(Span<byte> dest, ref int w, byte[] utf8)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(dest[w..], (ushort)utf8.Length);
        w += 2;
        utf8.CopyTo(dest[w..]);
        w += utf8.Length;
    }

    private static HelloPacket DecodeHello(Guid transferId, ReadOnlySpan<byte> payload)
    {
        var r = 0;
        var name = ReadUtf8(payload, ref r, ServerSettings.MaxFileNameBytes);
        Need(payload, r, 8 + 4 + 4 + Sha256Size);
        var fileSize = BinaryPrimitives.ReadInt64LittleEndian(payload[r..]);
        r += 8;
        var chunkSize = BinaryPrimitives.ReadUInt32LittleEndian(payload[r..]);
        r += 4;
        var totalChunks = BinaryPrimitives.ReadUInt32LittleEndian(payload[r..]);
        r += 4;
        var sha = payload.Slice(r, Sha256Size).ToArray();
        return new HelloPacket(transferId, name, fileSize, chunkSize, totalChunks, sha);
    }

    private static HelloAckPacket DecodeHelloAck(Guid transferId, ReadOnlySpan<byte> payload)
    {
        var r = 0;
        Need(payload, r, 1 + 4 + 4);
        var status = (HelloAckStatus)payload[r++];
        var chunkSize = BinaryPrimitives.ReadUInt32LittleEndian(payload[r..]);
        r += 4;
        var windowSize = BinaryPrimitives.ReadUInt32LittleEndian(payload[r..]);
        r += 4;
        var reason = ReadUtf8(payload, ref r, 1024);
        return new HelloAckPacket(transferId, status, chunkSize, windowSize, reason);
    }

    private static DataPacket DecodeData(Guid transferId, ReadOnlySpan<byte> payload)
    {
        Need(payload, 0, 6);
        var seq = BinaryPrimitives.ReadUInt32LittleEndian(payload);
        var len = BinaryPrimitives.ReadUInt16LittleEndian(payload[4..]);
        Need(payload, 6, len);
        if (payload.Length != 6 + len)
            throw new InvalidDataException("Несовпадение длины пакета Data.");
        return new DataPacket(transferId, seq, payload.Slice(6, len).ToArray());
    }

    private static SackPacket DecodeSack(Guid transferId, ReadOnlySpan<byte> payload)
    {
        var (firstMissing, bitmap) = ReadSackBody(payload, 0);
        return new SackPacket(transferId, firstMissing, bitmap);
    }

    private static FinPacket DecodeFin(Guid transferId, ReadOnlySpan<byte> payload)
    {
        Need(payload, 0, 4 + Sha256Size);
        var totalChunks = BinaryPrimitives.ReadUInt32LittleEndian(payload);
        var sha = payload.Slice(4, Sha256Size).ToArray();
        return new FinPacket(transferId, totalChunks, sha);
    }

    private static FinAckPacket DecodeFinAck(Guid transferId, ReadOnlySpan<byte> payload)
    {
        Need(payload, 0, 1);
        var status = (FinAckStatus)payload[0];
        var (firstMissing, bitmap) = ReadSackBody(payload, 1);
        return new FinAckPacket(transferId, status, firstMissing, bitmap);
    }

    private static AbortPacket DecodeAbort(Guid transferId, ReadOnlySpan<byte> payload)
    {
        var r = 0;
        var reason = ReadUtf8(payload, ref r, 1024);
        return new AbortPacket(transferId, reason);
    }

    private static (uint FirstMissing, byte[] Bitmap) ReadSackBody(ReadOnlySpan<byte> payload, int offset)
    {
        Need(payload, offset, 6);
        var firstMissing = BinaryPrimitives.ReadUInt32LittleEndian(payload[offset..]);
        var bitmapLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[(offset + 4)..]);
        Need(payload, offset + 6, bitmapLen);
        return (firstMissing, payload.Slice(offset + 6, bitmapLen).ToArray());
    }

    private static string ReadUtf8(ReadOnlySpan<byte> payload, ref int r, int maxBytes)
    {
        Need(payload, r, 2);
        var len = BinaryPrimitives.ReadUInt16LittleEndian(payload[r..]);
        r += 2;
        if (len > maxBytes)
            throw new InvalidDataException("Слишком длинная строка.");
        Need(payload, r, len);
        var value = Encoding.UTF8.GetString(payload.Slice(r, len));
        r += len;
        return value;
    }

    private static void Need(ReadOnlySpan<byte> payload, int offset, int count)
    {
        if (offset < 0 || count < 0 || offset + count > payload.Length)
            throw new InvalidDataException("Обрезанный пакет.");
    }
}
