namespace LocalFlowServer;

internal static class Log
{
    public static void Info(string message) => Write("ИНФО", message);

    public static void Warn(string message) => Write("ВНИМ", message);

    public static void Error(string message) => Write("ОШИБ", message);

    private static void Write(string level, string message)
    {
        Console.WriteLine($"{DateTime.Now:HH:mm:ss}  {level}  {message}");
    }

    public static string FormatSize(long bytes)
    {
        return bytes switch
        {
            < 1024 => $"{bytes} Б",
            < 1024 * 1024 => $"{bytes / 1024.0:0.#} КБ",
            < 1024L * 1024 * 1024 => $"{bytes / (1024.0 * 1024):0.#} МБ",
            _ => $"{bytes / (1024.0 * 1024 * 1024):0.##} ГБ"
        };
    }
}
