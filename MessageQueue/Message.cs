
namespace MessageQueue
{
    public class Message
    {
        public string Content { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; } = DateTime.UtcNow;

        public override string ToString()
        {
            return $"{Content} - [{Timestamp:HH:mm:ss}]";
        }
    }
}
