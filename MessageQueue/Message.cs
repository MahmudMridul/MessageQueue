
namespace MessageQueue
{
    public class Message
    {
        public string Id { get; set; } = Guid.NewGuid().ToString();
        public string Content { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; } = DateTime.UtcNow;

        public override string ToString()
        {
            return $"[{Id}] - {Content} - [{Timestamp:HH:mm:ss}]";
        }
    }
}
