namespace MessageQueue
{
    public class MessageProducer
    {
        private readonly MessageQueue<Message> _queue;
        private readonly string _name;

        public MessageProducer(MessageQueue<Message> queue, string name)
        {
            _queue = queue;
            _name = name;
        }

        public async Task ProduceMessagesAsync(int count, int delayMs = 1000, CancellationToken cancellationToken = default)
        {
            for (int i = 1; i <= count; i++)
            {
                var message = new Message
                {
                    Content = $"[{_name} - Message - 0{i}]"
                };

                await _queue.EnqueueAsync(message, _name, cancellationToken);
                await Task.Delay(delayMs, cancellationToken);
            }

            Console.WriteLine($"{_name} finished producing {count} messages | [{DateTime.UtcNow:HH:mm:ss.fff}]");
        }
    }
}