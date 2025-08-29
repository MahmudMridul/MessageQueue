

namespace MessageQueue
{
    public class MessageProducer
    {
        private readonly MessageQueue _queue;
        private readonly string _name;

        public MessageProducer(MessageQueue queue, string name)
        {
            _queue = queue;
            _name = name;
        }

        public async Task ProduceMessagesAsync(int count, int delayMs = 1000)
        {
            for (int i = 1; i <= count; i++)
            {
                var message = new Message
                {
                    Content = $"{_name} message-{i}"
                };

                _queue.Enqueue(message, _name);
                await Task.Delay(delayMs);
            }

            Console.WriteLine($"{_name} finished producing {count} messages");
        }
    }
}
