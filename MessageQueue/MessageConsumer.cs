namespace MessageQueue
{
    public class MessageConsumer
    {
        private readonly MessageQueue _queue;
        private readonly string _name;

        public MessageConsumer(MessageQueue queue, string name)
        {
            _queue = queue;
            _name = name;
        }

        public async void ConsumeMessagesAsync(int delayMs = 500)
        {
            Console.WriteLine($"{_name} started consuming...");

            while (!_queue.isEmpty()) 
            {
                var message = _queue.Dequeue(_name);

                if (message != null)
                {
                    await ProcessMessageAsync(message, delayMs);
                }
            }

            Console.WriteLine($"{_name} stopped consuming");
        }

        private async Task ProcessMessageAsync(Message message, int delayMs)
        {
            Console.WriteLine($"{_name} processing {message}");

            // Simulate processing time
            await Task.Delay(delayMs);

            Console.WriteLine($"{_name} completed processing {message}");
        }
    }
}
