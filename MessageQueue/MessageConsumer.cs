namespace MessageQueue
{
    public class MessageConsumer
    {
        private readonly MessageQueue<Message> _queue;
        private readonly string _name;

        public MessageConsumer(MessageQueue<Message> queue, string name)
        {
            _queue = queue;
            _name = name;
        }

        public async Task ConsumeMessagesAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine($"{_name} started consuming...");

            while (!cancellationToken.IsCancellationRequested)
            {
                var message = await _queue.DequeueAsync(cancellationToken);

                if (message != null)
                {
                    await ProcessMessageAsync(message);
                }
            }

            Console.WriteLine($"{_name} stopped consuming");
        }

        private async Task ProcessMessageAsync(Message message)
        {
            Console.WriteLine($"{_name} processing: {message}");

            // Simulate processing time
            await Task.Delay(500);

            Console.WriteLine($"{_name} completed: {message.Content}");
        }
    }
}
