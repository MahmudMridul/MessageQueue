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
            Console.WriteLine($"{_name} started consuming... | [{DateTime.UtcNow:HH:mm:ss.fff}]");

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var message = await _queue.DequeueAsync(_name, cancellationToken);

                    if (message != null)
                    {
                        await ProcessMessageAsync(message);
                    }
                    else
                    {
                        break; // Queue was stopped
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected when cancellation is requested
            }

            Console.WriteLine($"{_name} stopped consuming | [{DateTime.UtcNow:HH:mm:ss.fff}]");
        }

        private async Task ProcessMessageAsync(Message message)
        {
            Console.WriteLine($"{_name} processing {message} | [{DateTime.UtcNow:HH:mm:ss.fff}]");

            // Simulate processing time
            await Task.Delay(500);

            Console.WriteLine($"{_name} completed {message.Content} | [{DateTime.UtcNow:HH:mm:ss.fff}]");
        }
    }
}