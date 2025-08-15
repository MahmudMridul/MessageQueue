using System.Threading.Channels;

namespace MessageQueue
{
    public class MessageQueue<T>
    {
        private readonly Channel<T> _channel;
        private readonly ChannelWriter<T> _writer;
        private readonly ChannelReader<T> _reader;

        public MessageQueue(int capacity = 100)
        {
            var options = new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false
            };

            _channel = Channel.CreateBounded<T>(options);
            _writer = _channel.Writer;
            _reader = _channel.Reader;
        }

        public int Count => _reader.Count;
        public bool IsEmpty => _reader.Count == 0;

        public async Task EnqueueAsync(T message, string producerName, CancellationToken cancellationToken = default)
        {
            await _writer.WriteAsync(message, cancellationToken);
            Console.WriteLine($"{producerName} enqueued {message}");
        }

        public async Task<T?> DequeueAsync(string consumerName, CancellationToken cancellationToken = default)
        {
            try
            {
                var message = await _reader.ReadAsync(cancellationToken);
                Console.WriteLine($"{consumerName} dequeued {message}");
                return message;
            }
            catch (InvalidOperationException)
            {
                // Channel was completed
                return default(T);
            }
        }

        public void Stop()
        {
            _writer.Complete();
            Console.WriteLine("Channel queue stopped");
        }
    }
}