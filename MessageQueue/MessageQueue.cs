using System.Threading.Channels;

namespace MessageQueue
{
    public class MessageQueue<T> : IDisposable
    {
        private readonly Channel<T> _channel;
        private readonly ChannelWriter<T> _writer;
        private readonly ChannelReader<T> _reader;
        private readonly IPersistentStorage<T>? _persistentStorage;
        private readonly CancellationTokenSource _persistenceCts;
        private readonly List<T> _persistedMessages;
        private readonly SemaphoreSlim _persistenceSemaphore;

        public MessageQueue(int capacity = 100, IPersistentStorage<T>? persistentStorage = null)
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
            _persistentStorage = persistentStorage;
            _persistenceCts = new CancellationTokenSource();
            _persistedMessages = new List<T>();
            _persistenceSemaphore = new SemaphoreSlim(1, 1);

            // Load persisted messages on startup
            if (_persistentStorage != null)
            {
                _ = Task.Run(LoadPersistedMessages);
            }
        }

        public int Count => _reader.Count;
        public bool IsEmpty => _reader.Count == 0;

        public async Task EnqueueAsync(T message, string producerName, CancellationToken cancellationToken = default)
        {
            await _writer.WriteAsync(message, cancellationToken);
            Console.WriteLine($"{producerName} enqueued {message} | [{DateTime.UtcNow:HH:mm:ss.fff}]");
            
            // Persist immediately when enqueuing
            if (_persistentStorage != null)
            {
                await _persistenceSemaphore.WaitAsync(cancellationToken);
                try
                {
                    _persistedMessages.Add(message);
                    await UpdatePersistentStorage();
                    Console.WriteLine($"[PERSISTENCE] Saved message to storage | [{DateTime.UtcNow:HH:mm:ss.fff}]");
                }
                finally
                {
                    _persistenceSemaphore.Release();
                }
            }
        }

        public async Task<T?> DequeueAsync(string consumerName, CancellationToken cancellationToken = default)
        {
            try
            {
                var message = await _reader.ReadAsync(cancellationToken);
                Console.WriteLine($"{consumerName} dequeued {message} | [{DateTime.UtcNow:HH:mm:ss.fff}]");
                
                // Remove from persistence when successfully dequeued
                if (_persistentStorage != null && message != null)
                {
                    await _persistenceSemaphore.WaitAsync(cancellationToken);
                    try
                    {
                        _persistedMessages.Remove(message);
                        await UpdatePersistentStorage();
                        Console.WriteLine($"[PERSISTENCE] Message consumed, removed from storage | [{DateTime.UtcNow:HH:mm:ss.fff}]");
                    }
                    finally
                    {
                        _persistenceSemaphore.Release();
                    }
                }
                
                return message;
            }
            catch (InvalidOperationException)
            {
                // Channel was completed
                return default(T);
            }
        }

        private async Task UpdatePersistentStorage()
        {
            if (_persistentStorage == null) return;

            // Clear and rewrite all remaining messages
            await _persistentStorage.ClearAsync();
            foreach (var message in _persistedMessages)
            {
                await _persistentStorage.WriteAsync(message);
            }
        }

        private async Task LoadPersistedMessages()
        {
            if (_persistentStorage == null) return;

            try
            {
                Console.WriteLine($"[PERSISTENCE] Loading persisted messages... | [{DateTime.UtcNow:HH:mm:ss.fff}]");
                int loadedCount = 0;

                await _persistenceSemaphore.WaitAsync();
                try
                {
                    while (await _persistentStorage.HasDataAsync())
                    {
                        var message = await _persistentStorage.ReadAsync();
                        if (message != null)
                        {
                            await _writer.WriteAsync(message);
                            _persistedMessages.Add(message);
                            loadedCount++;
                        }
                        else
                        {
                            break;
                        }
                    }
                }
                finally
                {
                    _persistenceSemaphore.Release();
                }

                if (loadedCount > 0)
                {
                    Console.WriteLine($"[PERSISTENCE] Loaded {loadedCount} messages from storage | [{DateTime.UtcNow:HH:mm:ss.fff}]");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[PERSISTENCE] Error loading messages | [{DateTime.UtcNow:HH:mm:ss.fff}] - {ex.Message}");
            }
        }

        public void Stop()
        {
            _writer.Complete();
            _persistenceCts.Cancel();
            Console.WriteLine($"Channel queue stopped | [{DateTime.UtcNow:HH:mm:ss.fff}]");
        }

        public void Dispose()
        {
            Stop();
            _persistenceCts.Dispose();
            _persistenceSemaphore.Dispose();
        }
    }
}