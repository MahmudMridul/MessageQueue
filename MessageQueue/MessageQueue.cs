using System.Collections.Concurrent;

namespace MessageQueue
{
    public class MessageQueue<T>
    {
        private readonly ConcurrentQueue<T> _queue = new ConcurrentQueue<T>();
        private readonly object _lockObject = new object();
        private volatile bool _isRunning = true;

        public int Count => _queue.Count;
        public bool isEmpty => _queue.IsEmpty;

        public void Enqueue(T message)
        {
            if (!_isRunning)
                throw new InvalidOperationException("Queue is stopped");

            _queue.Enqueue(message);
            Console.WriteLine($"Enqueued: {message}");
        }

        public bool TryDequeue(out T? message)
        {
            return _queue.TryDequeue(out message);
        }

        public async Task<T?> DequeueAsync(CancellationToken cancellationToken = default)
        {
            while (_isRunning && !cancellationToken.IsCancellationRequested)
            {
                if (_queue.TryDequeue(out T? message))
                {
                    Console.WriteLine($"Dequeued: {message}");
                    return message;
                }

                // Wait a bit before checking again (simple polling)
                await Task.Delay(100, cancellationToken);
            }

            return default(T);
        }

        public void Stop()
        {
            _isRunning = false;
            Console.WriteLine("Queue stopped");
        }

    }
}
