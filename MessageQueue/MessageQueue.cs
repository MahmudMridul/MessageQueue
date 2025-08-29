using System.Collections.Concurrent;

namespace MessageQueue
{
    public class MessageQueue
    {
        private readonly Queue<Message> _queue = new Queue<Message>();
        private readonly Object _lock = new Object();

        public bool isEmpty()
        {
            lock (_lock)
            {
                return _queue.Count == 0;
            }
        }

        public void Enqueue(Message message, string producerName)
        {
            lock (_lock)
            {
                _queue.Enqueue(message);
            }
            Console.WriteLine($"{producerName} enqueued {message.ToString()}");
        }

        public Message? Dequeue(string consumerName)
        {
            Message? result;
            lock (_lock)
            {
                _queue.TryDequeue(out result);
            }
            Console.WriteLine($"{consumerName} dequeued {result?.ToString()}");
            return result;
        }

    }
}
