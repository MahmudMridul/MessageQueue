

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
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{producerName} enqueued {message.ToString()}");
            Console.ResetColor();
        }

        public Message? Dequeue(string consumerName)
        {
            Message? result;
            lock (_lock)
            {
                _queue.TryDequeue(out result);
            }
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"{consumerName} dequeued {result?.ToString()}");
            Console.ResetColor();
            return result;
        }

    }
}
