namespace MessageQueue
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("Simple Message Queue Demo");
            Console.WriteLine("============================");

            // Create the message queue
            var messageQueue = new MessageQueue<Message>();

            // Create cancellation token for clean shutdown
            using var cts = new CancellationTokenSource();

            // Handle Ctrl+C for graceful shutdown
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true;
                cts.Cancel();
                Console.WriteLine("\nShutting down...");
            };

            try
            {
                await RunDemo(messageQueue, cts.Token);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Operation cancelled");
            }
            finally
            {
                messageQueue.Stop();
                Console.WriteLine("Demo finished. Press any key to exit...");
                Console.ReadKey();
            }
        }

        static async Task RunDemo(MessageQueue<Message> queue, CancellationToken cancellationToken)
        {
            // Scenario 1: Single Producer, Single Consumer
            Console.WriteLine("\nScenario 1: Single Producer → Single Consumer");
            await RunSingleProducerSingleConsumer(queue, cancellationToken);

            Console.WriteLine("\nWaiting 2 seconds...\n");
            await Task.Delay(2000, cancellationToken);

            // Scenario 2: Multiple Producers, Single Consumer
            Console.WriteLine("Scenario 2: Multiple Producers → Single Consumer");
            await RunMultipleProducersSingleConsumer(queue, cancellationToken);

            Console.WriteLine("\nWaiting 2 seconds...\n");
            await Task.Delay(2000, cancellationToken);

            // Scenario 3: Single Producer, Multiple Consumers
            Console.WriteLine("Scenario 3: Single Producer → Multiple Consumers");
            await RunSingleProducerMultipleConsumers(queue, cancellationToken);
        }

        static async Task RunSingleProducerSingleConsumer(MessageQueue<Message> queue, CancellationToken cancellationToken)
        {
            var producer = new MessageProducer(queue, "Producer-A");
            var consumer = new MessageConsumer(queue, "Consumer-1");

            // Start consumer in background
            var consumerTask = Task.Run(() => consumer.ConsumeMessagesAsync(cancellationToken));

            // Start producer
            await producer.ProduceMessagesAsync(3, 1000);

            // Let consumer finish processing
            await Task.Delay(2000, cancellationToken);
        }

        static async Task RunMultipleProducersSingleConsumer(MessageQueue<Message> queue, CancellationToken cancellationToken)
        {
            var producer1 = new MessageProducer(queue, "Producer-1");
            var producer2 = new MessageProducer(queue, "Producer-2");
            var consumer = new MessageConsumer(queue, "Consumer-A");

            // Start consumer in background
            var consumerTask = Task.Run(() => consumer.ConsumeMessagesAsync(cancellationToken));

            // Start multiple producers concurrently
            var producerTasks = new[]
            {
                Task.Run(() => producer1.ProduceMessagesAsync(3, 800)),
                Task.Run(() => producer2.ProduceMessagesAsync(3, 1200))
            };

            await Task.WhenAll(producerTasks);

            // Let consumer finish processing
            await Task.Delay(3000, cancellationToken);
        }

        static async Task RunSingleProducerMultipleConsumers(MessageQueue<Message> queue, CancellationToken cancellationToken)
        {
            var producer = new MessageProducer(queue, "Producer-X");
            var consumer1 = new MessageConsumer(queue, "Consumer-1");
            var consumer2 = new MessageConsumer(queue, "Consumer-2");

            // Start multiple consumers in background
            var consumerTasks = new[]
            {
                Task.Run(() => consumer1.ConsumeMessagesAsync(cancellationToken)),
                Task.Run(() => consumer2.ConsumeMessagesAsync(cancellationToken))
            };

            // Start producer
            await producer.ProduceMessagesAsync(6, 500);

            // Let consumers finish processing
            await Task.Delay(4000, cancellationToken);
        }
    }
}
