namespace MessageQueue
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var messageQueue = new MessageQueue();
            try
            {
                await RunDemo(messageQueue);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Operation cancelled");
            }
            finally
            {
                Console.WriteLine("Demo finished. Press any key to exit...");
                Console.ReadKey();
            }
        }

        static async Task RunDemo(MessageQueue queue)
        {
            // Scenario 1: Single Producer, Single Consumer
            //Console.WriteLine("\nScenario 1: Single Producer → Single Consumer");
            //await RunSingleProducerSingleConsumer(queue);

            //Console.WriteLine("\nWaiting 2 seconds...\n");
            //await Task.Delay(2000);

            // Scenario 2: Multiple Producers, Single Consumer
            //Console.WriteLine("Scenario 2: Multiple Producers → Single Consumer");
            //await RunMultipleProducersSingleConsumer(queue);

            //Console.WriteLine("\nWaiting 2 seconds...\n");
            //await Task.Delay(2000);

            // Scenario 3: Single Producer, Multiple Consumers
            //Console.WriteLine("Scenario 3: Single Producer → Multiple Consumers");
            //await RunSingleProducerMultipleConsumers(queue);

            // Scenario 4: Multiple Producer, Multiple Consumers
            Console.WriteLine("Scenario 4: Multiple Producer → Multiple Consumers");
            await RunMultipleProducerMultipleConsumer(queue);
        }

        static async Task RunSingleProducerSingleConsumer(MessageQueue queue)
        {
            var producer = new MessageProducer(queue, "Producer-A");
            var consumer = new MessageConsumer(queue, "Consumer-1");

            // Start consumer in background
            var consumerTask = Task.Run(() => consumer.ConsumeMessagesAsync());

            // Start producer
            await producer.ProduceMessagesAsync(3, 1000);

            // Let consumer finish processing
            await Task.Delay(2000);
        }

        static async Task RunMultipleProducersSingleConsumer(MessageQueue queue)
        {
            var producer1 = new MessageProducer(queue, "Producer-1");
            var producer2 = new MessageProducer(queue, "Producer-2");
            var consumer = new MessageConsumer(queue, "Consumer-A");

            // Start consumer in background
            var consumerTask = Task.Run(() => consumer.ConsumeMessagesAsync());

            // Start multiple producers concurrently
            var producerTasks = new[]
            {
                Task.Run(() => producer1.ProduceMessagesAsync(3, 800)),
                Task.Run(() => producer2.ProduceMessagesAsync(3, 1200))
            };

            await Task.WhenAll(producerTasks);

            // Let consumer finish processing
            await Task.Delay(3000);
        }

        static async Task RunSingleProducerMultipleConsumers(MessageQueue queue)
        {
            var producer = new MessageProducer(queue, "Producer-X");
            var consumer1 = new MessageConsumer(queue, "Consumer-1");
            var consumer2 = new MessageConsumer(queue, "Consumer-2");

            // Start multiple consumers in background
            var consumerTasks = new[]
            {
                Task.Run(() => consumer1.ConsumeMessagesAsync()),
                Task.Run(() => consumer2.ConsumeMessagesAsync())
            };

            // Start producer
            await producer.ProduceMessagesAsync(6, 500);

            // Let consumers finish processing
            await Task.Delay(4000);
        }

        static async Task RunMultipleProducerMultipleConsumer(MessageQueue queue)
        {
            var producerA = new MessageProducer(queue, "Google");
            var producerB = new MessageProducer(queue, "Amazon");
            //var producerC = new MessageProducer(queue, "Producer-C");
            var consumer1 = new MessageConsumer(queue, "Bob");
            var consumer2 = new MessageConsumer(queue, "Jack");

            var allTasks = new[]
            {
                Task.Run(() => producerA.ProduceMessagesAsync(5, 200)),
                Task.Run(() => producerB.ProduceMessagesAsync(3, 400)),
                //Task.Run(() => producerC.ProduceMessagesAsync(3, 500)),
                Task.Run(() => consumer1.ConsumeMessagesAsync(500)),
                Task.Run(() => consumer2.ConsumeMessagesAsync(1000))
            };
            
            await Task.WhenAll(allTasks);
        }
    }
}
