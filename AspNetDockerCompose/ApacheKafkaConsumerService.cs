using Confluent.Kafka;
using System.Diagnostics;
using System.Text.Json;

namespace AspNetDockerCompose
{
    public class ApacheKafkaConsumerService : BackgroundService
    {
        private readonly string topic = "test";
        private readonly string groupId = "test_group";
        private readonly string bootstrapServers = "kafka:9092";

        //public Task StartAsync(CancellationToken cancellationToken)
        //{
        //    var config = new ConsumerConfig
        //    {
        //        GroupId = groupId,
        //        BootstrapServers = bootstrapServers,
        //        AutoOffsetReset = AutoOffsetReset.Earliest
        //    };

        //    try
        //    {
        //        using (var consumerBuilder = new ConsumerBuilder<Ignore, string>(config).Build())
        //        {
        //            consumerBuilder.Subscribe(topic);
        //            var cancelToken = new CancellationTokenSource();

        //            try
        //            {
        //                // TODO nie odpala UI bo czeka wewnątrz while, trzeba to lepiej odpalić (async)
        //                while (!cancellationToken.IsCancellationRequested)
        //                {
        //                    var consumer = consumerBuilder.Consume(cancelToken.Token);
        //                    var orderRequest = JsonSerializer.Deserialize<OrderProcessingRequest>(consumer.Message.Value);
        //                    Debug.WriteLine($"Processing Order Id: {orderRequest.OrderId}");
        //                }
        //            }
        //            catch (OperationCanceledException)
        //            {
        //                consumerBuilder.Close();
        //            }
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        System.Diagnostics.Debug.WriteLine(ex.Message);
        //    }

        //    return Task.CompletedTask;
        //}
        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await BackgroundProcessing(stoppingToken);
        }

        private async Task BackgroundProcessing(CancellationToken cancellationToken)
        {
            await Task.Yield(); // TODO check how it was done in TW to not block thread

            var config = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            try
            {
                using (var consumerBuilder = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    consumerBuilder.Subscribe(topic);
                    var cancelToken = new CancellationTokenSource();

                    try
                    {
                        while (!cancellationToken.IsCancellationRequested)
                        {
                            var consumer = consumerBuilder.Consume(cancelToken.Token);
                            var orderRequest = JsonSerializer.Deserialize<OrderProcessingRequest>(consumer.Message.Value);
                            Debug.WriteLine($"Processing Order Id: {orderRequest.OrderId}");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumerBuilder.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(ex.Message);
            }

            return;
        }
    }
}
