using PubSub_Demo.Configuration;
using PubSub_Demo.Middleware;
using PubSub_Demo.Models;
using PubSub_Demo.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PubSub_Demo
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("╔═══════════════════════════════════════════════╗");
            Console.WriteLine("║   Cliente Pub/Sub Robusto - .NET 4.8          ║");
            Console.WriteLine("╚═══════════════════════════════════════════════╝\n");

            var config = new PubSubConfiguration
            {
                ProjectId = "proyecto-gcp",
                TopicId = "topic",
                SubscriptionId = "subscription",
                ServiceAccountPath = @"C:\security\service-account.json",
                DeadLetterTopicId = "topic-dlq",

                // Configuración de resiliencia
                MaxRetryAttempts = 3,
                InitialRetryDelay = TimeSpan.FromSeconds(1),
                MaxRetryDelay = TimeSpan.FromSeconds(30),
                CircuitBreakerThreshold = 5,
                CircuitBreakerTimeout = TimeSpan.FromMinutes(1),

                // Idempotencia
                EnableIdempotencyCheck = true,
                IdempotencyWindowDuration = TimeSpan.FromHours(1)
            };

            try
            {
                config.Validate();

                Console.WriteLine("Selecciona una opción:");
                Console.WriteLine("1. Publicar mensajes");
                Console.WriteLine("2. Escuchar mensajes");
                Console.WriteLine("3. Ambos (Publisher + Subscriber)");
                Console.Write("\nOpción: ");

                var option = Console.ReadLine();

                switch (option)
                {
                    case "1":
                        await RunPublisherAsync(config);
                        break;
                    case "2":
                        await RunSubscriberAsync(config);
                        break;
                    case "3":
                        await RunBothAsync(config);
                        break;
                    default:
                        Console.WriteLine("Opción inválida");
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ Error fatal: {ex.Message}");
                Console.WriteLine($"StackTrace: {ex.StackTrace}");
            }

            Console.WriteLine("\nPresiona Enter para salir...");
            Console.ReadLine();
        }

        static async Task RunPublisherAsync(PubSubConfiguration config)
        {
            using (var publisher = new ResilientPubSubPublisher(config))
            {
                Console.WriteLine("\n=== MODO PUBLISHER ===\n");

                for (int i = 1; i <= 5; i++)
                {
                    try
                    {
                        var envelope = new MessageEnvelope<OrderCreatedEvent>
                        {
                            EventType = "OrderCreated",
                            Source = "ConsoleApp",
                            Payload = new OrderCreatedEvent
                            {
                                OrderId = $"ORD-{i:D4}",
                                CustomerId = $"CUST-{100 + i}",
                                TotalAmount = 99.99m * i,
                                Items = new[] { "Item1", "Item2" }
                            }
                        };

                        envelope.Metadata.Add("priority", i % 2 == 0 ? "high" : "normal");

                        var messageId = await publisher.PublishAsync(envelope);

                        await Task.Delay(1000); // Simular carga de trabajo
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error publicando mensaje {i}: {ex.Message}");
                    }
                }

                Console.WriteLine("\n✅ Todos los mensajes enviados");
            }
        }

        static async Task RunSubscriberAsync(PubSubConfiguration config)
        {
            using (var subscriber = new ResilientPubSubSubscriber(config))
            {
                Console.WriteLine("\n=== MODO SUBSCRIBER ===\n");
                Console.WriteLine("Presiona Ctrl+C para detener...\n");

                var cts = new CancellationTokenSource();
                Console.CancelKeyPress += (s, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    await subscriber.StartAsync(ProcessOrderMessage, cts.Token);
                }
                catch (TaskCanceledException)
                {
                    Console.WriteLine("\n🛑 Deteniendo subscriber...");
                }

                await subscriber.StopAsync();
            }
        }

        static async Task RunBothAsync(PubSubConfiguration config)
        {
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (s, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            // Iniciar subscriber en background
            var subscriberTask = Task.Run(async () =>
            {
                using (var subscriber = new ResilientPubSubSubscriber(config))
                {
                    await subscriber.StartAsync(ProcessOrderMessage, cts.Token);
                    await subscriber.StopAsync();
                }
            });

            // Esperar un poco para que el subscriber esté listo
            await Task.Delay(2000);

            // Publicar mensajes
            await RunPublisherAsync(config);

            // Esperar a que termine el subscriber
            try
            {
                await subscriberTask;
            }
            catch (TaskCanceledException) { }
        }

        // Handler de ejemplo para procesar mensajes
        static async Task<ProcessingResult> ProcessOrderMessage(MessageEnvelope<object> envelope)
        {
            try
            {
                // Simular procesamiento
                Console.WriteLine($"   📦 Procesando orden...");
                Console.WriteLine($"      EventType: {envelope.EventType}");
                Console.WriteLine($"      Source: {envelope.Source}");
                Console.WriteLine($"      CorrelationId: {envelope.CorrelationId}");

                // Simular trabajo
                await Task.Delay(500);

                // Simular fallo ocasional (10% de probabilidad)
                if (new Random().Next(100) < 10)
                {
                    throw new Exception("Simulación de error transiente");
                }

                return ProcessingResult.Succeeded(
                    envelope.MessageId,
                    TimeSpan.FromMilliseconds(500)
                );
            }
            catch (Exception ex)
            {
                return ProcessingResult.Failed(
                    envelope.MessageId,
                    ex.Message,
                    shouldRetry: RetryPolicy.IsTransientError(ex)
                );
            }
        }
    }

    // Ejemplo de evento de dominio
    public class OrderCreatedEvent
    {
        public string OrderId { get; set; }
        public string CustomerId { get; set; }
        public decimal TotalAmount { get; set; }
        public string[] Items { get; set; }
    }
}
