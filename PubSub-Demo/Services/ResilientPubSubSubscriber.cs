using Google.Apis.Auth.OAuth2;
using Google.Cloud.PubSub.V1;
using PubSub_Demo.Configuration;
using PubSub_Demo.Middleware;
using PubSub_Demo.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PubSub_Demo.Services
{
    public class ResilientPubSubSubscriber : IPubSubSubscriber, IDisposable
    {
        private readonly PubSubConfiguration _config;
        private readonly CircuitBreaker _circuitBreaker;
        private SubscriberClient _subscriber;
        private readonly SemaphoreSlim _initLock = new SemaphoreSlim(1, 1);
        private bool _isInitialized;
        private bool _isRunning;

        // Tracking de mensajes procesados para idempotencia
        private readonly ConcurrentDictionary<string, DateTime> _processedHashes;

        // Métricas
        private long _messagesProcessed;
        private long _messagesSucceeded;
        private long _messagesFailed;

        public ResilientPubSubSubscriber(PubSubConfiguration config)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _config.Validate();

            _circuitBreaker = new CircuitBreaker(
                _config.CircuitBreakerThreshold,
                _config.CircuitBreakerTimeout
            );

            _processedHashes = new ConcurrentDictionary<string, DateTime>();

            StartMetricsReporting();
        }

        private async Task EnsureInitializedAsync()
        {
            if (_isInitialized) return;

            await _initLock.WaitAsync();
            try
            {
                if (_isInitialized) return;

                Console.WriteLine("🔧 Inicializando Subscriber...");
                GoogleCredential credential;
                using (var jsonStream = System.IO.File.OpenRead(_config.ServiceAccountPath))
                {
                    credential = GoogleCredential.FromStream(jsonStream)
                        .CreateScoped(SubscriberServiceApiClient.DefaultScopes);
                }

                SubscriptionName subscriptionName = SubscriptionName.FromProjectSubscription(
                    _config.ProjectId,
                    _config.SubscriptionId
                );

                // Usar SubscriberClientBuilder
                var clientBuilder = new SubscriberClientBuilder
                {
                    SubscriptionName = subscriptionName,
                    Credential = credential,
                    Settings = new SubscriberClient.Settings
                    {
                        AckDeadline = _config.AckDeadline,
                        AckExtensionWindow = TimeSpan.FromSeconds(_config.AckDeadline.TotalSeconds * 0.8),
                        FlowControlSettings = new Google.Api.Gax.FlowControlSettings(
                            maxOutstandingElementCount: 1000,
                            maxOutstandingByteCount: 100_000_000
                        )
                    }
                };

                _subscriber = await clientBuilder.BuildAsync();

                _isInitialized = true;

                Console.WriteLine("✅ Subscriber inicializado correctamente");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error inicializando Subscriber: {ex.Message}");
                throw;
            }
            finally
            {
                _initLock.Release();
            }
        }

        public async Task StartAsync(
            Func<MessageEnvelope<object>, Task<ProcessingResult>> messageHandler,
            CancellationToken cancellationToken = default)
        {
            if (messageHandler == null)
                throw new ArgumentNullException(nameof(messageHandler));

            await EnsureInitializedAsync();

            _isRunning = true;
            Console.WriteLine("👂 Subscriber escuchando mensajes...\n");

            try
            {
                await _subscriber.StartAsync(async (PubsubMessage message, CancellationToken cancel) =>
                {
                    return await ProcessMessageAsync(message, messageHandler, cancel);
                });

                // Mantener corriendo hasta cancelación
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            catch (TaskCanceledException)
            {
                Console.WriteLine("🛑 Subscriber detenido por cancelación");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error en Subscriber: {ex.Message}");
                throw;
            }
        }

        private async Task<SubscriberClient.Reply> ProcessMessageAsync(
            PubsubMessage message,
            Func<MessageEnvelope<object>, Task<ProcessingResult>> messageHandler,
            CancellationToken cancellationToken)
        {
            var stopwatch = Stopwatch.StartNew();
            Interlocked.Increment(ref _messagesProcessed);

            try
            {
                // Circuit Breaker check
                if (!_circuitBreaker.CanExecute())
                {
                    Console.WriteLine($"⚠️ Circuit Breaker OPEN - Rechazando mensaje {message.MessageId}");
                    return SubscriberClient.Reply.Nack; // Reintentar más tarde
                }

                // Deserializar mensaje
                string messageText = message.Data.ToStringUtf8();
                var envelope = Newtonsoft.Json.JsonConvert.DeserializeObject<MessageEnvelope<object>>(messageText);

                Console.WriteLine($"📨 [{DateTime.Now:HH:mm:ss}] Procesando mensaje: {envelope.MessageId}");

                // Idempotency check
                if (_config.EnableIdempotencyCheck &&
                    message.Attributes.TryGetValue("contentHash", out string contentHash))
                {
                    if (_processedHashes.TryGetValue(contentHash, out var processedTime))
                    {
                        var age = DateTime.UtcNow - processedTime;
                        if (age < _config.IdempotencyWindowDuration)
                        {
                            Console.WriteLine($"   ⚠️ Mensaje duplicado (ya procesado hace {age.TotalSeconds:F0}s)");
                            stopwatch.Stop();
                            return SubscriberClient.Reply.Ack; // ACK sin procesar
                        }
                    }
                }

                // Procesar con handler del usuario
                var result = await messageHandler(envelope);

                stopwatch.Stop();

                if (result.Success)
                {
                    // Registrar en cache de idempotencia
                    if (_config.EnableIdempotencyCheck &&
                        message.Attributes.TryGetValue("contentHash", out contentHash))
                    {
                        _processedHashes.TryAdd(contentHash, DateTime.UtcNow);
                    }

                    _circuitBreaker.RecordSuccess();
                    Interlocked.Increment(ref _messagesSucceeded);

                    Console.WriteLine($"   ✅ Procesado exitosamente ({stopwatch.ElapsedMilliseconds}ms)");
                    return SubscriberClient.Reply.Ack;
                }
                else
                {
                    _circuitBreaker.RecordFailure();
                    Interlocked.Increment(ref _messagesFailed);

                    Console.WriteLine($"   ❌ Error procesando: {result.Error} ({stopwatch.ElapsedMilliseconds}ms)");

                    return result.ShouldRetry
                        ? SubscriberClient.Reply.Nack
                        : SubscriberClient.Reply.Ack; // ACK para no reintentar
                }
            }
            catch (Exception ex)
            {
                _circuitBreaker.RecordFailure();
                Interlocked.Increment(ref _messagesFailed);
                stopwatch.Stop();

                Console.WriteLine($"   ❌ Excepción no manejada: {ex.Message} ({stopwatch.ElapsedMilliseconds}ms)");
                Console.WriteLine($"   StackTrace: {ex.StackTrace}");

                // Determinar si es error transiente
                bool isTransient = Middleware.RetryPolicy.IsTransientError(ex);
                return isTransient
                    ? SubscriberClient.Reply.Nack
                    : SubscriberClient.Reply.Ack;
            }
        }

        public async Task StopAsync()
        {
            if (!_isRunning) return;

            Console.WriteLine("🛑 Deteniendo Subscriber...");

            _isRunning = false;

            if (_subscriber != null)
            {
                await _subscriber.StopAsync(CancellationToken.None);
                Console.WriteLine("✅ Subscriber detenido correctamente");
            }
        }

        private void StartMetricsReporting()
        {
            Task.Run(async () =>
            {
                while (true)
                {
                    await Task.Delay(TimeSpan.FromMinutes(1));

                    if (_messagesProcessed > 0)
                    {
                        var successRate = (_messagesSucceeded * 100.0) / _messagesProcessed;
                        Console.WriteLine($"\n📊 Métricas:");
                        Console.WriteLine($"   Total procesados: {_messagesProcessed}");
                        Console.WriteLine($"   Exitosos: {_messagesSucceeded}");
                        Console.WriteLine($"   Fallidos: {_messagesFailed}");
                        Console.WriteLine($"   Tasa de éxito: {successRate:F2}%");
                        Console.WriteLine($"   Circuit Breaker: {_circuitBreaker.State}\n");
                    }
                }
            });
        }

        public void Dispose()
        {
            StopAsync().Wait();
            _initLock?.Dispose();
        }
    }
}
