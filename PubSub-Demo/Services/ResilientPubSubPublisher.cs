using Google.Apis.Auth.OAuth2;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using PubSub_Demo.Configuration;
using PubSub_Demo.Middleware;
using PubSub_Demo.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Remoting.Channels;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PubSub_Demo.Services
{
    public class ResilientPubSubPublisher : IPubSubPublisher, IDisposable
    {
        private readonly PubSubConfiguration _config;
        private readonly Middleware.RetryPolicy _retryPolicy;
        private readonly CircuitBreaker _circuitBreaker;
        private PublisherClient _publisher;
        private Channel _channel;
        private readonly SemaphoreSlim _initLock = new SemaphoreSlim(1, 1);
        private bool _isInitialized;

        // Cache para idempotencia (en producción usar Redis/Memcached)
        private readonly ConcurrentDictionary<string, DateTime> _processedMessages;

        public ResilientPubSubPublisher(PubSubConfiguration config)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _config.Validate();

            _retryPolicy = new Middleware.RetryPolicy(
                _config.MaxRetryAttempts,
                _config.InitialRetryDelay,
                _config.MaxRetryDelay,
                _config.RetryBackoffMultiplier
            );

            _circuitBreaker = new CircuitBreaker(
                _config.CircuitBreakerThreshold,
                _config.CircuitBreakerTimeout
            );

            _processedMessages = new ConcurrentDictionary<string, DateTime>();

            // Limpieza periódica del cache de idempotencia
            StartIdempotencyCacheCleanup();
        }

        private async Task EnsureInitializedAsync()
        {
            if (_isInitialized) return;

            await _initLock.WaitAsync();
            try
            {
                if (_isInitialized) return;

                Console.WriteLine("🔧 Inicializando Publisher...");

                // Cargar credenciales desde el archivo JSON
                GoogleCredential credential;
                using (var jsonStream = System.IO.File.OpenRead(_config.ServiceAccountPath))
                {
                    credential = GoogleCredential.FromStream(jsonStream)
                        .CreateScoped(PublisherServiceApiClient.DefaultScopes);
                }

                TopicName topicName = TopicName.FromProjectTopic(
                    _config.ProjectId,
                    _config.TopicId
                );

                // Crear con credenciales específicas
                var clientBuilder = new PublisherClientBuilder
                {
                    TopicName = topicName,
                    Credential = credential,
                    Settings = new PublisherClient.Settings
                    {
                        // Configuraciones opcionales
                        EnableMessageOrdering = false
                    }
                };

                _publisher = await clientBuilder.BuildAsync();

                _isInitialized = true;
                Console.WriteLine("✅ Publisher inicializado correctamente");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error inicializando Publisher: {ex.Message}");
                throw;
            }
            finally
            {
                _initLock.Release();
            }
        }

        public async Task<string> PublishAsync<T>(
            MessageEnvelope<T> envelope,
            CancellationToken cancellationToken = default)
        {
            if (envelope == null)
                throw new ArgumentNullException(nameof(envelope));

            // Circuit Breaker check
            if (!_circuitBreaker.CanExecute())
            {
                throw new InvalidOperationException(
                    "Circuit Breaker está OPEN - Sistema temporalmente no disponible");
            }

            await EnsureInitializedAsync();

            var stopwatch = Stopwatch.StartNew();

            try
            {
                // Idempotency check
                if (_config.EnableIdempotencyCheck)
                {
                    envelope.ContentHash = envelope.ComputeHash();

                    if (_processedMessages.TryGetValue(envelope.ContentHash, out var processedTime))
                    {
                        Console.WriteLine($"⚠️ Mensaje duplicado detectado: {envelope.MessageId} (hash: {envelope.ContentHash.Substring(0, 8)}...)");
                        return envelope.MessageId; // Ya fue procesado
                    }
                }

                // Publicar con retry policy
                var messageId = await _retryPolicy.ExecuteAsync(
                    async () => await PublishInternalAsync(envelope, cancellationToken),
                    Middleware.RetryPolicy.IsTransientError,
                    cancellationToken
                );

                // Registrar en cache de idempotencia
                if (_config.EnableIdempotencyCheck)
                {
                    _processedMessages.TryAdd(envelope.ContentHash, DateTime.UtcNow);
                }

                _circuitBreaker.RecordSuccess();

                stopwatch.Stop();
                Console.WriteLine($"✅ Mensaje publicado: {messageId} ({stopwatch.ElapsedMilliseconds}ms)");

                return messageId;
            }
            catch (Exception ex)
            {
                _circuitBreaker.RecordFailure();
                stopwatch.Stop();

                Console.WriteLine($"❌ Error publicando mensaje: {ex.Message} ({stopwatch.ElapsedMilliseconds}ms)");

                // Intentar enviar a Dead Letter Queue
                await TrySendToDeadLetterAsync(envelope, ex);

                throw;
            }
        }

        private async Task<string> PublishInternalAsync<T>(
            MessageEnvelope<T> envelope,
            CancellationToken cancellationToken)
        {
            string jsonMessage = Newtonsoft.Json.JsonConvert.SerializeObject(envelope);

            var pubsubMessage = new PubsubMessage
            {
                Data = Google.Protobuf.ByteString.CopyFromUtf8(jsonMessage),
                Attributes =
                {
                    { "messageId", envelope.MessageId },
                    { "correlationId", envelope.CorrelationId },
                    { "eventType", envelope.EventType },
                    { "source", envelope.Source },
                    { "timestamp", envelope.Timestamp.ToString("O") },
                    { "version", envelope.Version.ToString() }
                }
            };

            if (_config.EnableIdempotencyCheck && !string.IsNullOrEmpty(envelope.ContentHash))
            {
                pubsubMessage.Attributes.Add("contentHash", envelope.ContentHash);
            }

            return await _publisher.PublishAsync(pubsubMessage);
        }

        private async Task TrySendToDeadLetterAsync<T>(
            MessageEnvelope<T> envelope,
            Exception error)
        {
            if (string.IsNullOrWhiteSpace(_config.DeadLetterTopicId))
                return;

            try
            {
                Console.WriteLine($"📮 Enviando a Dead Letter Queue: {envelope.MessageId}");

                // Aquí implementarías el envío al DLQ
                // Por brevedad, solo lo logueamos
                Console.WriteLine($"   Error: {error.Message}");
                Console.WriteLine($"   Payload: {Newtonsoft.Json.JsonConvert.SerializeObject(envelope)}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error enviando a DLQ: {ex.Message}");
            }
        }

        private void StartIdempotencyCacheCleanup()
        {
            Task.Run(async () =>
            {
                while (true)
                {
                    try
                    {
                        await Task.Delay(TimeSpan.FromMinutes(5));
                        CleanupIdempotencyCache();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error limpiando cache: {ex.Message}");
                    }
                }
            });
        }

        private void CleanupIdempotencyCache()
        {
            var threshold = DateTime.UtcNow - _config.IdempotencyWindowDuration;
            var keysToRemove = new System.Collections.Generic.List<string>();

            foreach (var kvp in _processedMessages)
            {
                if (kvp.Value < threshold)
                {
                    keysToRemove.Add(kvp.Key);
                }
            }

            foreach (var key in keysToRemove)
            {
                _processedMessages.TryRemove(key, out _);
            }

            if (keysToRemove.Count > 0)
            {
                Console.WriteLine($"🧹 Cache limpiado: {keysToRemove.Count} entradas removidas");
            }
        }

        public void Dispose()
        {
            _publisher?.ShutdownAsync(TimeSpan.FromSeconds(5)).Wait();
            _channel?.ShutdownAsync().Wait();
            _initLock?.Dispose();
        }
    }
}
