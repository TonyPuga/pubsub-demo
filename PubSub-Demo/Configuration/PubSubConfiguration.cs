using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PubSub_Demo.Configuration
{
    public class PubSubConfiguration
    {
        public string ProjectId { get; set; }
        public string TopicId { get; set; }
        public string SubscriptionId { get; set; }
        public string ServiceAccountPath { get; set; }
        public string DeadLetterTopicId { get; set; }

        // Configuración de reintentos
        public int MaxRetryAttempts { get; set; } = 3;
        public TimeSpan InitialRetryDelay { get; set; } = TimeSpan.FromSeconds(1);
        public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(30);
        public double RetryBackoffMultiplier { get; set; } = 2.0;

        // Circuit Breaker
        public int CircuitBreakerThreshold { get; set; } = 5;
        public TimeSpan CircuitBreakerTimeout { get; set; } = TimeSpan.FromMinutes(1);

        // Timeouts
        public TimeSpan AckDeadline { get; set; } = TimeSpan.FromSeconds(60);
        public TimeSpan PublishTimeout { get; set; } = TimeSpan.FromSeconds(10);

        // Idempotencia
        public bool EnableIdempotencyCheck { get; set; } = true;
        public TimeSpan IdempotencyWindowDuration { get; set; } = TimeSpan.FromHours(1);

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(ProjectId))
                throw new ArgumentException("ProjectId is required");
            if (string.IsNullOrWhiteSpace(ServiceAccountPath))
                throw new ArgumentException("ServiceAccountPath is required");
            if (MaxRetryAttempts < 0)
                throw new ArgumentException("MaxRetryAttempts must be >= 0");
        }
    }
}
