using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PubSub_Demo.Models
{
    // <summary>
    /// Envelope pattern: envuelve el mensaje con metadata para trazabilidad
    /// </summary>
    public class MessageEnvelope<T>
    {
        public string MessageId { get; set; }
        public string CorrelationId { get; set; }
        public DateTime Timestamp { get; set; }
        public string Source { get; set; }
        public string EventType { get; set; }
        public int Version { get; set; }
        public T Payload { get; set; }
        public Dictionary<string, string> Metadata { get; set; }

        // Para idempotencia: hash del contenido
        public string ContentHash { get; set; }

        public MessageEnvelope()
        {
            MessageId = Guid.NewGuid().ToString();
            CorrelationId = Guid.NewGuid().ToString();
            Timestamp = DateTime.UtcNow;
            Version = 1;
            Metadata = new Dictionary<string, string>();
        }

        public string ComputeHash()
        {
            var content = $"{EventType}:{Timestamp:O}:{Newtonsoft.Json.JsonConvert.SerializeObject(Payload)}";
            using (var sha256 = System.Security.Cryptography.SHA256.Create())
            {
                var bytes = System.Text.Encoding.UTF8.GetBytes(content);
                var hash = sha256.ComputeHash(bytes);
                return Convert.ToBase64String(hash);
            }
        }
    }
}
