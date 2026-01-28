using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PubSub_Demo.Models
{
    public class ProcessingResult
    {
        public bool Success { get; set; }
        public string MessageId { get; set; }
        public string Error { get; set; }
        public int RetryCount { get; set; }
        public TimeSpan ProcessingTime { get; set; }
        public bool ShouldRetry { get; set; }

        public static ProcessingResult Succeeded(string messageId, TimeSpan processingTime)
        {
            return new ProcessingResult
            {
                Success = true,
                MessageId = messageId,
                ProcessingTime = processingTime
            };
        }

        public static ProcessingResult Failed(string messageId, string error, bool shouldRetry = true)
        {
            return new ProcessingResult
            {
                Success = false,
                MessageId = messageId,
                Error = error,
                ShouldRetry = shouldRetry
            };
        }
    }
}
