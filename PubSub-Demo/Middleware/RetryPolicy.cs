using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PubSub_Demo.Middleware
{
    public class RetryPolicy
    {
        private readonly int _maxAttempts;
        private readonly TimeSpan _initialDelay;
        private readonly TimeSpan _maxDelay;
        private readonly double _backoffMultiplier;

        public RetryPolicy(
            int maxAttempts,
            TimeSpan initialDelay,
            TimeSpan maxDelay,
            double backoffMultiplier)
        {
            _maxAttempts = maxAttempts;
            _initialDelay = initialDelay;
            _maxDelay = maxDelay;
            _backoffMultiplier = backoffMultiplier;
        }

        public async Task<T> ExecuteAsync<T>(
            Func<Task<T>> operation,
            Func<Exception, bool> shouldRetry = null,
            CancellationToken cancellationToken = default)
        {
            Exception lastException = null;

            for (int attempt = 1; attempt <= _maxAttempts; attempt++)
            {
                try
                {
                    return await operation();
                }
                catch (Exception ex)
                {
                    lastException = ex;

                    // Verificar si debemos reintentar
                    if (shouldRetry != null && !shouldRetry(ex))
                    {
                        Console.WriteLine($"❌ Error no retryable: {ex.Message}");
                        throw;
                    }

                    if (attempt == _maxAttempts)
                    {
                        Console.WriteLine($"❌ Máximo de reintentos alcanzado ({_maxAttempts})");
                        throw;
                    }

                    // Calcular delay con jitter para evitar thundering herd
                    var delay = CalculateDelay(attempt);
                    var jitter = TimeSpan.FromMilliseconds(new Random().Next(0, 100));
                    var totalDelay = delay + jitter;

                    Console.WriteLine($"⏳ Reintento {attempt}/{_maxAttempts} en {totalDelay.TotalSeconds:F2}s - Error: {ex.Message}");

                    await Task.Delay(totalDelay, cancellationToken);
                }
            }

            throw lastException;
        }

        private TimeSpan CalculateDelay(int attempt)
        {
            var exponentialDelay = TimeSpan.FromMilliseconds(
                _initialDelay.TotalMilliseconds * Math.Pow(_backoffMultiplier, attempt - 1)
            );

            return exponentialDelay > _maxDelay ? _maxDelay : exponentialDelay;
        }

        public static bool IsTransientError(Exception ex)
        {
            // Errores que vale la pena reintentar
            return ex is TimeoutException
                || ex is TaskCanceledException
                || (ex.Message?.Contains("unavailable") ?? false)
                || (ex.Message?.Contains("deadline exceeded") ?? false)
                || (ex.Message?.Contains("connection") ?? false);
        }
    }
}
