using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PubSub_Demo.Middleware
{
    public class CircuitBreaker
    {
        private readonly int _threshold;
        private readonly TimeSpan _timeout;
        private int _failureCount;
        private DateTime _lastFailureTime;
        private CircuitState _state;
        private readonly object _lock = new object();

        public enum CircuitState
        {
            Closed,   // Funcionamiento normal
            Open,     // Rechazando peticiones (está "abierto" el circuito)
            HalfOpen  // Probando si puede recuperarse
        }

        public CircuitBreaker(int threshold, TimeSpan timeout)
        {
            _threshold = threshold;
            _timeout = timeout;
            _state = CircuitState.Closed;
        }

        public CircuitState State
        {
            get
            {
                lock (_lock)
                {
                    return _state;
                }
            }
        }

        public void RecordSuccess()
        {
            lock (_lock)
            {
                _failureCount = 0;
                _state = CircuitState.Closed;
            }
        }

        public void RecordFailure()
        {
            lock (_lock)
            {
                _failureCount++;
                _lastFailureTime = DateTime.UtcNow;

                if (_failureCount >= _threshold)
                {
                    _state = CircuitState.Open;
                    Console.WriteLine($"⚠️ Circuit Breaker OPEN - Fallos: {_failureCount}");
                }
            }
        }

        public bool CanExecute()
        {
            lock (_lock)
            {
                if (_state == CircuitState.Closed)
                    return true;

                if (_state == CircuitState.Open)
                {
                    var timeSinceLastFailure = DateTime.UtcNow - _lastFailureTime;
                    if (timeSinceLastFailure >= _timeout)
                    {
                        _state = CircuitState.HalfOpen;
                        Console.WriteLine("🔄 Circuit Breaker HALF-OPEN - Probando conexión...");
                        return true;
                    }
                    return false;
                }

                // HalfOpen: permitir un intento
                return true;
            }
        }
    }
}
