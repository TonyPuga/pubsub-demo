using PubSub_Demo.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PubSub_Demo.Services
{
    public interface IPubSubSubscriber
    {
        Task StartAsync(
            Func<MessageEnvelope<object>, Task<ProcessingResult>> messageHandler,
            CancellationToken cancellationToken = default);

        Task StopAsync();
    }
}
