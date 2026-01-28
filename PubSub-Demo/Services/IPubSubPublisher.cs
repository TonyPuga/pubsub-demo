using PubSub_Demo.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PubSub_Demo.Services
{
    public interface IPubSubPublisher
    {
        Task<string> PublishAsync<T>(
            MessageEnvelope<T> envelope,
            CancellationToken cancellationToken = default);
    }
}
