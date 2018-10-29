using System;
using System.Collections.Generic;
using System.Text;
using System.Reactive.Linq;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Store events in a list in memory
    /// </summary>
    public class ListEventSink : List<IEnvelope>, IEventSink
    {
        protected ILogger _logger;

        public ListEventSink() : this(null) { }

        public ListEventSink(ILogger logger)
        {
            _logger = logger;
        }

        public string Id { get; set; }

        public void OnCompleted()
        {
            _logger?.LogInformation($"{this.GetType()} {this.Id} completed.");
        }

        public void OnError(Exception error)
        {
            _logger?.LogCritical($"{this.GetType()} {this.Id} error: {error}.");
        }

        public void OnNext(IEnvelope value)
        {
            this.Add(value);
        }

        public void Start()
        {
            _logger?.LogInformation("ListEventSink started");
        }

        public void Stop()
        {
            _logger?.LogInformation("ListEventSink started");
        }
    }
}
