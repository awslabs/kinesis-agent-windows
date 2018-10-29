using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Reactive.Subjects;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Base class for Pipe
    /// </summary>
    /// <typeparam name="TIn">Input data type</typeparam>
    /// <typeparam name="TOut">Output data type</typeparam>
    public abstract class Pipe<TIn, TOut> : IPipe<TIn, TOut>
    {
        protected readonly IPlugInContext _context;
        protected readonly IConfiguration _config;
        protected readonly ILogger _logger;
        protected readonly IMetrics _metrics;
        protected readonly ISubject<IEnvelope<TOut>> _subject = new Subject<IEnvelope<TOut>>();

        /// <summary>
        /// EventSource constructor that takes a context
        /// </summary>
        /// <param name="context">The context object providing access to config, logging and metrics</param>
        protected Pipe(IPlugInContext context)
        {
            this._context = context;
            this._config = context.Configuration;
            this._logger = context.Logger;
            this._metrics = context.Metrics;
        }
        
        /// <summary>
        /// Get the Id of the pipe
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Implement the IObserver.OnCompleted
        /// </summary>
        public void OnCompleted()
        {
            _subject.OnCompleted();
        }

        /// <summary>
        /// Implement the IObserver.OnError
        /// </summary>
        /// <param name="error"></param>
        public void OnError(Exception error)
        {
            _subject.OnError(error);
        }

        /// <summary>
        /// The implementation needs to process the value and pass to the _subject when appropriate
        /// </summary>
        /// <param name="value"></param>
        public abstract void OnNext(IEnvelope<TIn> value);

        public void OnNext(IEnvelope value)
        {
            OnNext((IEnvelope<TIn>)value);
        }

        /// <summary>
        /// Start the pipe
        /// </summary>
        public abstract void Start();

        /// <summary>
        /// Stop the pipe
        /// </summary>
        public abstract void Stop();

        /// <summary>
        /// Allow sinks to subscribe the pipe
        /// </summary>
        /// <param name="observer">A sink</param>
        /// <returns></returns>
        public IDisposable Subscribe(IObserver<IEnvelope<TOut>> observer)
        {
            return _subject.Subscribe(observer);
        }

        public IDisposable Subscribe(IObserver<IEnvelope> observer)
        {
            return Subscribe((IObserver<IEnvelope<TOut>>)observer);
        }
    }
}
