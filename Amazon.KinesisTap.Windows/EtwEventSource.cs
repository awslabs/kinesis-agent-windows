/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
 using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Diagnostics.Tracing;
using Microsoft.Diagnostics.Tracing.Session;
using Microsoft.Diagnostics.Tracing.Parsers;
using System.Reactive.Subjects;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Windows
{
    /// <summary>
    /// A KinesisTap event source which captures ETW events from Windows.
    /// </summary>
    public class EtwEventSource : EventSource<EtwEvent>, IDisposable
    {
        private readonly string _providerName;
        private readonly TraceEventLevel _traceLevel;
        private readonly ulong _matchAnyKeywords;
        private string _sessionName;
        private TraceEventSession _session;
        private ETWTraceEventSource _source;
        private CancellationTokenSource _cancelTokenSource = new CancellationTokenSource();
        private CancellationToken _cancelToken;
        private Task _etwTask;
        private ISubject<IEnvelope<EtwEvent>> _eventSubject = new Subject<IEnvelope<EtwEvent>>();
        private bool _disposedValue = false; // To detect redundant calls

        /// <summary>
        /// When an event arrives, create and populate an EtwEvent from the trace data and then wrap the EtwEvent.
        /// </summary>
        /// <param name="traceData">The data produced by the ETW provider.</param>
        /// <returns>A wrapped EtwEvent.</returns>
        protected virtual EtwEventEnvelope WrapTraceEvent(TraceEvent traceData)
        {
            var envelope = new EtwEventEnvelope(traceData);
            envelope.Data.ExecutingThreadID = traceData.ThreadID;
            return envelope;
        }
        

        public EtwEventSource(string providerName, TraceEventLevel traceLevel, ulong matchAnyKeywords, IPlugInContext context) : base(context)
        {
            Guard.ArgumentNotNullOrEmpty(providerName, nameof(providerName));
            _providerName = providerName;
            _traceLevel = traceLevel;
            _matchAnyKeywords = matchAnyKeywords;

        }

        /// <summary>
        /// Begin gathering ETW events from Windows and reporting these events to KinesisTap.
        /// </summary>
        public override void Start()
        {
            _cancelToken = _cancelTokenSource.Token;
            _sessionName = $"KinesisTap-{Guid.NewGuid().ToString()}";
            _session = new TraceEventSession(_sessionName, null);  //Null means create a real-time session as opposed to a file dumping session.
            _session.StopOnDispose = true;
            _source = new ETWTraceEventSource(_sessionName, TraceEventSourceType.Session);
            var parser = new DynamicTraceEventParser(_source);
            parser.All += ProcessTraceEvent;

            EnableProvider();

            try
            {
                _metrics?.InitializeCounters(this.Id, MetricsConstants.CATEGORY_SOURCE, CounterTypeEnum.Increment,
                    new Dictionary<string, MetricValue>()
                    {
                        { MetricsConstants.ETWEVENT_SOURCE_EVENTS_READ, MetricValue.ZeroCount },
                        { MetricsConstants.ETWEVENT_SOURCE_EVENTS_ERROR, MetricValue.ZeroCount }
                    });

                _etwTask = Task.Factory.StartNew(() =>
                {
                    _cancelToken.ThrowIfCancellationRequested();
                    while (_source != null)
                    {
                        if (_cancelToken.IsCancellationRequested)
                        {
                            _cancelToken.ThrowIfCancellationRequested();
                        }

                       GatherSourceEvents();
                    }
                });

                _etwTask.Wait(TimeSpan.FromSeconds(1));  //See if there are any initial exceptions we need to handle.
                _logger?.LogInformation($"EtwEvent source id {this.Id} for provider {_providerName} with match any keywords {_matchAnyKeywords} started.");


            }
            catch (AggregateException ae)
            {
                DisposeSourceAndSession();
                if (ContainsOperationCancelled(ae))
                {
                    _logger?.LogWarning("EtwEventSource id {this.Id} for provider {_providerName} with match any keywords {_matchAnyKeywords} encountered task cancellation during start.");
                }
                ae.Handle((exception) =>
                {
                    _logger?.LogError($"EtwEventSource id {this.Id} for provider {_providerName} with match any keywords {_matchAnyKeywords} encountered exception {exception} during start");
                    return true;
                });
            }
        }

        /// <summary>
        /// Enable the ETW provider to request that Windows start reporting ETW events associated with that provider with appropriate 
        /// filtering applied.
        /// </summary>
        protected virtual void EnableProvider()
        {
            _session.EnableProvider(_providerName, _traceLevel, _matchAnyKeywords);
        }

        /// <summary>
        /// Start gathering events from the provider.
        /// </summary>
        protected virtual void GatherSourceEvents()
        {
            _source.Process();  //Note that this blocks so it needs to be run in its own task.
        }

        /// <summary>
        /// When an ETW event arrives from the provider, transform it and send it on to KinesisTap as a wrapped EtwEvent.
        /// </summary>
        /// <param name="traceData"></param>
        protected void ProcessTraceEvent(TraceEvent traceData)
        {
            if (traceData.EventName.Equals("Stop"))
            {
                DisposeSourceAndSession();
                throw new OperationCanceledException("Stop event received", _cancelToken);
            }
            try
            {
                _eventSubject.OnNext(WrapTraceEvent(traceData));
                _metrics?.PublishCounter(this.Id, MetricsConstants.CATEGORY_SOURCE, CounterTypeEnum.Increment,
                    MetricsConstants.ETWEVENT_SOURCE_EVENTS_READ, 1, MetricUnit.Count);
            }
            catch (Exception exception)
            {
                _logger?.LogError($"EtwEvent source id {this.Id} for provider {_providerName} with match any keywords {_matchAnyKeywords} encountered exception {exception} during event processing.");
                _metrics?.PublishCounter(this.Id, MetricsConstants.CATEGORY_SOURCE, CounterTypeEnum.Increment,
                    MetricsConstants.ETWEVENT_SOURCE_EVENTS_ERROR, 1, MetricUnit.Count);
            }

        }

        /// <summary>
        /// Be sure to dispose of any non-disposed ETW sources and sessions since there are 
        /// kernel level objects which will stay around until these objects are disposed.
        /// </summary>
        protected void DisposeSourceAndSession()
        {
            if (_source != null)
            {
                _source.Dispose();
                _source = null;
            }

            if (_session != null)
            {
                _session.Dispose();
                _session = null;
            }
        }

        private bool ContainsOperationCancelled(AggregateException ae)
        {
            if (ae.InnerExceptions != null)
            {
                foreach (Exception e in ae.InnerExceptions)
                {
                    if (e is AggregateException)
                    {
                        if (ContainsOperationCancelled((AggregateException)e))
                        {
                            return true;
                        }
                    }
                    else
                    {
                        if (e is OperationCanceledException)
                        {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        /// <summary>
        /// Stop collecting ETW events and reporting them to KinesisTap.
        /// </summary>
        public override void Stop()
        {
            try
            {
                _cancelTokenSource.Cancel();
                DisposeSourceAndSession();
                _etwTask.Wait(TimeSpan.FromSeconds(1));
                _logger?.LogInformation($"EtwEvent source id {this.Id} for provider {_providerName} with match any keywords {_matchAnyKeywords} stopped.");
            }
            catch (AggregateException ae)
            {
                if (ContainsOperationCancelled(ae))
                {
                    _logger?.LogInformation($"EtwEvent source id {this.Id} for provider {_providerName} with match any keywords {_matchAnyKeywords} stopped.");
                    return;
                }
                ae.Handle((exception) =>
                {
                    _logger?.LogError($"EtwEventSource id {this.Id} for provider {_providerName} with match any keywords {_matchAnyKeywords} encountered exception {exception} during stop.");
                    return true;
                });
            }
        }

        /// <summary>
        /// Used by KinesisTap to connect a source to whatever KinesisTap object will receive EtwEvents.
        /// </summary>
        /// <param name="observer"></param>
        /// <returns></returns>
        public override IDisposable Subscribe(IObserver<IEnvelope<EtwEvent>> observer)
        {
            return this._eventSubject.Subscribe(observer);
        }

        /// <summary>
        /// When we're done, make sure to release the source and session if they haven't already been disposed.
        /// </summary>
        public void Dispose()
        {
            if (!_disposedValue)
            {
                DisposeSourceAndSession();
                _disposedValue = true;
            }
        }

    }
}
