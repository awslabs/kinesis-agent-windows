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
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// This class is the EventSource base class.
    /// </summary>
    /// <typeparam name="T">The type of the event data</typeparam>
    public abstract class EventSource<T> : IEventSource<T>
    {
        protected readonly IPlugInContext _context;
        protected readonly IConfiguration _config;
        protected readonly ILogger _logger;
        protected readonly IMetrics _metrics;
        protected readonly bool _required;
        protected DateTime _initialPositionTimestamp = DateTime.MinValue.ToUniversalTime();

        /// <summary>
        /// EventSource constructor that takes a context
        /// </summary>
        /// <param name="context">The context object providing access to config, logging and metrics</param>
        public EventSource(IPlugInContext context)
        {
            _context = context;
            _config = context.Configuration;
            _logger = context.Logger;
            _metrics = context.Metrics;
            string required = _config?[ConfigConstants.REQUIRED];
            if (string.IsNullOrWhiteSpace(required))
            {
                _required = true;
            }
            else
            {
                _required = bool.Parse(required);
            }
        }

        /// <summary>
        /// Gets or Sets the Id of the EventSource
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Gets or sets initial position of the source
        /// </summary>
        public InitialPositionEnum InitialPosition { get; set; }

        /// <summary>
        /// Gets or sets optional InitialPositionTimestamp. Used only if InitialPosition == InitialPostionEnum.Timestamp
        /// This value will be converted to UTC-time.
        /// </summary>
        public DateTime InitialPositionTimestamp
        {
            get => _initialPositionTimestamp;
            set
            {

                if (value.Kind == DateTimeKind.Unspecified)
                {
                    throw new ArgumentException("InitialPositionTimestamp must have defined DateTimeKind");
                }

                if (value.Kind == DateTimeKind.Local)
                {
                    _initialPositionTimestamp = value.ToUniversalTime();
                }
                else
                {
                    _initialPositionTimestamp = value;
                }
            }
        }

        /// <summary>
        /// A helper method to load the common config parameters.
        /// </summary>
        /// <param name="config">The config section for the source</param>
        /// <param name="source">The source to be configured</param>
        public static void LoadCommonSourceConfig(IConfiguration config, EventSource<T> source)
        {
            InitialPositionEnum initialPosition = InitialPositionEnum.Bookmark;
            string initialPositionConfig = config["InitialPosition"];
            if (!string.IsNullOrEmpty(initialPositionConfig))
            {
                switch (initialPositionConfig.ToLower())
                {
                    case "eos":
                        initialPosition = InitialPositionEnum.EOS;
                        break;
                    case "0":
                        initialPosition = InitialPositionEnum.BOS;
                        break;
                    case "bookmark":
                        initialPosition = InitialPositionEnum.Bookmark;
                        break;
                    case "timestamp":
                        initialPosition = InitialPositionEnum.Timestamp;
                        string initialPositionTimeStamp = config["InitialPositionTimestamp"];
                        if (string.IsNullOrWhiteSpace(initialPositionTimeStamp))
                        {
                            throw new Exception("Missing initial position timestamp.");
                        }

                        try
                        {
                            var timeZone = Utility.ParseTimeZoneKind(config["TimeZoneKind"]);
                            var timestamp = DateTime.Parse(initialPositionTimeStamp, null, System.Globalization.DateTimeStyles.RoundtripKind);
                            source.InitialPositionTimestamp = timeZone == DateTimeKind.Utc
                                ? DateTime.SpecifyKind(timestamp, DateTimeKind.Utc)
                                : DateTime.SpecifyKind(timestamp, DateTimeKind.Local).ToUniversalTime();
                        }
                        catch
                        {
                            throw new Exception($"Invalid InitialPositionTimestamp {initialPositionTimeStamp}");
                        }

                        break;
                    default:
                        throw new Exception($"Invalid InitialPosition {initialPositionConfig}");
                }
            }

            source.InitialPosition = initialPosition;
        }

        /// <inheritdoc/>
        public virtual ValueTask StartAsync(CancellationToken stopToken)
        {
            Start();
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc/>
        public virtual ValueTask StopAsync(CancellationToken gracefulStopToken)
        {
            Stop();
            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// When implemented, start the source synchronously.
        /// Subclasses either implements this method or overwrite <see cref="StartAsync(CancellationToken)"/> if asynchronous start is required.
        /// </summary>
        public abstract void Start();

        /// <summary>
        /// When implemented, stop the source synchronously.
        /// Subclasses either implements this method or overwrite <see cref="StopAsync(CancellationToken)"/> if asynchronous stop is required.
        /// </summary>
        public abstract void Stop();

        /// <summary>
        /// Implementation of IObservable
        /// </summary>
        /// <param name="observer"></param>
        public abstract IDisposable Subscribe(IObserver<IEnvelope<T>> observer);

        public IDisposable Subscribe(IObserver<IEnvelope> observer)
        {
            return Subscribe((IObserver<IEnvelope<T>>)observer);
        }

        /// <summary>
        /// Default to true. If not EventSource is not required, suppress error if the event source cannot be started.
        /// Each individual source decides whether to support this flag
        /// </summary>
        public bool Required => _required;

        public Type GetOutputType() => typeof(T);
    }
}
