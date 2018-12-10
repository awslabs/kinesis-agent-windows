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
using System.IO;

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
        protected IPlugInContext _context;
        protected IConfiguration _config;
        protected ILogger _logger;
        protected IMetrics _metrics;

        /// <summary>
        /// EventSource constructor that takes a context
        /// </summary>
        /// <param name="context">The context object providing access to config, logging and metrics</param>
        public EventSource(IPlugInContext context)
        {
            this._context = context;
            this._config = context.Configuration;
            this._logger = context.Logger;
            this._metrics = context.Metrics;
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
        /// </summary>
        public DateTime? InitialPositionTimestamp { get; set; }

        /// <summary>
        /// A helper method to load the common config parameters.
        /// </summary>
        /// <param name="config">The config section for the source</param>
        /// <param name="source">The source to be configured</param>
        public static void LoadCommonSourceConfig(IConfiguration config, EventSource<T> source)
        {
            InitialPositionEnum initialPosition = InitialPositionEnum.EOS;
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
                            source.InitialPositionTimestamp = DateTime.Parse(initialPositionTimeStamp, null, System.Globalization.DateTimeStyles.RoundtripKind);
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

        /// <summary>
        /// The abstract Start method. Must be implemented by the subclass.
        /// </summary>
        public abstract void Start();

        /// <summary>
        /// The abstract Stop method. Must be implemented by the subclass.
        /// </summary>
        public abstract void Stop();

        /// <summary>
        /// Implementation of IObservable
        /// </summary>
        /// <param name="observer"></param>
        /// <returns></returns>
        public abstract IDisposable Subscribe(IObserver<IEnvelope<T>> observer);

        public IDisposable Subscribe(IObserver<IEnvelope> observer)
        {
            return Subscribe((IObserver<IEnvelope<T>>)observer);
        }

        /// <summary>
        /// Get the path for the bookmark file
        /// </summary>
        /// <returns></returns>
        protected string GetBookmarkFilePath()
        {
            return Path.Combine(Utility.GetKinesisTapProgramDataPath(), ConfigConstants.BOOKMARKS, $"{this.Id}.bm");
        }
    }
}
