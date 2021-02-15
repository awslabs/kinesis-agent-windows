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
            _logger?.LogInformation("ListEventSink stopped");
        }
    }
}
