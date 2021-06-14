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
using System.Timers;
using Amazon.KinesisTap.AWS.Failover.Strategy;
using Amazon.KinesisTap.Core;
using Amazon.Runtime;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.AWS.Failover
{
    /// <summary>
    /// A sink class that inherits from <see cref="EventSink"/>.
    /// </summary>
    public class FailoverSink<TAWSClient> : EventSink, IFailoverSink<TAWSClient>, IDisposable where TAWSClient : AmazonServiceClient
    {
        /// <summary>
        /// Maximum consecutive errors before failover.
        /// </summary>
        protected readonly int _maxErrorsCountBeforeFailover;

        /// <summary>
        /// Maximum wait interval before failover.
        /// </summary>
        protected readonly int _maxWaitIntervalBeforeFailover;

        /// <summary>
        /// Secondary Region Failover Timer.
        /// </summary>
        protected readonly Timer _secondaryRegionFailoverTimer;

        /// <summary>
        /// Secondary Region Failover Timer Activated.
        /// </summary>
        protected bool _secondaryRegionFailoverActivated = false;

        /// <summary>
        /// Sink Regional Strategy.
        /// </summary>
        protected readonly FailoverStrategy<TAWSClient> _failoverSinkRegionStrategy;

        /// <summary>
        /// Initializes a new instance of the <see cref="FailoverSink{TAWSClient}"/> class.
        /// </summary>
        /// <param name="context">The <see cref="IPlugInContext"/> that contains configuration info, logger, metrics etc.</param>
        /// <param name="failoverSinkRegionStrategy">The <see cref="FailoverStrategy{TAWSClient}"/> that defines sinks region selection strategy.</param>
        public FailoverSink(
            IPlugInContext context,
            FailoverStrategy<TAWSClient> failoverSinkRegionStrategy)
            : base(context)
        {
            // Parse or default
            // Max errors count before failover
            if (!int.TryParse(_config[ConfigConstants.MAX_ERRORS_COUNT_BEFORE_FAILOVER], out _maxErrorsCountBeforeFailover))
            {
                _maxErrorsCountBeforeFailover = ConfigConstants.DEFAULT_MAX_CONSECUTIVE_ERRORS_COUNT;
            }
            else if (_maxErrorsCountBeforeFailover < 1)
            {
                throw new ArgumentException(String.Format("Invalid \"{0}\" value, please provide positive integer.",
                    ConfigConstants.MAX_ERRORS_COUNT_BEFORE_FAILOVER));
            }

            // Max wait interval before failover
            if (!int.TryParse(_config[ConfigConstants.MAX_FAILOVER_INTERVAL_IN_MINUTES], out _maxWaitIntervalBeforeFailover))
            {
                _maxWaitIntervalBeforeFailover = ConfigConstants.DEFAULT_MAX_WAIT_BEFORE_REGION_FAILOVER_IN_MINUTES;
            }
            else if (_maxWaitIntervalBeforeFailover < 1)
            {
                throw new ArgumentException(String.Format("Invalid \"{0}\" value, please provide positive integer.",
                    ConfigConstants.MAX_FAILOVER_INTERVAL_IN_MINUTES));
            }

            // Setup Failover Strategy
            _failoverSinkRegionStrategy = failoverSinkRegionStrategy;

            // Setup Secondary Region Failover Timer
            _secondaryRegionFailoverTimer = new Timer(_maxWaitIntervalBeforeFailover * 60 * 1000);
            _secondaryRegionFailoverTimer.Elapsed += new ElapsedEventHandler(FailOverToSecondaryRegion);
            _secondaryRegionFailoverTimer.AutoReset = false;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _secondaryRegionFailoverTimer.Stop();
        }

        /// <inheritdoc/>
        public override void OnNext(IEnvelope envelope) => throw new NotImplementedException();

        /// <inheritdoc/>
        public override void Start() => throw new NotImplementedException();

        /// <inheritdoc/>
        public override void Stop() => throw new NotImplementedException();

        /// <inheritdoc/>
        public TAWSClient FailbackToPrimaryRegion(Throttle throttle)
        {
            _logger?.LogDebug($"FailoverSink id {Id} is trying to fail back to primary region.");
            // Setup client with Primary Region
            var client = _failoverSinkRegionStrategy.GetPrimaryRegionClient();
            if (client is not null)
            {
                // Reset Throttle
                throttle.SetSuccess();

                _logger?.LogInformation($"FailoverSink id {Id} failed back successfully to primary region {_failoverSinkRegionStrategy.GetCurrentRegion().Region.SystemName}.");
                return client;
            }
            else
            {
                _logger?.LogDebug($"FailoverSink id {Id} fail back to primary region unsuccessful, primary region currently is down or in use.");
            }

            return null;
        }

        /// <inheritdoc/>
        public TAWSClient FailOverToSecondaryRegion(Throttle throttle)
        {
            // Stop timer if no errors
            if (throttle.ConsecutiveErrorCount == 0)
            {
                _secondaryRegionFailoverTimer.Stop();
                _secondaryRegionFailoverActivated = false;
            }

            // Start timer on first error
            else if (throttle.ConsecutiveErrorCount > 0 && !_secondaryRegionFailoverTimer.Enabled)
            {
                _secondaryRegionFailoverTimer.Start();
            }

            // Failover to Secondary Region if available
            // Reaching maximum consecutive error counts or timeout
            if (throttle.ConsecutiveErrorCount >= _maxErrorsCountBeforeFailover || _secondaryRegionFailoverActivated)
            {
                _logger?.LogWarning($"FailoverSink id {Id} max consecutive errors count {throttle.ConsecutiveErrorCount}, trying to fail over to secondary region.");
                // Setup client with Secondary Region
                var client = _failoverSinkRegionStrategy.GetSecondaryRegionClient();
                if (client is not null)
                {
                    // Reset Throttle
                    throttle.SetSuccess();

                    _logger?.LogInformation($"FailoverSink id {Id} after reaching max consecutive errors limit to {throttle.ConsecutiveErrorCount}, failed over successfully to secondary region {_failoverSinkRegionStrategy.GetCurrentRegion().Region.SystemName}.");
                    return client;
                }
                else
                {
                    _logger?.LogError($"FailoverSink id {Id} fail over to secondary region unsuccessful, looks like all of secondary regions are currently down.");
                }
            }

            return null;
        }

        private void FailOverToSecondaryRegion(Object source, ElapsedEventArgs e)
        {
            _secondaryRegionFailoverActivated = true;
        }
    }
}
