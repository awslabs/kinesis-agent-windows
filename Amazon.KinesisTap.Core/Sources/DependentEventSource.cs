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
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Core
{
    // Provides support for KinesisTap sources which depend on a local resource being available in order
    // to work correctly.
    public abstract class DependentEventSource<T> : EventSource<T>, IDisposable
    {

        protected readonly Dependency _dependency;

        private DateTime? _dependencyFailStart = null;
        private DateTime? _dependencyFailLastReported = null;
        private CancellationTokenSource _cancellationTokenSource = null;
        private int _resetInProgress = 0;

        public TimeSpan DelayBetweenDependencyPoll { get; set; } = TimeSpan.FromMinutes(1);
        private static readonly TimeSpan MinimumDelayBetweenDependencyFailureLogging = TimeSpan.FromHours(1);

        public DependentEventSource(Dependency dependency, IPlugInContext context) : base(context)
        {
            Guard.ArgumentNotNull(dependency, "dependency");
            this._dependency = dependency;
        }

        /// <summary>
        /// Waits until the dependency is available or the polling is cancelled (during source stop),
        /// then calls the AfterDependencyRunning method.  Periodically logs during the dependency unavailable outage. 
        /// </summary>
        public virtual void Reset()
        {
            // Atomically test and set _resetInProgress to 1 if it is not already 1.  If oldValue != 0 then
            // some other thread beat us to the reset, so we yield to them.
            int oldValue = Interlocked.CompareExchange(ref this._resetInProgress, 1, 0);
            if (oldValue != 0)
            {
                return;
            }
            if (_cancellationTokenSource != null)
            {
                _cancellationTokenSource.Cancel();
                _cancellationTokenSource = null;
            }
            if (_dependency.IsDependencyAvailable())
            {
                try
                {
                    AfterDependencyAvailable();
                }
                catch (Exception e)
                {
                    _logger?.LogError($"Error invoking AfterDependencyRunning when dependent {_dependency.Name} for source {Id} transitioned to available: {e.ToMinimized()}");
                }
                finally
                {
                    _resetInProgress = 0;
                }
                return;
            }
            _cancellationTokenSource = new CancellationTokenSource();
            CancellationToken token = _cancellationTokenSource.Token;
            Task.Run(() =>
            {
                PollForDependency(token);
            }, 
            token);
        }

        /// <summary>
        /// Called once the dependency is available.  Must be overridden by any child classes.
        /// </summary>
        protected abstract void AfterDependencyAvailable();

        /// <summary>
        /// If polling is occurring, cancel the polling.
        /// </summary>
        protected void MaybeCancelPolling()
        {
            _cancellationTokenSource?.Cancel();
        }

        private void PollForDependency(CancellationToken token)
        {
            try
            {
                while (!_dependency.IsDependencyAvailable() && !token.IsCancellationRequested)
                {
                    try
                    {
                        if (!_dependencyFailStart.HasValue)
                        {
                            _dependencyFailStart = DateTime.UtcNow;
                        }
                        if (!_dependencyFailLastReported.HasValue)
                        {
                            _logger?.LogError($"Dependent {_dependency.Name} is not available so no events can be collected for source {Id}. Will check again in {DelayBetweenDependencyPoll.TotalSeconds} seconds.");
                            _dependencyFailLastReported = DateTime.UtcNow;
                        }
                        else if (DateTime.UtcNow - _dependencyFailLastReported.Value > MinimumDelayBetweenDependencyFailureLogging)
                        {
                            _logger?.LogError($"Dependent {_dependency.Name} has not been available for {(DateTime.UtcNow - _dependencyFailStart)}.  "
                                + $"No events have been collected for that period of time. Will check again in {DelayBetweenDependencyPoll.TotalSeconds} seconds.");
                            _dependencyFailLastReported = DateTime.UtcNow;
                        }
                        token.WaitHandle.WaitOne(DelayBetweenDependencyPoll);
                    }
                    catch (Exception e)
                    {
                        _logger?.LogError($"Error during polling of dependent {_dependency.Name} for source {Id}: {e.ToMinimized()}");
                    }
                }
                if (token.IsCancellationRequested)
                {
                    token.ThrowIfCancellationRequested();
                }

                try
                {
                    AfterDependencyAvailable();
                    _logger?.LogInformation($"Dependent {_dependency.Name} is now available and events are being collected.");
                }
                catch (Exception e)
                {
                    _logger?.LogError($"Error invoking AfterDependencyRunning when dependent {_dependency.Name} for source {Id} transitioned to running: {e.ToMinimized()}");
                }
            }
            finally
            {
                _cancellationTokenSource = null;
                _dependencyFailStart = null;
                _dependencyFailLastReported = null;
                _resetInProgress = 0;
            }
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        /// <summary>
        /// Disposes the dependency if desired.
        /// </summary>
        /// <param name="disposing">Should be true if disposing referenced objects is desired, otherwise false</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _dependency.Dispose();
                }
                disposedValue = true;
            }
        }

        /// <summary>
        /// Disposes of any managed resources which may also capture unmanaged resources in their internal implementation.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }
        #endregion

    }
}
