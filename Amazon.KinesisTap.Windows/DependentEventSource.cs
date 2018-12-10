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
using Amazon.KinesisTap.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.ServiceProcess;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Windows
{
    // Provides support for KinesisTap sources which depend on other Windows servers being in a running state in order
    // to work correctly.
    public abstract class DependentEventSource<T> : EventSource<T>, IDisposable
    {
        public string DependentServiceName { get; private set; }

        private DateTime? _dependencyFailStart = null;
        private DateTime? _dependencyFailLastReported = null;
        private CancellationTokenSource _cancellationTokenSource = null;
        private ServiceController _controller = null;
        private int _resetInProgress = 0;

        private static readonly TimeSpan DelayBetweenDependencyPoll = TimeSpan.FromMinutes(5);
        private static readonly TimeSpan MinimumDelayBetweenDependencyFailureLogging = TimeSpan.FromHours(1);


        public DependentEventSource(string dependentServiceName, IPlugInContext context) : base(context)
        {
            DependentServiceName = dependentServiceName;
        }

        /// <summary>
        /// Waits until the dependent service is running or the polling is cancelled (during source stop),
        /// then calls the AfterDependencyRunning method.  Periodically logs during the dependent service outage. 
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
            }
            if (IsDependencyRunning())
            {
                try
                {
                    AfterDependencyRunning();
                }
                catch (Exception e)
                {
                    _logger.LogError($"Error invoking AfterDependencyRunning when dependent service {DependentServiceName} for source {Id} transitioned to running: {e}");
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
                PollForService(token);
            }, 
            token);
        }

        /// <summary>
        /// Called once the dependency service is running.  Must be overridden by any child classes.
        /// </summary>
        protected abstract void AfterDependencyRunning();

        protected void MaybeCancelPolling()
        {
            if (_cancellationTokenSource != null)
            {
                _cancellationTokenSource.Cancel();
            }
        }

        /// <summary>
        /// Returns true if the dependent service is running.
        /// </summary>
        /// <returns>True if the dependent service is running</returns>
        protected bool IsDependencyRunning()
        {
            for (int i = 0; i < 2; i++)
            {
                try
                {
                    if (_controller == null)
                    {
                        _controller = new ServiceController(DependentServiceName);
                    }
                    else
                    {
                        _controller.Refresh();
                    }
                    _controller.WaitForStatus(ServiceControllerStatus.Running, TimeSpan.FromMilliseconds(200));
                    _controller.Refresh();
                    return _controller.Status.Equals(ServiceControllerStatus.Running);
                }
                catch (Exception)
                {
                    _controller = null;
                }
            }
            return false;
        }

        private void PollForService(CancellationToken token)
        {
            try
            {
                while (!IsDependencyRunning() && !token.IsCancellationRequested)
                {
                    try
                    {
                        if (!_dependencyFailStart.HasValue)
                        {
                            _dependencyFailStart = DateTime.UtcNow;
                        }
                        if (!_dependencyFailLastReported.HasValue)
                        {
                            _logger.LogError($"Dependent service {DependentServiceName} is not running so no events can be collected for source {Id}.");
                            _dependencyFailLastReported = DateTime.UtcNow;
                        }
                        else if (DateTime.UtcNow - _dependencyFailLastReported.Value > MinimumDelayBetweenDependencyFailureLogging)
                        {
                            _logger.LogError($"Dependent service {DependentServiceName} has not been running for {(DateTime.UtcNow - _dependencyFailStart)}.  "
                                + "No events have been collected for that period of time.");
                            _dependencyFailLastReported = DateTime.UtcNow;
                        }
                        token.WaitHandle.WaitOne(DelayBetweenDependencyPoll);
                    }
                    catch (Exception e)
                    {
                        _logger.LogError($"Error during polling of dependent service {DependentServiceName} for source {Id}: {e}");
                    }
                }
                if (token.IsCancellationRequested)
                {
                    token.ThrowIfCancellationRequested();
                }

                try
                {
                    AfterDependencyRunning();
                }
                catch (Exception e)
                {
                    _logger.LogError($"Error invoking AfterDependencyRunning when dependent service {DependentServiceName} for source {Id} transitioned to running: {e}");
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
        /// Disposes the service controller if not null.
        /// </summary>
        /// <param name="disposing">Should be true if disposing referenced objects is desired, otherwise false</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    if (_controller != null)
                    {
                        _controller.Dispose();
                        _controller = null;
                    }
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
