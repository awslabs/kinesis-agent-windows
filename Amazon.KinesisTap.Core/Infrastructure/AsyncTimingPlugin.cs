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
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Base class for plugins that execute periodically.
    /// </summary>
    public abstract class AsyncTimingPlugin : IGenericPlugin
    {
        protected readonly int _intervalMs;
        protected readonly bool _inclusiveTiming;
        protected readonly ILogger _logger;

        private Task _loop;

        /// <summary>
        /// Create a timing plugin.
        /// </summary>
        /// <param name="id">Plugin's ID</param>
        /// <param name="intervalMs">Execution interval in milliseconds</param>
        /// <param name="inclusiveTiming">Whether the execution time should be counted towards intervals</param>
        /// <param name="logger">Logger</param>
        public AsyncTimingPlugin(string id, int intervalMs, bool inclusiveTiming, ILogger logger)
        {
            Id = id;
            if (intervalMs <= 0)
            {
                throw new ArgumentException($"Interval must be positive", nameof(intervalMs));
            }
            _intervalMs = intervalMs;
            _inclusiveTiming = inclusiveTiming;
            _logger = logger;
        }

        public string Id { get; set; }

        /// <inheritdoc/>
        public virtual ValueTask StartAsync(CancellationToken stopToken)
        {
            _loop = AsyncLoop(stopToken);
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc/>
        public virtual async ValueTask StopAsync(CancellationToken stopToken)
        {
            if (_loop is not null)
            {
                await _loop;
            }
        }

        protected bool IsRunning() => _loop is not null && !_loop.IsCompleted;

        private async Task AsyncLoop(CancellationToken stopToken)
        {
            var delayMs = 0;
            while (!stopToken.IsCancellationRequested)
            {
                var startTimestamp = Utility.GetElapsedMilliseconds();
                try
                {
                    if (delayMs > 0)
                    {
                        _logger.LogTrace("AsyncTimerPlugin delaying {0} ms", delayMs);
                        await Task.Delay(delayMs, stopToken);
                        startTimestamp = Utility.GetElapsedMilliseconds();
                    }

                    await ExecuteActionAsync(stopToken);

                    delayMs = _intervalMs;
                    if (_inclusiveTiming)
                    {
                        delayMs -= (int)(Utility.GetElapsedMilliseconds() - startTimestamp);
                    }
                }
                catch (OperationCanceledException) when (stopToken.IsCancellationRequested)
                {
                    _logger.LogTrace("AsyncTimerPlugin loop stopping");
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error executing action");
                    delayMs = _intervalMs;
                    if (_inclusiveTiming)
                    {
                        delayMs -= (int)(Utility.GetElapsedMilliseconds() - startTimestamp);
                    }
                }
            }
        }

        /// <summary>
        /// When implemented, execute the scheduled action.
        /// </summary>
        /// <param name="stopToken">Token that throws when this plugin stops.</param>
        /// <returns>A task that completes when the action is done.</returns>
        protected abstract ValueTask ExecuteActionAsync(CancellationToken stopToken);
    }
}
