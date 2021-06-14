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
    /// Represents a source with a dependency on some other software on the system
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class AsyncDependentSource<T> : EventSource<T>
    {
        protected readonly Dependency _dependency;

        public AsyncDependentSource(Dependency dependency, IPlugInContext context) : base(context)
        {
            _dependency = dependency;
        }

        public TimeSpan DelayBetweenDependencyPoll { get; set; } = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Ensure that the dependency is available. If not, wait until it is available.
        /// </summary>
        /// <param name="cancellationToken">Cancel this taks.</param>
        protected virtual async ValueTask EnsureDependencyAvailable(CancellationToken cancellationToken)
        {
            var didFail = false;
            while (!IsDependencyAvailable())
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!didFail)
                {
                    await BeforeDependencyAvailable(cancellationToken);
                    didFail = true;
                }

                _logger.LogWarning($"Dependency '{_dependency.Name}' is not available so no events can be collected for source '{Id}'. Will check again in {DelayBetweenDependencyPoll.TotalSeconds} seconds.");
                await Task.Delay(DelayBetweenDependencyPoll, cancellationToken);
            }

            if (didFail)
            {
                _logger.LogInformation($"Dependency '{_dependency.Name}' is available and data can be collected.");
                await AfterDependencyAvailable(cancellationToken);
            }
        }

        protected bool IsDependencyAvailable()
        {
            try
            {
                return _dependency.IsDependencyAvailable();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while querying dependency '{0}'", _dependency.Name);
                return false;
            }
        }

        /// <summary>
        /// When implemented, execute code when the dependency was detected to be not-available.
        /// </summary>
        protected abstract ValueTask BeforeDependencyAvailable(CancellationToken cancellationToken);

        /// <summary>
        /// When implemented, execute code when the dependency is available again.
        /// </summary>
        protected abstract ValueTask AfterDependencyAvailable(CancellationToken cancellationToken);
    }
}
