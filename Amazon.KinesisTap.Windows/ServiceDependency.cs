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
using System.Runtime.Versioning;
using System.ServiceProcess;

namespace Amazon.KinesisTap.Windows
{
    /// <summary>
    /// Represents dependency on a Windows Service
    /// </summary>
    [SupportedOSPlatform("windows")]
    public class ServiceDependency : Dependency
    {
        /// <summary>
        /// Name of the service.
        /// </summary>
        private readonly string _dependentServiceName;

        /// <inheritdoc/>
        public override string Name => $"Service {_dependentServiceName}";

        private readonly ServiceController _controller;

        /// <summary>
        /// Initialize a <see cref="ServiceDependency"/> object.
        /// </summary>
        /// <param name="dependentServiceName"></param>
        public ServiceDependency(string dependentServiceName)
        {
            _dependentServiceName = dependentServiceName;
            _controller = new ServiceController(dependentServiceName);
        }

        /// <inheritdoc/>
        public override bool IsDependencyAvailable()
        {
            try
            {
                _controller.Refresh();
                _controller.WaitForStatus(ServiceControllerStatus.Running, TimeSpan.FromMilliseconds(200));
                return _controller.Status.Equals(ServiceControllerStatus.Running);
            }
            catch (InvalidOperationException)
            {
                return false;
            }
        }

        // To detect redundant calls
        private bool _disposed = false;

        // Protected implementation of Dispose pattern.
        protected override void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                // Dispose managed state (managed objects).
                _controller.Dispose();
            }

            _disposed = true;
        }
    }
}
