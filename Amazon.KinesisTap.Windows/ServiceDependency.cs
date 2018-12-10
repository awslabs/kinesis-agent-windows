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
using System.Threading.Tasks;
using System.ServiceProcess;

namespace Amazon.KinesisTap.Windows
{
    public class ServiceDependency : Dependency
    {
        public string DependentServiceName { get; private set; }

        public override string Name => $"Service {DependentServiceName}";

        private ServiceController _controller = null;


        public ServiceDependency(string dependentServiceName)
        {
            this.DependentServiceName = dependentServiceName;
        }

        public override bool IsDependencyAvailable()
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

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_controller != null)
                {
                    _controller.Dispose();
                    _controller = null;
                }
            }
        }
    }
}
