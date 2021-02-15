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
    public abstract class TimerPlugin : GenericPlugin
    {
        protected Timer _timer;
        protected readonly NetworkStatus _networkStatus;

        //Timestamp between plug-in invocation
        public TimeSpan Interval { get; protected set; }

        public TimerPlugin(IPlugInContext context) : base(context)
        {
            _networkStatus = context.NetworkStatus;
            _timer = new Timer(this.OnTimerInternal, null, Timeout.Infinite, Timeout.Infinite);
        }

        public override void Start()
        {
            //Randomize the first time
            int dueTime = Utility.Random.Next((int)Interval.TotalMilliseconds); //in milliseconds
            _timer.Change(dueTime, (int)Interval.TotalMilliseconds);
        }

        public override void Stop()
        {
            DisableTimer();
        }

        protected abstract Task OnTimer();

        private void DisableTimer()
        {
            _timer.Change(Timeout.Infinite, Timeout.Infinite);
        }

        private void EnableTimer()
        {
            _timer.Change((int)Interval.TotalMilliseconds, (int)Interval.TotalMilliseconds);
        }

        protected void OnTimerInternal(object stateInfo)
        {
            DisableTimer();
            try
            {
                OnTimer().Wait();
            }
            catch(Exception ex)
            {
                _logger?.LogError($"Plugin {this.Id} exception: {ex.ToMinimized()}");
            }
            EnableTimer();
        }
    }
}
