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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using AsyncFriendlyStackTrace;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Core
{
    public abstract class TimerPlugin : GenericPlugin
    {
        Timer _timer;

        public int Interval { get; set; }

        public TimerPlugin(IPlugInContext context) : base(context)
        {
            _timer = new Timer(this.OnTimerInternal, null, Timeout.Infinite, Timeout.Infinite);
        }

        public override void Start()
        {
            //Randomize the first time
            int dueTime = Utility.Random.Next(Interval * 60000); //in milliseconds
            _timer.Change(dueTime, Interval * 60000);
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
            _timer.Change(Interval * 60000, Interval * 60000);
        }

        private void OnTimerInternal(object stateInfo)
        {
            DisableTimer();
            try
            {
                OnTimer().Wait();
            }
            catch(Exception ex)
            {
                _logger?.LogError($"Plugin {this.Id} exception: {ex.ToAsyncString()}");
            }
            EnableTimer();
        }
    }
}
