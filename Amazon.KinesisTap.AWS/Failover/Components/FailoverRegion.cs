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

namespace Amazon.KinesisTap.AWS.Failover.Components
{
    /// <summary>
    /// A class for failover region state.
    /// </summary>
    public class FailoverRegion<TRegion> : IDisposable, IFailoverRegion<TRegion>
    {
        /// <summary>
        /// Region in use.
        /// </summary>
        protected bool _isRegionInUse;

        /// <summary>
        /// Region is available.
        /// </summary>
        protected bool _isRegionIsDown;

        /// <summary>
        /// Reset Timer.
        /// </summary>
        protected readonly Timer _resetTimer;

        /// <summary>
        /// Region value.
        /// </summary>
        public TRegion Region { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="FailoverRegion{TRegion}"/> class.
        /// </summary>
        /// <param name="region">Region name</param>
        /// <param name="regionResetWindowInMillis">Time interval to reset region availability after failure.</param>
        public FailoverRegion(TRegion region, long regionResetWindowInMillis)
        {
            // Region
            Region = region;

            // Timer
            _resetTimer = new Timer(regionResetWindowInMillis);

            // Flags
            Reset();
        }

        /// <inheritdoc/>
        public bool Available()
        {
            return !(_isRegionInUse || _isRegionIsDown);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            // Timer
            _resetTimer.Stop();
            _resetTimer.Dispose();
        }

        /// <inheritdoc/>
        public void MarkInUse()
        {
            if (_isRegionInUse) return;

            _isRegionInUse = true;
        }

        /// <inheritdoc/>
        public void MarkIsDown()
        {
            if (_isRegionIsDown) return;

            _isRegionIsDown = true;

            // Setup Timer to enable region after Timeout
            _resetTimer.Elapsed += new ElapsedEventHandler(Reset);
            _resetTimer.AutoReset = false;
            _resetTimer.Start();
        }

        /// <inheritdoc/>
        public void Reset()
        {
            // Flags
            _isRegionInUse = false;
            _isRegionIsDown = false;

            // Timer
            _resetTimer.Stop();
        }

        private void Reset(Object source, ElapsedEventArgs e)
        {
            Reset();
        }
    }
}
