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
namespace Amazon.KinesisTap.Windows
{
    internal class WindowsEventLogPollingSourceOptions : WindowsEventLogSourceOptions
    {
        /// <summary>
        /// Maximum delay between event log queries in milliseconds
        /// </summary>
        public int MaxReaderDelayMs { get; set; } = 5000;

        /// <summary>
        /// Minimum delay between event log queries in milliseconds
        /// </summary>
        public int MinReaderDelayMs { get; set; } = 100;

        /// <summary>
        /// Number of events read threshold at which the delay is adjusted
        /// </summary>
        public int DelayThreshold { get; set; } = 1000;
    }
}
