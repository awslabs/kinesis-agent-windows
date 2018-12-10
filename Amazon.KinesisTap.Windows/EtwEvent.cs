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
using System.Threading.Tasks;
using Microsoft.Diagnostics.Tracing;
using System.Net;
using System.Net.NetworkInformation;

namespace Amazon.KinesisTap.Windows
{
    /// <summary>
    /// The data we return to KinesisTap when an ETW event occurs.
    /// </summary>
    public class EtwEvent
    {
        /// <summary>
        /// What the event is called.
        /// </summary>
        public string EventName { get; set; }

        /// <summary>
        /// Which provider generated the event.
        /// </summary>
        public string ProviderName { get; set; }

        /// <summary>
        /// The summary of the event.
        /// </summary>
        public string FormattedMessage { get; set; }

        /// <summary>
        /// The identity of the process which generated the event.
        /// </summary>
        public int ProcessID { get; set; }

        /// <summary>
        /// The identity of the thread within the process which generated the event.
        /// </summary>
        public int ExecutingThreadID { get; set; }

        /// <summary>
        /// Which machine generated the event.
        /// </summary>
        public string MachineName { get; set; }

        private static string _machineName = null;

        /// <summary>
        /// Extra data supplied by the provider about the event.  This is provider and event specific.
        /// </summary>
        public Dictionary<string, object> Payload { get; } = new Dictionary<string, object>();

        public EtwEvent(TraceEvent traceData)
        {
            EventName = traceData.EventName;
            ProviderName = traceData.ProviderName;
            FormattedMessage = traceData.FormattedMessage;

            ProcessID = traceData.ProcessID;

            MachineName = GetFQDN();


            for (int i = 0; i < traceData.PayloadNames.Length; i++)
            {
                Payload.Add(traceData.PayloadNames[i], traceData.PayloadValue(i));
            }
        }

        /// <summary>
        /// Return the fully qualified domain name for the local host, or if not domain joined then return the NETBIOS name of the local host.
        /// </summary>
        /// <returns>The fully qualified domain name for the local host, or if not domain joined then return the NETBIOS name of the local host.</returns>
        public static string GetFQDN()
        {
            if (_machineName != null)
            {
                return _machineName;
            }

            string hostName;

            try
            {
                string domainName = IPGlobalProperties.GetIPGlobalProperties().DomainName;
                hostName = Dns.GetHostName();

                domainName = "." + domainName;
                if (!hostName.EndsWith(domainName))  // if hostname does not already include domain name
                {
                    hostName += domainName;   // add the domain name part
                }

                _machineName = hostName;
            }
            catch (Exception)
            {
                hostName = "unknown";
            }

            return hostName;                    // return the fully qualified name
        }

    }
}
