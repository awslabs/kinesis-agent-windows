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
using System.Diagnostics.Eventing.Reader;
using System.Linq;
using System.Runtime.Versioning;
using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.Windows
{
    /// <summary>
    /// Provide a list of named EventInfo filters
    /// </summary>
    [SupportedOSPlatform("windows")]
    public static class EventInfoFilters
    {
        public const string EXCLUDE_OWN_SECURITY_EVENTS = "ExcludeOwnSecurityEvents";
        private const string SECURITY_EVENTS_LABEL = "Security";

        private static readonly Dictionary<string, Func<EventRecord, bool>> _filters =
            new Dictionary<string, Func<EventRecord, bool>>(StringComparer.OrdinalIgnoreCase)
            {
                { EXCLUDE_OWN_SECURITY_EVENTS,  ExcludeKinesisTapSecurityEventsFilter}
            };

        /// <summary>
        /// Add a filter to the collection of filters. Throw if filter already exists so cannot override an existing one.
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <param name="filter">Filter to add</param>
        public static void AddFilter(string name, Func<EventRecord, bool> filter)
        {
            _filters.Add(name, filter);
        }

        /// <summary>
        /// Return a filter or null.
        /// </summary>
        /// <param name="name">Name of the filter</param>
        /// <returns></returns>
        public static Func<EventRecord, bool> GetFilter(string name)
        {
            if (_filters.TryGetValue(name, out Func<EventRecord, bool> filter)) return filter;

            return null;
        }

        /// <summary>
        /// Filter to exclude security event generated by KinesisTap itself
        /// That is when the full path of EventData is our exe path.
        /// </summary>
        /// <param name="eventInfo"></param>
        /// <returns>false if the event should be filtered out</returns>
        private static bool ExcludeKinesisTapSecurityEventsFilter(EventRecord eventInfo)
        {
            if (SECURITY_EVENTS_LABEL.Equals(eventInfo.LogName))
            {
                if (eventInfo.Properties != null)
                {
                    if (eventInfo.Properties.Any(o => (o.Value as string)?.EndsWith(ConfigConstants.KINESISTAP_EXE_NAME, StringComparison.OrdinalIgnoreCase) ?? false))
                        return false;
                }
            }
            return true;
        }
    }
}
