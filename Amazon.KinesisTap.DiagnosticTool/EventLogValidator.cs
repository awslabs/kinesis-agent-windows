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
using System.Diagnostics.Eventing.Reader;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Amazon.KinesisTap.DiagnosticTool.Core;

namespace Amazon.KinesisTap.DiagnosticTool
{
    /// <summary>
    /// The validator for the Windows Event log source from the configuration file
    /// </summary>
    public class EventLogValidator : ISourceValidator
    {

        /// <summary>
        /// Validate the Windows event log section
        /// </summary>
        /// <param name="sourceSection"></param>
        /// <param name="id"></param>
        /// <param name="messages"></param>
        /// <returns></returns>
        public bool ValidateSource(IConfigurationSection sourceSection, string id, IList<String> messages)
        {
            string logName = sourceSection["LogName"];

            EventLogQuery eventLogQuery = new EventLogQuery(logName, PathType.LogName);
            EventLogWatcher watcher = new EventLogWatcher(eventLogQuery, null, true);

            try
            {
                watcher.Enabled = true;
                return true;
            }
            catch (EventLogNotFoundException ex)
            {
                messages.Add(ex.Message);
                messages.Add($"Event Log Name: {logName} is not a valid log name in source ID: {id}!");
                return false;
            }
            catch (Exception ex)
            {
                messages.Add(ex.ToString());
                messages.Add($"Unexpected exceptions. Event Log Name: {logName} in source ID: {id}.");
                return false;
            }
            finally
            {
                watcher.Enabled = false;
                watcher.Dispose();
            }
        }
    }
}
