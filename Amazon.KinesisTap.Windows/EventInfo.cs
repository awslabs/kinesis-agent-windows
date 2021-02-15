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

using Newtonsoft.Json;

namespace Amazon.KinesisTap.Windows
{
    public class EventInfo
    {
        public static readonly char[] KeywordSeparator = new char[','];

        public int EventId { get; set; }
        public string Description { get; set; }
        public string LevelDisplayName { get; set; }
        public string LogName { get; set; }
        public string MachineName { get; set; }
        public string ProviderName { get; set; }
        public DateTime? TimeCreated { get; set; }
        public long? Index { get; set; }
        public string UserName { get; set; }
        public string Keywords { get; set; }
        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public List<object> EventData { get; set; }

        /*
          The following fields are used to support Windows Event XML format and therefore is ignored in JSON format.
        */

        /// <summary>
        /// Used to store XML returned from EventRecord.ToXml() method
        /// </summary>
        [JsonIgnore]
        public string Xml { get; set; }

        [JsonIgnore]
        public string Opcode { get; set; }

        [JsonIgnore]
        public string Task { get; set; }
    }
}
