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
using System.Text;
using System.Text.RegularExpressions;

namespace Amazon.KinesisTap.Core.Pipes
{
    /// <summary>
    /// Use regex to filter event on the string or any event with a string representation
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class RegexFilterPipe<T> : FilterPipe<T>
    {
        private readonly Regex _filter;

        public RegexFilterPipe(IPlugInContext context) : base(context)
        {
            var config = context.Configuration;
            string filterPattern = config["FilterPattern"];
            if (!string.IsNullOrWhiteSpace(filterPattern))
            {
                _filter = new Regex(filterPattern.Trim());
            }
        }

        protected override bool Filter(IEnvelope<T> value)
        {
            string textRecord = value.GetMessage(null);
            return _filter.IsMatch(textRecord);
        }
    }
}
