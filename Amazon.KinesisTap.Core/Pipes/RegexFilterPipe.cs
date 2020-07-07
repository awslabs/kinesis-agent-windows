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
using System.Text.RegularExpressions;

namespace Amazon.KinesisTap.Core.Pipes
{
    /// <summary>
    /// Use regex to filter event on the string or any event with a string representation
    /// </summary>
    /// <typeparam name="T">The record type of <see cref="IEnvelope"/></typeparam>
    public class RegexFilterPipe<T> : FilterPipe<T>
    {
        private readonly Regex _filter;
        private readonly bool _negate;

        public RegexFilterPipe(IPlugInContext context) : base(context)
        {
            var config = context.Configuration;
            var filterPattern = config[ConfigConstants.FILTER_PATTERN];
            if (string.IsNullOrWhiteSpace(filterPattern))
                throw new ArgumentNullException("'FilterPattern' property of RegexFilterPipe cannot be null or whitespace.");

            var options = RegexOptions.Compiled | RegexOptions.ExplicitCapture;
            if (bool.TryParse(config[ConfigConstants.MULTILINE], out var multiLine) && multiLine)
                options |= RegexOptions.Multiline;

            if (bool.TryParse(config[ConfigConstants.IGNORE_CASE], out var ignoreCase) && ignoreCase)
                options |= RegexOptions.IgnoreCase;

            if (bool.TryParse(config[ConfigConstants.RIGHT_TO_LEFT], out var rightToLeft) && rightToLeft)
                options |= RegexOptions.RightToLeft;

            _filter = new Regex(filterPattern.Trim(), options);

            // Negating matches in a regex is very expensive, so to make it cheaper, we'll add
            // in a flag that allows you to drop events that match the expression rather than allow.
            if (bool.TryParse(config[ConfigConstants.NEGATE], out var neg))
                _negate = neg;
        }

        protected override bool Filter(IEnvelope<T> value)
        {
            var isMatch = _filter.IsMatch(value.GetMessage(null));
            return _negate ? !isMatch : isMatch;
        }
    }
}
