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

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Implement a filter step
    /// </summary>
    /// <typeparam name="T">Type of the data to filter</typeparam>
    public class FilterStep<T> : Step<T, T>
    {
        private readonly Func<T, bool> _filter;

        public FilterStep(Func<T, bool> filter)
        {
            Guard.ArgumentNotNull(filter, "filter");
            _filter = filter;
        }

        public override void OnNext(T value)
        {
            if (_next != null && _filter(value))
                _next.OnNext(value);
        }
    }
}
