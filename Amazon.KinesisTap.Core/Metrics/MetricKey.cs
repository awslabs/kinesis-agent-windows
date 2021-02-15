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

namespace Amazon.KinesisTap.Core.Metrics
{
    /// <summary>
    /// A MetricKey uniquely defines a metrics, using three parameters: Category, Name, and Id.
    /// These parameters are used to determine metric name and dimensions when streaming to CloudWatch.
    /// They are also mapped to Windows Performance Counter's parameters, and thus is used to
    /// uniquely identify a counter in a WindowsPerformanceCounter source.
    /// </summary>
    public struct MetricKey : IEquatable<MetricKey>
    {
        /// <summary>
        /// The metric's category. This is mapped to Windows Performance Counter category.
        /// </summary>
        public string Category;

        /// <summary>
        /// The metric's name. This is mapped to Windows Performance Counter counter name.
        /// </summary>
        public string Name;

        /// <summary>
        /// The metric's ID. This is mapped to Windows Performance Counter instance name.
        /// </summary>
        public string Id;

        public override int GetHashCode()
        {
            // Combine hash code from member fields using prime numbers
            unchecked
            {
                int hash = 17;
                hash = hash * 23 + (string.IsNullOrEmpty(Category) ? 0 : Category.GetHashCode());
                hash = hash * 23 + (string.IsNullOrEmpty(Name) ? 0 : Name.GetHashCode());
                hash = hash * 23 + (string.IsNullOrEmpty(Id) ? 0 : Id.GetHashCode());
                return hash;
            }
        }

        public override bool Equals(object obj) => obj is MetricKey other && Equals(other);

        /// <summary>
        /// Implements <see cref="IEquatable{T}"/> to use this struct as a hash table key.
        /// </summary>
        /// <inheritdoc/>
        public bool Equals(MetricKey other) => Name == other.Name && Id == other.Id && Category == other.Category;
    }
}
