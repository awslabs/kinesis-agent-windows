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

namespace Amazon.KinesisTap.AWS.Failover.Extensions
{
    /// <summary>
    /// A extension class for selecting weighted random item.
    /// Alias Method - https://en.wikipedia.org/wiki/Alias_method
    /// https://www.sakya.it/wordpress/random-number-with-alias-method-in-c/
    /// </summary>
    public static class WeightedRandomExtension
    {
        /// <summary>
        /// Get random.
        /// </summary>
        /// <param name="random">Instance of <see cref="Random"/></param>
        /// <param name="weights">Instance of <see cref="List{Int32}"/></param>
        /// <returns>Return random index.</returns>
        public static int GetAlias(this Random random, List<Int32> weights)
        {
            var selected = random.Next(weights.Sum());
            for (int idx = 0, localSum = 0; idx < weights.Count; idx++)
            {
                localSum += weights[idx];
                if (localSum >= selected)
                    return idx;
            }

            // Default return last
            return weights.Count - 1;
        }
    }
}
