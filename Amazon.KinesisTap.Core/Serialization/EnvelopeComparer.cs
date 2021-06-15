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
using System.Collections.Generic;

namespace Amazon.KinesisTap.Core
{
    public class EnvelopeComparer<T> : IEqualityComparer<Envelope<T>>
    {
        private IEqualityComparer<T> _dataComparer;

        public EnvelopeComparer(IEqualityComparer<T> dataComparer)
        {
            _dataComparer = dataComparer;
        }

        public bool Equals(Envelope<T> x, Envelope<T> y)
        {
            return x.Timestamp == y.Timestamp &&
                _dataComparer.Equals(x.Data, y.Data);
        }

        public int GetHashCode(Envelope<T> obj)
        {
            return obj.GetHashCode();
        }
    }
}
