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

namespace Amazon.KinesisTap.Shared.Ast
{
    public class Location
    {
        protected readonly int _startIndex;
        protected readonly int _stopIndex;

        public Location(int startIndex, int stopIndex)
        {
            _startIndex = startIndex;
            _stopIndex = stopIndex;
        }

        public int StartIndex => _startIndex;

        public int StopIndex => _stopIndex;
    }
}
