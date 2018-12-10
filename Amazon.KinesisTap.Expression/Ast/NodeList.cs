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
using System.Collections.ObjectModel;
using System.Text;

namespace Amazon.KinesisTap.Expression.Ast
{
    public sealed class NodeList<T> : Node where T : Node
    {
        private readonly List<T> _list;

        public NodeList(Location location, List<T> list) : base(location)
        {
            _list = list;
        }

        /// <summary>
        /// User cannot add or remove element from the list.
        /// </summary>
        public ReadOnlyCollection<T> List => _list.AsReadOnly();
    }
}
