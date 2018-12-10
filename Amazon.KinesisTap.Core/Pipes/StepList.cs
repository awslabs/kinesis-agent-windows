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
    /// Implement a single linked-list for steps
    /// </summary>
    public class StepList<TIn>
    {
        private readonly IStep<TIn> _head;
        private IStep _tail;

        public StepList(IStep<TIn> head)
        {
            Guard.ArgumentNotNull(head, "head");
            _head = head;
            _tail = head;
        }

        public IStep<TIn> Head
        {
            get => _head;
        }

        public IStep Tail
        {
            get => _tail;
        }

        public void Append(IStep step)
        {
            _tail.Next = step;
            _tail = step;
        }
    }
}
