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
namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Implement the filter pipe semantics. The type does not change. Need to implement the filter method
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class FilterPipe<T> : Pipe<T, T>
    {
        protected FilterPipe(IPlugInContext context) : base(context)
        {
        }

        public override void OnNext(IEnvelope<T> value)
        {
            if (Filter(value))
            {
                _subject.OnNext(value);
            }
        }

        /// <summary>
        /// Override this method if need to do something in Start
        /// </summary>
        public override void Start()
        {
        }

        /// <summary>
        /// Override this method if need to do something in Stop
        /// </summary>
        public override void Stop()
        {
        }

        /// <summary>
        /// Event will propagate through the pipe only if the filter turns true
        /// </summary>
        /// <param name="value">The event to filter</param>
        /// <returns></returns>
        protected abstract bool Filter(IEnvelope<T> value);
    }    
}
