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
    /// Implement the basic semantics of conversion. The event is converted from TIn to TOut.
    /// Need to implement the Project method for the type conversion
    /// </summary>
    /// <typeparam name="TIn"></typeparam>
    /// <typeparam name="TOut"></typeparam>
    public abstract class ProjectionPipe<TIn, TOut> : Pipe<TIn, TOut>
    {
        protected ProjectionPipe(IPlugInContext context) : base(context)
        {
        }

        public override void OnNext(IEnvelope<TIn> value)
        {
            _subject.OnNext(Project(value));
        }

        protected abstract IEnvelope<TOut> Project(IEnvelope<TIn> value);
    }
}
