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

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Interface for Event source. It extends IObservable so that it can be subscribed.
    /// </summary>
    public interface IEventSource : ISource, IObservable<IEnvelope>
    {
        /// <returns>The output type of the data produced by the source (wrapped in IEnvelop<T>).</returns>
        /// <remarks>
        /// Pipes that process data (filtering, transformation etc.) expects an input type.
        /// This is meant to be a temporary fix to make all sources work with those pipes.
        /// </remarks>
        Type GetOutputType();
    }

    public interface IEventSource<out T> : IEventSource, IObservable<IEnvelope<T>>
    {
    }
}
