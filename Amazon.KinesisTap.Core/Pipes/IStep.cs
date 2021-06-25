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
    /// A Step is like a pipe but it is light weight
    /// A Step participates in a single linked-list so we execute step by step
    /// </summary>
    public interface IStep
    {
        /// <summary>
        /// The next step
        /// </summary>
        IStep Next { get; set; }
    }

    public interface IStep<in T> : IStep
    {
        //Called by the previous step to handle a value
        void OnNext(T value);
    }
}
