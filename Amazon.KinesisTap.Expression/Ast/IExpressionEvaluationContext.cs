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

using Microsoft.Extensions.Logging;

using Amazon.KinesisTap.Expression.Binder;

namespace Amazon.KinesisTap.Expression.Ast
{
    public interface IExpressionEvaluationContext<T>
    {
        /// <summary>
        /// Evaluate global variables, from environment variable, EC2 instance data, etc
        /// </summary>
        /// <param name="variableName">Variable name</param>
        /// <returns>Variable value</returns>
        string GetVariable(string variableName);

        /// <summary>
        /// Evaluate local or meta variables from record data
        /// </summary>
        /// <param name="variableName">Variable name</param>
        /// <param name="data">Data record</param>
        /// <returns></returns>
        object GetLocalVariable(string variableName, T data);

        /// <summary>
        /// Add additional data that may be useful for evaluation
        /// </summary>
        /// <param name="variableName">Variable name</param>
        /// <param name="data">data</param>
        void AddContextVariable(string variableName, object data);

        /// <summary>
        /// Return the FunctionBinder used by the context
        /// </summary>
        FunctionBinder FunctionBinder { get; }

        /// <summary>
        /// Return the logger used by the context
        /// </summary>
        ILogger Logger { get; }
    }
}
