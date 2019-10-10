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
    public class ExpressionEvaluationContext<T> : IExpressionEvaluationContext<T>
    {
        protected readonly Func<string, string> _globalEvaluator;
        protected readonly Func<string, T, object> _localEvaluator;
        protected readonly FunctionBinder _functionBinder;
        protected readonly ILogger _logger;
        protected readonly Dictionary<string, object> _contextVariables = new Dictionary<string, object>();

        public ExpressionEvaluationContext(Func<string, string> globalEvaluator, 
            Func<string, T, object> localEvaluator,
            FunctionBinder functionBinder,
            ILogger logger)
        {
            _globalEvaluator = globalEvaluator;
            _localEvaluator = localEvaluator;
            _functionBinder = functionBinder;
            _logger = logger;
        }

        public object GetLocalVariable(string variableName, T data)
        {
            if (_contextVariables.TryGetValue(variableName, out object value)) return value;

            return _localEvaluator(variableName, data);
        }

        public string GetVariable(string variableName)
        {
            return _globalEvaluator(variableName);
        }

        public void AddContextVariable(string variableName, object data)
        {
            _contextVariables[variableName] = data;
        }

        public FunctionBinder FunctionBinder => _functionBinder;

        public ILogger Logger => _logger;
    }
}
