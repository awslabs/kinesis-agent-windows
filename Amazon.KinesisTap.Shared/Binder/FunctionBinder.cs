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
using System.Linq;
using System.Reflection;
using System.Text;

namespace Amazon.KinesisTap.Shared.Binder
{
    /// <summary>
    /// Resolve the function from name, argument count and argument types
    /// </summary>
    public class FunctionBinder
    {
        private Type[] _classTypes;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="classTypes">The types of classes that contain the function in resolution order</param>
        public FunctionBinder(Type[] classTypes)
        {
            _classTypes = classTypes;
        }

        public MethodInfo Resolve(string functionName, Type[] argumentTypes)
        {
            MethodInfo[] candidates = GetCandidateMethods(functionName, argumentTypes.Length);

            if (candidates.Length == 0)
            {
                throw new ArgumentException($"Cannot resolve function {functionName} with {argumentTypes.Length} parameters");
            }

            return Resolve(candidates, argumentTypes);
        }

        public MethodInfo[] GetCandidateMethods(string functionName, int argumentCount)
        {
            return _classTypes.SelectMany(c => c.GetTypeInfo()
                .GetDeclaredMethods(functionName)
                .Where(m => m.GetParameters().Length == argumentCount))
                .ToArray();
        }

        private MethodInfo Resolve(MethodInfo[] candidates, Type[] argumentTypes)
        {
            //Step 1: Find the functions with the exact argument match
            var resolvedMethods = candidates.Where(m => ArgumentsMatch(m, argumentTypes, FunctionBinder.ExactMatch)).ToList();
            if (resolvedMethods.Count > 0) return resolvedMethods[0];

            //Step 2: Find the function with assignable argument match
            resolvedMethods = candidates.Where(m => ArgumentsMatch(m, argumentTypes, FunctionBinder.AssignableMatch)).ToList();
            if (resolvedMethods.Count > 0) return resolvedMethods[0];

            return null;
        }

        private static bool ArgumentsMatch(MethodInfo m, Type[] argumentTypes, Func<Type, Type, bool> match)
        {
            var parameters = m.GetParameters();

            if (parameters.Length != argumentTypes.Length) return false;

            for(int i = 0; i < parameters.Length; i++)
            {
                var parameterType = parameters[i].ParameterType;
                if (!match(parameterType, argumentTypes[i]))
                {
                    return false;
                }
            }

            return true;
        }

        private static bool ExactMatch(Type parameterType, Type argumentType) => parameterType == argumentType;
        
        private static bool AssignableMatch(Type parameterType, Type argumentType) => parameterType.IsAssignableFrom(argumentType);
    }
}
