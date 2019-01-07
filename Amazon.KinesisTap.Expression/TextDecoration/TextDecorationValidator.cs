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
using Amazon.KinesisTap.Expression.Ast;

namespace Amazon.KinesisTap.Expression.TextDecoration
{
    public class TextDecorationValidator<TData> : TextDecorationInterpreter<TData>
    {
        public TextDecorationValidator(IExpressionEvaluationContext<TData> evaludationContext) : base(evaludationContext)
        {
        }

        public override object VisitIdentifier(IdentifierNode identifierNode, TData data)
        {
            return null;
        }

        public override object VisitInvocationNode(InvocationNode invocationNode, TData data)
        {
            string functionName = invocationNode.FunctionName.Identifier;
            int argumentCount = invocationNode.Arguments.Count;
            var candidates = _evaluationContext.FunctionBinder.GetCandidateMethods(functionName, argumentCount);
            if (candidates.Length == 0)
            {
                throw new MissingMethodException($"Cannot resolve function {functionName} with {argumentCount} arguments.");
            }
            return null;
        }
    }
}
