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

using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Expression.Ast
{
    public class ExpressionInterpreter<TData> : IAstVisitor<TData, object>
    {
        protected readonly IExpressionEvaluationContext<TData> _evaluationContext;

        public ExpressionInterpreter(IExpressionEvaluationContext<TData> evaluationContext)
        {
            _evaluationContext = evaluationContext;
        }

        public virtual object Visit(Node node, TData data)
        {
            IdentifierNode identifierNode = node as IdentifierNode;
            if (identifierNode != null) return VisitIdentifier(identifierNode, data);

            InvocationNode invocationNode = node as InvocationNode;
            if (invocationNode != null) return VisitInvocationNode(invocationNode, data);

            NodeList<Node> nodeList = node as NodeList<Node>;
            if (nodeList != null) return VisitNodeList(nodeList, data);

            LiteralNode literalNode = node as LiteralNode;
            if (literalNode != null) return VisitLiteral(literalNode, data);

            throw new NotImplementedException();
        }

        public virtual object VisitIdentifier(IdentifierNode identifierNode, TData data)
        {
            var variableName = identifierNode.Identifier;
            if (IsLocal(variableName))
            {
                return _evaluationContext.GetLocalVariable(variableName, data);
            }
            else
            {
                return _evaluationContext.GetVariable(variableName);
            }
        }

        public virtual object VisitInvocationNode(InvocationNode invocationNode, TData data)
        {
            string functionName = invocationNode.FunctionName.Identifier;
            int argumentCount = invocationNode.Arguments.Count;
            object[] arguments = new object[argumentCount];
            Type[] argumentTypes = new Type[argumentCount];
            bool hasNullArguments = false;
            for (int i = 0; i < argumentCount; i++)
            {
                arguments[i] = Visit(invocationNode.Arguments[i], data);
                if (arguments[i] == null)
                {
                    hasNullArguments = true;
                    argumentTypes[i] = typeof(object);
                }
                else
                {
                    argumentTypes[i] = arguments[i].GetType();
                }
            }
            MethodInfo methodInfo = _evaluationContext.FunctionBinder.Resolve(functionName, argumentTypes);
            if (methodInfo == null)
            {
                //If we have null arguments, we will propagate null without warning.
                if (!hasNullArguments)
                {
                    _evaluationContext.Logger?.LogWarning($"Cannot resolve function {functionName} with argument types {string.Join(",", argumentTypes.Select(t => t.Name))}");
                }
                return null;
            }
            else
            {
                return methodInfo.Invoke(null, arguments);
            }
        }

        public virtual object VisitLiteral(LiteralNode literalNode, TData data)
        {
            return literalNode.Value;
        }

        public virtual object VisitNodeList(NodeList<Node> nodeList, TData data)
        {
            StringBuilder stringBuilder = new StringBuilder();
            foreach (var node in nodeList.List)
            {
                stringBuilder.Append(Visit(node, data));
            }
            return stringBuilder.ToString();
        }

        protected virtual bool IsLocal(string variableName)
        {
            return variableName.StartsWith("$")
                || variableName.StartsWith("_")
                || variableName.ToLower().Equals("timestamp");
        }
    }
}
