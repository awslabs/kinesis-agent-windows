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

using Amazon.KinesisTap.Shared.Ast;

namespace Amazon.KinesisTap.Shared.TextDecoration
{
    /// <summary>
    /// Evaluator for object decoration. Only need to NodeList and dependent nodes
    /// </summary>
    public class TextDecorationInterpreter<TData> : ExpressionInterpreter<TData>, ITextDecorationAstVisitor<TData, object>
    {
        public TextDecorationInterpreter(IExpressionEvaluationContext<TData> evaluationContext) : base(evaluationContext)
        {
        }

        public override object Visit(Node node, TData data)
        {
            return base.Visit(node, data);
        }

        public virtual object VisitTextDecoration(NodeList<Node> nodeList, TData data)
        {
            return VisitNodeList(nodeList, data);
        }
    }
}
