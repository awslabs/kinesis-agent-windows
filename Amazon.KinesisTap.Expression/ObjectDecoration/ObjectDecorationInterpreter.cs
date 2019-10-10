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

using Amazon.KinesisTap.Expression.Ast;

namespace Amazon.KinesisTap.Expression.ObjectDecoration
{
    /// <summary>
    /// Evaluator for object decoration. Only need to NodeList and dependent nodes
    /// </summary>
    public class ObjectDecorationInterpreter<TData> : ExpressionInterpreter<TData>, IObjectDecorationAstVisitor<TData, object>
    {
        public ObjectDecorationInterpreter(IExpressionEvaluationContext<TData> evaluationContext) : base(evaluationContext)
        {
        }

        public override object Visit(Node node, TData data)
        {
            KeyValuePairNode keyValuePairNode = node as KeyValuePairNode;
            if (keyValuePairNode != null) return VisitKeyValuePairNode(keyValuePairNode, data);

            NodeList<KeyValuePairNode> keyValuePairNodes = node as NodeList<KeyValuePairNode>;
            if (keyValuePairNodes != null) return VisitObjectDecoration(keyValuePairNodes, data);

            return base.Visit(node, data);
        }

        public virtual object VisitKeyValuePairNode(KeyValuePairNode keyValuePairNode, TData data)
        {
            string key = keyValuePairNode.Key;
            string value = $"{VisitNodeList((NodeList<Node>)(keyValuePairNode.Value), data)}";
            return new KeyValuePair<string, string>(key, value);
        }

        public virtual object VisitObjectDecoration(NodeList<KeyValuePairNode> keyValuePairNodes, TData data)
        {
            IDictionary<string, string> attributes = new Dictionary<string, string>();
            foreach(var keyValuePairNode in keyValuePairNodes.List)
            {
                var kv = (KeyValuePair<string, string>)Visit(keyValuePairNode, data);
                if (!string.IsNullOrWhiteSpace(kv.Value)) //Suppress white spaces
                {
                    _evaluationContext.AddContextVariable($"${kv.Key}", kv.Value); //Add to the evaluation context as local variable
                    attributes.Add(kv);
                }
            }
            return attributes;
        }
    }
}
