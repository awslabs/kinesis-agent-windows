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

namespace Amazon.KinesisTap.Expression.ObjectDecoration
{
    public interface IObjectDecorationAstVisitor<in TData, out Result>
    {
        Result VisitObjectDecoration(NodeList<KeyValuePairNode> nodeList, TData data);

        Result VisitLiteral(LiteralNode literalNode, TData data);

        Result VisitIdentifier(IdentifierNode identifierNode, TData data);

        Result VisitInvocationNode(InvocationNode invocationNode, TData data);

        Result VisitKeyValuePairNode(KeyValuePairNode keyValuePairNode, TData data);

        Result VisitNodeList(NodeList<Node> nodeList, TData data);
    }
}
