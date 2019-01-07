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

using Antlr4.Runtime;
using Antlr4.Runtime.Misc;
using Antlr4.Runtime.Tree;

using Amazon.KinesisTap.Expression.Ast;

namespace Amazon.KinesisTap.Expression.ObjectDecoration
{
    public class ObjectDecorationParserVisitor : ObjectDecorationParserBaseVisitor<Node>
    {
        public override Node VisitObjectDecoration([NotNull] ObjectDecorationParser.ObjectDecorationContext context)
        {
            List<KeyValuePairNode> keyValuePairs 
                = new List<KeyValuePairNode>();

            foreach (var node in context.children)
            {
                if (node is ObjectDecorationParser.KeyValuePairContext)
                {
                    keyValuePairs.Add((KeyValuePairNode)Visit(node));
                }
            }

            return new NodeList<KeyValuePairNode>(GetLocation(context), keyValuePairs);
        }

        public override Node VisitKeyValuePair([NotNull] ObjectDecorationParser.KeyValuePairContext context)
        {
            string key = context.key().GetText();
            var value = Visit(context.value());
            return new KeyValuePairNode(GetLocation(context),
                key,
                value);            
        }

        public override Node VisitValue(ObjectDecorationParser.ValueContext valueContext)
        {
            var valueList = new List<Node>();
            for (int i = 0; i < valueContext.ChildCount; i++)
            {
                var node = valueContext.GetChild(i);
                if (node is TerminalNodeImpl textNode)
                {
                    valueList.Add(new LiteralNode(GetLocation(textNode), LiteralTypeEnum.String, textNode.GetText()));
                }
                else //None terminal
                {
                    valueList.Add(Visit(node));
                }
            }
            return new NodeList<Node>(GetLocation(valueContext), valueList);
        }

        public override Node VisitKey([NotNull] ObjectDecorationParser.KeyContext context)
        {
            return new LiteralNode(GetLocation(context), LiteralTypeEnum.String, context.GetText());
        }

        public override Node VisitVariable([NotNull] ObjectDecorationParser.VariableContext context)
        {
            return Visit(context.expression());
        }

        public override Node VisitConstantExpression([NotNull] ObjectDecorationParser.ConstantExpressionContext context)
        {
            var symbol = ((ITerminalNode)context.children[0]).Symbol;
            switch(symbol.Type)
            {
                case ObjectDecorationParser.STRING:
                    return new LiteralNode(GetLocation(context), LiteralTypeEnum.String, Unescape(symbol.Text));
                case ObjectDecorationParser.NUMBER:
                    string number = symbol.Text;
                    if (int.TryParse(number, out int intValue))
                    {
                        return new LiteralNode(GetLocation(context), LiteralTypeEnum.Integer, intValue);
                    }
                    else
                    {
                        return new LiteralNode(GetLocation(context), LiteralTypeEnum.Decimal, Decimal.Parse(symbol.Text));
                    }
                case ObjectDecorationParser.TRUE:
                    return new LiteralNode(GetLocation(context), LiteralTypeEnum.Boolean, true);
                case ObjectDecorationParser.FALSE:
                    return new LiteralNode(GetLocation(context), LiteralTypeEnum.Boolean, false);
                case ObjectDecorationParser.NULL:
                    return new LiteralNode(GetLocation(context), LiteralTypeEnum.Null, null);
                default:
                    throw new NotImplementedException($"InvokingState {context.invokingState} not implemented");
            }
        }

        public override Node VisitIdentifierExpression([NotNull] ObjectDecorationParser.IdentifierExpressionContext context)
        {
            string identifier = context.GetText();
            if (context.start.Type == ObjectDecorationParser.QUOTED_IDENTIFIER)
            {
                identifier = "$" + Unescape(identifier.Substring(1));
            }
            return new IdentifierNode(GetLocation(context), identifier);
        }

        public override Node VisitInvocationExpression([NotNull] ObjectDecorationParser.InvocationExpressionContext context)
        {
            var identifier = context.IDENTIFIER();
            var functionNode = new IdentifierNode(GetLocation(identifier), identifier.GetText());
            List<ExpressionNode> arguments = new List<ExpressionNode>();
            var argumentsContext = context.arguments();
            if (argumentsContext != null)
            {
                foreach (var node in argumentsContext.children)
                {
                    if (node is ObjectDecorationParser.ExpressionContext expressionContext)
                        arguments.Add((ExpressionNode)Visit(expressionContext));
                }
            }
            return new InvocationNode(GetLocation(context), functionNode, arguments);
        }

        private static Location GetLocation(ParserRuleContext context)
        {
            return new Location(context.start.StartIndex, context.stop?.StopIndex ?? 0);
        }

        private static Location GetLocation(ITerminalNode terminal)
        {
            return new Location(terminal.Symbol.StartIndex, terminal.Symbol.StopIndex);
        }

        //Remove the beginning and ending quote and unescape special characters
        public static string Unescape(string input)
        {
            int idx = 1;    //Skipping the beginning quote
            int end = input.Length - 2; //Skipping the ending quote
            StringBuilder stringBuilder = new StringBuilder();
            while(idx <= end)
            {
                char c = input[idx];
                if (c == '\\')
                {
                    c = input[++idx];
                }
                stringBuilder.Append(c);
                idx++;
            }
            return stringBuilder.ToString();
        }
    }
}
