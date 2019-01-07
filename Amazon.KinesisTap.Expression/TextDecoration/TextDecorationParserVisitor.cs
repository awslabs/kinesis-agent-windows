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

namespace Amazon.KinesisTap.Expression.TextDecoration
{
    public class TextDecorationParserVisitor : TextDecorationParserBaseVisitor<Node>
    {
        public override Node VisitTextDecoration([NotNull] TextDecorationParser.TextDecorationContext context)
        {
            var valueList = new List<Node>();
            for (int i = 0; i < context.ChildCount; i++)
            {
                var node = context.GetChild(i);
                if (node is TerminalNodeImpl textNode)
                {
                    valueList.Add(new LiteralNode(GetLocation(textNode), LiteralTypeEnum.String, textNode.GetText().Replace("@{", "{")));
                }
                else //None terminal
                {
                    valueList.Add(Visit(node));
                }
            }
            return new NodeList<Node>(GetLocation(context), valueList);
        }

        public override Node VisitVariable([NotNull] TextDecorationParser.VariableContext context)
        {
            return Visit(context.expression());
        }

        public override Node VisitConstantExpression([NotNull] TextDecorationParser.ConstantExpressionContext context)
        {
            var symbol = ((ITerminalNode)context.children[0]).Symbol;
            switch(symbol.Type)
            {
                case TextDecorationParser.STRING:
                    return new LiteralNode(GetLocation(context), LiteralTypeEnum.String, Unescape(symbol.Text));
                case TextDecorationParser.NUMBER:
                    string number = symbol.Text;
                    if (int.TryParse(number, out int intValue))
                    {
                        return new LiteralNode(GetLocation(context), LiteralTypeEnum.Integer, intValue);
                    }
                    else
                    {
                        return new LiteralNode(GetLocation(context), LiteralTypeEnum.Decimal, Decimal.Parse(symbol.Text));
                    }
                case TextDecorationParser.TRUE:
                    return new LiteralNode(GetLocation(context), LiteralTypeEnum.Boolean, true);
                case TextDecorationParser.FALSE:
                    return new LiteralNode(GetLocation(context), LiteralTypeEnum.Boolean, false);
                case TextDecorationParser.NULL:
                    return new LiteralNode(GetLocation(context), LiteralTypeEnum.Null, null);
                default:
                    throw new NotImplementedException($"InvokingState {context.invokingState} not implemented");
            }
        }

        public override Node VisitIdentifierExpression([NotNull] TextDecorationParser.IdentifierExpressionContext context)
        {
            string identifier = context.GetText();
            if (context.start.Type == TextDecorationParser.QUOTED_IDENTIFIER)
            {
                identifier = "$" + Unescape(identifier.Substring(1));
            }
            return new IdentifierNode(GetLocation(context), identifier);
        }

        public override Node VisitInvocationExpression([NotNull] TextDecorationParser.InvocationExpressionContext context)
        {
            var identifier = context.IDENTIFIER();
            var functionNode = new IdentifierNode(GetLocation(identifier), identifier.GetText());
            List<ExpressionNode> arguments = new List<ExpressionNode>();
            var argumentsContext = context.arguments();
            if (argumentsContext != null)
            {
                foreach (var node in argumentsContext.children)
                {
                    if (node is TextDecorationParser.ExpressionContext expressionContext)
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
