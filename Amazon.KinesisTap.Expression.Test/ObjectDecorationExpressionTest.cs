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
using Xunit;
using Amazon.KinesisTap.Shared.Ast;
using Amazon.KinesisTap.Shared.ObjectDecoration;

namespace Amazon.KinesisTap.Shared.Test
{
    public class ObjectDecorationExpressionTest
    {
        private const string SIMPLE_EXPRESSION = "abc{regex_extract($message, 'From:')}def";
        private const string NESTED_EXPRESSION = "{substr(regex_extract($message, 'From:'), 5)}";

        [Fact]
        public void TestSimpleExpression()
        {
            var parseTree = ObjectDecorationParserFacade.ParseObjectDecorationValue(SIMPLE_EXPRESSION);
            Assert.Equal(3, parseTree.List.Count);
            Assert.IsType<LiteralNode>(parseTree.List[0]);
            Assert.IsType<LiteralNode>(parseTree.List[2]);

            var invocationExpression = (InvocationNode)parseTree.List[1];
            Assert.Equal("regex_extract", invocationExpression.FunctionName.Identifier);
            Assert.Equal(2, invocationExpression.Arguments.Count);
            Assert.IsType<IdentifierNode>(invocationExpression.Arguments[0]);
            Assert.IsType<LiteralNode>(invocationExpression.Arguments[1]);
        }

        [Fact]
        public void TestNestedExpression()
        {
            var parseTree = ObjectDecorationParserFacade.ParseObjectDecorationValue(NESTED_EXPRESSION);
            Assert.Single(parseTree.List);

            var invocationExpression = (InvocationNode)parseTree.List[0];
            Assert.Equal("substr", invocationExpression.FunctionName.Identifier);
            Assert.Equal(2, invocationExpression.Arguments.Count);
            var arg0 = (InvocationNode)invocationExpression.Arguments[0];
            Assert.Equal("regex_extract", arg0.FunctionName.Identifier);
            Assert.Equal(2, arg0.Arguments.Count);
            Assert.IsType<IdentifierNode>(arg0.Arguments[0]);
            Assert.IsType<LiteralNode>(arg0.Arguments[1]);

            Assert.IsType<LiteralNode>(invocationExpression.Arguments[1]);
        }
    }
}
