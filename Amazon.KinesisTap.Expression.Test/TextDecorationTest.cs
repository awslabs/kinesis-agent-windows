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
using Amazon.KinesisTap.Shared.Ast;
using Amazon.KinesisTap.Shared.Binder;
using Amazon.KinesisTap.Shared.TextDecoration;
using System;
using Xunit;

namespace Amazon.KinesisTap.Shared.Test
{
    public class TextDecorationTest
    {
        private TextDecorationInterpreter<object> _evaluator;

        public TextDecorationTest()
        {
            FunctionBinder binder = new FunctionBinder(new Type[] { typeof(BuiltInFunctions) });
            ExpressionEvaluationContext<object> context = new ExpressionEvaluationContext<object>(
                VariableMock.GetGlobalVariable, VariableMock.GetLocalVariable, binder, null);
            _evaluator = new TextDecorationInterpreter<object>(context);
        }

        [Theory]
        [InlineData("", "")]
        [InlineData("a{'b'}c", "abc")]
        [InlineData("{substr($b, parse_int('2'))}", "ello")]
        [InlineData("{coalesce(null, 'a')}", "a")]
        [InlineData("@{\"k\":\"{'v'}\"}", "{\"k\":\"v\"}")]
        public void TestTextDecoration(string textDecoration, string expected)
        {
            var tree = TextDecorationParserFacade.ParseTextDecoration(textDecoration);
            string actual = (string)_evaluator.Visit(tree, null);
            Assert.Equal(expected, actual);
        }
    }
}
