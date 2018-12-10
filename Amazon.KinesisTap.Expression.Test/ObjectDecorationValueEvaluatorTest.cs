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
using Amazon.KinesisTap.Expression.Binder;
using Amazon.KinesisTap.Expression.ObjectDecoration;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Amazon.KinesisTap.Expression.Test
{
    public class ObjectDecorationValueEvaluatorTest
    {
        private ObjectDecorationInterpreter<object> _evaluator;

        public ObjectDecorationValueEvaluatorTest()
        {
            FunctionBinder binder = new FunctionBinder(new Type[] { typeof(BuiltInFunctions) });
            ObjectDecorationEvaluationContext<object> context = new ObjectDecorationEvaluationContext<object>(
                GetGlobalVariable, GetLocalVariable, binder, null);
            _evaluator = new ObjectDecorationInterpreter<object>(context);
        }

        [Theory]
        [InlineData("", "")]
        [InlineData("{'a'}", "a")]
        [InlineData("{1}", "1")]
        [InlineData("{1.21}", "1.21")]
        [InlineData("{null}", "")]
        [InlineData("{a}", "gav")]
        [InlineData("{b}", "gbv")]
        [InlineData("{$a}", "9")]
        [InlineData("{$'a'}", "9")]
        [InlineData("{$b}", "hello")]
        [InlineData("{$c}", "1.24")]
        [InlineData("{length('abc')}", "3")]
        [InlineData("{length($b)}", "5")]
        [InlineData("{lower('ABC')}", "abc")]
        [InlineData("{upper('abc')}", "ABC")]
        [InlineData("{lpad($b, $a, '!')}", "!!!!hello")]
        [InlineData("{rpad($b, $a, '!')}", "hello!!!!")]
        [InlineData("{ltrim(' a ')}", "a ")]
        [InlineData("{rtrim(' a ')}", " a")]
        [InlineData("{trim(' a ')}", "a")]
        [InlineData("{substr($b, 2)}", "ello")]
        [InlineData("{substr($b, 2, 1)}", "e")]
        [InlineData("{regexp_extract('Info: MID 118667291 ICID 197973259 RID 0 To: <jd@acme.com>', 'To: \\\\S+')}", "To: <jd@acme.com>")]
        [InlineData("{regexp_extract('Info: MID 118667291 ICID 197973259 RID 0 To: <jd@acme.com>', 'To: (\\\\S+)', 1)}", "<jd@acme.com>")]
        [InlineData("{regexp_extract('Info: MID 118667291 ICID 197973259 RID 0 To: <jd@acme.com>', 'From: \\\\S+')}", "")]
        [InlineData("{regexp_extract('Info: MID 118667291 ICID 197973259 RID 0 To: <jd@acme.com>', 'From: (\\\\S+)', 1)}", "")]
        [InlineData("{format(date(2018, 11, 28), 'MMddyyyy')}", "11282018")]
        [InlineData("{format(parse_date('2018-11-28', 'yyyy-MM-dd'), 'MMddyyyy')}", "11282018")]
        [InlineData("{substr($b, '2')}", "")]
        [InlineData("{substr($b, parse_int('2'))}", "ello")]
        [InlineData("{coalesce(null, 'a')}", "a")]
        public void TestObjectDecorationValueEvaluator(string value, string expected)
        {
            var tree = ObjectDecorationParserFacade.ParseObjectDecorationValue(value);
            string actual = (string)_evaluator.Visit(tree, null);
            Assert.Equal(expected, actual);
        }

        private static string GetGlobalVariable(string variableName)
        {
            switch(variableName)
            {
                case "a":
                    return "gav";
                case "b":
                    return "gbv";
                default:
                    return null;
            }
        }

        private static object GetLocalVariable(string variableName, object data)
        {
            switch (variableName)
            {
                case "$a":
                    return 9;
                case "$b":
                    return "hello";
                case "$c":
                    return 1.24d;
                default:
                    return null;
            }
        }
    }
}
