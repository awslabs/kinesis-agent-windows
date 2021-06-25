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
using Xunit;
using Amazon.KinesisTap.Shared.Ast;
using Amazon.KinesisTap.Shared.ObjectDecoration;

namespace Amazon.KinesisTap.Shared.Test
{
    public class ObjectDecorationTest
    {
        private const string TWO_KEYVALUE_PARIS_WITH_ONE_VARIBLE = " key1 = b {var1} c;key2=val2";
        private const string INCOMPLETE_OBJECTDECORATION = " key1 = b {var1} c;key2val2";
        private const string OBJECTDECORATION_WITH_EXPRESSIONS = "key1={'const1'};key2={101};key3={true};key4={false};key5={null};key6={regexp_extract($message, '=;')}";

        [Fact]
        public void TestSimpleObjectDecoration()
        {
            var parseTree = ObjectDecorationParserFacade.ParseObjectDecoration(TWO_KEYVALUE_PARIS_WITH_ONE_VARIBLE);
            Assert.Equal(2, parseTree.List.Count);
            Assert.Equal(" key1 ", parseTree.List[0].Key);
            Assert.Equal(3, ((NodeList<Node>)parseTree.List[0].Value).List.Count);
            Assert.Equal("key2", parseTree.List[1].Key);
        }

        [Fact]
        public void TestObjectDecorationWithExpressions()
        {
            var parseTree = ObjectDecorationParserFacade.ParseObjectDecoration(OBJECTDECORATION_WITH_EXPRESSIONS);
            Assert.Equal(6, parseTree.List.Count);
        }

        [Fact]
        public void TestIncompleteObjectDecoration()
        {
            Assert.Throws<Exception>(() => ObjectDecorationParserFacade.ParseObjectDecoration(INCOMPLETE_OBJECTDECORATION));
        }
    }
}
