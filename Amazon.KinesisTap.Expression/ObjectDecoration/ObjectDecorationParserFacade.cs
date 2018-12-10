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

using Amazon.KinesisTap.Expression.Ast;

namespace Amazon.KinesisTap.Expression.ObjectDecoration
{
    public class ObjectDecorationParserFacade
    {
        /// <summary>
        /// Parse object decoration attribute into a list key value pairs
        /// </summary>
        /// <param name="objectDecoration">Object Decoration declaration to parse</param>
        /// <returns>List of key value pairs</returns>
        public static NodeList<KeyValuePairNode> ParseObjectDecoration(string objectDecoration)
        {
            SetupParser(objectDecoration, out ObjectDecorationParser parser, out ErrorListener errorListener);

            var tree = parser.objectDecoration();
            string errors = errorListener.Errors;

            if (!string.IsNullOrEmpty(errors))
            {
                throw new Exception(errors);
            }

            ObjectDecorationParserVisitor visitor = new ObjectDecorationParserVisitor();
            return (NodeList<KeyValuePairNode>)visitor.Visit(tree);
        }

        /// <summary>
        /// Parse a single object decoration value in key/value paris
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static NodeList<Node> ParseObjectDecorationValue(string value)
        {
            SetupParser(value, out ObjectDecorationParser parser, out ErrorListener errorListener);
            var tree = parser.value();
            string errors = errorListener.Errors;

            if (!string.IsNullOrEmpty(errors))
            {
                throw new Exception(errors);
            }

            ObjectDecorationParserVisitor visitor = new ObjectDecorationParserVisitor();
            return (NodeList<Node>)visitor.Visit(tree);
        }

        private static void SetupParser(string objectDecoration, out ObjectDecorationParser parser, out ErrorListener errorListener)
        {
            AntlrInputStream inputStream = new AntlrInputStream(objectDecoration);
            ObjectDecorationLexer lexer = new ObjectDecorationLexer(inputStream);
            CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
            parser = new ObjectDecorationParser(commonTokenStream);
            parser.RemoveErrorListeners();
            errorListener = new ErrorListener();
            parser.AddErrorListener(errorListener);
        }
    }
}
