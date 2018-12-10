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

using Amazon.KinesisTap.Expression;
using Amazon.KinesisTap.Expression.Ast;
using Amazon.KinesisTap.Expression.Binder;
using Amazon.KinesisTap.Expression.TextDecoration;

namespace Amazon.KinesisTap.Core
{
    public class TextDecorationExEvaluator : IEnvelopeEvaluator<string>
    {
        private readonly NodeList<Node> _tree;
        private readonly TextDecorationInterpreter<IEnvelope> _interpreter;

        public TextDecorationExEvaluator(string objectDecoration,
            Func<string, string> evaluateVariable,
            Func<string, IEnvelope, object> evaluateRecordVariable,
            IPlugInContext context
        )
        {
            _tree = TextDecorationParserFacade.ParseTextDecoration(objectDecoration);
            FunctionBinder binder = new FunctionBinder(new Type[] { typeof(BuiltInFunctions) });
            ExpressionEvaluationContext<IEnvelope> evalContext = new ExpressionEvaluationContext<IEnvelope>(
                evaluateVariable,
                evaluateRecordVariable,
                binder,
                context?.Logger
            );
            var validator = new TextDecorationValidator<IEnvelope>(evalContext);
            validator.Visit(_tree, null); //Should throw if cannot resolve function
            _interpreter = new TextDecorationInterpreter<IEnvelope>(evalContext);
        }

        public string Evaluate(IEnvelope envelope)
        {
            return (string)_interpreter.VisitTextDecoration(_tree, envelope);
        }
    }
}
