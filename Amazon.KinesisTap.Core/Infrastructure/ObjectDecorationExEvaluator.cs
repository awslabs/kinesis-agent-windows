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
using Amazon.KinesisTap.Expression.ObjectDecoration;

namespace Amazon.KinesisTap.Core
{
    public class ObjectDecorationExEvaluator : IEnvelopeEvaluator<IDictionary<string, string>>
    {
        private readonly NodeList<KeyValuePairNode> _tree;
        private readonly ObjectDecorationInterpreter<IEnvelope> _interpreter;

        public ObjectDecorationExEvaluator(string objectDecoration,
            Func<string, string> evaluateVariable,
            Func<string, IEnvelope, object> evaluateRecordVariable,
            IPlugInContext context
        )
        {
            _tree = ObjectDecorationParserFacade.ParseObjectDecoration(objectDecoration);
            FunctionBinder binder = new FunctionBinder(new Type[] { typeof(BuiltInFunctions) });
            ObjectDecorationEvaluationContext<IEnvelope> evalContext = new ObjectDecorationEvaluationContext<IEnvelope>(
                evaluateVariable,
                evaluateRecordVariable,
                binder,
                context?.Logger
            );
            var validator = new ObjectDecorationValidator<IEnvelope>(evalContext);
            validator.Visit(_tree, null); //Should throw if cannot resolve function
            _interpreter = new ObjectDecorationInterpreter<IEnvelope>(evalContext);
        }

        public IDictionary<string, string> Evaluate(IEnvelope envelope)
        {
            return (IDictionary<string, string>)_interpreter.VisitObjectDecoration(_tree, envelope);
        }
    }
}
