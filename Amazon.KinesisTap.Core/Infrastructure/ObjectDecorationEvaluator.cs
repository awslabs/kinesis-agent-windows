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

namespace Amazon.KinesisTap.Core
{
    public class ObjectDecorationEvaluator : IEnvelopeEvaluator<IDictionary<string, string>>
    {
        private string _objectDecoration;
        private Func<string, IEnvelope, string> _evaluateVariables;

        public ObjectDecorationEvaluator(string objectDecoration, Func<string, IEnvelope, string> evaluateVariables)
        {
            _objectDecoration = objectDecoration;
            _evaluateVariables = evaluateVariables;
        }

        public IDictionary<string, string> Evaluate(IEnvelope envelope)
        {
            IDictionary<string, string> attributes = new Dictionary<string, string>();
            string[] attributePairs = _objectDecoration.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var attributePair in attributePairs)
            {
                string[] keyValue = attributePair.Split('=');
                string value = _evaluateVariables(keyValue[1], envelope);
                if (!string.IsNullOrEmpty(value))
                {
                    attributes.Add(keyValue[0], value);
                }
            }
            return attributes;
        }
    }
}
