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
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace Amazon.KinesisTap.Core
{
    public abstract class EventSink : IEventSink
    {
        protected readonly IPlugInContext _context;
        protected readonly ILogger _logger;
        protected readonly IConfiguration _config;
        protected readonly IMetrics _metrics;
        protected readonly string _format;
        protected readonly IEnvelopeEvaluator<string> _textDecorationEvaluator;
        protected readonly IEnvelopeEvaluator<IDictionary<string, string>> _objectDecorationEvaluator;

        public EventSink(IPlugInContext context)
        {
            _context = context;
            _logger = context.Logger;
            _config = context.Configuration;
            _metrics = context.Metrics;
            this.Id = _config[ConfigConstants.ID];
            _format = _config[ConfigConstants.FORMAT];

            string textDecoration = _config[ConfigConstants.TEXT_DECORATION];
            if (!string.IsNullOrWhiteSpace(textDecoration))
            {
                _textDecorationEvaluator = new TextDecorationEvaluator(textDecoration, ResolveRecordVariables);
            }

            string textDecorationEx = _config[ConfigConstants.TEXT_DECORATION_EX];
            if (!string.IsNullOrWhiteSpace(textDecorationEx))
            {
                _textDecorationEvaluator = new TextDecorationExEvaluator(textDecorationEx, EvaluateVariable, ResolveRecordVariable, context);
            }

            string objectDecoration = _config[ConfigConstants.OBJECT_DECORATION];
            if (!string.IsNullOrWhiteSpace(objectDecoration))
            {
                _objectDecorationEvaluator = new ObjectDecorationEvaluator(objectDecoration, ResolveRecordVariables);
            }

            string objectDecorationEx = _config[ConfigConstants.OBJECT_DECORATION_EX];
            if (!string.IsNullOrWhiteSpace(objectDecorationEx))
            {
                _objectDecorationEvaluator = new ObjectDecorationExEvaluator(objectDecorationEx, EvaluateVariable, ResolveRecordVariable, context);
            }
            ValidateConfig();
        }

        public string Id { get; set; }

        public virtual void OnCompleted()
        {
            _logger?.LogInformation($"{this.GetType()} {this.Id} completed.");
        }

        public virtual void OnError(Exception error)
        {
            _logger?.LogCritical($"{this.GetType()} {this.Id} error: {error}.");
        }

        public abstract void OnNext(IEnvelope envelope);

        public abstract void Start();

        public abstract void Stop();

        protected virtual string ResolveVariables(string value)
        {
            return Utility.ResolveVariables(value, EvaluateVariable);
        }

        protected virtual string EvaluateVariable(string value)
        {
            return Utility.ResolveVariable(value);
        }

        protected virtual string GetRecord(IEnvelope envelope)
        {
            string record = envelope.GetMessage(_format);
            switch ((_format ?? string.Empty).ToLower())
            {
                case ConfigConstants.FORMAT_JSON:
                    if (_objectDecorationEvaluator != null)
                    {
                        IDictionary<string, string> attributes = _objectDecorationEvaluator.Evaluate(envelope);
                        record = JsonUtility.DecorateJson(record, attributes);
                    }
                    break;
                case ConfigConstants.FORMAT_XML:
                case ConfigConstants.FORMAT_XML_2:
                case ConfigConstants.FORMAT_RENDERED_XML:
                    //Do nothing until someone request this to be implemented
                    break;
                default:
                    if (_textDecorationEvaluator != null)
                    {
                        record = _textDecorationEvaluator.Evaluate(envelope);
                    }
                    break;
            }
            return record;
        }

        private string ResolveRecordVariables(string format, IEnvelope envelope)
        {
            string record = Utility.ResolveVariables(format, envelope, ResolveRecordVariable);
            record = ResolveVariables(record);
            return record;
        }

        private object ResolveRecordVariable(string variable, IEnvelope envelope)
        {
            if (variable.StartsWith("{"))
            {
                variable = variable.Substring(1, variable.Length - 2);
            }

            if (variable.StartsWith("$"))  //Local variable started with $
            {
                return envelope.ResolveLocalVariable(variable);
            }
            else if (variable.StartsWith("_"))  //Meta variable started with _
            {
                return envelope.ResolveMetaVariable(variable);
            }

            //Legacy timestamp, e.g., {timestamp:yyyyMmddHHmmss}. Add the curly braces back
            return Utility.ResolveTimestampVariable($"{{{variable}}}", envelope.Timestamp);
        }

        private void ValidateConfig()
        {
            if (string.IsNullOrWhiteSpace(_format)
                 || _format.Equals(ConfigConstants.FORMAT_JSON, StringComparison.CurrentCultureIgnoreCase)
                 || _format.Equals(ConfigConstants.FORMAT_XML, StringComparison.CurrentCultureIgnoreCase)
                 || _format.Equals(ConfigConstants.FORMAT_XML_2, StringComparison.CurrentCultureIgnoreCase)
                 || _format.Equals(ConfigConstants.FORMAT_RENDERED_XML, StringComparison.CurrentCultureIgnoreCase)
                 || _format.Equals(ConfigConstants.FORMAT_SUSHI, StringComparison.CurrentCultureIgnoreCase))
            {
                return;
            }
            _logger?.LogError($"Unexpected format '{_format}'");
        }
    }
}
