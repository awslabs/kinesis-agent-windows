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
        protected IPlugInContext _context;
        protected ILogger _logger;
        protected IConfiguration _config;
        protected IMetrics _metrics;
        protected string _format;
        protected string _textDecoration;
        protected string _objectDecoration;

        public EventSink(IPlugInContext context)
        {
            _context = context;
            _logger = context.Logger;
            _config = context.Configuration;
            _metrics = context.Metrics;
            this.Id = _config[ConfigConstants.ID];
            _format = _config[ConfigConstants.FORMAT];
            _textDecoration = _config[ConfigConstants.TEXT_DECORATION];
            _objectDecoration = _config[ConfigConstants.OBJECT_DECORATION];
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
            switch((_format ?? string.Empty).ToLower())
            {
                case "json":
                    if (!string.IsNullOrWhiteSpace(_objectDecoration))
                    {
                        IDictionary<string, string> attributes = new Dictionary<string, string>();
                        string[] attributePairs = _objectDecoration.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
                        foreach (var attributePair in attributePairs)
                        {
                            string[] keyValue = attributePair.Split('=');
                            string value = ResolveRecordVariables(keyValue[1], string.Empty, envelope);
                            if (!string.IsNullOrEmpty(value))
                            {
                                attributes.Add(keyValue[0], value);
                            }
                        }
                        record = JsonUtility.DecorateJson(record, attributes);
                    }
                    break;
                case "xml":
                    //Do nothing until someone request this to be implemented
                    break;
                default:
                    if (!string.IsNullOrWhiteSpace(_textDecoration))
                    {
                        record = ResolveRecordVariables(_textDecoration, record, envelope);
                    }
                    break;
            }
            return record;
        }

        private string ResolveRecordVariables(string format, string record, IEnvelope envelope)
        {
            record = Utility.ResolveVariables(format, (v) =>
            {
                string variable = v.ToLower();
                if (variable.Equals("{_record}"))
                {
                    return record;
                }
                if (envelope is ILogEnvelope log)
                {
                    switch(variable)
                    {
                        case "{_filepath}":
                            return log.FilePath;
                        case "{_filename}":
                            return log.FileName;
                        case "{_position}":
                            return log.Position.ToString();
                        case "{_linenumber}":
                            return log.LineNumber.ToString();
                    }
                }
                //Locall variable started with $
                if (v.StartsWith("{$"))
                {
                    return $"{envelope.ResolveLocalVariable(v.Substring(1, v.Length - 2))}"; //Strip off {}
                }
                return Utility.ResolveTimestampVariable(v, envelope.Timestamp);
            });
            record = ResolveVariables(record);
            return record;
        }

        private void ValidateConfig()
        {
            if (string.IsNullOrWhiteSpace(_format)
                || _format.Equals("json", StringComparison.CurrentCultureIgnoreCase)
                || _format.Equals("xml", StringComparison.CurrentCultureIgnoreCase))
            {
                return;
            }
            _logger?.LogError($"Unexpected format {_format}");
        }
    }
}
