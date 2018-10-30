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
using System.IO;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Schema;
using Newtonsoft.Json.Linq;
using System.Globalization;
using Amazon.KinesisTap.Core.Metrics;
using Amazon.KinesisTap.Windows;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging.Abstractions;
using System.Linq;
using System.Diagnostics;

namespace Amazon.KinesisTap.DiagnosticTool
{
    public class ConfigValidator
    {

        private readonly JSchema _schema;

        public ConfigValidator(string schemaBaseDirectory)
        {

            using (StreamReader schemaReader = File.OpenText(Path.Combine(schemaBaseDirectory, Constant.CONFIG_SCHEMA_FILE)))
            using (JsonTextReader jsonReader = new JsonTextReader(schemaReader))
            {
                _schema = JSchema.Load(jsonReader);
            }
 
        }

        public bool ValidateSchema(string configPath, out IList<string> messages)
        {
            // Split the path by the Directory Separator
            string[] entries = configPath.Split(Path.DirectorySeparatorChar);
            string configBaseDirectory = string.Join(Path.DirectorySeparatorChar.ToString(), entries.Take(entries.Length - 1));
            string configFile = entries.Last();
            return ValidateSchema(configBaseDirectory, configFile, out messages);
        }

        public bool ValidateSchema(string configBaseDirectory, string configFile, out IList<string> messages)
        {
            IConfigurationRoot config = LoadConfigFile(configBaseDirectory, configFile);
            return ValidateSchema(configBaseDirectory, configFile, config, out messages);
        }

        public bool ValidateSchema(string configBaseDirectory, string configFile, IConfigurationRoot config, out IList<string> messages)
        {
            IDictionary<String, String> sources = new Dictionary<String, String>();
            IDictionary<String, String> sinks = new Dictionary<String, String>();

            var configFilePath = Path.Combine(configBaseDirectory, configFile);
            using (StreamReader configReader = File.OpenText(configFilePath))
            using (JsonTextReader jsonReader = new JsonTextReader(configReader))
            {
                JToken token = JToken.ReadFrom(new JsonTextReader(configReader));

                return token.IsValid(_schema, out messages)
                        && LoadSources(sources, config, messages)
                        && LoadSinks(sinks, config, messages)
                        && CheckPipes(sources, sinks, config, messages);
            }
        }

        private bool LoadSources(IDictionary<String, String> sources, IConfigurationRoot config, IList<string> messages)
        {
            var sourcesSection = config.GetSection("Sources");
            var sourceSections = sourcesSection.GetChildren();
            var performanceCounterCategories = PerformanceCounterCategory.GetCategories();

            foreach (var sourceSection in sourceSections)
            {
                string id = sourceSection["Id"];
                string sourceType = sourceSection["SourceType"];
                string initialPosition = sourceSection["InitialPosition"];

                if (sourceType.Equals("DirectorySource"))
                {
                    string recordParser = sourceSection["RecordParser"];

                    if (recordParser.Equals("TimeStamp"))
                    {
                        string timestampFormat = sourceSection["TimestampFormat"];
                        if (string.IsNullOrEmpty(timestampFormat))
                        {
                            messages.Add($"Attribute 'TimestampFormat' is required in source ID: {id}.");
                            return false;
                        }
                    }
                    else if (recordParser.Equals("Regex"))
                    {
                        string pattern = sourceSection["Pattern"];
                        if (string.IsNullOrEmpty(pattern))
                        {
                            messages.Add($"Attribute 'Pattern' is required in source ID: {id}.");
                            return false;
                        }

                        string timestampFormat = sourceSection["TimestampFormat"];
                        if (string.IsNullOrEmpty(timestampFormat))
                        {
                            messages.Add($"Attribute 'TimestampFormat' is required in source ID: {id}.");
                            return false;
                        }
                    }
                    else if (recordParser.Equals("Delimited"))
                    {
                        string delimiter = sourceSection["Delimiter"];
                        string timestampField = sourceSection["TimestampField"];
                        string timestampFormat = sourceSection["TimestampFormat"];

                        if (string.IsNullOrEmpty(delimiter))
                        {
                            messages.Add($"Attribute 'Delimiter' is required in source ID: {id}.");
                            return false;
                        }

                        if (string.IsNullOrEmpty(timestampField))
                        {
                            messages.Add($"Attribute 'TimestampField' is required in source ID: {id}.");
                            return false;
                        }

                        if (string.IsNullOrEmpty(timestampFormat))
                        {
                            messages.Add($"Attribute 'TimestampFormat' is required in source ID: {id}.");
                            return false;
                        }
                    }
                }
                else if (sourceType.Equals("WindowsEventLogSource"))
                {
                    string logName = sourceSection["LogName"];
                    EventLogValidator eventLogValidator = new EventLogValidator();
                    if (!eventLogValidator.ValidateLogName(logName, messages))
                    {
                        return false;
                    }
                }
                else if (sourceType.Equals("WindowsPerformanceCounterSource"))
                {
                    var categoriesSection = sourceSection.GetSection("Categories");
                    var validator = new PerformanceCounterValidator(categoriesSection, performanceCounterCategories);
                    if (!validator.ValidateSource(messages))
                    {
                        return false;
                    }
                }

                if (!string.IsNullOrEmpty(initialPosition) && initialPosition.Equals("Timestamp"))
                {
                    string initialPositionTimestamp = sourceSection["InitialPositionTimestamp"];
                    if (string.IsNullOrEmpty(initialPositionTimestamp))
                    {
                        messages.Add($"InitialPositionTimestamp required in source ID: {id}.");
                        return false;
                    }
                    else
                    {
                        string timestampFormat = "yyyy-MM-dd HH:mm:ss.ffff";
                        if(!DateTime.TryParseExact(initialPositionTimestamp, timestampFormat, new CultureInfo("en-US"),
                            DateTimeStyles.None, out DateTime expectedDate))
                        {
                            messages.Add($"Timestamp doesn't match the DateTime format: {timestampFormat} in source {id}.");
                            return false;
                        }
                    }
                }

                sources.Add(id, sourceType);
            }

            return true;
        }

        private bool LoadSinks(IDictionary<String, String> sinks, IConfigurationRoot config, IList<string> messages)
        {
            HashSet<String> credentialIDs = new HashSet<String>();
            LoadCredentials(credentialIDs, config);

            var sinksSection = config.GetSection("Sinks");
            var sinkSections = sinksSection.GetChildren();

            foreach (var sinkSection in sinkSections)
            {
                string credentialRef = sinkSection["CredentialRef"];
                if (!String.IsNullOrEmpty(credentialRef))
                {
                    if (!credentialIDs.Contains(credentialRef))
                    {
                        messages.Add($"Credential: {credentialRef} required but not specified.");
                        return false;
                    }
                }

                string id = sinkSection["Id"];
                string sinkType = sinkSection["SinkType"];
                sinks.Add(id, sinkType);
            }

            return true;
        }
        
        private void LoadCredentials(HashSet<String> credentialIDs, IConfigurationRoot config)
        {
            var credendialsSection = config.GetSection("Credentials");
            var credentialSections = credendialsSection.GetChildren();

            foreach (var credentialSection in credentialSections)
            {
                string credentialID = credentialSection["Id"];
                credentialIDs.Add(credentialID);
            }
        }

        private bool CheckPipes(IDictionary<String, String> sources, IDictionary<String, String> sinks, IConfigurationRoot config, IList<string> messages)
        {
            var pipesSection = config.GetSection("Pipes");
            var pipeSections = pipesSection.GetChildren();

            foreach (var pipeSection in pipeSections)
            {
                string id = pipeSection["Id"];
                string sourceRef = pipeSection["SourceRef"];
                string sinkRef = pipeSection["SinkRef"];
                string type = pipeSection["Type"];
                string filterPattern = pipeSection["FilterPattern"];

                if (!sources.ContainsKey(sourceRef))
                {
                    messages.Add($"Source {sourceRef} has not been defined from the Pipe: {id}.");
                    return false;
                }

                if (!sinks.ContainsKey(sinkRef))
                {
                    messages.Add($"Sink {sinkRef} has not been defined from the Pipe: {id}.");
                    return false;
                }

                // WindowsPerformanceCounterSource only can connect to a CloudWatch sink
                if (sources[sourceRef].Equals("WindowsPerformanceCounterSource") && !sinks[sinkRef].Equals("CloudWatch"))
                {
                    messages.Add($"WindowsPerformanceCounterSource {sourceRef} should only connect to CloudWatch sink from the Pipe: {id}.");
                    return false;
                }

                // Regex filter pipe requires the attribute "FilterPattern"
                if (!string.IsNullOrEmpty(type))
                {
                    if (string.IsNullOrEmpty(filterPattern))
                    {
                        messages.Add($"Attribute 'FilterPattern' is required in the Regex filter pipe: {id}.");
                        return false;
                    }
                }
                
            }

            return true;
        }

        public static IConfigurationRoot LoadConfigFile(string configBaseDirectory, string configFile)
        {

            ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

            IConfigurationRoot config;

            config = configurationBuilder
                .SetBasePath(configBaseDirectory)
                .AddJsonFile(configFile, optional: false, reloadOnChange: true)
                .Build();

            return config;
        }
    }
}
