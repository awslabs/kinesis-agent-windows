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
using System.Linq;

namespace Amazon.KinesisTap.DiagnosticTool.Core
{
    /// <summary>
    /// The configuration file validator
    /// </summary>
    public class ConfigValidator
    {

        private readonly JSchema _schema;
        private IDictionary<String, ISourceValidator> _sourceValidators;
        private Func<String, String, IConfigurationRoot> _loadConfigFile;
        private readonly HashSet<String> _initialPositions = new HashSet<String>() { "timestamp", "eos", "0", "bookmark" };

        /// <summary>
        /// Configuration validator constructor
        /// </summary>
        /// <param name="schemaBaseDirectory"></param>
        /// <param name="sourceValidators"></param>
        /// <param name="loadConfigFile"></param>
        public ConfigValidator(string schemaBaseDirectory, IDictionary<String, ISourceValidator> sourceValidators, Func<string, string, IConfigurationRoot> loadConfigFile)
        {
            this._sourceValidators = sourceValidators;
            this._loadConfigFile = loadConfigFile;

            using (StreamReader schemaReader = File.OpenText(Path.Combine(schemaBaseDirectory, Constant.CONFIG_SCHEMA_FILE)))
            using (JsonTextReader jsonReader = new JsonTextReader(schemaReader))
            {
                _schema = JSchema.Load(jsonReader);
            }
        }

        /// <summary>
        /// Validate the configuration file against the schema
        /// </summary>
        /// <param name="configPath"></param>
        /// <param name="messages"></param>
        /// <returns></returns>
        public bool ValidateSchema(string configPath, out IList<string> messages)
        {
            // Split the path by the Directory Separator
            string[] entries = configPath.Split(Path.DirectorySeparatorChar);
            string configBaseDirectory = string.Join(Path.DirectorySeparatorChar.ToString(), entries.Take(entries.Length - 1));
            string configFile = entries.Last();
            return ValidateSchema(configBaseDirectory, configFile, out messages);
        }

        /// <summary>
        /// Validate the configuration file against the schema
        /// </summary>
        /// <param name="configBaseDirectory"></param>
        /// <param name="configFile"></param>
        /// <param name="messages"></param>
        /// <returns></returns>
        public bool ValidateSchema(string configBaseDirectory, string configFile, out IList<string> messages)
        {
            IConfigurationRoot config = this._loadConfigFile(configBaseDirectory, configFile);
            return ValidateSchema(configBaseDirectory, configFile, config, out messages);
        }

        /// <summary>
        /// Validate the configuration file against the schema
        /// </summary>
        /// <param name="configBaseDirectory"></param>
        /// <param name="configFile"></param>
        /// <param name="config"></param>
        /// <param name="messages"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Load the source
        /// </summary>
        /// <param name="sources"></param>
        /// <param name="config"></param>
        /// <param name="messages"></param>
        /// <returns></returns>
        private bool LoadSources(IDictionary<String, String> sources, IConfigurationRoot config, IList<string> messages)
        {
            var sourcesSection = config.GetSection("Sources");
            var sourceSections = sourcesSection.GetChildren();

            foreach (var sourceSection in sourceSections)
            {
                string id = sourceSection["Id"];
                string sourceType = sourceSection["SourceType"];
                string initialPosition = sourceSection["InitialPosition"];

                ISourceValidator sourceValidator;
                if (sourceType.Equals("DirectorySource"))
                {
                    if (_sourceValidators.TryGetValue("DirectorySource", out sourceValidator))
                    {
                        if (!sourceValidator.ValidateSource(sourceSection, id, messages))
                        {
                            return false;
                        }
                    }
                }
                else if (sourceType.Equals("WindowsEventLogSource"))
                {
                    if (_sourceValidators.TryGetValue("WindowsEventLogSource", out sourceValidator))
                    {
                        if (!sourceValidator.ValidateSource(sourceSection, id, messages))
                        {
                            return false;
                        }
                    }
                }
                else if (sourceType.Equals("WindowsPerformanceCounterSource"))
                {
                    if (_sourceValidators.TryGetValue("WindowsPerformanceCounterSource", out sourceValidator))
                    {
                        if (!sourceValidator.ValidateSource(sourceSection, id, messages))
                        {
                            return false;
                        }
                    }
                }

                if (!string.IsNullOrEmpty(initialPosition))
                {
                    initialPosition = initialPosition.ToLower();
                    if (!this._initialPositions.Contains(initialPosition))
                    {
                        messages.Add($"{initialPosition} is not a valid InitialPosition in source ID: {id}.");
                        return false;
                    }

                    if (initialPosition.Equals("timestamp"))
                    {
                        string initialPositionTimestamp = sourceSection["InitialPositionTimestamp"];
                        if (string.IsNullOrEmpty(initialPositionTimestamp))
                        {
                            messages.Add($"InitialPosition 'Timestamp' is required in source ID: {id}.");
                            return false;
                        }
                        else
                        {
                            string timestampFormat = "yyyy-MM-dd HH:mm:ss.ffff";
                            if (!DateTime.TryParseExact(initialPositionTimestamp, timestampFormat, new CultureInfo("en-US"),
                                DateTimeStyles.None, out DateTime expectedDate))
                            {
                                messages.Add($"Timestamp doesn't match the DateTime format: {timestampFormat} in source {id}.");
                                return false;
                            }
                        }
                    }
                }

                sources.Add(id, sourceType);
            }

            return true;
        }

        /// <summary>
        /// Load the sinks
        /// </summary>
        /// <param name="sinks"></param>
        /// <param name="config"></param>
        /// <param name="messages"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Load the credentials
        /// </summary>
        /// <param name="credentialIDs"></param>
        /// <param name="config"></param>
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

        /// <summary>
        /// Check the pipes
        /// </summary>
        /// <param name="sources"></param>
        /// <param name="sinks"></param>
        /// <param name="config"></param>
        /// <param name="messages"></param>
        /// <returns></returns>
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
    }
}
