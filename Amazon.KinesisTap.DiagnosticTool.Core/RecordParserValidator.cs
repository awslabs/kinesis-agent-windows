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
using System.Text;
using Microsoft.Extensions.Configuration;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Filesystem;
using Microsoft.Extensions.Logging.Abstractions;

namespace Amazon.KinesisTap.DiagnosticTool.Core
{
    /// <summary>
    /// The class for record parser validator
    /// </summary>
    public class RecordParserValidator
    {
        private readonly string _schemaBaseDirectory;
        private readonly IDictionary<string, ISourceValidator> _sourceValidators;
        private readonly Func<string, string, IConfigurationRoot> _loadConfigFile;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="schemaBaseDirectory"></param>
        /// <param name="sourceValidators"></param>
        /// <param name="loadConfigFile"></param>
        public RecordParserValidator(string schemaBaseDirectory, IDictionary<string, ISourceValidator> sourceValidators, Func<string, string, IConfigurationRoot> loadConfigFile)
        {
            _schemaBaseDirectory = schemaBaseDirectory;
            _sourceValidators = sourceValidators;
            _loadConfigFile = loadConfigFile;
        }

        /// <summary>
        /// Validate record parser
        /// </summary>
        /// <param name="id"></param>
        /// <param name="logName"></param>
        /// <param name="configBaseDirectory"></param>
        /// <param name="configFile"></param>
        /// <param name="messages"></param>
        /// <returns></returns>
        public bool ValidateRecordParser(string id, string logName, string configBaseDirectory, string configFile, out IList<string> messages)
        {
            IConfigurationRoot config = _loadConfigFile(configBaseDirectory, configFile);

            var configFileValidator = new ConfigValidator(_schemaBaseDirectory, _sourceValidators, _loadConfigFile);

            bool isValid = configFileValidator.ValidateSchema(configBaseDirectory, configFile, config, out messages);

            if (isValid)
            {
                var sourcesSection = config.GetSection("Sources");
                var sourceSections = sourcesSection.GetChildren();

                foreach (var sourceSection in sourceSections)
                {
                    string curId = config[$"{sourceSection.Path}:{"Id"}"];

                    if (curId.Equals(id))
                    {

                        string sourceType = config[$"{sourceSection.Path}:{"SourceType"}"];

                        if (!sourceType.Equals("DirectorySource"))
                        {
                            messages.Add("This tool only diagnose DirectorySource SourceType.");
                            return true;
                        }

                        string recordParser = config[$"{sourceSection.Path}:{"RecordParser"}"];
                        string directory = config[$"{sourceSection.Path}:{"Directory"}"];
                        string fileNameFilter = config[$"{sourceSection.Path}:{"FileNameFilter"}"];

                        string[] files = Directory.GetFiles(directory, fileNameFilter ?? "*.*");

                        if (files.Length != 1 && logName == null)
                        {
                            messages.Add("You have no files or more than one files in this extension, please note that this tool only can validate one log file at a time: ");
                            foreach (string file in files)
                            {
                                messages.Add(file);
                            }
                            return false;
                        }

                        if (recordParser.Equals("Timestamp"))
                        {
                            return ValidateTimeStamp(directory, logName ?? files[0], config, sourceSection, curId, messages);
                        }
                        else if (recordParser.Equals("Regex"))
                        {
                            return ValidateRegex(directory, logName ?? files[0], config, sourceSection, curId, messages);
                        }
                        else
                        {
                            messages.Add("No needs to validate Timestamp/Regex for the Record Parser: " + recordParser);
                            return true;
                        }

                    }
                }

                messages.Add("Source ID not found: " + id);
                return false;
            }

            messages.Add("Invalid configuration file format detected.");
            return false;
        }

        /// <summary>
        /// Validate timestamp
        /// </summary>
        /// <param name="directory"></param>
        /// <param name="logName"></param>
        /// <param name="config"></param>
        /// <param name="sourceSection"></param>
        /// <param name="curId"></param>
        /// <param name="messages"></param>
        /// <returns>True iff the logs has valid timestamps</returns>
        private bool ValidateTimeStamp(string directory, string logName, IConfigurationRoot config, IConfigurationSection sourceSection, string curId, IList<string> messages)
        {
            string timestampFormat = config[$"{sourceSection.Path}:TimestampFormat"];
            var parser = new TimestampLogParser(NullLogger.Instance, new RegexParserOptions
            {
                TimestampFormat = timestampFormat,
                TimeZoneKind = DateTimeKind.Utc
            }, null, 1024);

            var records = new List<IEnvelope<IDictionary<string, string>>>();
            parser.ParseRecordsAsync(new RegexLogContext
            {
                FilePath = Path.Combine(directory, logName)
            }, records, 2).GetAwaiter().GetResult();

            if (records.Count == 1)
            {
                messages.Add("Invalid Timestamp format at source ID: " + curId);
                return false;
            }
            else
            {
                messages.Add("Valid Timestamp format at source ID: " + curId);
                return true;
            }
        }

        /// <summary>
        /// Valdiate regex
        /// </summary>
        /// <param name="directory"></param>
        /// <param name="logName"></param>
        /// <param name="config"></param>
        /// <param name="sourceSection"></param>
        /// <param name="curId"></param>
        /// <param name="messages"></param>
        /// <returns>True iff the logs matches the regex pattern in the configuration</returns>
        private bool ValidateRegex(string directory, string logName, IConfigurationRoot config, IConfigurationSection sourceSection,
            string curId, IList<string> messages)
        {
            string pattern = config[$"{sourceSection.Path}:{"Pattern"}"];
            string timestampFormat = config[$"{sourceSection.Path}:{"TimestampFormat"}"];
            string extractionPattern = config[$"{sourceSection.Path}:{"ExtrationPattern"}"];
            string extractionRegexOptions = config[$"{sourceSection.Path}:{"ExtractionRegexOptions"}"];

            var records = new List<IEnvelope<IDictionary<string, string>>>();
            var parser = new RegexLogParser(NullLogger.Instance, pattern, new RegexParserOptions
            {
                TimestampFormat = timestampFormat,
                ExtractionPattern = extractionPattern,
                ExtractionRegexOptions = extractionRegexOptions,
                TimeZoneKind = DateTimeKind.Utc
            }, Encoding.UTF8, 1024);

            parser.ParseRecordsAsync(new RegexLogContext { FilePath = Path.Combine(directory, logName) }, records, 100).GetAwaiter().GetResult();
            if (records.Count == 1)
            {
                messages.Add("Invalid Regex at source ID: " + curId);
                return false;
            }
            else
            {
                messages.Add("Valid Regex at source ID: " + curId);
                return true;
            }
        }
    }
}
