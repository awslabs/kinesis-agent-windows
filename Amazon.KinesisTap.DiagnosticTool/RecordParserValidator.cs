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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.DiagnosticTool
{
    public class RecordParserValidator
    {
        private readonly string _schemaBaseDirectory;

        public RecordParserValidator(string schemaBaseDirectory)
        {
            this._schemaBaseDirectory = schemaBaseDirectory;
        }
        public bool ValidateRecordParser(string id, string logName, string configBaseDirectory, string configFile, out IList<String> messages)
        {
            IConfigurationRoot config = ConfigValidator.LoadConfigFile(configBaseDirectory, configFile);

            var configFileValidator = new ConfigValidator(_schemaBaseDirectory);

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

        private bool ValidateTimeStamp(string directory, string logName, IConfigurationRoot config, IConfigurationSection sourceSection, string curId, IList<String> messages)
        {
            string log = GetLog(directory, logName).ToString();

            using (Stream stream = Utility.StringToStream(log))
            using (StreamReader sr = new StreamReader(stream))
            {
                string timestampFormat = config[$"{sourceSection.Path}:{"TimestampFormat"}"];
                TimeStampRecordParser parser = new TimeStampRecordParser(timestampFormat, null, DateTimeKind.Utc);
                var records = parser.ParseRecords(sr, new LogContext()).ToList();
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
        }

        private bool ValidateRegex(string directory, string logName, IConfigurationRoot config, IConfigurationSection sourceSection, string curId, IList<String> messages)
        {
            string log = GetLog(directory, logName).ToString();

            using (Stream stream = Utility.StringToStream(log))
            using (StreamReader sr = new StreamReader(stream))
            {
                string pattern = config[$"{sourceSection.Path}:{"Pattern"}"];
                string timestampFormat = config[$"{sourceSection.Path}:{"TimestampFormat"}"];
                string extractionPattern = config[$"{sourceSection.Path}:{"ExtrationPattern"}"];

                RegexRecordParser parser = new RegexRecordParser(pattern, timestampFormat, null, extractionPattern, DateTimeKind.Utc);
                var records = parser.ParseRecords(sr, new LogContext()).ToList();
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

        private string GetLog(string directory, string logName)
        {
            string line;
            StringBuilder sb = new StringBuilder();
            using (StreamReader LogReader = new StreamReader(Path.Combine(directory, logName)))
            {
                while ((line = LogReader.ReadLine()) != null)
                {
                    sb.AppendLine(line);
                }

                LogReader.Close();
            }

            return sb.ToString();
        }
    }
}
