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
using System.IO;
using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.Hosting
{
    /// <summary>
    /// Provide ability to read/save configurations in key=value per line.
    /// </summary>
    public class SimpleParameterStore : IParameterStore
    {
        private readonly string _configPath;
        private readonly IDictionary<string, string> _parameters;

        public SimpleParameterStore(string filePath)
        {
            _configPath = filePath;
            EnsureFile();
            _parameters = new Dictionary<string, string>();
            LoadParameters();
        }

        public string GetParameter(string name)
        {
            if (_parameters.TryGetValue(name, out string value))
            {
                return value;
            }
            return null;
        }

        public void SetParameter(string name, string value)
        {
            _parameters[name] = value;
            SaveParameters();
        }

        private void EnsureFile()
        {
            string dirPath = Path.GetDirectoryName(_configPath);
            if (!Directory.Exists(dirPath))
            {
                Directory.CreateDirectory(dirPath);
            }
            if (!File.Exists(_configPath))
            {
                File.WriteAllText(_configPath, string.Empty);
            }
        }

        private void LoadParameters()
        {
            using (var textReader = File.OpenText(_configPath))
            {
                int lineNumber = 0;
                while (!textReader.EndOfStream)
                {
                    lineNumber++;
                    string line = textReader.ReadLine();
                    if (!string.IsNullOrWhiteSpace(line))
                    {
                        //Split by first '=' so that the value can contain '='
                        int posEqual = line.IndexOf('=');
                        if (posEqual < 0)
                        {
                            throw new Exception($"Error reading config file {_configPath} line {lineNumber}");
                        }
                        else
                        {
                            _parameters[line.Substring(0, posEqual)] = line.Substring(posEqual + 1);
                        }
                    }
                }
            }
        }

        private void SaveParameters()
        {
            using (var fileStream = new FileStream(_configPath, FileMode.Create, FileAccess.Write, FileShare.None))
            using (var fileWriter = new StreamWriter(fileStream))
            {
                foreach (var keyValuePair in _parameters)
                {
                    fileWriter.WriteLine($"{keyValuePair.Key}={keyValuePair.Value}");
                }
            }
        }
    }
}
