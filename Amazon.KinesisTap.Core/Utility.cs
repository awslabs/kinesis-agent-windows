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
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;

namespace Amazon.KinesisTap.Core
{
    public static class Utility
    {
        public static readonly bool IsWindow = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
        public static Func<string, string> ResolveEnvironmentVariable = Environment.GetEnvironmentVariable; //Can override this function for different OS

        private static string _computerName;
        private static string _hostName;

        private static readonly Random _random = new Random(
            (Utility.ComputerName + DateTime.UtcNow.ToString())
            .GetHashCode()
        );
        private static Stopwatch _stopwatch = Stopwatch.StartNew();

        public static long GetElapsedMilliseconds()
        {
            return _stopwatch.ElapsedMilliseconds;
        }

        public static string ComputerName
        {
            get
            {
                if (_computerName == null)
                {
                    try
                    {
                        //On Linux, system does not create environment variable for nologin users so use Dns.GetHostName();
                        //Dns.GetHostName on Linux eventually call gethostname() system function but it first check if socket exists.
                        //In later version of .net, can just use Environment.MachineName
                        if (IsWindow)
                        {
                            _computerName = Environment.GetEnvironmentVariable("COMPUTERNAME");
                        }
                        else
                        {
                            string hostName = Dns.GetHostName();
                            int dotPos = hostName.IndexOf('.');
                            _computerName = dotPos == -1 ? hostName : hostName.Substring(0, dotPos);
                        }
                    }
                    catch { }
                }
                return _computerName ?? string.Empty;
            }
        }

        public static string HostName
        {
            get
            {
                if (_hostName == null)
                {
                    try
                    {
                        //On Linux, Dns.GetHostEntryAsync("LocalHost") will return "LocalHost"
                        _hostName = IsWindow ? Dns.GetHostEntryAsync("LocalHost").Result.HostName : Dns.GetHostName();
                    }
                    catch { }
                }
                return _hostName ?? "unresolved";
            }
        }

        public static readonly Regex VARIABLE_REGEX = new Regex("{[^}]+}");

        public static MemoryStream StringToStream(string str)
        {
            return StringToStream(str, null);
        }

        public static MemoryStream StringToStream(string str, string delimiter)
        {
            var memStream = new MemoryStream();
            var textWriter = new StreamWriter(memStream);
            textWriter.Write(str);
            if (!string.IsNullOrEmpty(delimiter))
            {
                textWriter.Write(delimiter);
            }
            textWriter.Flush();
            memStream.Seek(0, SeekOrigin.Begin);

            return memStream;
        }

        public static string ResolveVariables(string value, Func<string, string> evaluator)
        {
            return VARIABLE_REGEX.Replace(value, m => evaluator(m.Groups[0].Value));
        }

        public static string ResolveVariables(string value, IEnvelope envelope, Func<string, IEnvelope, object> evaluator)
        {
            return VARIABLE_REGEX.Replace(value, m => $"{evaluator(m.Groups[0].Value, envelope)}");
        }

        /// <summary>
        /// Resolve a variable. If the variable does not have a prefix, it tries to resolve to environment variable or return the variable itself if it cannot resolve.
        /// If the variable has a prefix, it will resolve the variable if the prefix is for environment variable or return the variable for the next step.
        /// </summary>
        /// <param name="variable">The name of the variable to resolve</param>
        /// <returns></returns>
        public static string ResolveVariable(string variable)
        {
            if (variable != null && variable.StartsWith("{"))
            {
                if (variable.EndsWith("}"))
                {
                    variable = variable.Substring(1, variable.Length - 2);
                }
                else
                {
                    throw new ArgumentException("variable must be in the format of \"{variable}\" or \"{prefix:variable}\".");
                }
            }

            if (string.IsNullOrWhiteSpace(variable))
            {
                throw new ArgumentException("Missing variable name.");
            }

            (string prefix, string variableNoPrefix) = SplitPrefix(variable, ':');
            if (!string.IsNullOrWhiteSpace(prefix) && !"env".Equals(prefix, StringComparison.CurrentCultureIgnoreCase))
            {
                //I don't know the prefix. Return the original form to let others resolve
                return $"{{{variable}}}";
            }

            string value = ResolveEnvironmentVariable(variableNoPrefix);
            if ("env".Equals(prefix, StringComparison.CurrentCultureIgnoreCase))
            {
                //User specifically asking for environment variable
                return value;
            }
            else
            {
                //return the variable itself for the next step in the pipeline to resolve
                return string.IsNullOrWhiteSpace(value) ? $"{{{variable}}}" : value;
            }
        }

        /// <summary>
        /// This function will resolve the variable if it is a time stamp variable or return the variable itself for the next step in the pipeline to resolve
        /// </summary>
        /// <param name="variable">The name of the variable</param>
        /// <param name="timestamp">The timestamp to resolve to</param>
        /// <returns></returns>
        public static string ResolveTimestampVariable(string variable, DateTime timestamp)
        {
            if (!variable.StartsWith("{") || !variable.EndsWith("}"))
            {
                return variable;
            }

            (string prefix, string variableNoPrefix) = Utility.SplitPrefix(variable.Substring(1, variable.Length - 2), ':');
            if ("timestamp".Equals(prefix, StringComparison.CurrentCultureIgnoreCase))
            {
                return timestamp.ToString(variableNoPrefix);
            }
            else
            {
                return variable;
            }
        }

        public static (string prefix, string suffix) SplitPrefix(string variable, char separator)
        {
            int x = variable.IndexOf(separator);
            string prefix = null;
            if (x > -1)
            {
                prefix = variable.Substring(0, x);
                variable = variable.Substring(x + 1);
            }
            return (prefix, variable);
        }

        public static IEnumerable<string> ParseCSVLine(string input, StringBuilder stringBuilder)
        {
            const char columnSeparator = ',';
            if (string.IsNullOrEmpty(input))
            {
                yield break;
            }

            stringBuilder.Clear();

            int index = 0;
            int escapeCount = 0;

            for (; index < input.Length; index++)
            {
                if (input[index] == '"')
                {
                    escapeCount++;
                    stringBuilder.Append('"');
                }
                else if (input[index] == columnSeparator)
                {
                    if ((escapeCount % 2) == 0)
                    {
                        if (escapeCount == 0)
                        {
                            yield return stringBuilder
                                .ToString();
                        }
                        else
                        {
                            yield return stringBuilder
                                .Extract('"')
                                .Replace(@"""""", @"""");
                        }

                        stringBuilder.Clear();
                        escapeCount = 0;
                    }
                    else
                    {
                        stringBuilder.Append(columnSeparator);
                    }
                }
                else
                {
                    stringBuilder.Append(input[index]);
                }
            }

            if (escapeCount == 0)
            {
                yield return stringBuilder
                    .ToString();
            }
            else
            {
                yield return stringBuilder
                    .Extract('"')
                    .Replace(@"""""", @"""");
            }
        }

        //Should return something like c:\ProgramData\Amazon\KinesisTap
        public static string GetKinesisTapProgramDataPath()
        {
            string kinesisTapProgramDataPath = Environment.GetEnvironmentVariable(ConfigConstants.KINESISTAP_PROGRAM_DATA);
            if (string.IsNullOrWhiteSpace(kinesisTapProgramDataPath))
            {
                kinesisTapProgramDataPath = Path.Combine(Environment.GetEnvironmentVariable("ProgramData"), "Amazon\\AWSKinesisTap");
            }
            return kinesisTapProgramDataPath;
        }

        public static string GetKinesisTapConfigPath()
        {
            string kinesisTapConfigPath = Environment.GetEnvironmentVariable(ConfigConstants.KINESISTAP_CONFIG_PATH);
            if (string.IsNullOrWhiteSpace(kinesisTapConfigPath))
            {
                kinesisTapConfigPath = AppContext.BaseDirectory;
            }
            return kinesisTapConfigPath;
        }

        public static string ProperCase(string constant)
        {
            if (string.IsNullOrWhiteSpace(constant))
            {
                return constant;
            }
            else
            {
                return string.Join("", constant.Split(new char[] { '_' }, StringSplitOptions.RemoveEmptyEntries).Select(s => s[0].ToString().ToUpper() + s.Substring(1).ToLower()).ToArray());
            }
        }

        public static Random Random => _random;

        public static T[] CloneArray<T>(T[] array)
        {
            T[] clone = new T[array.Length];
            array.CopyTo(clone, 0);
            return clone;
        }

        public static int ParseInteger(string value, int defaultValue)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return defaultValue;
            }
            else if (int.TryParse(value, out int result))
            {
                return result;
            }
            else
            {
                return defaultValue;
            }
        }

        public static DateTime? ToUniversalTime(DateTime? datetime)
        {
            if (datetime.HasValue)
            {
                return datetime.Value.ToUniversalTime();
            }
            else
            {
                return datetime;
            }
        }

        /// <summary>
        /// Detect if a path expression is a wildcard expressions, containing ? or *
        /// </summary>
        /// <param name="nameOrPattern">Expression to check</param>
        /// <returns>true or false</returns>
        public static bool IsWildcardExpression(string nameOrPattern)
        {
            return (nameOrPattern.IndexOf("*") > -1) || (nameOrPattern.IndexOf("?") > -1);
        }

        /// <summary>
        /// Convert a wildcard expression to regular expression. 
        /// Match '?' to a single character and '*' to any single characters
        /// Escape all special characters
        /// </summary>
        /// <param name="pattern">The wildcard expression to convert</param>
        /// <returns>Regular expressions converted from wildcard expression</returns>
        public static string WildcardToRegex(string pattern, bool matchWholePhrase)
        {
            const string CHARS_TO_ESCAPE = ".+|{}()[]^$\\";
            string regex = string.Concat(pattern.Select(c => CHARS_TO_ESCAPE.IndexOf(c) > -1 ? "\\" + c : c.ToString()))
                .Replace("?", ".")
                .Replace("*", ".*");
            return matchWholePhrase ? "^" + regex + "$" : regex;
        }

        public static T ParseEnum<T>(string value)
        {
            return (T)Enum.Parse(typeof(T), value, true);
        }

        /// <summary>
        /// Extract fields from a string using regex named groups
        /// </summary>
        /// <param name="extractionRegex">Regex used for extracting fields</param>
        /// <param name="rawRecord">string</param>
        /// <returns>A dictionary of fields and values</returns>
        public static IDictionary<string, string> ExtractFields(Regex extractionRegex, string rawRecord)
        {
            IDictionary<string, string> fields = new Dictionary<string, string>();
            Match extractionMatch = extractionRegex.Match(rawRecord);
            if (extractionMatch.Success)
            {
                GroupCollection groups = extractionMatch.Groups;
                string[] groupNames = extractionRegex.GetGroupNames();
                foreach (string groupName in extractionRegex.GetGroupNames())
                {
                    if (!int.TryParse(groupName, out int n))
                    {
                        fields[groupName] = groups[groupName].Value;
                    }
                }
            }

            return fields;
        }

        /// <summary>
        /// Extend the DateTime.ParseExact to support additional formats such as epoch
        /// </summary>
        /// <param name="value"></param>
        /// <param name="format"></param>
        /// <returns></returns>
        public static DateTime ParseDatetime(string value, string format)
        {
            if (ConfigConstants.EPOCH.Equals(format, StringComparison.CurrentCultureIgnoreCase))
            {
                return FromEpochTime(long.Parse(value));
            }
            else
            {
                return DateTime.ParseExact(value, format, CultureInfo.InvariantCulture);
            }
        }

        public static DateTime FromEpochTime(long epochTime)
        {
            return epoch.AddMilliseconds(epochTime);
        }

        /// <summary>
        /// Strip quotes from a string if it is quoted
        /// </summary>
        /// <param name="value">string to strip</param>
        /// <returns>stripped string</returns>
        public static string StripQuotes(string value)
        {
            if (string.IsNullOrWhiteSpace(value)) return value;
            
            if (value.StartsWith("'") || value.StartsWith("\""))
            {
                return value.Substring(1, value.Length - 2);
            }

            return value;
        }

        /// <summary>
        /// Path of the process main module
        /// </summary>
        public static string MainModulePath
        {
            get { return Process.GetCurrentProcess().MainModule.FileName; }
        }

        private static readonly DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    }

    internal static class StringBuilderExtensions
    {
        public static string Extract(this StringBuilder input, char character)
        {
            var startIndex = input.IndexOf(character);
            var lastIndex = input.LastIndexOf(character);

            var result = input.ToString(
                startIndex + 1,
                lastIndex - startIndex - 1);

            return result;
        }

        public static int LastIndexOf(this StringBuilder input, char character)
        {
            for (int i = input.Length - 1; i >= 0; i--)
            {
                if (input[i] == character)
                {
                    return i;
                }
            }

            return -1;
        }

        public static int IndexOf(this StringBuilder input, char character)
        {
            for (int i = 0; i < input.Length; i++)
            {
                if (input[i] == character)
                {
                    return i;
                }
            }

            return -1;
        }
    }

    public static class LinqExtensions
    {
        public static IEnumerable<IList<T>> Chunk<T>(this IEnumerable<T> source, int chunkSize)
        {
            return source
            .Select((x, i) => new { Index = i, Value = x })
            .GroupBy(x => x.Index / chunkSize)
            .Select(x => x.Select(v => v.Value).ToList());
        }
    }

    public static class DateTimeExtensions
    {
        public static DateTime Round(this DateTime date)
        {
            long ticks = (date.Ticks + (TimeSpan.TicksPerSecond / 2) + 1) / TimeSpan.TicksPerSecond;
            return new DateTime(ticks * TimeSpan.TicksPerSecond);
        }
    }
}
