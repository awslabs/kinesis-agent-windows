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
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace Amazon.KinesisTap.Core
{
    public static class Utility
    {
        // Cache the OS platform information
        public static readonly bool IsWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
        public static readonly bool IsMacOs = RuntimeInformation.IsOSPlatform(OSPlatform.OSX);
        public static readonly bool IsLinux = RuntimeInformation.IsOSPlatform(OSPlatform.Linux);
        public static readonly string Platform = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Windows" :
            RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ? "Linux" :
            RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? "macOS" : "Unknown";
        public const string DefaultExtraConfigDirectoryName = "configs";

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

        public static string AgentId { get; set; }

        public static string UserId { get; set; }

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
                        if (IsWindows)
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
                        _hostName = IsWindows ? Dns.GetHostEntryAsync("LocalHost").Result.HostName : Dns.GetHostName();
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

        /// <summary>
        /// Returns the ProgramData path, used to store bookmarks, logs, and update packages.
        /// </summary>
        public static string GetKinesisTapProgramDataPath()
        {
            string kinesisTapProgramDataPath = Environment.GetEnvironmentVariable(ConfigConstants.KINESISTAP_PROGRAM_DATA);
            if (string.IsNullOrWhiteSpace(kinesisTapProgramDataPath))
            {
                if (IsWindows)
                {
                    kinesisTapProgramDataPath = Path.Combine(Environment.GetEnvironmentVariable("ProgramData"), "Amazon\\KinesisTap");
                }
                else
                {
                    kinesisTapProgramDataPath = ConfigConstants.LINUX_DEFAULT_PROGRAM_DATA_PATH;
                }
            }
            return kinesisTapProgramDataPath;
        }

        /// <summary>
        /// Returns the path to the directory that stores the appsettings.json configuration file.
        /// </summary>
        public static string GetKinesisTapConfigPath()
        {
            string kinesisTapConfigPath = Environment.GetEnvironmentVariable(ConfigConstants.KINESISTAP_CONFIG_PATH);
            if (string.IsNullOrWhiteSpace(kinesisTapConfigPath))
            {
                if (IsWindows)
                {
                    // For windows, use the installation path
                    kinesisTapConfigPath = AppContext.BaseDirectory;
                }
                else
                {
                    kinesisTapConfigPath = ConfigConstants.LINUX_DEFAULT_CONFIG_PATH;
                }
            }
            return kinesisTapConfigPath;
        }

        /// <summary>
        /// Resolve the directory that contains the extra configuration files.
        /// </summary>
        public static string GetKinesisTapExtraConfigPath()
        {
            string kinesisTapChildConfigPath = Environment.GetEnvironmentVariable(ConfigConstants.KINESISTAP_EXTRA_CONFIG_DIR_PATH);
            if (!string.IsNullOrWhiteSpace(kinesisTapChildConfigPath))
            {
                return kinesisTapChildConfigPath;
            }

            return Path.Combine(GetKinesisTapConfigPath(), DefaultExtraConfigDirectoryName);
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
        /// Parse the time zone kind from the configuration value.
        /// </summary>
        /// <param name="config"></param>
        /// <returns>UTC (default) or Local DateTimeKind</returns>
        public static DateTimeKind ParseTimeZoneKind(string config)
        {
            string timeZoneKindConfig = ProperCase(config);
            DateTimeKind timeZoneKind = DateTimeKind.Utc;
            if (!string.IsNullOrWhiteSpace(timeZoneKindConfig))
            {
                timeZoneKind = (DateTimeKind)Enum.Parse(typeof(DateTimeKind), timeZoneKindConfig);
            }
            return timeZoneKind;
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

        public static long ToEpochSeconds(DateTime utcTime)
        {
            return Convert.ToInt64((utcTime - epoch).TotalSeconds);
        }

        public static long ToEpochMilliseconds(DateTime utcTime)
        {
            return Convert.ToInt64((utcTime - epoch).TotalMilliseconds);
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

        /// <summary>
        /// Exchange the value of a <see cref="Int64"/> field a comparand number is greater than that field.
        /// This method is thread-safe.
        /// </summary>
        /// <param name="location">Reference to the field to exchange.</param>
        /// <param name="value">Value to exchange.</param>
        /// <param name="comparand">Comparand number</param>
        /// <returns>The value in <paramref name="location"/> before exchanging</returns>
        public static long InterlockedExchangeIfGreaterThan(ref long location, long value, long comparand)
        {
            long original;
            do
            {
                // first we store the original value. Note that by the time this assignment completes, the value at location might already change
                original = Interlocked.Read(ref location);
                if (comparand <= original)
                {
                    // if the condition is not satisfied, we return the original
                    return original;
                }
                // if the condition is met, we exchange the value if and only if the location value hasn't changed
                // if the location value has changed, we simply retry, hence the while loop
            }
            while (Interlocked.CompareExchange(ref location, value, original) != original);
            return original;
        }

        /// <summary>
        /// A helper method that will return the value of the defaultValue parameter if the value is null, empty, or whitespace.
        /// </summary>
        /// <param name="value">The string value that is expected to be not null or whitespace.</param>
        /// <param name="defaultValue">The default value to return if the value parameter is null or whitespace.</param>
        public static string ValueOrDefault(string value, string defaultValue)
        {
            if (!string.IsNullOrWhiteSpace(value)) return value;
            return defaultValue;
        }

        /// <summary>
        /// A helper method that will throw an exception if a config property is null or whitespace.
        /// </summary>
        /// <param name="config">The <see cref="IConfiguration"/> to retrieve the property from.</param>
        /// <param name="propertyName">The name of the property to retrieve.</param>
        public static string GetRequiredConfigValue(IConfiguration config, string propertyName)
        {
            if (!string.IsNullOrWhiteSpace(config[propertyName])) return config[propertyName];
            throw new ArgumentException($"Configuration property '{propertyName}' cannot be null or whitespace");
        }
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
