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
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Amazon.KinesisTap.Common
{
    public static class Utility
    {
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

        public static readonly Regex VARIABLE_REGEX = new Regex("{[^}]+}");
        public static string ResolveVariables(string value, Func<string, string> evaluator)
        {
            return VARIABLE_REGEX.Replace(value, m => evaluator(m.Groups[0].Value));
        }

        public static string ResolveVariable(string variable)
        {
            if (string.IsNullOrEmpty(variable)
                || variable.Length < 3
                || variable[0] != '{'
                || variable[variable.Length - 1] != '}')
            {
                throw new ArgumentException("variable must be in the format of \"{variable}\" or \"{prefix:variable}\".");
            }

            (string prefix, string variableNoPrefix) = SplitPrefix(variable.Substring(1, variable.Length - 2), ':');
            if (!string.IsNullOrEmpty(prefix) && !"env".Equals(prefix, StringComparison.CurrentCultureIgnoreCase))
            {
                //I don't know the prefix. Return the original form to let others resolve
                return variable;
            }

            string value = Environment.GetEnvironmentVariable(variableNoPrefix);
            if (string.IsNullOrEmpty(value))
            {
                return variable;
            }
            else
            {
                return value;
            }
        }

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
        public static string GetProgramDataPathForKinesisTap()
        {
            return Path.Combine(Environment.GetEnvironmentVariable("ProgramData"), "Amazon\\KinesisTap");
        }

        public static string ProperCase(string constant)
        {
            return string.Join("", constant.Split(new char[] { '_' }, StringSplitOptions.RemoveEmptyEntries).Select(s => s[0] + s.Substring(1).ToLower()).ToArray());
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
            .GroupBy(x => x.Index / 3)
            .Select(x => x.Select(v => v.Value).ToList());
        }
    }
}
