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
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

using Microsoft.Extensions.Caching.Memory;

using AsyncFriendlyStackTrace;

namespace Amazon.KinesisTap.Core
{
    public static class StackTraceMinimizerExceptionExtensions
    {
        private static IMemoryCache memoryCache = new MemoryCache(new MemoryCacheOptions());

        //Try to minimize the stacktrace with 2 strategies
        //1. Minimize async stacktrace
        //2. Suppress stacktrace seen before for the configured period
        public static string ToMinimized(this Exception ex)
        {
            string stacktrace = NeedToMinimizeStackTrace() ? ex.ToAsyncString() : ex.ToString();
            return DoCompressStackTrace ? CompressStackTrace(stacktrace) : stacktrace;
        }

        /// <summary>
        /// Allow turning on/off the stack trace compression.
        /// </summary>
        public static bool DoCompressStackTrace { get; set; }

        /// <summary>
        /// Allow users to set how long we will filter a stack trace
        /// </summary>
        public static TimeSpan StackTraceCompressionKeyExpiration { get; set; } = TimeSpan.FromMinutes(60);

        /// <summary>
        /// Compress the stack trace. Show full stacktrace once every StackTraceCompressionKeyExpiration interval. Otherwise, write a hash in lieu of stack trace to that user can look up stacktrace in the log.
        /// </summary>
        /// <param name="stackTrace">Stack trace to be compressed</param>
        /// <returns></returns>
        public static string CompressStackTrace(string stackTrace)
        {
            var trimmedTrace = stackTrace.Trim();
            int indexOfLineFeed = trimmedTrace.IndexOf('\n');
            if (indexOfLineFeed < 0) return trimmedTrace;

            //Windows linefeed is \r\n while Linux is just \n
            string firstline = indexOfLineFeed > 0 && trimmedTrace[indexOfLineFeed - 1] == '\r' ? trimmedTrace.Substring(0, indexOfLineFeed - 1) 
                : trimmedTrace.Substring(0, indexOfLineFeed);
            string remaining = trimmedTrace.Substring(indexOfLineFeed + 1);

            if (string.IsNullOrWhiteSpace(remaining)) return trimmedTrace;

            StringBuilder stackTraceBuilder = new StringBuilder()
                .AppendLine(firstline);

            (int Length, int CheckSum, int HashCode) key = (remaining.Length, remaining.CheckSum(), remaining.GetHashCode());
            if (IsInCache(key))
            {
                return stackTraceBuilder.Append($"@stacktrace_ref {key.Length} {key.CheckSum} {key.HashCode}").ToString();
            }
            else
            {
                return stackTraceBuilder.AppendLine($"@stacktrace_id {key.Length} {key.CheckSum} {key.HashCode}")
                    .Append(remaining)
                    .ToString();
            }
        }

        private static bool IsInCache((int HashCode, int Length, int CheckSum) key)
        {
            bool inCache;

            lock (memoryCache)
            {
                if (memoryCache.TryGetValue(key, out object value))
                {
                    inCache = true;
                }
                else
                {
                    inCache = false;
                    memoryCache.Set(key, new object(), StackTraceCompressionKeyExpiration);
                }
            }
            return inCache;
        }

        private static bool NeedToMinimizeStackTrace()
        {
            //.net core 2.1 is already sanitized. Assume our .net core host will always be 2.1 or later.
            //We could test version string in the framework description. However, Microsoft version string is currently out of sync so this could be a moving target
            //See: https://github.com/dotnet/corefx/issues/9725
            return !RuntimeInformation.FrameworkDescription.StartsWith(".NET Core");
        }

        private static int CheckSum(this string input)
        {
            return input.Sum(c => (int)c);
        }
    }
}
