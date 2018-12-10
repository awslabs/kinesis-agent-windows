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
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace Amazon.KinesisTap.Expression
{
    /// <summary>
    /// This class provides built-in functions in the expression evaluator
    /// </summary>
    public static class BuiltInFunctions
    {
        #region string functions
        public static int length(string input) => input?.Length ?? 0;

        public static string lower(string input) => input?.ToLower();

        public static string lpad(string input, int size, string padstring) => input?.PadLeft(size, padstring[0]);

        public static string ltrim(string input) => input?.TrimStart();

        public static string rpad(string input, int size, string padstring) => input?.PadRight(size, padstring[0]);

        public static string rtrim(string input) => input?.TrimEnd();

        //Start starts from 1
        public static string substr(string input, int start) => input?.Substring(start - 1);

        //Start starts from 1
        public static string substr(string input, int start, int length) => input?.Substring(start - 1, length);

        public static string trim(string input) => input?.Trim();

        public static string upper(string str) => str?.ToUpper();
        #endregion

        #region regular expression functions
        public static string regexp_extract(string input, string pattern)
        {
            if (string.IsNullOrEmpty(input)) return input;

            Match match = Regex.Match(input, pattern);
            return match.Success ? match.Value : string.Empty;
        }

        public static string regexp_extract(string input, string pattern, int group)
        {
            if (string.IsNullOrEmpty(input)) return input;

            Match match = Regex.Match(input, pattern);
            return match.Success ? match.Groups[group].Value : string.Empty;
        }
        #endregion

        #region date functions
        public static DateTime date(int year, int month, int day) => new DateTime(year, month, day);

        public static DateTime date(int year, int month, int day, int hour, int minute, int second) 
            => new DateTime(year, month, day, hour, minute, second);

        public static DateTime date(int year, int month, int day, int hour, int minute, int second, int millisecond)
            => new DateTime(year, month, day, hour, minute, second, millisecond);
        #endregion

        #region conversion functions
        public static int? parse_int(string input)
        {
            if (int.TryParse(input, out int result)) return result;

            return null;
        }

        public static decimal? parse_decimal(string input)
        {
            if (decimal.TryParse(input, out decimal result)) return result;

            return null;
        }

        public static DateTime? parse_date(string input, string format)
        {
            if (DateTime.TryParseExact(input, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime result)) return result;

            return null;
        }

        public static string format(object o, string format)
        {
            return string.Format($"{{0:{format}}}", o);
        }
        #endregion

        #region coalesce functions
        public static object coalesce(object obj1, object obj2) => obj1 == null ? obj2 : obj1;

        public static object coalesce(object obj1, object obj2, object obj3) => obj1 == null ? coalesce(obj2, obj3) : obj1;

        public static object coalesce(object obj1, object obj2, object obj3, object obj4) => obj1 == null ? coalesce(obj2, obj3, obj4) : obj1;

        public static object coalesce(object obj1, object obj2, object obj3, object obj4, object obj5) 
            => obj1 == null ? coalesce(obj2, obj3, obj4, obj5) : obj1;

        public static object coalesce(object obj1, object obj2, object obj3, object obj4, object obj5, object obj6)
            => obj1 == null ? coalesce(obj2, obj3, obj4, obj5, obj6) : obj1;
        #endregion
    }
}
