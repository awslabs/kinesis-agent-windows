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
using System.Collections.Generic;

namespace Amazon.KinesisTap.Filesystem.Test
{
    static class LineReaderTestData
    {
        private static readonly char[] _charData;
        private static readonly char[] _smallData;
        private static readonly char[] _largeData;

        public static object FirstObject { get; } = 1;
        public static object SecondObject { get; } = "[second object]";
        public static object ThirdObject { get; } = "<third object>";
        public static object[] MultipleObjects { get; } = new object[] { FirstObject, SecondObject, ThirdObject };

        public static string FormatStringOneObject { get; } = "Object is {0}";
        public static string FormatStringTwoObjects { get; } = $"Object are '{0}', {SecondObject}";
        public static string FormatStringThreeObjects { get; } = $"Objects are {0}, {SecondObject}, {ThirdObject}";
        public static string FormatStringMultipleObjects { get; } = "Multiple Objects are: {0}, {1}, {2}";

        static LineReaderTestData()
        {
            _charData = new char[]
            {
                char.MinValue,
                char.MaxValue,
                '\t',
                ' ',
                '$',
                '@',
                '#',
                '\0',
                '\v',
                '\'',
                '\u3190',
                '\uC3A0',
                'A',
                '5',
                '\r',
                '\uFE70',
                '-',
                ';',
                '\r',
                '\n',
                'T',
                '3',
                '\n',
                'K',
                '\u00E6'
            };

            _smallData = "HELLO".ToCharArray();

            var data = new List<char>();
            for (var count = 0; count < 1000; ++count)
            {
                data.AddRange(_smallData);
            }
            _largeData = data.ToArray();
        }

        public static char[] CharData => _charData;

        public static char[] SmallData => _smallData;

        public static char[] LargeData => _largeData;
    }
}
