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

namespace Amazon.KinesisTap.Filesystem.Test
{
    public class LineProcessorTestBase
    {
        public static IEnumerable<object[]> DetectableEncodings => new List<object[]>
        {
            new object[] { new UTF8Encoding(false) },
            new object[] { new UTF8Encoding(true) },
            new object[] { new UnicodeEncoding(true, true) },
            new object[] { new UnicodeEncoding(false, true) },
            new object[] { new UTF32Encoding(true, true) },
            new object[] { new UTF32Encoding(false, true) }
        };

        public static IEnumerable<object[]> AllEncodings => new List<object[]>
        {
            new object[] { new UTF8Encoding(true) },
            new object[] { new UTF8Encoding(false) },
            new object[] { new UnicodeEncoding(true, false) },
            new object[] { new UnicodeEncoding(true, true) },
            new object[] { new UnicodeEncoding(false, false) },
            new object[] { new UnicodeEncoding(false, true) },
            new object[] { new UTF32Encoding(true, false) },
            new object[] { new UTF32Encoding(true, true) },
            new object[] { new UTF32Encoding(false, false) },
            new object[] { new UTF32Encoding(false, true) }
        };

        public static IEnumerable<object[]> TestLines => File.ReadAllLines("Samples/utf8samples.txt")
            .Select(l => new object[] { l }).ToArray();

        protected static byte[] WriteToMemory(IEnumerable<string> texts, Encoding encoding) =>
            WriteToMemory(texts, encoding, Environment.NewLine);

        protected static byte[] WriteToMemory(IEnumerable<string> texts, Encoding encoding, string newlineSequence)
        {
            using (var memoryStream = new MemoryStream())
            using (var sw = new StreamWriter(memoryStream, encoding))
            {
                foreach (var text in texts)
                {
                    sw.Write(text);
                    sw.Write(newlineSequence);
                }
                sw.Flush();
                return memoryStream.ToArray();
            }
        }
    }
}
