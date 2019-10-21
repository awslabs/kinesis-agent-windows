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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Amazon.KinesisTap.Core.Test.Components
{
    public class BufferTest
    {
        [Fact]
        [Trait("Category", "Integration")]
        public void TestBuffer()
        {
            ManualResetEvent sinkWaitHandle = new ManualResetEvent(false);
            Buffer<int> buffer = new Buffer<int>(10, null, l =>
            {
                sinkWaitHandle.WaitOne();
            });
            for (int i = 0; i < 5; i++)
            {
                TestBufferInternal(i, sinkWaitHandle, buffer);
                sinkWaitHandle.Reset();
            }
        }

        private static void TestBufferInternal(int interation, ManualResetEvent sinkWaitHandle, Buffer<int> buffer)
        {
            Task.Run(() =>
            {
                for (int i = 0; i < 20; i++)
                {
                    buffer.Add(i);
                }
            });
            Thread.Sleep(500);
            //Should block
            int count = buffer.Count;
            Assert.InRange(count, 10, 11);
            buffer.Requeue(-1, true);
            Assert.Equal(count + 1, buffer.Count);
            sinkWaitHandle.Set();
            Thread.Sleep(500);
            Assert.Equal(0, buffer.Count);
        }
    }
}
