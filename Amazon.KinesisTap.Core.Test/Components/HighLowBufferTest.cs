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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Amazon.KinesisTap.Core.Test.Components
{
    public class HighLowBufferTest
    {
        [Fact]
        [Trait("Category", "Integration")]
        public void TestHighLowBuffer()
        {
            List<int> output = new List<int>();
            ManualResetEvent sinkWaitHandle = new ManualResetEvent(false);
            HiLowBuffer<int> buffer = new HiLowBuffer<int>(1, null, l =>
            {
                sinkWaitHandle.WaitOne();
                output.Add(l);
            }, new InMemoryQueue<int>(100));
            Task.Run(() =>
            {
                for (int i = 0; i < 2; i++)
                {
                    buffer.Add(i);
                }
            });
            //should block
            Thread.Sleep(500);
            buffer.Requeue(-1, false);
            sinkWaitHandle.Set();
            Thread.Sleep(500);
            Assert.Equal(3, output.Count);
            Assert.Equal(-1, output[2]); //Requeue item come out last
        }

        [Fact]
        [Trait("Category", "Integration")]
        public void TestHighLowBufferWithPersistentQueue()
        {
            List<int> output = new List<int>();
            ManualResetEvent sinkWaitHandle = new ManualResetEvent(false);
            string directory = Path.Combine(FilePersistenceQueueTest.QueueDirectory, "HighLowTest");
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, true);
            }
            BinarySerializer<int> integerSerializer = new BinarySerializer<int>(
                (w, i) => w.Write(i),
                (r) => r.ReadInt32());
            FilePersistentQueue<int> queue = new FilePersistentQueue<int>(
                100,
                directory,
                integerSerializer);
            HiLowBuffer<int> buffer = new HiLowBuffer<int>(1, null, l =>
            {
                sinkWaitHandle.WaitOne();
                output.Add(l);
            }, queue);
            Task.Run(() =>
            {
                for (int i = 0; i < 2; i++)
                {
                    buffer.Add(i);
                }
            });
            //should block
            Thread.Sleep(500);
            buffer.Requeue(-1, false);
            sinkWaitHandle.Set();
            Thread.Sleep(500);
            Assert.Equal(3, output.Count);
            Assert.Equal(-1, output[2]); //Requeue item come out last
            Assert.True(File.Exists(Path.Combine(directory, "index")));
        }
    }
}
