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
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    [Collection(nameof(FilePersistenceQueueTest))]
    public class FilePersistenceQueueTest
    {
        private readonly string _appDataDir = Path.Combine(TestUtility.GetTestHome(), Guid.NewGuid().ToString());
        private readonly string _queueDirectory = "queue";
        private readonly IAppDataFileProvider _dataFileProvider;

        public FilePersistenceQueueTest()
        {
            _dataFileProvider = new ProtectedAppDataFileProvider(_appDataDir);
        }

        [Fact]
        public void TestFilePersistentQueue()
        {
            var serializer = new BinarySerializer<MockClass>(BinarySerializerTest.MockSerializer, BinarySerializerTest.MockDeserializer);

            var queue = new FilePersistentQueue<MockClass>(1000, _queueDirectory, serializer, _dataFileProvider, NullLogger.Instance);
            var list = BinarySerializerTest.CreateList().Where(i => queue.TryEnqueue(i)).ToList();

            Assert.Equal(list.Count, queue.Count);
            Assert.Equal(list.Count, Directory.GetFiles(Path.Combine(_appDataDir, _queueDirectory), "0*").Count());

            var list2 = new List<MockClass>();
            while (queue.TryDequeue(out var item))
            {
                list2.Add(item);
            }

            Assert.True(list.SequenceEqual(list2, new MockClassComparer()));
            Assert.Equal(0, queue.Count);
            Assert.Empty(Directory.GetFiles(Path.Combine(_appDataDir, _queueDirectory), "0*"));
        }

        [Fact]
        public void TestEmptyQueue()
        {
            var queue = CreateQueue();
            Assert.False(queue.TryDequeue(out _));
        }

        [Fact]
        public void TestQueueCapacity()
        {
            var queue = CreateQueue();
            for (int i = 0; i < 10; i++)
            {
                Assert.True(queue.TryEnqueue(i));
            }

            Assert.False(queue.TryEnqueue(10));
        }

        /// <summary>
        /// A test that ensures a bad index file is detected during load.
        /// </summary>
        /// <param name="contents">The contents of the index file to test against</param>
        /// <param name="isValid">Whether or not the index file contents are valid</param>
        [Theory]
        [InlineData("101 202", true)]
        [InlineData("", false)]
        [InlineData("101 102 202", false)]
        [InlineData("hey you", false)]
        public void TestLoadIndex(string contents, bool isValid)
        {
            var serializer = new BinarySerializer<MockClass>(BinarySerializerTest.MockSerializer, BinarySerializerTest.MockDeserializer);

            var logger = new MemoryLogger("TestLoadIndex");
            var queue = new FilePersistentQueue<MockClass>(1000, _queueDirectory, serializer, _dataFileProvider, logger);
            LoadQueue(queue);

            var index = Path.Combine(_appDataDir, queue.QueueDirectory, "Index");
            File.WriteAllText(index, contents);
            Assert.Equal(isValid, queue.LoadIndex());
            if (isValid)
            {
                Assert.Equal(101, queue.Head);
                Assert.Equal(202, queue.Tail);
                Assert.DoesNotContain(logger.Entries, i => i.Contains("ERROR", StringComparison.CurrentCultureIgnoreCase));
            }
            else
            {
                Assert.Equal(0, queue.Head);
                Assert.Equal(10, queue.Tail);
                Assert.Contains(logger.Entries, i => i.Contains("Error", StringComparison.CurrentCultureIgnoreCase));
            }
        }

        /// <summary>
        /// A Test that ensures that the DiscoverIndex feature accurately rebuilds an index.
        /// </summary>
        [Fact]
        public void TestDiscoverIndex()
        {
            var serializer = new BinarySerializer<MockClass>(BinarySerializerTest.MockSerializer, BinarySerializerTest.MockDeserializer);

            var logger = new MemoryLogger("TestDiscoverIndex");
            var queue = new FilePersistentQueue<MockClass>(1000, _queueDirectory, serializer, _dataFileProvider, logger);
            LoadQueue(queue);

            // Increment the tail to 20 to simulate the first records being bad ones.
            // This is testing the function where only the last item in a consecutive sequence from the tail are processed.
            queue.Tail = 20;
            LoadQueue(queue);

            var indexFile = Path.Combine(_appDataDir, queue.QueueDirectory, "Index");
            File.Delete(indexFile);
            queue.Head = -10;
            queue.Tail = -10;
            queue.DiscoverIndex();
            Assert.Equal(30, queue.Tail);
            Assert.Equal(20, queue.Head);

            Assert.True(queue.TryDequeue(out _));
            Assert.Equal(21, queue.Head);
        }

        private static void LoadQueue(FilePersistentQueue<MockClass> queue)
        {
            var random = Utility.Random;
            for (int i = 0; i < 10; i++)
            {
                queue.TryEnqueue(new MockClass
                {
                    AnInt = random.Next(),
                    ALong = random.Next(),
                    ADateTime = DateTime.Now,
                    AString = TestUtility.RandomString(1000),
                    AMemortySteam = Utility.StringToStream(TestUtility.RandomString(1000))
                });
            }
        }

        private FilePersistentQueue<int> CreateQueue()
        {
            var serializer = new BinarySerializer<int>(
                (writer, i) => writer.Write(i),
                (reader) => reader.ReadInt32()
            );

            return new FilePersistentQueue<int>(10, _queueDirectory, serializer, _dataFileProvider, NullLogger.Instance);
        }
    }
}
