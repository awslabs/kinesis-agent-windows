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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace Amazon.KinesisTap.Core.Test
{
    [Collection(nameof(AsyncBatchQueueTest_Secondary))]
    public class AsyncBatchQueueTest_Secondary
    {
        private readonly ITestOutputHelper _output;

        public AsyncBatchQueueTest_Secondary(ITestOutputHelper output)
        {
            _output = output;
        }

        private class IntegerListSerializer : ISerializer<List<int>>
        {
            public List<int> Deserialize(Stream stream)
            {
                var results = new List<int>();
                using var reader = new StreamReader(stream);
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    results.Add(int.Parse(line));
                }
                return results;
            }

            public void Serialize(Stream stream, List<int> data)
            {
                using var writer = new StreamWriter(stream);
                foreach (var i in data)
                {
                    writer.WriteLine(i);
                }
            }
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(100)]
        public async Task InMemoryQueue_PushAndGet(int batchCount)
        {
            var secondary = new InMemoryQueue<List<string>>(10);

            var q = new AsyncBatchQueue<string>(1000,
                new long[] { batchCount },
                new Func<string, long>[] { s => 1 }, secondary);

            await q.PushSecondaryAsync(Enumerable.Repeat("a", batchCount + 1).ToList());

            var output = new List<string>();
            var getTask = q.GetNextBatchAsync(output, 10 * 1000).AsTask();
            Assert.True(getTask.Wait(1000));
            Assert.Equal(batchCount, output.Count);

            if (batchCount == 0)
            {
                return;
            }

            // second call should return the last item
            output.Clear();
            await q.GetNextBatchAsync(output, 100);
            Assert.Single(output);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(100)]
        public async Task PersistentQueue_PushAndGet(int batchCount)
        {
            var dataDir = Path.Combine(AppContext.BaseDirectory, Guid.NewGuid().ToString());
            Directory.CreateDirectory(dataDir);

            try
            {
                var secondary = new FilePersistentQueue<List<int>>(100, dataDir, new IntegerListSerializer(), NullLogger.Instance);

                var q = new AsyncBatchQueue<int>(1000,
                    new long[] { batchCount },
                    new Func<int, long>[] { s => 1 }, secondary);

                await q.PushSecondaryAsync(Enumerable.Range(0, batchCount + 1).ToList());

                var output = new List<int>();
                var getTask = q.GetNextBatchAsync(output, 10 * 1000).AsTask();
                Assert.True(getTask.Wait(1000));
                Assert.Equal(batchCount, output.Count);

                if (batchCount == 0)
                {
                    return;
                }

                // second call should return the last item
                output.Clear();
                await q.GetNextBatchAsync(output, 100);
                Assert.Single(output);
            }
            finally
            {
                if (Directory.Exists(dataDir))
                {
                    Directory.Delete(dataDir, true);
                }
            }
        }

        [Fact]
        public async Task InMemoryQueue_PullBothQueues()
        {
            var secondary = new InMemoryQueue<List<int>>(10);

            var q = new AsyncBatchQueue<int>(500,
                new long[] { 500 },
                new Func<int, long>[] { s => 1 }, secondary);

            await q.PushSecondaryAsync(Enumerable.Range(0, 500).ToList());
            for (var i = 500; i < 1000; i++)
            {
                await q.PushAsync(i);
            }

            // pull 3 times
            var output = new List<int>();
            await q.GetNextBatchAsync(output, 1000);
            await q.GetNextBatchAsync(output, 1000);
            await q.GetNextBatchAsync(output, 1000);

            Assert.Equal(1000, output.Distinct().Count());
        }

        [Fact]
        public async Task PersistentQueue_PullBothQueues()
        {
            var dataDir = Path.Combine(AppContext.BaseDirectory, Guid.NewGuid().ToString());
            Directory.CreateDirectory(dataDir);

            try
            {
                var secondary = new FilePersistentQueue<List<int>>(10, dataDir, new IntegerListSerializer(), NullLogger.Instance);
                var q = new AsyncBatchQueue<int>(500,
                    new long[] { 500 },
                    new Func<int, long>[] { s => 1 }, secondary);

                await q.PushSecondaryAsync(Enumerable.Range(0, 500).ToList());
                for (var i = 500; i < 1000; i++)
                {
                    await q.PushAsync(i);
                }

                // pull 3 times
                var output = new List<int>();
                await q.GetNextBatchAsync(output, 1000);
                await q.GetNextBatchAsync(output, 1000);
                await q.GetNextBatchAsync(output, 1000);

                Assert.Equal(1000, output.Distinct().Count());
            }
            finally
            {
                if (Directory.Exists(dataDir))
                {
                    Directory.Delete(dataDir, true);
                }
            }
        }

        [Theory]
        [InlineData(10, 1000)]
        [InlineData(20, 15)]
        public async Task InMemoryQueue_ConcurrentRead(int readerCount, int itemCount)
        {
            var secondary = new InMemoryQueue<List<int>>(1000);
            using var cts = new CancellationTokenSource();
            using var semaphore = new SemaphoreSlim(0, readerCount);
            var results = new List<int>();
            var q = new AsyncBatchQueue<int>(10000,
                new long[] { 100 },
                new Func<int, long>[] { s => 1 }, secondary);

            async Task readerTask()
            {
                var output = new List<int>();

                await semaphore.WaitAsync();
                // we're trying to test that the readers will 'eventually' read all the items, so we do several pulls here
                await q.GetNextBatchAsync(output, 500);
                await q.GetNextBatchAsync(output, 500);
                await q.GetNextBatchAsync(output, 500);

                await Task.Delay(100);
                lock (results)
                {
                    results.AddRange(output);
                }
            };

            var readers = new Task[readerCount];
            for (var i = 0; i < readerCount; i++)
            {
                readers[i] = readerTask();
            }

            for (var i = 0; i < itemCount; i++)
            {
                if (i % 2 == 0)
                {
                    await q.PushAsync(i);
                }
                else
                {
                    await q.PushSecondaryAsync(new List<int> { i });
                }
            }
            semaphore.Release(readerCount);
            await Task.WhenAll(readers);

            _output.WriteLine(results.Count.ToString());
            _output.WriteLine(q.EstimateSize().ToString());
            _output.WriteLine(q.EstimateSecondaryQueueSize().ToString());
            Assert.Equal(itemCount, results.Distinct().Count());
        }

        [Theory]
        [InlineData(10, 1000)]
        [InlineData(100, 70)]
        public async Task PersistentQueue_ConcurrentRead(int readerCount, int itemCount)
        {
            var dataDir = Path.Combine(AppContext.BaseDirectory, Guid.NewGuid().ToString());
            Directory.CreateDirectory(dataDir);

            try
            {
                var secondary = new FilePersistentQueue<List<int>>(100000, dataDir, new IntegerListSerializer(), NullLogger.Instance);
                using var cts = new CancellationTokenSource();
                using var semaphore = new SemaphoreSlim(0, readerCount);
                var results = new List<int>();
                var q = new AsyncBatchQueue<int>(10000,
                    new long[] { 100 },
                    new Func<int, long>[] { s => 1 }, secondary);

                async Task readerTask()
                {
                    var output = new List<int>();

                    await semaphore.WaitAsync();
                    // we're trying to test that the readers will 'eventually' read all the items, so we do several pulls here
                    await q.GetNextBatchAsync(output, 500);
                    await q.GetNextBatchAsync(output, 500);
                    await q.GetNextBatchAsync(output, 500);

                    await Task.Delay(100);
                    lock (results)
                    {
                        results.AddRange(output);
                    }
                };

                var readers = new Task[readerCount];
                for (var i = 0; i < readerCount; i++)
                {
                    readers[i] = readerTask();
                }

                for (var i = 0; i < itemCount; i++)
                {
                    if (i % 2 == 0)
                    {
                        await q.PushAsync(i);
                    }
                    else
                    {
                        await q.PushSecondaryAsync(new List<int> { i });
                    }
                }

                semaphore.Release(readerCount);
                await Task.WhenAll(readers);

                _output.WriteLine(results.Count.ToString());
                _output.WriteLine(q.EstimateSize().ToString());
                _output.WriteLine(q.EstimateSecondaryQueueSize().ToString());
                Assert.Equal(itemCount, results.Distinct().Count());
            }
            finally
            {
                if (Directory.Exists(dataDir))
                {
                    Directory.Delete(dataDir, true);
                }
            }
        }
    }
}
