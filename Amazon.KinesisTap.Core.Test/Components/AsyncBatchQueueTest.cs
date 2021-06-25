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
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Amazon.KinesisTap.Core.Test
{
    [Collection(nameof(AsyncBatchQueueTest))]
    public class AsyncBatchQueueTest
    {
        private readonly ITestOutputHelper _output;

        public AsyncBatchQueueTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(100)]
        public async Task GetBatchWhenLimitIsReached(int batchCount)
        {
            var q = new AsyncBatchQueue<string>(10000,
                new long[] { batchCount },
                new Func<string, long>[] { s => 1 });

            var output = new List<string>();
            for (var i = 0; i < batchCount + 1; i++)
            {
                await q.PushAsync("a");
            }

            var getTask = q.GetNextBatchAsync(output, 10 * 1000).AsTask();

            // first call should returns a full batch 
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

        [Fact]
        public async Task LimitIsReached_OrderIsPresevered()
        {
            var q = new AsyncBatchQueue<int>(10000,
                new long[] { 100 },
                new Func<int, long>[] { s => 1 });

            for (var i = 0; i < 150 + 1; i++)
            {
                await q.PushAsync(i);
            }

            var output = new List<int>();
            await q.GetNextBatchAsync(output, 1000);

            // pull again
            await q.GetNextBatchAsync(output, 100);

            for (var i = 0; i < 150; i++)
            {
                Assert.Equal(i, output[i]);
            }
        }

        [Theory]
        [InlineData(100, 10)]
        [InlineData(1000, 20)]
        public async Task GetBatchWhenTimerExpires(int batchCount, int remaining)
        {
            var q = new AsyncBatchQueue<string>(10000,
                new long[] { batchCount },
                new Func<string, long>[] { s => 1 });

            var output = new List<string>();

            for (var i = 0; i < batchCount - remaining; i++)
            {
                await q.PushAsync("a");
            }

            var getTask = q.GetNextBatchAsync(output, 500).AsTask();

            Assert.True(getTask.Wait(5000));
            Assert.Equal(batchCount - remaining, output.Count);
        }

        [Fact]
        public async Task GetBatch_Cancellation()
        {
            using var cts = new CancellationTokenSource();
            var output = new List<string>();
            var q = new AsyncBatchQueue<string>(10000,
                new long[] { 100 },
                new Func<string, long>[] { s => 1 });

            for (var i = 0; i < 99; i++)
            {
                await q.PushAsync("a");
            }

            var getTask = q.GetNextBatchAsync(output, int.MaxValue, cts.Token).AsTask();
            cts.Cancel();

            await Task.Delay(200);
            Assert.True(getTask.IsCompleted);
        }

        [Fact]
        public async Task PushToFullQueue_Cancellation()
        {
            using var cts = new CancellationTokenSource();
            var output = new List<string>();
            var q = new AsyncBatchQueue<string>(1000,
                new long[] { 100 },
                new Func<string, long>[] { s => 1 });

            for (var i = 0; i < 1000; i++)
            {
                await q.PushAsync("a");
            }

            var pushTask = q.PushAsync("b", cts.Token).AsTask();
            cts.Cancel();

            await Task.Delay(100);
            Assert.True(pushTask.IsCompleted);
        }

        [Theory]
        [InlineData(10, 1000)]
        [InlineData(100, 70)]
        public async Task ConcurrentRead(int readerCount, int itemCount)
        {
            using var semaphore = new SemaphoreSlim(0, readerCount);
            using var cts = new CancellationTokenSource();
            var results = new List<int>();
            var q = new AsyncBatchQueue<int>(10000,
                new long[] { 100 },
                new Func<int, long>[] { s => 1 });

            for (var i = 0; i < itemCount; i++)
            {
                await q.PushAsync(i);
            }

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

            semaphore.Release(readerCount);
            await Task.WhenAll(readers);

            _output.WriteLine(results.Count.ToString());
            _output.WriteLine(q.EstimateSize().ToString());
            _output.WriteLine(q.EstimateSecondaryQueueSize().ToString());
            Assert.Equal(itemCount, results.Distinct().Count());
        }
    }
}
