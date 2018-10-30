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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Amazon.KinesisTap.Core.Test.Components
{
    public class BatchTest
    {
        [Fact]
        public void TestBatch()
        {
            List<long> list = new List<long>();
            FlushReason lastReason = FlushReason.AfterAdd;
            Batch<long> batch = new Batch<long>(TimeSpan.FromSeconds(0.2), 1000L, l => l,
                (lst, counts, reason) =>
                {
                    list.AddRange(lst);
                    lastReason = reason;
                });

            list.Clear();
            batch.Add(500);
            Thread.Sleep(1000);
            //Should flush
            Assert.Single(list);
            Assert.Equal(FlushReason.Timer, lastReason);

            list.Clear();
            batch.Add(500);
            batch.Add(700);
            //Should flush
            Assert.Single(list);
            Assert.Equal(FlushReason.BeforeAdd, lastReason);

            list.Clear();
            batch.Add(300);
            //Should flush
            Assert.Equal(2, list.Count);
            Assert.Equal(FlushReason.AfterAdd, lastReason);

            list.Clear();
            batch.Add(500);
            //Should not flush
            Assert.Empty(list);
            batch.Stop();
            //Should flush now
            Assert.Single(list);
            Assert.Equal(FlushReason.Stop, lastReason);
        }
    }
}
