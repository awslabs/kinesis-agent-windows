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
