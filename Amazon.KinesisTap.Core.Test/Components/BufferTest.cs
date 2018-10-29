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
            Buffer<int> buffer = new Buffer<int>(10, l =>
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
