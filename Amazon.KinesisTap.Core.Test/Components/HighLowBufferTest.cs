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
            HiLowBuffer<int> buffer = new HiLowBuffer<int>(1, l =>
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
            //shoud block
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
            HiLowBuffer<int> buffer = new HiLowBuffer<int>(1, l =>
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
            //shoud block
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
