using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    public class FilePersistenceQueueTest
    {
        public static readonly string QueueDirectory = Path.Combine(TestUtility.GetTestHome(), "queue");

        [Fact]
        public void TestFilePersistentQueue()
        {
            BinarySerializer<MockClass> serializer = new BinarySerializer<MockClass>(
                BinarySerializerTest.MockSerializer,
                BinarySerializerTest.MockDeserializer
            );
            string directory = Path.Combine(QueueDirectory, "TestFilePersistentQueue");
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, true);
            }
            FilePersistentQueue<MockClass> queue = new FilePersistentQueue<MockClass>(
                1000,
                directory,
                serializer
            );

            List<MockClass> list = BinarySerializerTest.CreateList();
            foreach(var item in list)
            {
                queue.Enqueue(item);
            }

            Assert.Equal(list.Count, queue.Count);
            Assert.Equal(list.Count, Directory.GetFiles(directory, "0*").Count());

            List<MockClass> list2 = new List<MockClass>();
            while(queue.Count > 0)
            {
                list2.Add(queue.Dequeue());
            }

            Assert.True(list.SequenceEqual(list2, new MockClassComparer()));
            Assert.Equal(0, queue.Count);
            Assert.Empty(Directory.GetFiles(directory, "0*"));
        }

        [Fact]
        public void TestEmptyQueue()
        {
            FilePersistentQueue<int> queue = CreateQueue("empty");
            Assert.Throws<InvalidOperationException>(() =>
            {
                int i = queue.Dequeue();
            });
        }

        [Fact]
        public void TestQueueCapacity()
        {
            FilePersistentQueue<int> queue = CreateQueue("capacity");
            Assert.Throws<InvalidOperationException>(() =>
            {
                for (int i = 0; i < 11; i++)
                {
                    queue.Enqueue(i);
                }
            });

        }

        private FilePersistentQueue<int> CreateQueue(string id)
        {
            string directory = Path.Combine(QueueDirectory, id);
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, true);
            }
            BinarySerializer<int> serializer = new BinarySerializer<int>(
                (writer, i) => writer.Write(i),
                (reader) => reader.ReadInt32()
            );

            return new FilePersistentQueue<int>(
                10,
                directory,
                serializer
            );
        }
    }
}
