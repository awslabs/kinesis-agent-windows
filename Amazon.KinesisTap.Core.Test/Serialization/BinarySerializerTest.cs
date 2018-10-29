using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    public class BinarySerializerTest
    {
        [Fact]
        public void TestBinarySerializer()
        {
            List<MockClass> list = CreateList();
            MemoryStream stream = new MemoryStream();
            ListBinarySerializer<MockClass> listSerializer = new ListBinarySerializer<MockClass>
                (
                    MockSerializer,
                    MockDeserializer
                );

            BinarySerializer<List<MockClass>> serializer = new BinarySerializer<List<MockClass>>(
                listSerializer.Serialize,
                listSerializer.Deserialize);
            serializer.Serialize(stream, list);
            stream.Position = 0;
            List<MockClass> list2 = serializer.Deserialize(stream);
            Assert.True(list.SequenceEqual(list2, new MockClassComparer()));
        }

        internal static List<MockClass> CreateList()
        {
            Random random = Utility.Random;
            List<MockClass> list = new List<MockClass>();
            for (int i = 0; i < 500; i++)
            {
                list.Add(new MockClass
                {
                    AnInt = random.Next(),
                    ALong = random.Next(),
                    ADateTime = DateTime.Now,
                    AString = TestUtility.RandomString(1000),
                    AMemortySteam = Utility.StringToStream(TestUtility.RandomString(1000))
                });
            }

            return list;
        }

        internal static Action<BinaryWriter, MockClass> MockSerializer = (bw, o) =>
        {
            bw.Write(o.AnInt);
            bw.Write(o.ALong);
            bw.WriteDateTime(o.ADateTime);
            bw.WriteNullableString(o.AString);
            bw.WriteNullableString(o.AnotherString);
            bw.WriteMemoryStream(o.AMemortySteam);
        };

        internal static Func<BinaryReader, MockClass> MockDeserializer = br => new MockClass
        {
            AnInt = br.ReadInt32(),
            ALong = br.ReadInt64(),
            ADateTime = br.ReadDateTime(),
            AString = br.ReadNullableString(),
            AnotherString = br.ReadNullableString(),
            AMemortySteam = br.ReadMemoryStream()
        };
    }

    internal class MockClass
    {
        public int AnInt { get; set; }
        public long ALong { get; set; }
        public DateTime ADateTime { get; set; }
        public string AString { get; set; }
        public string AnotherString { get; set; }
        public MemoryStream AMemortySteam { get; set; }
    }

    internal class MockClassComparer : IEqualityComparer<MockClass>
    {
        public bool Equals(MockClass x, MockClass y)
        {
            return x.AnInt == y.AnInt &&
                x.ALong == y.ALong &&
                x.ADateTime == y.ADateTime &&
                x.AString == y.AString &&
                x.AnotherString == y.AnotherString &&
                x.AMemortySteam.ToArray().SequenceEqual(y.AMemortySteam.ToArray());
        }

        public int GetHashCode(MockClass obj)
        {
            return obj.GetHashCode();
        }
    }
}
