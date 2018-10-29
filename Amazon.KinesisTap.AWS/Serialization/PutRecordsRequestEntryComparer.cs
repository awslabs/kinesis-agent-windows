using Amazon.Kinesis.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Amazon.KinesisTap.AWS
{
    public class PutRecordsRequestEntryComparer : IEqualityComparer<PutRecordsRequestEntry>
    {
        public bool Equals(PutRecordsRequestEntry x, PutRecordsRequestEntry y)
        {
            return x.PartitionKey == y.PartitionKey
                && x.ExplicitHashKey == y.ExplicitHashKey
                && x.Data.ToArray().SequenceEqual(y.Data.ToArray());
        }

        public int GetHashCode(PutRecordsRequestEntry obj)
        {
            return obj.GetHashCode();
        }
    }
}
