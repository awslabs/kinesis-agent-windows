using Amazon.KinesisFirehose.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Amazon.KinesisTap.AWS
{
    public class RecordComparer : IEqualityComparer<Record>
    {
        public bool Equals(Record x, Record y)
        {
            return x.Data.ToArray().SequenceEqual(y.Data.ToArray());
        }

        public int GetHashCode(Record obj)
        {
            return obj.GetHashCode();
        }
    }
}
