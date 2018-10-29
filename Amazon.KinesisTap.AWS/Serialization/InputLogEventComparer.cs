using Amazon.CloudWatchLogs.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.AWS
{
    public class InputLogEventComparer : IEqualityComparer<InputLogEvent>
    {
        public bool Equals(InputLogEvent x, InputLogEvent y)
        {
            return x.Timestamp == y.Timestamp
                && x.Message == y.Message;
        }

        public int GetHashCode(InputLogEvent obj)
        {
            return obj.GetHashCode();
        }
    }
}
