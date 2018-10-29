using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public enum InitialPositionEnum
    {
        EOS, //Default
        BOS,
        Bookmark,
        Timestamp
    }
}
