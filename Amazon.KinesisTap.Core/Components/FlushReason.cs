using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    //Used by Batch to indicate the reason the queue is flushed
    public enum FlushReason
    {
        Timer,
        BeforeAdd,
        AfterAdd,
        Stop
    }
}
