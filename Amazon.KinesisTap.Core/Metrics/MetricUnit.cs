using System.ComponentModel;

namespace Amazon.KinesisTap.Core.Metrics
{
    public enum MetricUnit
    {
        None,
        Bits,
        TerabytesSecond,
        Terabytes,
        TerabitsSecond,
        Terabits,
        Seconds,
        Percent,
        Milliseconds,
        Microseconds,
        MegabytesSecond,
        Megabytes,
        MegabitsSecond,
        Megabits,
        Kilobytes,
        KilobitsSecond,
        Kilobits,
        GigabytesSecond,
        Gigabytes,
        GigabitsSecond,
        Gigabits,
        CountSecond,
        Count,
        BytesSecond,
        Bytes,
        BitsSecond,
        KilobytesSecond,

        //CloudWatch does not have this unit. Need to convert
        HundredNanoseconds
    }
}
