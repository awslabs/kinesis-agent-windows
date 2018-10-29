using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;
using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    public class TimeStampParserTest
    {
        [Fact]
        public void TestTimestampLog()
        {
            DateTime expectedTime1 = new DateTime(2017, 5, 18, 0, 0, 28).AddTicks(665000);
            DateTime expectedTime2 = new DateTime(2017, 5, 18, 0, 0, 56).AddTicks(8128000);
            DateTimeKind timeZoneKind = DateTimeKind.Utc;

            TestTimestampLogInternal(expectedTime1, expectedTime2, timeZoneKind);
        }

        private static void TestTimestampLogInternal(DateTime expectedTime1, DateTime expectedTime2, DateTimeKind timeZoneKind)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("2017-05-18 00:00:28.0665 Quartz.Core.QuartzSchedulerThread DEBUG Batch acquisition of 0 triggers");
            sb.Append("2017-05-18 00:00:56.8128 Quartz.Core.QuartzSchedulerThread DEBUG Batch acquisition of 0 triggers");
            string log = sb.ToString();
            using (Stream stream = Utility.StringToStream(log))
            using (StreamReader sr = new StreamReader(stream))
            {
                TimeStampRecordParser parser = new TimeStampRecordParser("yyyy-MM-dd HH:mm:ss.ffff", null, timeZoneKind);
                var records = parser.ParseRecords(sr, new LogContext()).ToList();
                Assert.Equal("2017-05-18 00:00:28.0665 Quartz.Core.QuartzSchedulerThread DEBUG Batch acquisition of 0 triggers", records[0].GetMessage(null));
                Assert.Equal(expectedTime1, records[0].Timestamp);

                Assert.Equal("2017-05-18 00:00:56.8128 Quartz.Core.QuartzSchedulerThread DEBUG Batch acquisition of 0 triggers", records[1].GetMessage(null));
                Assert.Equal(expectedTime2, records[1].Timestamp);
            }
        }

        [Fact]
        public void TestSSMLog()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("2017-03-31 10:06:20 ERROR [instanceID=i-0ad46e850f00b20ba] [MessageProcessor] error when calling AWS APIs. error details - GetMessages Error: AccessDeniedException: User: arn:aws:sts::266928793956:assumed-role/ds2amazonli/i-0ad46e850f00b20ba is not authorized to perform: ec2messages:GetMessages on resource: *");
            sb.AppendLine("status code: 400, request id: afe4d9d7-15f9-11e7-8eb7-193e481a50d1");
            sb.AppendLine("2017-03-31 10:06:21 INFO [instanceID=i-0ad46e850f00b20ba] [MessageProcessor] increasing error count by 1");
            string log = sb.ToString();
            using (Stream stream = Utility.StringToStream(log))
            using (StreamReader sr = new StreamReader(stream))
            {
                TimeStampRecordParser parser = new TimeStampRecordParser("yyyy-MM-dd HH:mm:ss", null, DateTimeKind.Utc);
                var records = parser.ParseRecords(sr, new LogContext()).ToList();
                Assert.Equal("2017-03-31 10:06:20 ERROR [instanceID=i-0ad46e850f00b20ba] [MessageProcessor] error when calling AWS APIs. error details - GetMessages Error: AccessDeniedException: User: arn:aws:sts::266928793956:assumed-role/ds2amazonli/i-0ad46e850f00b20ba is not authorized to perform: ec2messages:GetMessages on resource: *" + 
                    Environment.NewLine + "status code: 400, request id: afe4d9d7-15f9-11e7-8eb7-193e481a50d1", records[0].GetMessage(null));
                Assert.Equal(new DateTime(2017, 3, 31, 10, 6, 20), records[0].Timestamp);

                Assert.Equal("2017-03-31 10:06:21 INFO [instanceID=i-0ad46e850f00b20ba] [MessageProcessor] increasing error count by 1", records[1].GetMessage(null));
                Assert.Equal(new DateTime(2017, 3, 31, 10, 6, 21), records[1].Timestamp);
            }
        }

        [Fact]
        public void TestTimeZoneConversion()
        {
            DateTime expectedTime1 = new DateTime(2017, 5, 18, 0, 0, 28).AddTicks(665000).ToUniversalTime();
            DateTime expectedTime2 = new DateTime(2017, 5, 18, 0, 0, 56).AddTicks(8128000).ToUniversalTime();
            DateTimeKind timeZoneKind = DateTimeKind.Local;

            TestTimestampLogInternal(expectedTime1, expectedTime2, timeZoneKind);
        }
    }
}
