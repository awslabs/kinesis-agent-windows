using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    public class SingleLineJsonParserTest
    {
        const string LOG = @"{""ul-log-data"":{""method_name"":""UserProfile"",""module_name"":""ACMEController""},""ul-tag-status"":""INFO"",""ul-timestamp-epoch"":1537519130972, ""Timestamp"":""2018-09-21T08:38:50.972Z""}
{""ul-log-data"":{""http_url"":""http://acme/org"",""http_request_headers"":{""Accept"":""*/*"",""Accept-Encoding"":""gzip;deflate""},""http_response_code"":401},""ul-tag-status"":""INFO"",""ul-timestamp-epoch"":1537519131241, ""Timestamp"":""2018-09-21T08:38:51.241Z""}";

        [Theory]
        [InlineData("JsonLog1")]
        [InlineData("JsonLog2")]
        public void TestJsonLog(string sourceId)
        {
            using (Stream stream = Utility.StringToStream(LOG))
            using (StreamReader sr = new StreamReader(stream))
            {
                var config = TestUtility.GetConfig("Sources", sourceId);
                var records = ParseRecords(sr, config);

                Assert.Equal(2, records.Count);

                var record0 = records[0];
                Assert.Equal(new DateTime(2018, 9, 21, 8, 38, 50, 972), records[0].Timestamp);
                Assert.Equal("UserProfile", record0.Data["ul-log-data"]["method_name"]);
                Assert.Equal("INFO", record0.Data["ul-tag-status"]);
            }
        }

        private static List<IEnvelope<JObject>> ParseRecords(StreamReader sr, Microsoft.Extensions.Configuration.IConfigurationSection config)
        {
            string timetampFormat = config["TimestampFormat"];
            string timestampField = config["TimestampField"];
            var parser = new SingleLineJsonParser(timestampField, timetampFormat);
            var records = parser.ParseRecords(sr, new DelimitedLogContext()).ToList();
            return records;
        }
    }
}
