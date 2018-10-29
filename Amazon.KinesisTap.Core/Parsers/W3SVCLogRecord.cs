using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public class W3SVCLogRecord : DelimitedLogRecordBase, IJsonConvertable
    {
        public W3SVCLogRecord(string[] data, DelimitedLogContext context) : base(data, context)
        {
        }

        /*
"date time s-sitename s-computername s-ip cs-method cs-uri-stem cs-uri-query s-port cs-username c-ip cs-version cs(User-Agent) cs(Cookie) cs(Referer) cs-host sc-status sc-substatus sc-win32-status sc-bytes cs-bytes time-taken"
.Split(' ')
.Select(s => string.Format("public string {0} {{ get {{ return GetValue(\"{1}\");  }} }}", s.Replace('-', '_').Replace('(', '_').Replace(")", string.Empty), s))
*/

        public override DateTime TimeStamp => DateTime.Parse(this["date"] + "T" + this["time"] + "Z", null, System.Globalization.DateTimeStyles.RoundtripKind);

        public string ToJson()
        {
            return JsonConvert.SerializeObject(this, new DelimitedLogRecordJsonConverter());
        }
    }
}
