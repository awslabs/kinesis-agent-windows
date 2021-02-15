/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
using Newtonsoft.Json;
using System;

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
