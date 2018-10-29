using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public class SysLogParser : RegexRecordParser
    {
        private const string MONTH = "(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)";
        private const string DAY = @"\d{1,2}";
        private const string TIME = @"\d{2}:\d{2}:\d{2}";
        private const string SYSLOG_TIMESTAMP = MONTH + " " + DAY + " " + TIME;
        private const string HOST_NAME = @"\S*";
        private const string PROGRAM = @"\S*";
        private const string MESSAGE = @".*";
        private const string TIMESTAMP_PATTERN = "^(?<TimeStamp>" + SYSLOG_TIMESTAMP + ")";
        private const string TIMESTAMP_FORMAT = "MMM dd HH:mm:ss";
        private const string EXTRACTION_PATTERN = "^(?<SysLogTimeStamp>" + SYSLOG_TIMESTAMP + ") "
            + "(?<Hostname>" + HOST_NAME + ") "
            + "(?<Program>" + PROGRAM + ") "
            + "(?<Message>" + MESSAGE + ")$";

        public SysLogParser(ILogger logger, DateTimeKind timeZoneKind) : this(logger, timeZoneKind,
            new RegexRecordParserOptions())
        {

        }

        public SysLogParser(ILogger logger, DateTimeKind timeZoneKind, RegexRecordParserOptions parserOptions) : 
            base(TIMESTAMP_PATTERN, TIMESTAMP_FORMAT, logger, EXTRACTION_PATTERN, timeZoneKind, parserOptions)
        {
        }
    }
}
