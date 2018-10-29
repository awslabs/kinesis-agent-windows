using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

namespace Amazon.KinesisTap.Core
{
    public class TimeStampRecordParser : RegexRecordParser
    {
        protected string _timeStamp;

        public TimeStampRecordParser(string timeStamp, ILogger logger, DateTimeKind timeZoneKind) :
            this(timeStamp, logger, timeZoneKind, new RegexRecordParserOptions())
        {

        }

        public TimeStampRecordParser(string timeStamp, ILogger logger, DateTimeKind timeZoneKind, RegexRecordParserOptions parserOptions) : 
            base(ConvertTimeStampToRegex(timeStamp), timeStamp, logger, null, timeZoneKind, parserOptions)
        {
            _timeStamp = timeStamp; //e.g.: "MM/dd/yyyy HH:mm:ss"
        }

        private static string ConvertTimeStampToRegex(string timeStamp)
        {
            char[] timeStampCharacters = new[] { 'd', 'M', 'm', 'y', 'H', 'h', 's', 'f' };
            string regex = timeStamp;
            foreach(char c in timeStampCharacters)
            {
                regex = regex.Replace(c.ToString(), @"\d");
            }
            return $"^(?<TimeStamp>{regex})";
        }
    }
}
