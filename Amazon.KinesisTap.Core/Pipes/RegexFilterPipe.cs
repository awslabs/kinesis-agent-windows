using Amazon.KinesisTap.Core;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace Amazon.KinesisTap.Core.Pipes
{
    /// <summary>
    /// Use regex to filter event on the string or any event with a string representation
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class RegexFilterPipe<T> : FilterPipe<T>
    {
        private readonly Regex _filter;

        public RegexFilterPipe(IPlugInContext context) : base(context)
        {
            var config = context.Configuration;
            string filterPattern = config["FilterPattern"];
            if (!string.IsNullOrWhiteSpace(filterPattern))
            {
                _filter = new Regex(filterPattern.Trim());
            }
        }

        protected override bool Filter(IEnvelope<T> value)
        {
            string textRecord = value.GetMessage(null);
            return _filter.IsMatch(textRecord);
        }
    }
}
