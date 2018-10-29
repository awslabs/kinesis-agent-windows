using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;

namespace Amazon.KinesisTap.Windows
{
    public class EventInfo
    {
        public int EventId { get; set; }
        public string Description { get; set; }
        public string LevelDisplayName { get; set; }
        public string LogName { get; set; }
        public string MachineName { get; set; }
        public string ProviderName { get; set; }
        public DateTime? TimeCreated { get; set; }
        public long? Index { get; set; }
        public string UserName { get; set; }
        public string Keywords { get; set; }
        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public List<object> EventData { get; set; }
    }
}
