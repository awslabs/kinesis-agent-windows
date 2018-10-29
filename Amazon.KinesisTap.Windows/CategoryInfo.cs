using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Windows
{
    public class CategoryInfo
    {
        public CategoryInfo(string categoryName, string[] instances, string[] counters)
        {
            this.CategoryName = categoryName;
            this.InstanceFilters = instances;
            this.CounterFilters = counters;
        }

        public string CategoryName { get; private set; }
        public string[] InstanceFilters { get; private set; }
        public  string[] CounterFilters { get; private set; }
    }
}
