using Amazon.KinesisTap.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Windows
{
    public class PerformanceCounterSinkFactory : IFactory<IEventSink>
    {
        private const string PERFORMANCE_COUNTER = "PerformanceCounter";

        public IEventSink CreateInstance(string entry, IPlugInContext context)
        {
            return new PerformanceCounterSink(5, context);
        }

        public void RegisterFactory(IFactoryCatalog<IEventSink> catalog)
        {
            catalog.RegisterFactory(PERFORMANCE_COUNTER, this);
        }
    }
}
