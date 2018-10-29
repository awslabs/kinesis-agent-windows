using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.DiagnosticTool
{
    class WindowsEventLogSimulatorCommand : ICommand
    { 

        public int ParseAndRunArgument(string[] args)
        {
            if (args.Length < 2)
            {
                WriteUsage();
                return Constant.INVALID_ARGUMENT;
            }

            using (var eventLogSimulator = new WindowsEventLogSimulator(args))
            {
                eventLogSimulator.Start();
                Console.WriteLine("Type any key to exit this program...");
                Console.ReadKey();
                return Constant.NORMAL;
            }
        }

        public void WriteUsage()
        {
            Console.WriteLine("Simulate Windows Event Log:");
            Console.WriteLine();
            Console.WriteLine("ktdiag /e logName [-tn] [-sm] [-bk]");
            Console.WriteLine("\t -tn:n is the interval between writing log records in millisecond. The default 1000 millisecond or 1 second.");
            Console.WriteLine("\t -sm:m is the size of each log record in bytes. The default 1000 bytes or 1 KB. The maximum is 32766.");
            Console.WriteLine("\t -bk:k is the batch size. The default is 1.");
            Console.WriteLine();
        }
    }
}
