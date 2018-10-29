using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.DiagnosticTool
{
    class RecordParserValidatorCommand : ICommand
    {
        public int ParseAndRunArgument(string[] args)
        {
            if (args.Length == 2 || args.Length == 3)
            {
                RecordParserValidator validator = new RecordParserValidator(AppContext.BaseDirectory);

                try
                {

                    string sourceID = args[1];

                    string LogName = null;
                    if (args.Length == 3)
                    {
                        LogName = args[2];
                    }

                    bool isValid = validator.ValidateRecordParser(sourceID, LogName, AppContext.BaseDirectory, Constant.CONFIG_FILE, out IList<string> messages);

                    if (isValid)
                    {
                        Console.WriteLine($"Record Parser is valid for Source ID: {sourceID}.");
                    }
                    else
                    {
                        foreach (string message in messages)
                        {
                            Console.WriteLine(message);
                        }
                    }
                    return Constant.NORMAL;
                }
                catch (FormatException ex)
                {
                    Console.WriteLine(ex.ToString());
                    return Constant.INVALID_FORMAT;
                }

            }
            else
            {
                WriteUsage();
                return Constant.INVALID_ARGUMENT;
            }
        }

        public void WriteUsage()
        {
            Console.WriteLine("Validate RecordParser in configuration file:");
            Console.WriteLine();
            Console.WriteLine("ktdiag /r sourceID [-logName]");
            Console.WriteLine("\t -LogName: Log file name.");
            Console.WriteLine();
        }
    }
}
