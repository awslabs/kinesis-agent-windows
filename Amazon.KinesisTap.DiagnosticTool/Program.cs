using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.DiagnosticTool
{
    class Program
    {

        static void Main(string[] args)
        {

            int exitCode = InvokeCommand(args);

            Environment.Exit(exitCode);
        }

        private static int InvokeCommand(string[] args)
        {
            if (args.Length == 0)
            {
                WriteUsage();
                return Constant.NORMAL;
            }

            switch (args[0])
            {
                case "/w":
                case "-w":
                    return new DirectoryWatcherCommand().ParseAndRunArgument(args);

                case "/log4net":
                    return new Log4NetSimulatorCommand().ParseAndRunArgument(args);

                case "/c":
                case "/config":
                    return new ConfigValidatorCommand().ParseAndRunArgument(args);

                case "/r":
                    return new RecordParserValidatorCommand().ParseAndRunArgument(args);

                case "/e":
                    return new WindowsEventLogSimulatorCommand().ParseAndRunArgument(args);

                case "/p":   // Validate the PackageVersion.json
                    return new PackageVersionValidatorCommand().ParseAndRunArgument(args);

                default:
                    WriteUsage();
                    return Constant.INVALID_ARGUMENT;
            }
        }

        private static void WriteUsage()
        {
            List<ICommand> commands = new List<ICommand>();
            ICommand log4NetSimulatorCommand = new Log4NetSimulatorCommand();
            ICommand windowsEventLogSimulatorCommand = new WindowsEventLogSimulatorCommand();
            ICommand directoryWatcherCommand = new DirectoryWatcherCommand();
            ICommand configFileValidatorCommand = new ConfigValidatorCommand();
            ICommand recordParserValidatorCommand = new RecordParserValidatorCommand();
            ICommand packageVersionValidatorCommand = new PackageVersionValidatorCommand();

            commands.Add(log4NetSimulatorCommand);
            commands.Add(windowsEventLogSimulatorCommand);
            commands.Add(directoryWatcherCommand);
            commands.Add(configFileValidatorCommand);
            commands.Add(recordParserValidatorCommand);
            commands.Add(packageVersionValidatorCommand);

            foreach (ICommand command in commands)
            {
                command.WriteUsage();
            }
        }

    }
}
