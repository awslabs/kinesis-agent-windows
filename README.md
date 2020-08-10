# Amazon Kinesis Agent for Microsoft Windows

The **Amazon Kinesis Agent for Microsoft Windows** is a configurable and extensible agent. It runs on Windows systems, either on-premises or in the AWS Cloud. Kinesis Agent efficiently and reliably gathers, parses, transforms, and streams logs, events, and metrics to various AWS services, including [Amazon Kinesis Data Streams][kinesis-stream], [Amazon Kinesis Data Firehose][kinesis-firehose], [Amazon CloudWatch][cloudwatch], and [Amazon CloudWatch Logs][cloudwatch-logs].

*	[Amazon Kinesis details][kinesis]
*	[Kinesis Agent for Windows User Guide][kinesis-agent-windows-user-guide]
*	[Reporting Issues][kinesis-agent-windows-issues]

## Features

* Monitors log files, Windows Event Logs, Event Tracing for Windows (ETW), Windows Performance Counters and sends data records to AWS services
* Parses generic logs formats as well as special log formats commonly found in Windows environments, Domain Controllers, Internet Information (IIS)/W3SVC, Exchange family of logs, SharePoint, DHCP, Radius, and SQL Server
* Performs data extraction, filtering, decoration, and formats data as structure logs.
* Handles different kinds of log rotation approaches and accessing log files even when those logs files are locked by log writers
* Provides data about the health of the agent itself which confirms the accuracy and completeness of the data collected and streamed

## Getting started

1.	Minimum requirements — To start the Amazon Kinesis Agent for Windows, you need Microsoft .NET Framework 4.6.
2.	Installing, configurating and starting Kinesis Agent for Windows — For more information, see [Kinesis Agent for Windows User Guide][kinesis-agent-windows-user-guide].

## Installing Amazon Kinesis Agent for Windows

From an elevated PowerShell command prompt window, execute the following command:

```powershell
Invoke-Expression ((New-Object System.Net.WebClient).DownloadString('https://s3-us-west-2.amazonaws.com/kinesis-agent-windows/downloads/InstallKinesisAgent.ps1'))
```

For other installation options, visit the [Kinesis Agent for Windows download page][kinesis-agent-windows-downloads].

For beta versions, visit the [Kinesis Agent for Windows beta download page][kinesis-agent-windows-beta-downloads]. Visit [commit history][commit-history] for changes in each beta version.

## Configuring and starting Amazon Kinesis Agent for Windows

**Note**: During the development of Kinesis Agent for Windows, the internal name was
"AWSKinesisTap". To maintain backward compatibility, we have maintained this terminology
within the agent's configuration.

After the Kinesis Agent for Windows is installed, the configuration file can be found in C:\Program Files\Amazon\AWSKinesisTap\appsettings.json. You need to modify this configuration file to set the data destinations and AWS credentials, and to point the agent to the data sources to push. After you complete the configuration, you can start the agent using the following command from an elevated PowerShell command prompt window:

```powershell
Start-Service -Name AWSKinesisTap
```

You can make sure the agent is running with the following command:

```powershell
Get-Service -Name AWSKinesisTap
```

To stop the agent, use the following command:

```powershell
Stop-Service -Name AWSKinesisTap
```

## Viewing the Amazon Kinesis Agent for Windows log file

The agent writes its logs to C:\ProgramData\Amazon\AWSKinesisTap\logs\KinesisTap.log.

## Uninstalling Amazon Kinesis Agent for Windows

To uninstall the agent, go to “Add or remove program” applet, locate AWSKinesisTap and click Uninstall.

## Telemetry

So that we can provide better support, by default, Amazon Kinesis Agent for Microsoft Windows collects statistics about the operation of the agent and sends them to AWS. This information contains no personally identiﬁable information, and it doesn't include any data that you gather or stream to AWS services. You can [opt-out][opt-out] of telemetry collection.

## Building from the source code

You need Visual Studio 2017 Community, Professional or Enterprise on Windows to build the project. To run and debug the project in Visual Studio, open AWSKinesisTap.sln in the project root directory. To build the nuget package, make sure you have [nuget.exe][nuget] in the path, open an elevated PowerShell command prompt, navigate to the project root directory, and run “.\build.ps1”.

## Release Notes

[Prod] Release 1.1.216.4 (August 10, 2020)
*   1.1.216.4 [Release Notes][release1.1.216.4]

[Prod] Release 1.1.216.2 (May 28, 2020)
*   1.1.216.2 [Release Notes][release1.1.216.2]

[Prod] Release 1.1.212.1 (February 26, 2020)
*   1.1.212.1 [Release Notes][release1.1.212.1]

[Prod] Release 1.1.168.1 (June 24, 2019)
*   1.1 [Release Notes][release1.1]

[Prod] Release 1.0.0.115 (November 6, 2018)
*	This is the first release.

## Other resources
*	[Amazon Kinesis details][kinesis]
*	[Amazon Kinesis Agent for Windows User Guide][kinesis-agent-windows-user-guide]
*	[Amazon Kinesis Agent for Linux][kinesis-agent-linux]

[cloudwatch]: https://aws.amazon.com/cloudwatch/
[cloudwatch-logs]: https://aws.amazon.com/cloudwatch/features/#Collect
[commit-history]: https://github.com/awslabs/kinesis-agent-windows/commits/master
[kinesis]: http://aws.amazon.com/kinesis
[kinesis-agent-linux]: https://github.com/awslabs/amazon-kinesis-agent/
[kinesis-agent-windows-beta-downloads]: https://s3-us-west-2.amazonaws.com/kinesis-agent-windows/beta/index.html
[kinesis-agent-windows-downloads]: https://s3-us-west-2.amazonaws.com/kinesis-agent-windows/downloads/index.html
[kinesis-agent-windows-issues]: https://github.com/awslabs/kinesis-agent-windows/issues
[kinesis-agent-windows-user-guide]: https://docs.aws.amazon.com/kinesis-agent-windows/latest/userguide/what-is-kinesis-agent-windows.html
[kinesis-stream]: https://aws.amazon.com/kinesis/streams/
[kinesis-firehose]: https://aws.amazon.com/kinesis/firehose/
[nuget]: https://dist.nuget.org/win-x86-commandline/latest/nuget.exe
[opt-out]: https://docs.aws.amazon.com/kinesis-agent-windows/latest/userguide/telemetrics-configuration-option.html
[release1.1]: release_1.1.md
[release1.1.212.1]: release_1.2.md
[release1.1.215.1]: release_1.1.215.1.md
[release1.1.216.2]: release_1.1.216.2.md
[release1.1.216.4]: release_1.1.216.4.md
