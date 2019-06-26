# Amazon Kinesis Agent for Windows 1.1 release notes

## MSI Installer

Now you can install Amazon Kinesis Agent for Windows with an msi installer. See [details](msi.md).

## Object and text decoration with Expressions

See [details](decoration_with_expressions.md).

## DirectorySource: the FileNameFilter attribute now accepts multiple filters separated by "|".

If you have multiple log filename patterns, this feature allows you to use a single DirectorySource, for example:

```
FileNameFilter: “*.log|*.txt”
```

## DirectorySource: if you specify "\*.\*" in FileNameFilter, known compressed files are now excluded.

System administrators sometimes compress log files before achiving them. This feature prevents .zip, .gz and .bz2 files from accidentally streamed.

## DirectorySource: the new Ecoding attribute.

By default, Kinesis Agent can automatically detect the encoding from bytemark. However, the automatic encoding may not work correctly on some older unicode formats. For example, to stream Microsoft SQL Server log, you need to specify:

```
"Encoding": "utf-16"
```

The possible values of encoding are in name column of this [page][msdn-encoding].

## DirectorySource: the new ExtractionRegexOptions attribute.

You can now use the ExtractionRegexOptions to simplify regular expression. For example, the "." expression will match any character including \r\n if you specify:

```
"ExtractionRegexOptions" = "Multiline"
```


The possible values of ExtractionRegexOptions are in this [page][msdn-regex-options]. The default is "None".

## DirectorySource: you can now use the ExtractionPattern attribute with the Timestamp parser.

Previously, only "Regex" parsers supports "ExtractionPattern". Now you can use "ExtractionPattern" attribute with Timestamp parser which is simpler to confgure.

## WindowsPerformanceCounterSource: you can now use InstanceRegex attribute to select counter instances.

Previously, the "Instances" attribute only allows wildcard characters. "InstanceRegex" accepts regular expression which is more powerful and allows you to use in the scenario where the instance name itself contains the "\*" character, for example:

```
    {
      "Id": "myPerformanceCounter",
      "SourceType": "WindowsPerformanceCounterSource",
      "Categories": [
        {
          "Category": "Network Adapter",
          "InstanceRegex": "^Local Area Connection\\* \\d$",
          "Counters" : [ "Bytes Received/sec", "Bytes Sent/sec" ]
        },
      ]
    }
```

## KinesisFirehose sink: you can now use the CombineRecords attribute to combine multiple small records into one large record upto 5 KB.

According to [Amazon Kinesis Data Firehose pricing][firehose-pricing], if a record is less than 5KB, it is rounded up to the nearest 5K for pricing. Combining records can save ingestion cost. To combine multiple small records, specify:

```
"CombineRecords": "true"
```

**Note that if you use Lambda to transform Firehose record, your Lambda needs to account for the fact that combined records are separated by \n if you turn on this option.**

## Use ProfileRefreshingAWSCredentialProvider to refresh AWS credentials

If you use [AWS Systems Manager for Hybrid Environments][ssm-on-prem] to manage AWS credentials, SSM rotates session credentials in c:\Windows\System32\config\systemprofile\\.aws\credentials. Because the AWS .net SDK does not pick up new credentials automatically, we provide the ProfileRefreshingAWSCredentialProvider plug-in to refresh credentials. You just need to configure the ProfileRefreshingAWSCredentialProvider plug-in and reference the plug-in using the "CredentialRef" attribute of any AWS Sink, for example:

```
{
    "Sinks": [
        {
            "Id": "myCloudWatchLogsSink",
            "SinkType": "CloudWatchLogs",
            "CredentialRef": "ssmcred",
            "Region": "us-west-2",
            "LogGroup": "myLogGroup",
            "LogStream": "myLogStream"
        },
    ],
    "Credentials": [
        {
            "Id": "ssmcred",
            "CredentialType": "ProfileRefreshingAWSCredentialProvider",
            "Profile": "default", //Optional, the default is "default"
            "FilePath": "path_to_credential_file", //Optional, the default is %USERPROFILE%/.aws/credentials
            "RefreshingInterval": 300, //Optional, the default is 300 seconds. 
        }
    ]
}
```

[firehose-pricing]: https://aws.amazon.com/kinesis/data-firehose/pricing/
[msdn-encoding]: https://docs.microsoft.com/en-us/dotnet/api/system.text.encoding?view=netframework-4.8&viewFallbackFrom=netframework-4.7.2.
[msdn-regex-options]: https://docs.microsoft.com/en-us/dotnet/api/system.text.regularexpressions.regexoptions?view=netframework-4.7.2
[ssm-on-prem]: https://docs.aws.amazon.com/systems-manager/latest/userguide/systems-manager-managedinstances.html