# Amazon Kinesis Agent for Windows 1.1.216.1 (Beta) release notes
You can download Kinesis Agent for Windows [here](https://s3-us-west-2.amazonaws.com/kinesis-agent-windows/beta/index.html).

## New feature: VPC Endpoint support for AWS Sinks

This new feature allows users to supply a VPC endpoint in the sink configuration for the CloudWatchLogs, CloudWatch, KinesisStreams, and KinesisFirehose sinks.
A VPC endpoint enables you to privately connect your VPC to supported AWS services and VPC endpoint services powered by AWS PrivateLink without requiring an internet gateway, NAT device, VPN connection, or AWS Direct Connect connection. Instances in your VPC do not require public IP addresses to communicate with resources in the service. Traffic between your VPC and the other service does not leave the Amazon network.
See [here](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-endpoints.html) for information about VPC endpoints.

The VPC endpoint is specified using the "ServiceURL" property. Here is an example CloudWatchLogs sink configuration that uses a VPC endpoint:
```javascript
{
  "Id": "myCloudWatchLogsSink",
  "SinkType": "CloudWatchLogs",
  "LogGroup": "EC2Logs",
  "LogStream": "logs-{instance_id}",
  "ServiceURL": "https://vpce-sd7s876fg68-mm5ztgca.logs.us-east-1.vpce.amazonaws.com" // use the value that is displayed in the VPC endpoint details tab in the VPC console
}
```

## New feature: Support for STS Regional Endpoints when using RoleARN property in AWS Sinks

This new feature only affects users who are using the "RoleARN" property of the AWS sinks to assume an external role to authenticate with the destination AWS services, and only on EC2 instances.
When specified, the agent will use a regional endpoint for performing the "AssumeRole" operation (e.g. https\://sts.us-east-1.amazonaws.com) instead of the global endpoint (https\://sts.amazonaws.com).
Using Regional STS endpoints reduces both the round-trip latency for this operation, and also limits the blast radius for failures in the global endpoint service.

The use of Regional STS endpoints is configured using the "UseSTSRegionalEndpoints" property, as in the example below:
```javascript
{
  "Id": "myCloudWatchLogsSink",
  "SinkType": "CloudWatchLogs",
  "LogGroup": "EC2Logs",
  "LogStream": "logs-{instance_id}",
  "RoleARN": "arn:aws:iam::123456789012:role/KinesisTapRole",
  "UseSTSRegionalEndpoints": "true"
}
```

## New feature: Alternate means of configuring a proxy server

Previously the only way to configure the agent to use a proxy was to use a native feature of .NET, which automatically routed all HTTP/S requests through the proxy defined in the proxy file.
This new feature allows you to configure a proxy server in the Sink configuration using the proxy support built in to the AWS SDK instead of .NET.
If you are currently using the agent with a proxy server, there is no need to change over to use this method. It is primarily intended to satisfy a future requirement of a cross-platform build.

The use of a proxy is configured using the "ProxyHost" and "ProxyPort" properties, as in the example below:
```javascript
{
  "Id": "myCloudWatchLogsSink",
  "SinkType": "CloudWatchLogs",
  "LogGroup": "EC2Logs",
  "LogStream": "logs-{instance_id}",
  "Region": "us-east-1",
  "ProxyHost": "myproxy.mydnsdomain.com",
  "ProxyPort": "8080"
}
```