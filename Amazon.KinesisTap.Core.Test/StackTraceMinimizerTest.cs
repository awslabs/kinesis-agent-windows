/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    public class StackTraceMinimizerTest
    {
        [Fact]
        public void TestStackTraceMinimizer()
        {
            StackTraceMinimizerExceptionExtensions.DoCompressStackTrace = true;
            StackTraceMinimizerExceptionExtensions.StackTraceCompressionKeyExpiration = TimeSpan.FromSeconds(1);

            string staceTrace = @"2018-12-19 01:18:43.1534 Amazon.KinesisTap.Hosting.LogManager ERROR KinesisFirehoseSink client KinesisS3Json exception: Amazon.KinesisFirehose.AmazonKinesisFirehoseException: User: arn:aws:sts::470394006140:assumed-role/CloudWatchLogsAssumeRole/KinesisTap-EC2AMAZ-HCNHA1G is not authorized to perform: firehose:PutRecordBatch on resource: arn:aws:firehose:us-west-2:470394006140:deliverystream/LogAgentTestJson ---> Amazon.Runtime.Internal.HttpErrorResponseException: The remote server returned an error: (400) Bad Request. ---> System.Net.WebException: The remote server returned an error: (400) Bad Request.
   at System.Net.HttpWebRequest.EndGetResponse(IAsyncResult asyncResult)
   at System.Threading.Tasks.TaskFactory`1.FromAsyncCoreLogic(IAsyncResult iar, Func`2 endFunction, Action`1 endAction, Task`1 promise, Boolean requiresSynchronization)
   at async Amazon.Runtime.Internal.HttpRequest.GetResponseAsync(?)
   -- - End of inner exception stack trace-- -
    at async Amazon.Runtime.Internal.HttpRequest.GetResponseAsync(?)
   at async Amazon.Runtime.Internal.HttpHandler`1.InvokeAsync[](?)
   at async Amazon.Runtime.Internal.Unmarshaller.InvokeAsync[](?)
   at async Amazon.Runtime.Internal.ErrorHandler.InvokeAsync[](?)
   -- - End of inner exception stack trace-- -
    at Amazon.Runtime.Internal.HttpErrorResponseExceptionHandler.HandleException(IExecutionContext executionContext, HttpErrorResponseException exception)
   at Amazon.Runtime.Internal.ErrorHandler.ProcessException(IExecutionContext executionContext, Exception exception)
   at async Amazon.Runtime.Internal.ErrorHandler.InvokeAsync[](?)
   at async Amazon.Runtime.Internal.CallbackHandler.InvokeAsync[](?)
   at async Amazon.Runtime.Internal.CredentialsRetriever.InvokeAsync[](?)
   at async Amazon.Runtime.Internal.RetryHandler.InvokeAsync[](?)
   at async Amazon.Runtime.Internal.RetryHandler.InvokeAsync[](?)
   at async Amazon.Runtime.Internal.CallbackHandler.InvokeAsync[](?)
   at async Amazon.Runtime.Internal.CallbackHandler.InvokeAsync[](?)
   at async Amazon.Runtime.Internal.ErrorCallbackHandler.InvokeAsync[](?)
   at async Amazon.Runtime.Internal.MetricsHandler.InvokeAsync[](?)
   at async Amazon.KinesisTap.AWS.KinesisFirehoseSink.OnNextAsync(?)";
            int originalLineCount = staceTrace.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).Length;

            //Send the log and should expect an extra line with hash
            TestStackTraceCompression(staceTrace, originalLineCount + 1, "@stacktrace_id");

            //Send a log with same stack trace and it should be compressed
            TestStackTraceCompression(staceTrace, 2, "@stacktrace_ref");

            //Wait for 2 seconds and should and should write the full stack strace
            Thread.Sleep(2000);
            TestStackTraceCompression(staceTrace, originalLineCount + 1, "@stacktrace_id");
        }

        private static void TestStackTraceCompression(string stackTrace, int expectedLineCount, string expectedHashStart)
        {

            string output = StackTraceMinimizerExceptionExtensions.CompressStackTrace(stackTrace);
            string[] lines = output.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);

            //Line count should increase by 1 because we emit hash
            Assert.Equal(expectedLineCount, lines.Length);
            Assert.StartsWith(expectedHashStart, lines[1]);
        }
    }
}
