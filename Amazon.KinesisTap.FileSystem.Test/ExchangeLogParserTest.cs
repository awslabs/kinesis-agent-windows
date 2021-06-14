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
using System.IO;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Amazon.KinesisTap.Filesystem.Test
{
    public class ExchangeLogParserTest : IDisposable
    {
        private readonly string _testFile = Path.Combine(AppContext.BaseDirectory, Guid.NewGuid().ToString() + ".txt");

        public void Dispose()
        {
            if (File.Exists(_testFile))
            {
                File.Delete(_testFile);
            }
        }

        [Fact]
        public async Task TestExchangeLogRecord()
        {
            var log = @"#Software: Microsoft Exchange Server
#Version: 15.00.1104.000
#Log-type: Message Tracking Log
#Date: 2017-05-22T17:33:35.632Z
#Fields: date-time,client-ip,client-hostname,server-ip,server-hostname,source-context,connector-id,source,event-id,internal-message-id,message-id,network-message-id,recipient-address,recipient-status,total-bytes,recipient-count,related-recipient-address,reference,message-subject,sender-address,return-path,message-info,directionality,tenant-id,original-client-ip,original-server-ip,custom-data
2017-05-22T17:33:35.632Z,,EX13D13UWA001,,,,,AGENT,AGENTINFO,64712272257893,<1028007174.22872.1495474412586.JavaMail.apollo@apollo-adaws-onebox-acc-1a-4c722f6e.us-east-1.amazon.com>,13fad5b7-39c5-46e7-130e-08d4a138ac67,bryanson@amazon.com,,7631,1,,,Started JlbRelayShortCircuit/us-west-2/PDX1/Prod deployment,apollo@amazon.com,jlb-approval@amazon.com,,Incoming,,10.25.10.214,10.43.162.232,S:AMA=SUM|action=p|error=|atch=0;S:CompCost=|ETR=0;S:DeliveryPriority=Normal;S:OriginalFromAddress=jlb-approval@amazon.com;S:AccountForest=ant.amazon.com
2017-05-22T17:33:35.695Z,,,,EX13D13UWA001,""EX13D13UWB004.ant.amazon.com = 250 2.6.0 < 1028007174.22872.1495474412586.JavaMail.apollo@apollo - adaws - onebox - acc - 1a - 4c722f6e.us - east - 1.amazon.com > [InternalId = EX13D13UWB004.ant.amazon.com, Hostname = 64712272257897] Queued mail for redundancy"",,SMTP,HAREDIRECT,64712272257894,<1028007174.22872.1495474412586.JavaMail.apollo@apollo-adaws-onebox-acc-1a-4c722f6e.us-east-1.amazon.com>,13fad5b7-39c5-46e7-130e-08d4a138ac67,abannert@amazon.com;bryanson@amazon.com;zihoncao@amazon.com,,7312,3,,,Started JlbRelayShortCircuit/us-west-2/PDX1/Prod deployment,apollo@amazon.com,jlb-approval@amazon.com,,Incoming,,,,S:DeliveryPriority=Normal;S:OriginalFromAddress=jlb-approval@amazon.com;S:AccountForest=ant.amazon.com
";
            await File.WriteAllTextAsync(_testFile, log);

            await TestExchangeLogParser(new DelimitedTextLogContext
            {
                FilePath = _testFile,
                Position = 0
            });

            //Retest for the case we start with position > 0
            long position = 0;
            using var fs = File.OpenRead(_testFile);
            using var reader = new LineReader(fs);
            for (var i = 0; i < 5; i++)
            {
                // read past the header line to test that the source will read from the beginning to find it
                var (_, consumed) = await reader.ReadAsync();
                position += consumed;
            }

            await TestExchangeLogParser(new DelimitedTextLogContext
            {
                FilePath = _testFile,
                Position = position,
                LineNumber = 5
            });
        }

        private static async Task TestExchangeLogParser(DelimitedTextLogContext context)
        {
            var output = new List<IEnvelope<KeyValueLogRecord>>();
            var parser = new AsyncExchangeLogParser(NullLogger.Instance, null, null, 1024);
            await parser.ParseRecordsAsync(context, output, 10);

            Assert.Equal(2, output.Count);
            var record = output[0].Data;
            Assert.Equal("2017-05-22T17:33:35.632Z", record["date-time"]);
            Assert.Equal(new DateTime(2017, 5, 22, 17, 33, 35, 632, DateTimeKind.Utc).ToLocalTime(), record.Timestamp);
            Assert.Equal(string.Empty, record["client-ip"]);
            Assert.Equal("EX13D13UWA001", record["client-hostname"]);
            Assert.Equal("S:AMA=SUM|action=p|error=|atch=0;S:CompCost=|ETR=0;S:DeliveryPriority=Normal;S:OriginalFromAddress=jlb-approval@amazon.com;S:AccountForest=ant.amazon.com", record["custom-data"]);
            Assert.Equal("S:DeliveryPriority=Normal;S:OriginalFromAddress=jlb-approval@amazon.com;S:AccountForest=ant.amazon.com", output[1].Data["custom-data"]);

            var envelope = (ILogEnvelope)output[0];
            Assert.Equal(6, envelope.LineNumber);
        }

        [Fact]
        public async Task TestExchangeWebLogRecord()
        {
            var log = @"DateTime,RequestId,MajorVersion,MinorVersion,BuildVersion,RevisionVersion,ClientRequestId,AuthenticationType,IsAuthenticated,AuthenticatedUser,Organization,UserAgent,VersionInfo,ClientIpAddress,ServerHostName,FrontEndServer,SoapAction,HttpStatus,RequestSize,ResponseSize,ErrorCode,ImpersonatedUser,ProxyAsUser,ActAsUser,Cookie,CorrelationGuid,PrimaryOrProxyServer,TaskType,RemoteBackendCount,LocalMailboxCount,RemoteMailboxCount,LocalIdCount,RemoteIdCount,BeginBudgetConnections,EndBudgetConnections,BeginBudgetHangingConnections,EndBudgetHangingConnections,BeginBudgetAD,EndBudgetAD,BeginBudgetCAS,EndBudgetCAS,BeginBudgetRPC,EndBudgetRPC,BeginBudgetFindCount,EndBudgetFindCount,BeginBudgetSubscriptions,EndBudgetSubscriptions,MDBResource,MDBHealth,MDBHistoricalLoad,ThrottlingPolicy,ThrottlingDelay,ThrottlingRequestType,TotalDCRequestCount,TotalDCRequestLatency,TotalMBXRequestCount,TotalMBXRequestLatency,RecipientLookupLatency,ExchangePrincipalLatency,HttpPipelineLatency,CheckAccessCoreLatency,AuthModuleLatency,CallContextInitLatency,PreExecutionLatency,CoreExecutionLatency,TotalRequestTime,DetailedExchangePrincipalLatency,ClientStatistics,GenericInfo,AuthenticationErrors,GenericErrors,Puid
#Software: Microsoft Exchange Server
#Version: 15.00.1104.002
#Log-type: EWS Logs
#Date: 2017-07-21T22:18:48.921Z
#Fields: DateTime,RequestId,MajorVersion,MinorVersion,BuildVersion,RevisionVersion,ClientRequestId,AuthenticationType,IsAuthenticated,AuthenticatedUser,Organization,UserAgent,VersionInfo,ClientIpAddress,ServerHostName,FrontEndServer,SoapAction,HttpStatus,RequestSize,ResponseSize,ErrorCode,ImpersonatedUser,ProxyAsUser,ActAsUser,Cookie,CorrelationGuid,PrimaryOrProxyServer,TaskType,RemoteBackendCount,LocalMailboxCount,RemoteMailboxCount,LocalIdCount,RemoteIdCount,BeginBudgetConnections,EndBudgetConnections,BeginBudgetHangingConnections,EndBudgetHangingConnections,BeginBudgetAD,EndBudgetAD,BeginBudgetCAS,EndBudgetCAS,BeginBudgetRPC,EndBudgetRPC,BeginBudgetFindCount,EndBudgetFindCount,BeginBudgetSubscriptions,EndBudgetSubscriptions,MDBResource,MDBHealth,MDBHistoricalLoad,ThrottlingPolicy,ThrottlingDelay,ThrottlingRequestType,TotalDCRequestCount,TotalDCRequestLatency,TotalMBXRequestCount,TotalMBXRequestLatency,RecipientLookupLatency,ExchangePrincipalLatency,HttpPipelineLatency,CheckAccessCoreLatency,AuthModuleLatency,CallContextInitLatency,PreExecutionLatency,CoreExecutionLatency,TotalRequestTime,DetailedExchangePrincipalLatency,ClientStatistics,GenericInfo,AuthenticationErrors,GenericErrors,Puid
2017-07-21T22:18:48.921Z,,,,,,,,,,,,,,EX13D07UWB003,,Sbsc_ConnStatus,,,,,,,,,72e8afbe-cb42-404c-87ad-91561b16f984,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,tid=7;,,,
2017-07-21T22:18:48.952Z,,,,,,,,,,,,,,EX13D07UWB003,,Sbsc_EndConnSuccess,,,,,,,,,21b0e0c4-7379-4475-a2a6-672671da3a4a,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,tid=509;,,,
2017-07-21T22:18:48.952Z,8e8d720b-9a51-4ec8-a642-6e1f85c23cf2,15,0,1104,2,,Negotiate,true,rmdsss@amazon.com,ant.amazon.com,OC/16.0.4546.1000 (Skype for Business),Target=None;Req=Exchange2012/Exchange2013;,10.43.162.194,EX13D07UWB003,EX13D13UWA004.ANT.AMAZON.COM,GetStreamingEvents,200,960,,,rmdsss@amazon.com,,,37c2965e2e714502aca628b4bc921c38,21b0e0c4-7379-4475-a2a6-672671da3a4a,PrimaryServer,LocalTask,0,0,0,0,0,0,0,1,1,,,,,,,,,11 Max,11 Max,,,,GlobalThrottlingPolicy_755156ba-bb79-49b3-ae2a-a08357d24244,0.0383,[C],0,0,0,0,,,1,0,,0,3,0,900002,,,BackEndAuthenticator=WindowsAuthenticator;TotalBERehydrationModuleLatency=0;SubscribedMailboxes=rmdsss@amazon.com/rmdsss@amazon.com/rmdsss@amazon.com;MailboxTypeCacheSize=426770;S:WLM.Cl=InternalMaintenance;S:ServiceTaskMetadata.ADCount=0;S:WLM.Type=Ews;S:ServiceTaskMetadata.ADLatency=0;S:WLM.Int=True;S:ServiceTaskMetadata.RpcCount=0;S:WLM.SvcA=False;S:ServiceTaskMetadata.RpcLatency=0;S:ServiceTaskMetadata.WatsonReportCount=0;S:WLM.Bal=300000;S:ServiceTaskMetadata.ServiceCommandBegin=4;S:ServiceTaskMetadata.ServiceCommandEnd=4;S:ActivityStandardMetadata.Component=Ews;S:WLM.BT=Ews;S:BudgetMetadata.MaxConn=27;S:BudgetMetadata.MaxBurst=300000;S:BudgetMetadata.BeginBalance=300000;S:BudgetMetadata.Cutoff=3000000;S:BudgetMetadata.RechargeRate=900000;S:BudgetMetadata.IsServiceAct=False;S:BudgetMetadata.LiveTime=00:34:09.5718618;S:BudgetMetadata.EndBalance=300000;Dbl:WLM.TS=900002;Dbl:BudgUse.T[]=2;Dbl:CCpu.T[CMD]=0,,,
2017-07-21T22:18:48.968Z,,,,,,,,,,,,,,EX13D07UWB003,,Sbsc_ConnStatus,,,,,,,,,2a6c8469-c938-49f9-bfcd-73ce746e8c88,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,tid=603;,,,
2017-07-21T22:18:49.109Z,6324ca6d-c615-42eb-ace7-d411f9d5c3d2,15,0,1104,2,2420837419f043a89a8a975a39e388d8,Negotiate,true,ANT\EX13D15UWB002$,ant.amazon.com,ASProxy/CrossSite/Directory/EXCH/15.00.1104.000,Target=None;Req=Exchange2012/Exchange2013;,10.43.161.9,EX13D07UWB003,,GetUserAvailability,200,4320,,,meetings-acct-13-2@amazon.com,,meetings-acct-13-2@amazon.com,d50b0526b1164a4c8e1c5195f19db84d,983fb83e-bda8-44d6-84d4-7e84d8bc07c6,PrimaryServer,LocalTask,0,0,0,0,0,0,1,0,0,,,,,,,,,0 Max,0 Max,,,,RoomFinderPolicy,0.0385,[C],2,49,0,0,3,,12,0,,5,20,175,198,,,TotalBERehydrationModuleLatency=0;ADIdentityCache=Miss;MailboxTypeCacheSize=426770;S:WLM.Cl=InternalMaintenance;S:ServiceTaskMetadata.ADCount=2;S:WLM.Type=Ews;S:AS.IntC=0;S:ServiceTaskMetadata.ADLatency=50.1332998275757;S:WLM.Int=True;S:AS.PASQ1=0;S:ServiceTaskMetadata.RpcCount=0;S:ServiceTaskMetadata.RpcLatency=0;S:WLM.SvcA=True;S:AS.PASQ2=0;S:AS.PASQT=0;S:ServiceTaskMetadata.WatsonReportCount=0;S:ServiceTaskMetadata.ServiceCommandBegin=20;S:AS.TimeInAS=175;S:ServiceTaskMetadata.ServiceCommandEnd=196;S:AS.PEL=20;S:ActivityStandardMetadata.Component=Ews;'S:AS.ReqStats=AddressCount=1;MessageId=urn:uuid:23a0e761-f80b-4c06-aa8e-3f1edc13c48a;Requester=S-1-5-21-1407069837-2091007605-538272213-23368297;local=1;Threads.Worker.Available=3193;Threads.Worker.InUse=1;Threads.IO.Available=3197;Threads.IO.InUse=2;Target-cd0806a5-c49f-49cc-9dab-d12ca72217fb|QT-Local|Cnt-1|[EPL-1|];Failures=0;EXP=<>;TAQ=175;MRPC.T=0;MRPC.R=0;AD.T=49;AD.R=2;RCM=15;LFTE=0;LLPE.T=121;LLPE.D=EX13D07UWB003.ant.amazon.com;LLP.T=7;LLP.R=19;LLP.D=EX13D07UWB003.ant.amazon.com;LT.T=121;LT.R=1;TCLP.T=31;TCLP.D=LocalRequest.Execute;PreGetQueries=62;PostGetQueries=0;RequestDispatcher.PreQuery=0;RequestDispatcher.BeginInvoke=0;RequestDispatcher.Complete=109;PostQuery=0;';S:WLM.BT=Ews;S:BudgetMetadata.MaxConn=54;S:BudgetMetadata.MaxBurst=Unlimited;S:BudgetMetadata.BeginBalance=$null;S:BudgetMetadata.Cutoff=Unlimited;S:BudgetMetadata.RechargeRate=Unlimited;S:BudgetMetadata.IsServiceAct=True;S:BudgetMetadata.LiveTime=00:02:00.4647609;S:BudgetMetadata.EndBalance=$null;Dbl:WLM.TS=198;Dbl:BudgUse.T[]=80.4794998168945;I32:ATE.C[DC-UW2B-EX-08.ant.amazon.com]=1;F:ATE.AL[DC-UW2B-EX-08.ant.amazon.com]=0;I32:ADS.C[DC-UW2B-EX-02]=1;F:ADS.AL[DC-UW2B-EX-02]=1.8741;I32:ADS.C[DC-UW2B-EX-03]=1;F:ADS.AL[DC-UW2B-EX-03]=48.5905;I32:ADR.C[DC-UW2B-EX-05]=1;F:ADR.AL[DC-UW2B-EX-05]=1.2041;I32:ADS.C[DC-UW2B-EX-08]=1;F:ADS.AL[DC-UW2B-EX-08]=1.5428;I32:ATE.C[DC-UW2B-EX-03.ant.amazon.com]=1;F:ATE.AL[DC-UW2B-EX-03.ant.amazon.com]=0;I32:ATE.C[DC-UW2B-EX-02.ant.amazon.com]=1;F:ATE.AL[DC-UW2B-EX-02.ant.amazon.com]=0;I32:ATE.C[DC-UW2B-EX-05.ant.amazon.com]=1;F:ATE.AL[DC-UW2B-EX-05.ant.amazon.com]=0;Dbl:CCpu.T[CMD]=15.625,,,
2017-07-21T22:18:49.109Z,198b7dbe-391e-4f3b-bf8a-aaded3ed7fe2,15,0,1104,2,{086A717F-4A00-4DB9-8A5F-F9C162FC29A9},NTLM,true,ibted@amazon.com,ant.amazon.com,MacOutlook/15.35.0.170610 (Intelx64 Mac OS X Version 10.12.5 (Build 16F73)),Target=None;Req=Exchange2010_SP2/Exchange2010_SP2;,10.43.161.204,EX13D07UWB003,EX13D09UWC001.ANT.AMAZON.COM,GetEvents,200,789,,,ibted@amazon.com,,,179af92d240042be81619f62fee3c11e,1e67b994-95a2-404d-bee3-8262d83e0cd2,PrimaryServer,LocalTask,0,0,0,0,0,0,1,0,0,,,,,,,,,1 Max,1 Max,,,,GlobalThrottlingPolicy_755156ba-bb79-49b3-ae2a-a08357d24244,0.0392,[C],0,0,0,0,,,8,0,,0,11,0,15,,DeviceID_0=93CF1BC7-58AA-5053-998E-3207FD3125D3; SessionID_1=DA9D10D7-B4C0-460F-8B60-26044002EE76;,BackEndAuthenticator=WindowsAuthenticator;TotalBERehydrationModuleLatency=0;SubscriptionType=Pull;MailboxTypeCacheSize=426770;S:WLM.Cl=InternalMaintenance;S:ServiceTaskMetadata.ADCount=0;S:WLM.Type=Ews;S:ServiceTaskMetadata.ADLatency=0;S:WLM.Int=True;S:ServiceTaskMetadata.RpcCount=0;S:WLM.SvcA=False;S:ServiceTaskMetadata.RpcLatency=0;S:ServiceTaskMetadata.WatsonReportCount=0;S:WLM.Bal=299979.3;S:ServiceTaskMetadata.ServiceCommandBegin=11;S:ServiceTaskMetadata.ServiceCommandEnd=11;S:ActivityStandardMetadata.Component=Ews;S:WLM.BT=Ews;S:BudgetMetadata.MaxConn=27;S:BudgetMetadata.MaxBurst=300000;S:BudgetMetadata.BeginBalance=300000;S:BudgetMetadata.Cutoff=3000000;S:BudgetMetadata.RechargeRate=900000;S:BudgetMetadata.IsServiceAct=False;S:BudgetMetadata.LiveTime=00:04:46.0371950;S:BudgetMetadata.EndBalance=299979.3;Dbl:WLM.TS=15;Dbl:BudgUse.T[]=24.6100006103516;Dbl:CCpu.T[CMD]=0,,,
2017-07-21T22:18:49.140Z,893e4b19-9b98-4064-b9b6-ea50f32577ed,15,0,1104,2,577a17d2abf043819439b5d0b1a4925e,Negotiate,true,ANT\EX13D15UWA004$,ant.amazon.com,ASProxy/CrossSite/Directory/EXCH/15.00.1104.000,Target=None;Req=Exchange2012/Exchange2013;,10.43.160.219,EX13D07UWB003,,GetUserAvailability,200,4320,,,meetings-acct-13-1@amazon.com,,meetings-acct-13-1@amazon.com,077759d4b5b14bd6a36bca16e5fde1c5,5aa9f4e5-9f42-499f-8a94-3a635aa560c8,PrimaryServer,LocalTask,0,0,0,0,0,0,1,0,0,,,,,,,,,0 Max,0 Max,,,,RoomFinderPolicy,0.0381,[C],2,2,0,0,,,4,0,,1,8,136,145,,,TotalBERehydrationModuleLatency=0;MailboxTypeCacheSize=426770;S:WLM.Cl=InternalMaintenance;S:ServiceTaskMetadata.ADCount=2;S:WLM.Type=Ews;S:AS.IntC=0;S:ServiceTaskMetadata.ADLatency=3.04549998044968;S:WLM.Int=True;S:AS.PASQ1=0;S:ServiceTaskMetadata.RpcCount=0;S:ServiceTaskMetadata.RpcLatency=0;S:WLM.SvcA=True;S:AS.PASQ2=0;S:AS.PASQT=0;S:ServiceTaskMetadata.WatsonReportCount=0;S:ServiceTaskMetadata.ServiceCommandBegin=8;S:AS.TimeInAS=135;S:ServiceTaskMetadata.ServiceCommandEnd=144;S:AS.PEL=8;S:ActivityStandardMetadata.Component=Ews;'S:AS.ReqStats=AddressCount=1;MessageId=urn:uuid:9d9294eb-0a78-4c7c-ba75-0b6f22874cd9;Requester=S-1-5-21-1407069837-2091007605-538272213-23289108;local=1;Threads.Worker.Available=3195;Threads.Worker.InUse=0;Threads.IO.Available=3198;Threads.IO.InUse=1;Target-1998f3cc-2174-42d1-8a61-fa15301a241a|QT-Local|Cnt-1|[EPL-1|];Failures=0;EXP=<>;TAQ=135;MRPC.T=0;MRPC.R=0;AD.T=2;AD.R=2;RCM=0;LFTE=0;LLPE.T=129;LLPE.D=EX13D07UWB003.ant.amazon.com;LLP.T=10;LLP.R=18;LLP.D=EX13D07UWB003.ant.amazon.com;LT.T=129;LT.R=1;TCLP.T=31;TCLP.D=LocalRequest.Execute;PreGetQueries=0;PostGetQueries=0;RequestDispatcher.PreQuery=0;RequestDispatcher.BeginInvoke=0;RequestDispatcher.Complete=140;PostQuery=0;';S:WLM.BT=Ews;S:BudgetMetadata.MaxConn=54;S:BudgetMetadata.MaxBurst=Unlimited;S:BudgetMetadata.BeginBalance=$null;S:BudgetMetadata.Cutoff=Unlimited;S:BudgetMetadata.RechargeRate=Unlimited;S:BudgetMetadata.IsServiceAct=True;S:BudgetMetadata.LiveTime=00:01:42.9028591;S:BudgetMetadata.EndBalance=$null;Dbl:WLM.TS=145;Dbl:BudgUse.T[]=6;I32:ADS.C[DC-UW2B-EX-04]=1;F:ADS.AL[DC-UW2B-EX-04]=2.2333;I32:ADR.C[DC-UW2B-EX-05]=1;F:ADR.AL[DC-UW2B-EX-05]=0.9791;I32:ADS.C[DC-UW2B-EX-05]=1;F:ADS.AL[DC-UW2B-EX-05]=0.8122;I32:ATE.C[DC-UW2B-EX-04.ant.amazon.com]=1;F:ATE.AL[DC-UW2B-EX-04.ant.amazon.com]=0;I32:ATE.C[DC-UW2B-EX-05.ant.amazon.com]=2;F:ATE.AL[DC-UW2B-EX-05.ant.amazon.com]=0;Dbl:CCpu.T[CMD]=0,,,
";
            await File.WriteAllTextAsync(_testFile, log);
            var output = new List<IEnvelope<KeyValueLogRecord>>();
            var parser = new AsyncExchangeLogParser(NullLogger.Instance, null, null, 1024);
            await parser.ParseRecordsAsync(new DelimitedTextLogContext
            {
                FilePath = _testFile
            }, output, 10);
            Assert.Equal(7, output.Count);

            var record = output[0].Data;
            Assert.Equal("2017-07-21T22:18:48.921Z", record["DateTime"]);
            Assert.Equal(new DateTime(2017, 7, 21, 22, 18, 48, 921, DateTimeKind.Utc), record.Timestamp.ToUniversalTime());

            var envelope = (ILogEnvelope)output[6];
            Assert.Equal(13, envelope.LineNumber);
        }

        [Fact]
        public async Task TestExchangeCPActivityLog()
        {
            var log = @"#Software: Microsoft Exchange Server
#Version: 15.0.0.0
#Log-type: ECP Activity Context Log
#Date: 2017-06-28T00:01:37.052Z
#Fields: MyTimeStamp,ServerName,EventId,EventData
2017-06-28T00:01:37.052Z,EX13D07UWB003,GlobalActivity,S:Bld=15.0.1104.5;S:ActID=513c2a05-428c-448c-af36-6419c8f49015;I32:ATE.C[SUPPR]=1;F:ATE.AL[SUPPR]=0;I32:ADS.C[SUPPR]=2;F:ADS.AL[SUPPR]=1.24695;Dbl:WLM.TS=600031
2017-06-28T00:06:37.044Z,EX13D07UWB003,GlobalActivity,S:Bld=15.0.1104.5;S:ActID=b28a3442-1829-479c-b781-12e0a1862213;I32:ATE.C[SUPPR]=1;F:ATE.AL[SUPPR]=0;I32:ADS.C[SUPPR]=4;F:ADS.AL[SUPPR]=0.87045;Dbl:WLM.TS=600014
";
            await File.WriteAllTextAsync(_testFile, log);
            var output = new List<IEnvelope<KeyValueLogRecord>>();
            var parser = new AsyncExchangeLogParser(NullLogger.Instance, "MyTimeStamp", null, 1024);
            await parser.ParseRecordsAsync(new DelimitedTextLogContext
            {
                FilePath = _testFile
            }, output, 10);
            Assert.Equal(2, output.Count);

            var record = output[0].Data;
            Assert.Equal("2017-06-28T00:01:37.052Z", record["MyTimeStamp"]);
            Assert.Equal(new DateTime(2017, 6, 28, 0, 1, 37, 52, DateTimeKind.Utc), record.Timestamp.ToUniversalTime());

            record = output[1].Data;
            Assert.Equal("2017-06-28T00:06:37.044Z", record["MyTimeStamp"]);
            Assert.Equal(new DateTime(2017, 6, 28, 0, 6, 37, 44, DateTimeKind.Utc), record.Timestamp.ToUniversalTime());

            var envelope = (ILogEnvelope)output[1];
            Assert.Equal(7, envelope.LineNumber);
        }
    }
}
