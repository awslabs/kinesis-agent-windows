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
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace Amazon.KinesisTap.Core
{

    public class ExchangeLogRecord : DelimitedLogRecordBase
    {
        //Record layout
        //"date-time,client-ip,client-hostname,server-ip,server-hostname,source-context,connector-id,source,event-id,internal-message-id,message-id,network-message-id,recipient-address,recipient-status,total-bytes,recipient-count,related-recipient-address,reference,message-subject,sender-address,return-path,message-info,directionality,tenant-id,original-client-ip,original-server-ip,custom-data"
        //Or could be
        //DateTime,RequestId,MajorVersion,MinorVersion,BuildVersion,RevisionVersion,ClientRequestId,AuthenticationType,IsAuthenticated,AuthenticatedUser,Organization,UserAgent,VersionInfo,ClientIpAddress,ServerHostName,FrontEndServer,SoapAction,HttpStatus,RequestSize,ResponseSize,ErrorCode,ImpersonatedUser,ProxyAsUser,ActAsUser,Cookie,CorrelationGuid,PrimaryOrProxyServer,TaskType,RemoteBackendCount,LocalMailboxCount,RemoteMailboxCount,LocalIdCount,RemoteIdCount,BeginBudgetConnections,EndBudgetConnections,BeginBudgetHangingConnections,EndBudgetHangingConnections,BeginBudgetAD,EndBudgetAD,BeginBudgetCAS,EndBudgetCAS,BeginBudgetRPC,EndBudgetRPC,BeginBudgetFindCount,EndBudgetFindCount,BeginBudgetSubscriptions,EndBudgetSubscriptions,MDBResource,MDBHealth,MDBHistoricalLoad,ThrottlingPolicy,ThrottlingDelay,ThrottlingRequestType,TotalDCRequestCount,TotalDCRequestLatency,TotalMBXRequestCount,TotalMBXRequestLatency,RecipientLookupLatency,ExchangePrincipalLatency,HttpPipelineLatency,CheckAccessCoreLatency,AuthModuleLatency,CallContextInitLatency,PreExecutionLatency,CoreExecutionLatency,TotalRequestTime,DetailedExchangePrincipalLatency,ClientStatistics,GenericInfo,AuthenticationErrors,GenericErrors,Puid
        public ExchangeLogRecord(string[] data, DelimitedLogContext context) : base(data, context)
        {
            _context = context;
        }

        public override DateTime TimeStamp
        {
            get
            {
                return DateTime.Parse(this[_context.TimeStampField], null, System.Globalization.DateTimeStyles.RoundtripKind);
            }
        }
    }
}
