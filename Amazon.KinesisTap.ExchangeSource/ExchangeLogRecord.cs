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
