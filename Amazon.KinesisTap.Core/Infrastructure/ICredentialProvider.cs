using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public interface ICredentialProvider
    {
        string Id { get; set; }
    }

    public interface ICredentialProvider<out TCredentials> : ICredentialProvider
    {
        TCredentials GetCredentials();
    }
}
