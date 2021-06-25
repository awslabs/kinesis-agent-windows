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
using System.Linq;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions;

namespace Amazon.KinesisTap.Core
{
    public static class CertificateUtility
    {
        private const string DnsNameOid = "2.5.29.17";
        private const string EnhancedKeyUsageOid = "2.5.29.37";
        private const string ClientAuthenticationOid = "1.3.6.1.5.5.7.3.2";
        private const string TemplateNameOid = "1.3.6.1.4.1.311.21.7";

        public static X509Certificate2 GetCertificate(StoreLocation storeLocation, string username, string templateNameRegex = null)
        {
            if (string.IsNullOrWhiteSpace(username)) return null;

            string extractionRegex = null, nameMatchRegex = null;
            if (storeLocation == StoreLocation.LocalMachine)
            {
                extractionRegex = "DNS Name=([a-zA-Z0-9@\\.\\-_]+)";
                nameMatchRegex = $"^{username}(.+)*";
            }
            else
            {
                extractionRegex = "Principal Name=([a-zA-Z0-9@\\.\\-_]+)";
                nameMatchRegex = $"^{username}(@.+)*";
            }

            using (var store = new X509Store(StoreName.My, storeLocation))
            {
                store.Open(OpenFlags.OpenExistingOnly);
                var candidateCertificates = store.Certificates.Find(X509FindType.FindBySubjectName, username, false)
                    .Find(X509FindType.FindByExtension, DnsNameOid, false)
                    .Find(X509FindType.FindByExtension, EnhancedKeyUsageOid, true);

                if (candidateCertificates.Count > 0)
                {
                    foreach (var cert in candidateCertificates)
                    {
                        if (cert.Subject == cert.Issuer)
                        {
                            continue;
                        }

                        if (cert.Extensions[EnhancedKeyUsageOid] is X509EnhancedKeyUsageExtension eku && eku.EnhancedKeyUsages[ClientAuthenticationOid] == null)
                        {
                            continue;
                        }

                        var dnsNameExtension = cert.Extensions[DnsNameOid];
                        if (dnsNameExtension == null)
                        {
                            continue;
                        }

                        if (!string.IsNullOrWhiteSpace(templateNameRegex))
                        {
                            var templateNameExtension = cert.Extensions[TemplateNameOid];
                            if (templateNameExtension == null)
                            {
                                continue;
                            }

                            var templateName = templateNameExtension.Format(false).Split('(').FirstOrDefault()?.Split('=').LastOrDefault()?.Trim();
                            if (string.IsNullOrWhiteSpace(templateName))
                            {
                                continue;
                            }

                            if (!Regex.IsMatch(templateName, templateNameRegex, RegexOptions.IgnoreCase))
                            {
                                continue;
                            }
                        }

                        var nameToMatch = dnsNameExtension.Format(false);

                        var nameMatches = Regex.Match(nameToMatch, extractionRegex);
                        if (nameMatches != null || nameMatches.Groups.Count == 2 || Regex.IsMatch(nameMatches.Groups[1].Value, nameMatchRegex))
                        {
                            return cert;
                        }
                    }
                }

                return null;
            }
        }

        public static byte[] SignContents(X509Certificate2 certificate, byte[] contents)
        {
            using (var pk = certificate.GetRSAPrivateKey())
            {
                return pk.SignData(contents, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            }
        }
    }
}
