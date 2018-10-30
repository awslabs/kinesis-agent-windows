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
using System.Text;

using Amazon.KinesisTap.Core;
using Newtonsoft.Json;

namespace Amazon.KinesisTap.AutoUpdate
{
    /// <summary>
    /// Utilities used by the Package and Configuration File updater
    /// </summary>
    public static class UpdateUtility
    {
        /// <summary>
        /// Create a downloader according to the url
        /// </summary>
        /// <param name="url">Could be https://, s3:// or file:// urls</param>
        /// <param name="context">Standard plugin context used by KinesisTap plugins</param>
        /// <returns>One of the File Downloaders</returns>
        public static IFileDownloader CreateDownloaderFromUrl(string url, IPlugInContext context)
        {
            if (Uri.TryCreate(url, UriKind.Absolute, out Uri uri))
            {
                switch (uri.Scheme)
                {
                    case "https":
                        return new HttpDownloader();
                    case "file":
                        return new FileDownloader();
                    case "s3":
                        return new S3Downloader(context);
                    default:
                        throw new InvalidOperationException("Invalid PackageVersion. Only https://, file:// and s3// are supported");
                }
            }
            else
            {
                throw new InvalidOperationException("PackageVersion is not a valid Url");
            }
        }

        /// <summary>
        /// Parse the packageVersion string and return a model
        /// </summary>
        /// <param name="packageVersionString">Contents of packageVersion.json file</param>
        /// <returns>PackageVersionInfo model</returns>
        public static PackageVersionInfo ParsePackageVersion(string packageVersionString)
        {
            return JsonConvert.DeserializeObject<PackageVersionInfo>(packageVersionString);
        }

        /// <summary>
        /// Parse version string, such as 1.0.0.86 into a data structure
        /// </summary>
        /// <param name="versionString">Version string</param>
        /// <returns>Version data structure</returns>
        public static Version ParseVersion(string versionString)
        {
            if (Version.TryParse(versionString, out Version version))
            {
                return version;
            }
            else
            {
                throw new ArgumentException($"{versionString} is not a valid version string.");
            }
        }

        /// <summary>
        /// Resolve relative path (to KinesisTap) and expand environment variables such as %temp%
        /// </summary>
        /// <param name="path">Path to resolve</param>
        /// <returns>Resolved path</returns>
        public static string ResolvePath(string path)
        {
            string directory = Path.GetDirectoryName(path);
            string filename = Path.GetFileName(path);
            if (string.IsNullOrEmpty(directory) || directory.Equals("."))
            {
                string kinesisTapDirectory = Path.GetDirectoryName(ProgramInfo.KinesisTapPath);
                path = Path.Combine(kinesisTapDirectory, filename);
            }
            return Environment.ExpandEnvironmentVariables(path);
        }
    }
}
