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
 using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Amazon.S3;
using Amazon.S3.Model;

using Amazon.KinesisTap.AWS;
using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.AutoUpdate
{
    /// <summary>
    /// Download file from s3 using S3Client
    /// </summary>
    public class S3Downloader : IFileDownloader
    {
        private IAmazonS3 _s3Client;

        public S3Downloader(IPlugInContext context)
        {
            _s3Client = AWSUtilities.CreateAWSClient<AmazonS3Client>(context);
        }

        public async Task DownloadFileAsync(string url, string path)
        {
            GetObjectResponse response = await GetS3Object(url);
            await response.WriteResponseStreamToFileAsync(path, false, default(CancellationToken));
        }

        public async Task<string> ReadFileAsStringAsync(string url)
        {
            GetObjectResponse response = await GetS3Object(url);
            using (StreamReader reader = new StreamReader(response.ResponseStream))
            {
                return await reader.ReadToEndAsync();
            }
        }

        private async Task<GetObjectResponse> GetS3Object(string url)
        {
            (string bucketName, string key) = AWSUtilities.ParseS3Url(url);
            var response = await _s3Client.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = key
            });
            return response;
        }
    }
}