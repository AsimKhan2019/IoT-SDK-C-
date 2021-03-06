// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Devices.Common.Service.Auth
{
    using System;

    /// <summary>
    /// Authentication method that uses a shared access policy token. 
    /// </summary>
    internal sealed class ServiceAuthenticationWithSharedAccessPolicyToken : IAuthenticationMethod
    {
        string policyName;
        string token;

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceAuthenticationWithSharedAccessPolicyToken"/> class.
        /// </summary>
        /// <param name="policyName">Name of the shared access policy to use.</param>
        /// <param name="token">Token associated with the shared access policy.</param>
        public ServiceAuthenticationWithSharedAccessPolicyToken(string policyName, string token)
        {
            this.SetPolicyName(policyName);
            this.SetToken(token);
        }

        public string PolicyName
        {
            get { return this.policyName; }
            set { this.SetPolicyName(value); }
        }

        public string Token
        {
            get { return this.token; }
            set { this.SetToken(value); }
        }

        public ServiceConnectionStringBuilder Populate(ServiceConnectionStringBuilder provisioningConnectionStringBuilder)
        {
            if (provisioningConnectionStringBuilder == null)
            {
                throw new ArgumentNullException(nameof(provisioningConnectionStringBuilder));
            }

            provisioningConnectionStringBuilder.SharedAccessKeyName = this.PolicyName;
            provisioningConnectionStringBuilder.SharedAccessSignature = this.Token;
            provisioningConnectionStringBuilder.SharedAccessKey = null;

            return provisioningConnectionStringBuilder;
        }

        private void SetPolicyName(string policyName)
        {
            if (string.IsNullOrWhiteSpace(policyName))
            {
                throw new ArgumentNullException(nameof(policyName));
            }

            this.policyName = policyName;
        }

        private void SetToken(string token)
        {
            if (string.IsNullOrWhiteSpace(token))
            {
                throw new ArgumentNullException(nameof(token));
            }

            if (!token.StartsWith(SharedAccessSignatureConstants.SharedAccessSignature, StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException("Token must be of type SharedAccessSignature");
            }

            this.token = token;
        }
    }
}
