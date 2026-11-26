// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package storage_test

import (
	// stdlib
	"os"
	"testing"

	// 3p
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
)

// The account key is a synthetic base64-encoded value for test-account-key.
const testConnectionString = "DefaultEndpointsProtocol=https;AccountName=connectionstringaccount;AccountKey=dGVzdC1hY2NvdW50LWtleQ==;EndpointSuffix=core.windows.net"

// The path where the AKS workload identity webhook mounts the service account token.
const testFederatedTokenFile = "/var/run/secrets/azure/tokens/azure-identity-token"

func TestNewAzureBlobClient(t *testing.T) {
	t.Parallel()

	t.Run("uses system-assigned managed identity", func(t *testing.T) {
		t.Parallel()
		// WHEN
		client, err := storage.NewAzureBlobClient(storage.AzureBlobConfig{
			AccountName: "identityaccount",
			Credential:  "managedidentity",
		})

		// THEN
		require.NoError(t, err)
		assert.Equal(t, "https://identityaccount.blob.core.windows.net", client.URL())
	})

	t.Run("uses a configured blob service URI", func(t *testing.T) {
		t.Parallel()
		client, err := storage.NewAzureBlobClient(storage.AzureBlobConfig{
			BlobServiceURI: "https://identityaccount.blob.core.chinacloudapi.cn",
			Credential:     "managedidentity",
		})

		require.NoError(t, err)
		assert.Equal(t, "https://identityaccount.blob.core.chinacloudapi.cn", client.URL())
	})

	t.Run("managed identity credential comparison is case insensitive", func(t *testing.T) {
		t.Parallel()
		client, err := storage.NewAzureBlobClient(storage.AzureBlobConfig{
			AccountName: "identityaccount",
			Credential:  "ManagedIdentity",
		})

		require.NoError(t, err)
		assert.Equal(t, "https://identityaccount.blob.core.windows.net", client.URL())
	})

	t.Run("requires an account name or blob service URI for managed identity", func(t *testing.T) {
		t.Parallel()
		client, err := storage.NewAzureBlobClient(storage.AzureBlobConfig{
			Credential: "managedidentity",
		})

		require.Error(t, err)
		assert.Nil(t, client)
	})

	t.Run("uses user-assigned managed identity", func(t *testing.T) {
		t.Parallel()
		// WHEN
		client, err := storage.NewAzureBlobClient(storage.AzureBlobConfig{
			AccountName:             "identityaccount",
			Credential:              "managedidentity",
			ManagedIdentityClientID: "00000000-0000-0000-0000-000000000000",
		})

		// THEN
		require.NoError(t, err)
		assert.Equal(t, "https://identityaccount.blob.core.windows.net", client.URL())
	})

	t.Run("uses connection string when credential is empty", func(t *testing.T) {
		t.Parallel()
		// WHEN
		client, err := storage.NewAzureBlobClient(storage.AzureBlobConfig{
			ConnectionString: testConnectionString,
		})

		// THEN
		require.NoError(t, err)
		assert.Equal(t, "https://connectionstringaccount.blob.core.windows.net/", client.URL())
	})

	t.Run("uses connection string when credential is empty and a federated token file is set", func(t *testing.T) {
		t.Parallel()
		// WHEN
		client, err := storage.NewAzureBlobClient(storage.AzureBlobConfig{
			ConnectionString:   testConnectionString,
			FederatedTokenFile: testFederatedTokenFile,
		})

		// THEN
		require.NoError(t, err)
		assert.Equal(t, "https://connectionstringaccount.blob.core.windows.net/", client.URL())
	})

	t.Run("uses connection string when credential is unsupported", func(t *testing.T) {
		t.Parallel()
		// WHEN
		client, err := storage.NewAzureBlobClient(storage.AzureBlobConfig{
			ConnectionString: testConnectionString,
			Credential:       "unsupported",
		})

		// THEN
		require.NoError(t, err)
		assert.Equal(t, "https://connectionstringaccount.blob.core.windows.net/", client.URL())
	})
}

func TestNewAzureBlobClientWithWorkloadIdentity(t *testing.T) {
	// The workload identity credential reads AZURE_TENANT_ID and AZURE_CLIENT_ID, so these subtests cannot run in parallel.
	t.Setenv("AZURE_TENANT_ID", "11111111-1111-1111-1111-111111111111")

	t.Run("uses the client ID set by the workload identity webhook", func(t *testing.T) {
		t.Setenv("AZURE_CLIENT_ID", "00000000-0000-0000-0000-000000000000")
		// WHEN
		client, err := storage.NewAzureBlobClient(storage.AzureBlobConfig{
			AccountName:        "identityaccount",
			Credential:         "managedidentity",
			FederatedTokenFile: testFederatedTokenFile,
		})

		// THEN
		require.NoError(t, err)
		assert.Equal(t, "https://identityaccount.blob.core.windows.net", client.URL())
	})

	t.Run("uses a configured client ID", func(t *testing.T) {
		unsetEnv(t, "AZURE_CLIENT_ID")
		// WHEN
		client, err := storage.NewAzureBlobClient(storage.AzureBlobConfig{
			AccountName:             "identityaccount",
			Credential:              "managedidentity",
			ManagedIdentityClientID: "00000000-0000-0000-0000-000000000000",
			FederatedTokenFile:      testFederatedTokenFile,
		})

		// THEN
		require.NoError(t, err)
		assert.Equal(t, "https://identityaccount.blob.core.windows.net", client.URL())
	})

	t.Run("requires a client ID", func(t *testing.T) {
		unsetEnv(t, "AZURE_CLIENT_ID")
		// WHEN
		client, err := storage.NewAzureBlobClient(storage.AzureBlobConfig{
			AccountName:        "identityaccount",
			Credential:         "managedidentity",
			FederatedTokenFile: testFederatedTokenFile,
		})

		// THEN
		// Managed identity accepts the same config without a client ID, so this error shows that the token file
		// selected workload identity.
		require.ErrorContains(t, err, "no client ID specified")
		assert.Nil(t, client)
	})
}

// unsetEnv unsets an environment variable for the test, since the Azure SDK treats a set but empty variable as set.
func unsetEnv(t *testing.T, key string) {
	t.Setenv(key, "")
	require.NoError(t, os.Unsetenv(key))
}

func TestAzureBlobConfigFromEnvironment(t *testing.T) {
	t.Setenv("AzureWebJobsStorage", testConnectionString)
	t.Setenv("AzureWebJobsStorage__accountName", "identityaccount")
	t.Setenv("AzureWebJobsStorage__blobServiceUri", "https://custom.blob.core.windows.net")
	t.Setenv("AzureWebJobsStorage__credential", "managedidentity")
	t.Setenv("AzureWebJobsStorage__clientId", "00000000-0000-0000-0000-000000000000")
	t.Setenv("AZURE_FEDERATED_TOKEN_FILE", testFederatedTokenFile)

	config := storage.AzureBlobConfigFromEnvironment()

	assert.Equal(t, storage.AzureBlobConfig{
		ConnectionString:        testConnectionString,
		AccountName:             "identityaccount",
		BlobServiceURI:          "https://custom.blob.core.windows.net",
		Credential:              "managedidentity",
		ManagedIdentityClientID: "00000000-0000-0000-0000-000000000000",
		FederatedTokenFile:      testFederatedTokenFile,
	}, config)
}
