// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package storage_test

import (
	// stdlib
	"testing"

	// 3p
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
)

const testConnectionString = "DefaultEndpointsProtocol=https;AccountName=connectionstringaccount;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;EndpointSuffix=core.windows.net"

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

func TestAzureBlobConfigFromEnvironment(t *testing.T) {
	t.Setenv("AzureWebJobsStorage", testConnectionString)
	t.Setenv("AzureWebJobsStorage__accountName", "identityaccount")
	t.Setenv("AzureWebJobsStorage__credential", "managedidentity")
	t.Setenv("AzureWebJobsStorage__clientId", "00000000-0000-0000-0000-000000000000")

	config := storage.AzureBlobConfigFromEnvironment()

	assert.Equal(t, storage.AzureBlobConfig{
		ConnectionString:        testConnectionString,
		AccountName:             "identityaccount",
		Credential:              "managedidentity",
		ManagedIdentityClientID: "00000000-0000-0000-0000-000000000000",
	}, config)
}
