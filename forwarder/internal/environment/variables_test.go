package environment_test

import (
	// stdlib
	"os"
	"testing"

	// 3p
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/environment"
)

func TestEnabled(t *testing.T) {
	t.Parallel()

	t.Run("enabled returns true when env var set to true", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		testKey := uuid.New().String()
		err := os.Setenv(testKey, "true")
		require.NoError(t, err)

		// WHEN
		got := environment.Enabled(testKey)

		// THEN
		assert.True(t, got)
	})

	t.Run("enabled returns false when env var set to false", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		testKey := uuid.New().String()
		err := os.Setenv(testKey, "false")
		require.NoError(t, err)

		// WHEN
		got := environment.Enabled(testKey)

		// THEN
		assert.False(t, got)
	})

	t.Run("enabled returns false when env var is not set", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		testKey := uuid.New().String()

		// WHEN
		got := environment.Enabled(testKey)

		// THEN
		assert.False(t, got)
	})
}

func TestGetEnv(t *testing.T) {
	t.Parallel()

	t.Run("GetEnv returns correct value if env var is set", func(t *testing.T) {
		// GIVEN
		testKey := uuid.New().String()
		const testValue = "hello world"
		err := os.Setenv(testKey, testValue)
		require.NoError(t, err)

		// WHEN
		envVarValue := environment.GetEnvVar(testKey)

		// THEN
		assert.Equal(t, envVarValue, testValue, "values should be equal")
	})

	t.Run("GetEnv returns empty string if env var is not set", func(t *testing.T) {
		// GIVEN
		testKey := uuid.New().String()

		// WHEN
		envVarValue := environment.GetEnvVar(testKey)

		// THEN
		assert.Equal(t, "", envVarValue, "envVarValue should be empty string")
	})
}
