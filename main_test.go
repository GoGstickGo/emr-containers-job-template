package main

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfigFromEnv(t *testing.T) {
	t.Parallel()
	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	// Set environment variables for testing
	os.Setenv("AWS_REGION", "us-west-2")
	os.Setenv("PATH_YAML", "test.yaml")
	os.Setenv("SSM_PM_NAMES", "test-ssm")

	defer func() {
		os.Unsetenv("AWS_REGION")
		os.Unsetenv("PATH_YAML")
		os.Unsetenv("SSM_PM_NAMES")
	}()

	cfg, _ := loadConfigFromEnv(logger)

	assert.Equal(t, "us-west-2", cfg.AWSRegion)
	assert.Equal(t, "test.yaml", cfg.PathYAML)
	assert.Equal(t, "test-ssm", cfg.PmNames[0])
}

func TestLoadConfigFromEnvMoreSSM(t *testing.T) {
	t.Parallel()
	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	// Set environment variables for testing
	os.Setenv("AWS_REGION", "us-west-2")
	os.Setenv("PATH_YAML", "test.yaml")
	os.Setenv("SSM_PM_NAMES", "test-ssm,test-ssm2,test-ssm3")

	defer func() {
		os.Unsetenv("AWS_REGION")
		os.Unsetenv("PATH_YAML")
		os.Unsetenv("SSM_PM_NAMES")
	}()

	cfg, _ := loadConfigFromEnv(logger)

	assert.Equal(t, "us-west-2", cfg.AWSRegion)
	assert.Equal(t, "test.yaml", cfg.PathYAML)
	assert.Equal(t, "test-ssm", cfg.PmNames[0])
	assert.Equal(t, "test-ssm2", cfg.PmNames[1])
	assert.Equal(t, "test-ssm3", cfg.PmNames[2])
}

func TestLoadConfigFromEnv_Defaults(t *testing.T) {
	t.Parallel()
	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	// Unset environment variables to test defaults
	os.Unsetenv("AWS_REGION")
	os.Unsetenv("PATH_YAML")
	os.Setenv("SSM_PM_NAMES", "test-ssm")

	defer func() {
		os.Unsetenv("SSM_PM_NAMES")
	}()

	cfg, _ := loadConfigFromEnv(logger)

	assert.Equal(t, "us-east-1", cfg.AWSRegion)
	assert.Equal(t, "example.yaml", cfg.PathYAML)
	assert.Equal(t, "test-ssm", cfg.PmNames[0])
}

func TestLoadConfigFromEnv_MissingSSMName(t *testing.T) {
	t.Parallel()
	logger := logrus.New()
	//logger.SetOutput(nil) // Suppress logs during testing

	// Backup existing SSM_NAME to restore after test
	originalSSMName, exists := os.LookupEnv("SSM_PM_NAMES")
	if exists {
		defer os.Setenv("SSM_PM_NAMES", originalSSMName)
	} else {
		defer os.Unsetenv("SSM_PM_NAMES")
	}
	// Unset SSM_NAME to simulate missing configuration
	os.Unsetenv("SSM_PM_NAMES")
	// Call loadConfigFromEnv
	cfg, err := loadConfigFromEnv(logger)

	// Assertions
	require.Error(t, err, "Expected an error due to missing SSM_PM_NAMES")
	assert.Equal(t, "SSM parameter name must be defined", err.Error())
	assert.Empty(t, cfg.PmNames, "Expected SSMName to be empty")
}
