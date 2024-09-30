package main

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestLoadConfigFromEnv(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	// Set environment variables for testing
	os.Setenv("AWS_REGION", "us-west-2")
	os.Setenv("PATH_YAML", "test.yaml")
	os.Setenv("SSM_NAME", "test-ssm")

	defer func() {
		os.Unsetenv("AWS_REGION")
		os.Unsetenv("PATH_YAML")
		os.Unsetenv("SSM_NAME")
	}()

	cfg, _ := loadConfigFromEnv(logger)

	assert.Equal(t, "us-west-2", cfg.AWSRegion)
	assert.Equal(t, "test.yaml", cfg.PathYAML)
	assert.Equal(t, "test-ssm", cfg.SSMName)
}

func TestLoadConfigFromEnv_Defaults(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	// Unset environment variables to test defaults
	os.Unsetenv("AWS_REGION")
	os.Unsetenv("PATH_YAML")
	os.Setenv("SSM_NAME", "test-ssm")

	defer func() {
		os.Unsetenv("SSM_NAME")
	}()

	cfg, _ := loadConfigFromEnv(logger)

	assert.Equal(t, "us-east-1", cfg.AWSRegion)
	assert.Equal(t, "example.yaml", cfg.PathYAML)
	assert.Equal(t, "test-ssm", cfg.SSMName)
}

func TestLoadConfigFromEnv_MissingSSMName(t *testing.T) {
	logger := logrus.New()
	//logger.SetOutput(nil) // Suppress logs during testing

	// Backup existing SSM_NAME to restore after test
	originalSSMName, exists := os.LookupEnv("SSM_NAME")
	if exists {
		defer os.Setenv("SSM_NAME", originalSSMName)
	} else {
		defer os.Unsetenv("SSM_NAME")
	}
	// Unset SSM_NAME to simulate missing configuration
	os.Unsetenv("SSM_NAME")
	// Call loadConfigFromEnv
	cfg, err := loadConfigFromEnv(logger)

	// Assertions
	assert.Error(t, err, "Expected an error due to missing SSM_NAME")
	assert.Equal(t, "SSM parameter name must be defined", err.Error())
	assert.Equal(t, "us-east-1", cfg.AWSRegion, "Expected AWSRegion to be set to default 'us-east-1'")
	assert.Equal(t, "example.yaml", cfg.PathYAML, "Expected PathYAML to be set to default 'example.yaml'")
	assert.Empty(t, cfg.SSMName, "Expected SSMName to be empty")
}
