package awsutils

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/stretchr/testify/assert"
)

type MockSSMAWSConfigLoader struct {
	LoadConfigFunc func(ctx context.Context, region string) (aws.Config, error)
}

// LoadConfig calls the mock function, allowing the user to define the behavior.
func (m *MockSSMAWSConfigLoader) LoadConfig(ctx context.Context, region string) (aws.Config, error) {
	return m.LoadConfigFunc(ctx, region)
}

// MockSSMClient is a mock implementation of SSMClient
type MockSSMClient struct {
	PutParameterFunc func(ctx context.Context, params *ssm.PutParameterInput, optFns ...func(*ssm.Options)) (*ssm.PutParameterOutput, error)
}

func (m *MockSSMClient) PutParameter(ctx context.Context, params *ssm.PutParameterInput, optFns ...func(*ssm.Options)) (*ssm.PutParameterOutput, error) {
	return m.PutParameterFunc(ctx, params, optFns...)
}

func TestUpdateSSMParameter_Success(t *testing.T) {
	// Mock the AWS configuration loader
	mockLoader := &MockSSMAWSConfigLoader{
		LoadConfigFunc: func(ctx context.Context, region string) (aws.Config, error) {
			// Return a dummy AWS config here
			return aws.Config{}, nil
		},
	}

	// Call the LoadConfig method on the mock loader
	cfg, err := mockLoader.LoadConfig(context.TODO(), "us-east-1")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Assert the returned config (you can further assert the values if needed)
	if cfg.Region != "" {
		t.Errorf("Expected empty region, got %s", cfg.Region)
	}

	mockClient := &MockSSMClient{
		PutParameterFunc: func(ctx context.Context, params *ssm.PutParameterInput, optFns ...func(*ssm.Options)) (*ssm.PutParameterOutput, error) {
			// Simulate success
			return &ssm.PutParameterOutput{}, nil
		},
	}

	name := "test-parameter"
	value := "test-value"
	ctxTimeOut, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Act
	err = UpdateSSMParameter(ctxTimeOut, mockClient, name, value)

	// Assert
	assert.NoError(t, err)
}

func TestUpdateSSMParameter_Failure(t *testing.T) {
	// Mock the AWS configuration loader
	mockLoader := &MockSSMAWSConfigLoader{
		LoadConfigFunc: func(ctx context.Context, region string) (aws.Config, error) {
			// Return a dummy AWS config here
			return aws.Config{}, nil
		},
	}

	// Call the LoadConfig method on the mock loader
	cfg, err := mockLoader.LoadConfig(context.TODO(), "us-east-1")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Assert the returned config (you can further assert the values if needed)
	if cfg.Region != "" {
		t.Errorf("Expected empty region, got %s", cfg.Region)
	}

	mockClient := &MockSSMClient{
		PutParameterFunc: func(ctx context.Context, params *ssm.PutParameterInput, optFns ...func(*ssm.Options)) (*ssm.PutParameterOutput, error) {
			// Simulate failure
			return nil, fmt.Errorf("failed success")
		},
	}

	name := "test-parameter"
	value := "test-value"
	ctxTimeOut, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Act
	err = UpdateSSMParameter(ctxTimeOut, mockClient, name, value)

	// Assert
	assert.ErrorContainsf(t, err, "ssm update failed err", err.Error())
}
