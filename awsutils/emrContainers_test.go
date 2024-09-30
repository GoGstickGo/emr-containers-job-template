package awsutils

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/GoGstickGo/emr-containers-template/template"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emrcontainers"
	"github.com/aws/aws-sdk-go-v2/service/emrcontainers/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var def string = "value"

type MockEMRAWSConfigLoader struct {
	LoadConfigFunc func(ctx context.Context, region string) (aws.Config, error)
}

// LoadConfig calls the mock function, allowing the user to define the behavior.
func (m *MockEMRAWSConfigLoader) LoadConfig(ctx context.Context, region string) (aws.Config, error) {
	return m.LoadConfigFunc(ctx, region)
}

// MockEMRCclient is a mock implementation of SSMClient
type MockEMRCclient struct {
	DescribeJobTemplateFunc func(ctx context.Context, params *emrcontainers.DescribeJobTemplateInput, optFns ...func(*emrcontainers.Options)) (*emrcontainers.DescribeJobTemplateOutput, error)
	CreateJobTemplateFunc   func(ctx context.Context, params *emrcontainers.CreateJobTemplateInput, optFns ...func(*emrcontainers.Options)) (*emrcontainers.CreateJobTemplateOutput, error)
}

func (m *MockEMRCclient) DescribeJobTemplate(ctx context.Context, params *emrcontainers.DescribeJobTemplateInput, optFns ...func(*emrcontainers.Options)) (*emrcontainers.DescribeJobTemplateOutput, error) {
	return m.DescribeJobTemplateFunc(ctx, params, optFns...)
}
func (m *MockEMRCclient) CreateJobTemplate(ctx context.Context, params *emrcontainers.CreateJobTemplateInput, optFns ...func(*emrcontainers.Options)) (*emrcontainers.CreateJobTemplateOutput, error) {
	return m.CreateJobTemplateFunc(ctx, params, optFns...)
}

// MockParameterConfigurator is a mock implementation of ParameterConfigurator
type MockParameterConfigurator struct {
	mock.Mock
}

func (m *MockParameterConfigurator) Configure(paramConfig map[string]template.TemplateParameterConfiguration) (map[string]types.TemplateParameterConfiguration, error) {
	args := m.Called(paramConfig)
	var output map[string]types.TemplateParameterConfiguration
	if temp := args.Get(0); temp != nil {
		output = temp.(map[string]types.TemplateParameterConfiguration)
	}
	return output, args.Error(1)
}

// MockSparkSubmitCommandBuilder is a mock implementation of SparkSubmitCommandBuilder
type MockSparkSubmitCommandBuilder struct {
	mock.Mock
}

func (m *MockSparkSubmitCommandBuilder) Build(params template.SparkSubmitParameters) (string, error) {
	args := m.Called(params)
	return args.String(0), args.Error(1)
}

func MockRandomIntn(returnValue int) func(int) int {
	return func(n int) int {
		return returnValue
	}
}

func TestPrepareJobTemplateInput_Success(t *testing.T) {
	// Prepare jobConfig with necessary fields
	jobConfig := template.JobTemplateConfig{
		Name: "test-job-template",
		Tags: map[string]string{
			"Environment": "test",
		},
		ExecutionRoleArn: "arn:aws:iam::123456789012:role/EMRExecutionRole",
		ReleaseLabel:     "emr-6.2.0",
		EntryPoint:       "s3://my-bucket/my-script.py",
		EntryPointArguments: []string{
			"--input", "s3://my-bucket/input",
			"--output", "s3://my-bucket/output",
		},
		PersistentAppUI: "ENABLED",
		LogGroupName:    "/aws/emr-containers/jobs",
		SparkSubmitParameters: template.SparkSubmitParameters{
			Master:     "yarn",
			DeployMode: "cluster",
			Class:      "org.apache.spark.examples.SparkPi",
			Conf:       []string{"spark.executor.memory=2g"},
			Packages:   "org.apache.spark:spark-sql_2.12:3.0.1",
		},
		ParameterConfiguration: map[string]template.TemplateParameterConfiguration{
			"Param1": {
				DefaultValue: &def,
				Type:         "STRING",
			},
		},
		ApplicationConfigurations: []template.ApplicationConfiguration{
			{
				Classification: "spark-defaults",
				Properties: map[string]string{
					"spark.dynamicAllocation.enabled": "false",
				},
			},
		},
	}

	// Expected outputs from helper functions
	expectedParameterConfig := map[string]types.TemplateParameterConfiguration{
		"Param1": {
			DefaultValue: aws.String(def),
			Type:         types.TemplateParameterDataTypeString,
		},
	}

	expectedSparkSubmitParametersConfig := "--master yarn --deploy-mode cluster --class org.apache.spark.examples.SparkPi --conf spark.executor.memory=2g --packages org.apache.spark:spark-sql_2.12:3.0.1"

	// Initialize mocks
	mockConfigurator := new(MockParameterConfigurator)
	mockConfigurator.On("Configure", jobConfig.ParameterConfiguration).Return(expectedParameterConfig, nil)

	mockCommandBuilder := new(MockSparkSubmitCommandBuilder)
	mockCommandBuilder.On("Build", jobConfig.SparkSubmitParameters).Return(expectedSparkSubmitParametersConfig, nil)

	// Mock randomIntn to return a fixed value
	mockRandom := MockRandomIntn(12345)

	// Call PrepareJobTemplateInput
	input, err := PrepareJobTemplateInput(jobConfig, mockConfigurator, mockCommandBuilder, mockRandom)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, input)
	assert.Equal(t, aws.String("test-job-template"), input.Name)
	assert.Equal(t, aws.String("12345"), input.ClientToken)
	assert.Equal(t, aws.String("arn:aws:iam::123456789012:role/EMRExecutionRole"), input.JobTemplateData.ExecutionRoleArn)
	assert.Equal(t, aws.String("emr-6.2.0"), input.JobTemplateData.ReleaseLabel)
	assert.Equal(t, aws.String("ENABLED"), input.JobTemplateData.ConfigurationOverrides.MonitoringConfiguration.PersistentAppUI)
	assert.Equal(t, aws.String("/aws/emr-containers/jobs"), input.JobTemplateData.ConfigurationOverrides.MonitoringConfiguration.CloudWatchMonitoringConfiguration.LogGroupName)
	assert.Equal(t, aws.String("test-job-template"), input.JobTemplateData.ConfigurationOverrides.MonitoringConfiguration.CloudWatchMonitoringConfiguration.LogStreamNamePrefix)

	// Verify SparkSubmitJobDriver fields
	jobDriver := input.JobTemplateData.JobDriver.SparkSubmitJobDriver
	assert.Equal(t, aws.String("s3://my-bucket/my-script.py"), jobDriver.EntryPoint)
	assert.Equal(t, []string{"--input", "s3://my-bucket/input", "--output", "s3://my-bucket/output"}, jobDriver.EntryPointArguments)
	assert.Equal(t, aws.String(expectedSparkSubmitParametersConfig), jobDriver.SparkSubmitParameters)

	// Verify ConfigurationOverrides
	configOverrides := input.JobTemplateData.ConfigurationOverrides
	assert.Len(t, configOverrides.ApplicationConfiguration, 1)
	assert.Equal(t, aws.String("spark-defaults"), configOverrides.ApplicationConfiguration[0].Classification)
	assert.Equal(t, map[string]string{"spark.dynamicAllocation.enabled": "false"}, configOverrides.ApplicationConfiguration[0].Properties)

	// Verify ParameterConfiguration
	assert.Equal(t, expectedParameterConfig, input.JobTemplateData.ParameterConfiguration)

	// Verify JobTags
	expectedTags := map[string]string{
		"Environment": "test",
		"Name":        "test-job-template",
	}
	assert.Equal(t, expectedTags, input.JobTemplateData.JobTags)

	// Assert that the mocks were called as expected
	mockConfigurator.AssertExpectations(t)
	mockCommandBuilder.AssertExpectations(t)
}

func TestPrepareJobTemplateInput_HelperParameterConfigurationError(t *testing.T) {
	// Prepare jobConfig with necessary fields
	jobConfig := template.JobTemplateConfig{
		Name: "test-job-template",
		Tags: map[string]string{
			"Environment": "test",
		},
		ExecutionRoleArn: "arn:aws:iam::123456789012:role/EMRExecutionRole",
		ReleaseLabel:     "emr-6.2.0",
		EntryPoint:       "s3://my-bucket/my-script.py",
		EntryPointArguments: []string{
			"--input", "s3://my-bucket/input",
			"--output", "s3://my-bucket/output",
		},
		PersistentAppUI: "ENABLED",
		LogGroupName:    "/aws/emr-containers/jobs",
		SparkSubmitParameters: template.SparkSubmitParameters{
			Master:     "yarn",
			DeployMode: "cluster",
			Class:      "org.apache.spark.examples.SparkPi",
			Conf:       []string{"spark.executor.memory=2g"},
			Packages:   "org.apache.spark:spark-sql_2.12:3.0.1",
		},
		ParameterConfiguration: map[string]template.TemplateParameterConfiguration{
			"Param1": {
				DefaultValue: &def,
				Type:         "STRING",
			},
		},
		ApplicationConfigurations: []template.ApplicationConfiguration{
			{
				Classification: "spark-defaults",
				Properties: map[string]string{
					"spark.dynamicAllocation.enabled": "false",
				},
			},
		},
	}

	// Initialize mocks
	mockConfigurator := new(MockParameterConfigurator)
	mockConfigurator.On("Configure", jobConfig.ParameterConfiguration).Return(nil, fmt.Errorf("mocked helperParameterConfiguration error"))

	// The command builder should not be called; hence, no expectation set
	mockCommandBuilder := new(MockSparkSubmitCommandBuilder)

	// Mock randomIntn (should not be used)
	mockRandom := MockRandomIntn(12345)

	// Call PrepareJobTemplateInput
	input, err := PrepareJobTemplateInput(jobConfig, mockConfigurator, mockCommandBuilder, mockRandom)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parameter configuration block failed")
	assert.Nil(t, input)

	// Assert that helperParameterConfiguration was called
	mockConfigurator.AssertExpectations(t)
	// Since helpersBuildSparkSubmitCommand should not be called, verify no expectations
	mockCommandBuilder.AssertExpectations(t)
}

func TestPrepareJobTemplateInput_HelpersBuildSparkSubmitCommandError(t *testing.T) {
	// Prepare jobConfig with necessary fields
	jobConfig := template.JobTemplateConfig{
		Name: "test-job-template",
		Tags: map[string]string{
			"Environment": "test",
		},
		ExecutionRoleArn: "arn:aws:iam::123456789012:role/EMRExecutionRole",
		ReleaseLabel:     "emr-6.2.0",
		EntryPoint:       "s3://my-bucket/my-script.py",
		EntryPointArguments: []string{
			"--input", "s3://my-bucket/input",
			"--output", "s3://my-bucket/output",
		},
		PersistentAppUI: "ENABLED",
		LogGroupName:    "/aws/emr-containers/jobs",
		SparkSubmitParameters: template.SparkSubmitParameters{
			Master:     "yarn",
			DeployMode: "cluster",
			Class:      "org.apache.spark.examples.SparkPi",
			Conf:       []string{"spark.executor.memory=2g"},
			Packages:   "org.apache.spark:spark-sql_2.12:3.0.1",
		},
		ParameterConfiguration: map[string]template.TemplateParameterConfiguration{
			"Param1": {
				DefaultValue: &def,
				Type:         "STRING",
			},
		},
		ApplicationConfigurations: []template.ApplicationConfiguration{
			{
				Classification: "spark-defaults",
				Properties: map[string]string{
					"spark.dynamicAllocation.enabled": "false",
				},
			},
		},
	}

	// Expected outputs from helperParameterConfiguration
	expectedParameterConfig := map[string]types.TemplateParameterConfiguration{
		"Param1": {
			DefaultValue: aws.String(def),
			Type:         types.TemplateParameterDataTypeString,
		},
	}

	// Initialize mocks
	mockConfigurator := new(MockParameterConfigurator)
	mockConfigurator.On("Configure", jobConfig.ParameterConfiguration).Return(expectedParameterConfig, nil)

	mockCommandBuilder := new(MockSparkSubmitCommandBuilder)
	mockCommandBuilder.On("Build", jobConfig.SparkSubmitParameters).Return("", fmt.Errorf("mocked helpersBuildSparkSubmitCommand error"))

	// Mock randomIntn (should not be used)
	mockRandom := MockRandomIntn(12345)

	// Call PrepareJobTemplateInput
	input, err := PrepareJobTemplateInput(jobConfig, mockConfigurator, mockCommandBuilder, mockRandom)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sparkSubmitParameters configuration block failed")
	assert.Nil(t, input)

	// Assert that the mocks were called as expected
	mockConfigurator.AssertExpectations(t)
	mockCommandBuilder.AssertExpectations(t)
}

func TestPrepareJobTemplateInput_NilTags(t *testing.T) {
	// Prepare jobConfig with Tags as nil
	jobConfig := template.JobTemplateConfig{
		Name:             "test-job-template",
		Tags:             nil, // Tags are nil
		ExecutionRoleArn: "arn:aws:iam::123456789012:role/EMRExecutionRole",
		ReleaseLabel:     "emr-6.2.0",
		EntryPoint:       "s3://my-bucket/my-script.py",
		EntryPointArguments: []string{
			"--input", "s3://my-bucket/input",
			"--output", "s3://my-bucket/output",
		},
		PersistentAppUI: "ENABLED",
		LogGroupName:    "/aws/emr-containers/jobs",
		SparkSubmitParameters: template.SparkSubmitParameters{
			Master:     "yarn",
			DeployMode: "cluster",
			Class:      "org.apache.spark.examples.SparkPi",
			Conf:       []string{"spark.executor.memory=2g"},
			Packages:   "org.apache.spark:spark-sql_2.12:3.0.1",
		},
		ParameterConfiguration: map[string]template.TemplateParameterConfiguration{
			"Param1": {
				DefaultValue: &def,
				Type:         "STRING",
			},
		},
		ApplicationConfigurations: []template.ApplicationConfiguration{
			{
				Classification: "spark-defaults",
				Properties: map[string]string{
					"spark.dynamicAllocation.enabled": "false",
				},
			},
		},
	}

	// Expected outputs from helper functions
	expectedParameterConfig := map[string]types.TemplateParameterConfiguration{
		"Param1": {
			DefaultValue: aws.String(def),
			Type:         types.TemplateParameterDataTypeString,
		},
	}

	expectedSparkSubmitParametersConfig := "--master yarn --deploy-mode cluster --class org.apache.spark.examples.SparkPi --conf spark.executor.memory=2g --packages org.apache.spark:spark-sql_2.12:3.0.1"

	// Initialize mocks
	mockConfigurator := new(MockParameterConfigurator)
	mockConfigurator.On("Configure", jobConfig.ParameterConfiguration).Return(expectedParameterConfig, nil)

	mockCommandBuilder := new(MockSparkSubmitCommandBuilder)
	mockCommandBuilder.On("Build", jobConfig.SparkSubmitParameters).Return(expectedSparkSubmitParametersConfig, nil)

	// Mock randomIntn to return a fixed value
	mockRandom := MockRandomIntn(12345)

	// Call PrepareJobTemplateInput
	input, err := PrepareJobTemplateInput(jobConfig, mockConfigurator, mockCommandBuilder, mockRandom)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, input)
	assert.Equal(t, aws.String("test-job-template"), input.Name)
	assert.Equal(t, aws.String("12345"), input.ClientToken)
	assert.Equal(t, aws.String("arn:aws:iam::123456789012:role/EMRExecutionRole"), input.JobTemplateData.ExecutionRoleArn)
	assert.Equal(t, aws.String("emr-6.2.0"), input.JobTemplateData.ReleaseLabel)
	assert.Equal(t, aws.String("ENABLED"), input.JobTemplateData.ConfigurationOverrides.MonitoringConfiguration.PersistentAppUI)
	assert.Equal(t, aws.String("/aws/emr-containers/jobs"), input.JobTemplateData.ConfigurationOverrides.MonitoringConfiguration.CloudWatchMonitoringConfiguration.LogGroupName)
	assert.Equal(t, aws.String("test-job-template"), input.JobTemplateData.ConfigurationOverrides.MonitoringConfiguration.CloudWatchMonitoringConfiguration.LogStreamNamePrefix)

	// Verify SparkSubmitJobDriver fields
	jobDriver := input.JobTemplateData.JobDriver.SparkSubmitJobDriver
	assert.Equal(t, aws.String("s3://my-bucket/my-script.py"), jobDriver.EntryPoint)
	assert.Equal(t, []string{"--input", "s3://my-bucket/input", "--output", "s3://my-bucket/output"}, jobDriver.EntryPointArguments)
	assert.Equal(t, aws.String(expectedSparkSubmitParametersConfig), jobDriver.SparkSubmitParameters)

	// Verify ConfigurationOverrides
	configOverrides := input.JobTemplateData.ConfigurationOverrides
	assert.Len(t, configOverrides.ApplicationConfiguration, 1)
	assert.Equal(t, aws.String("spark-defaults"), configOverrides.ApplicationConfiguration[0].Classification)
	assert.Equal(t, map[string]string{"spark.dynamicAllocation.enabled": "false"}, configOverrides.ApplicationConfiguration[0].Properties)

	// Verify ParameterConfiguration
	assert.Equal(t, expectedParameterConfig, input.JobTemplateData.ParameterConfiguration)

	// Verify JobTags
	expectedTags := map[string]string{
		"Name": "test-job-template",
	}
	assert.Equal(t, expectedTags, input.JobTemplateData.JobTags)

	// Assert that the mocks were called as expected
	mockConfigurator.AssertExpectations(t)
	mockCommandBuilder.AssertExpectations(t)
}

func TestDescribeJobTemplate_Success(t *testing.T) {
	ctxTimeOut, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Mock the AWS configuration loader
	mockLoader := &MockEMRAWSConfigLoader{
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
	jobTemplateID := "test"

	mockClient := &MockEMRCclient{
		DescribeJobTemplateFunc: func(ctx context.Context, params *emrcontainers.DescribeJobTemplateInput, optFns ...func(*emrcontainers.Options)) (*emrcontainers.DescribeJobTemplateOutput, error) {
			return &emrcontainers.DescribeJobTemplateOutput{}, nil
		},
	}

	_, err = DescribeJobTemplate(ctxTimeOut, mockClient, jobTemplateID)

	assert.NoError(t, err)
}

func TestDescribeJobTemplate_Failure(t *testing.T) {
	ctxTimeOut, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Mock the AWS configuration loader
	mockLoader := &MockEMRAWSConfigLoader{
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
	jobTemplateID := "test"

	mockClient := &MockEMRCclient{
		DescribeJobTemplateFunc: func(ctx context.Context, params *emrcontainers.DescribeJobTemplateInput, optFns ...func(*emrcontainers.Options)) (*emrcontainers.DescribeJobTemplateOutput, error) {
			return nil, fmt.Errorf("failed success")
		},
	}

	_, err = DescribeJobTemplate(ctxTimeOut, mockClient, jobTemplateID)

	// Assert
	assert.ErrorContainsf(t, err, "failed to describe job template:", err.Error())
}
func Test_helperParameterConfiguration(t *testing.T) {
	var def string = "default"

	type args struct {
		paramConfig map[string]template.TemplateParameterConfiguration
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]types.TemplateParameterConfiguration
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "valid input with STRING type",
			args: args{
				paramConfig: map[string]template.TemplateParameterConfiguration{
					"param1": {
						DefaultValue: &def,
						Type:         "STRING",
					},
				},
			},
			want: map[string]types.TemplateParameterConfiguration{
				"param1": {
					DefaultValue: &def,
					Type:         types.TemplateParameterDataTypeString,
				},
			},
			wantErr: false,
		}, {
			name: "invalid parameter type",
			args: args{
				paramConfig: map[string]template.TemplateParameterConfiguration{
					"param1": {
						DefaultValue: &def,
						Type:         "INVALID",
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := helperParameterConfiguration(tt.args.paramConfig)
			if (err != nil) != tt.wantErr {
				t.Errorf("helperParameterConfiguration() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("helperParameterConfiguration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_helpersBuildSparkSubmitCommand(t *testing.T) {
	type args struct {
		params template.SparkSubmitParameters
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Valid Input",
			args: args{
				params: template.SparkSubmitParameters{
					Master:     "local[*]",
					DeployMode: "client",
					Class:      "org.example.Main",
					Conf:       []string{"spark.executor.memory=2g", "spark.driver.memory=1g"},
					Packages:   "org.apache.spark:spark-sql_2.12:3.0.1",
				},
			},
			want:    "--master local[*] --deploy-mode client --class org.example.Main --conf spark.executor.memory=2g --conf spark.driver.memory=1g --packages org.apache.spark:spark-sql_2.12:3.0.1",
			wantErr: false,
		},
		{
			name: "Missing Master Parameter",
			args: args{
				params: template.SparkSubmitParameters{
					Master:     "",
					DeployMode: "client",
					Class:      "org.example.Main",
					Conf:       []string{"spark.executor.memory=2g"},
					Packages:   "org.apache.spark:spark-sql_2.12:3.0.1",
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "Missing DeployMode Parameter",
			args: args{
				params: template.SparkSubmitParameters{
					Master:     "local[*]",
					DeployMode: "",
					Class:      "org.example.Main",
					Conf:       []string{"spark.executor.memory=2g"},
					Packages:   "org.apache.spark:spark-sql_2.12:3.0.1",
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "Missing Class Parameter",
			args: args{
				params: template.SparkSubmitParameters{
					Master:     "local[*]",
					DeployMode: "client",
					Class:      "",
					Conf:       []string{"spark.executor.memory=2g"},
					Packages:   "org.apache.spark:spark-sql_2.12:3.0.1",
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "Missing Packages Parameter",
			args: args{
				params: template.SparkSubmitParameters{
					Master:     "local[*]",
					DeployMode: "client",
					Class:      "org.example.Main",
					Conf:       []string{"spark.executor.memory=2g"},
					Packages:   "",
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "Empty Conf Entry",
			args: args{
				params: template.SparkSubmitParameters{
					Master:     "local[*]",
					DeployMode: "client",
					Class:      "org.example.Main",
					Conf:       []string{"spark.executor.memory=2g", ""},
					Packages:   "org.apache.spark:spark-sql_2.12:3.0.1",
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "Empty Conf Slice",
			args: args{
				params: template.SparkSubmitParameters{
					Master:     "local[*]",
					DeployMode: "client",
					Class:      "org.example.Main",
					Conf:       []string{},
					Packages:   "org.apache.spark:spark-sql_2.12:3.0.1",
				},
			},
			want:    "--master local[*] --deploy-mode client --class org.example.Main --packages org.apache.spark:spark-sql_2.12:3.0.1",
			wantErr: false,
		},
		{
			name: "Parameters with Whitespace",
			args: args{
				params: template.SparkSubmitParameters{
					Master:     " local[*] ",
					DeployMode: " client ",
					Class:      " org.example.Main ",
					Conf:       []string{" spark.executor.memory=2g "},
					Packages:   " org.apache.spark:spark-sql_2.12:3.0.1 ",
				},
			},
			want:    "--master  local[*]  --deploy-mode  client  --class  org.example.Main  --conf  spark.executor.memory=2g  --packages  org.apache.spark:spark-sql_2.12:3.0.1",
			wantErr: false,
		},
		{
			name: "Special Characters in Conf",
			args: args{
				params: template.SparkSubmitParameters{
					Master:     "yarn",
					DeployMode: "cluster",
					Class:      "org.example.Main",
					Conf:       []string{"spark.executor.extraJavaOptions='-Dconfig.file=path/to/config'"},
					Packages:   "org.apache.spark:spark-sql_2.12:3.0.1",
				},
			},
			want:    "--master yarn --deploy-mode cluster --class org.example.Main --conf spark.executor.extraJavaOptions='-Dconfig.file=path/to/config' --packages org.apache.spark:spark-sql_2.12:3.0.1",
			wantErr: false,
		},
		{
			name: "Large Number of Conf Entries",
			args: args{
				params: template.SparkSubmitParameters{
					Master:     "local[*]",
					DeployMode: "client",
					Class:      "org.example.Main",
					Conf: []string{"spark.executor.memory=2g", "spark.driver.memory=1g",
						"spark.executor.cores=4",
						"spark.driver.cores=2",
						"spark.executor.instances=5",
					},
					Packages: "org.apache.spark:spark-sql_2.12:3.0.1",
				},
			},
			want:    "--master local[*] --deploy-mode client --class org.example.Main --conf spark.executor.memory=2g --conf spark.driver.memory=1g --conf spark.executor.cores=4 --conf spark.driver.cores=2 --conf spark.executor.instances=5 --packages org.apache.spark:spark-sql_2.12:3.0.1",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := helpersBuildSparkSubmitCommand(tt.args.params)
			if (err != nil) != tt.wantErr {
				t.Errorf("helpersBuildSparkSubmitCommand() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("helpersBuildSparkSubmitCommand() = %v, want %v", got, tt.want)
			}
		})
	}
}
