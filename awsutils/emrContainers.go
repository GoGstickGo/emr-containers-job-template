package awsutils

import (
	"context"
	"fmt"
	"strings"

	"github.com/GoGstickGo/emr-containers-template/template"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emrcontainers"
	"github.com/aws/aws-sdk-go-v2/service/emrcontainers/types"
)

type ParameterConfigurator interface {
	Configure(map[string]template.TemplateParameterConfiguration) (map[string]types.TemplateParameterConfiguration, error)
}

type SparkSubmitCommandBuilder interface {
	Build(template.SparkSubmitParameters) (string, error)
}

type RealParameterConfigurator struct{}

func (r *RealParameterConfigurator) Configure(paramConfig map[string]template.TemplateParameterConfiguration) (map[string]types.TemplateParameterConfiguration, error) {
	return HelperParameterConfiguration(paramConfig)
}

type RealSparkSubmitCommandBuilder struct{}

func (r *RealSparkSubmitCommandBuilder) Build(params template.SparkSubmitParameters) (string, error) {
	return HelpersBuildSparkSubmitCommand(params)
}

type EMRC interface {
	DescribeJobTemplate(ctx context.Context, params *emrcontainers.DescribeJobTemplateInput, optFns ...func(*emrcontainers.Options)) (*emrcontainers.DescribeJobTemplateOutput, error)
	CreateJobTemplate(ctx context.Context, params *emrcontainers.CreateJobTemplateInput, optFns ...func(*emrcontainers.Options)) (*emrcontainers.CreateJobTemplateOutput, error)
}

func DescribeJobTemplate(ctx context.Context, client EMRC, jobTemplateID string) (*types.JobTemplate, error) {
	// Prepare the input parameters for the DescribeJobTemplate API call.
	input := &emrcontainers.DescribeJobTemplateInput{
		Id: &jobTemplateID,
	}

	// Call the DescribeJobTemplate API.
	resp, err := client.DescribeJobTemplate(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to describe job template: %w", err)
	}

	// Return the job template details.
	return resp.JobTemplate, nil
}

func PrepareJobTemplateInput(
	jobConfig template.JobTemplateConfig,
	parameterConfigurator ParameterConfigurator,
	sparkSubmitCommandBuilder SparkSubmitCommandBuilder,
	randomIntn func(int) int,
) (*emrcontainers.CreateJobTemplateInput, error) {
	// Ensure the "Name" tag is set
	if jobConfig.Tags == nil {
		jobConfig.Tags = make(map[string]string)
	}
	jobConfig.Tags["Name"] = jobConfig.Name

	// Call helperParameterConfiguration.
	parameterConfig, err := parameterConfigurator.Configure(jobConfig.ParameterConfiguration)
	if err != nil {
		return nil, fmt.Errorf("parameter configuration block failed: %w", err)
	}

	// Call helpersBuildSparkSubmitCommand.
	sparkSubmitParametersConfig, err := sparkSubmitCommandBuilder.Build(jobConfig.SparkSubmitParameters)
	if err != nil {
		return nil, fmt.Errorf("sparkSubmitParameters configuration block failed: %w", err)
	}

	// Prepare application configurations.
	var appConfigs []types.Configuration
	for _, appConfig := range jobConfig.ApplicationConfigurations {
		appConfigs = append(appConfigs, types.Configuration{
			Classification: aws.String(appConfig.Classification),
			Properties:     appConfig.Properties,
		})
	}

	// Generate a client token using the injected randomIntn function.
	clientToken := aws.String(fmt.Sprintf("%d", randomIntn(100000)))

	// Construct the CreateJobTemplateInput.
	input := &emrcontainers.CreateJobTemplateInput{
		Name: aws.String(jobConfig.Name),
		JobTemplateData: &types.JobTemplateData{
			ExecutionRoleArn: aws.String(jobConfig.ExecutionRoleArn),
			ReleaseLabel:     aws.String(jobConfig.ReleaseLabel),
			JobDriver: &types.JobDriver{
				SparkSubmitJobDriver: &types.SparkSubmitJobDriver{
					EntryPoint:            aws.String(jobConfig.EntryPoint),
					EntryPointArguments:   jobConfig.EntryPointArguments,
					SparkSubmitParameters: aws.String(sparkSubmitParametersConfig),
				},
			},
			ConfigurationOverrides: &types.ParametricConfigurationOverrides{
				ApplicationConfiguration: appConfigs,
				MonitoringConfiguration: &types.ParametricMonitoringConfiguration{
					PersistentAppUI: aws.String(jobConfig.PersistentAppUI),
					CloudWatchMonitoringConfiguration: &types.ParametricCloudWatchMonitoringConfiguration{
						LogGroupName:        aws.String(jobConfig.LogGroupName),
						LogStreamNamePrefix: aws.String(jobConfig.Name),
					},
				},
			},
			ParameterConfiguration: parameterConfig,
			JobTags:                jobConfig.Tags,
		},
		ClientToken: clientToken,
		Tags:        jobConfig.Tags,
	}

	return input, nil
}

func CreateJobTemplate(ctx context.Context, client EMRC, input *emrcontainers.CreateJobTemplateInput) (string, error) {
	result, err := client.CreateJobTemplate(ctx, input)
	if err != nil {
		return "", fmt.Errorf("create job template returned error: %w", err)
	}

	return aws.ToString(result.Id), nil
}

// convertParameterConfiguration converts YAML parameter configuration to AWS SDK format.
func HelperParameterConfiguration(paramConfig map[string]template.TemplateParameterConfiguration) (map[string]types.TemplateParameterConfiguration, error) {
	converted := make(map[string]types.TemplateParameterConfiguration)
	for key, param := range paramConfig {
		var paramType types.TemplateParameterDataType
		switch param.Type {
		case "STRING":
			paramType = types.TemplateParameterDataTypeString
		case "NUMBER":
			paramType = types.TemplateParameterDataTypeNumber
		default:
			return nil, fmt.Errorf("unknown parameter type: %s", param.Type)
		}

		converted[key] = types.TemplateParameterConfiguration{
			DefaultValue: param.DefaultValue,
			Type:         paramType,
		}
	}

	return converted, nil
}

// Function to build the command string from SparkSubmitParameters with error handling.
func HelpersBuildSparkSubmitCommand(params template.SparkSubmitParameters) (string, error) {
	// Validate required fields
	if params.Master == "" {
		return "", fmt.Errorf("missing required parameter: master")
	}
	if params.DeployMode == "" {
		return "", fmt.Errorf("missing required parameter: deploy_mode")
	}
	if params.Class == "" {
		return "", fmt.Errorf("missing required parameter: class")
	}

	// Build the command string.
	var result strings.Builder

	result.WriteString("--master " + params.Master + " ")
	result.WriteString("--deploy-mode " + params.DeployMode + " ")
	result.WriteString("--class " + params.Class + " ")

	for _, conf := range params.Conf {
		if conf != "" {
			result.WriteString("--conf " + conf + " ")
		} else {
			return "", fmt.Errorf("conf contains an empty value")
		}
	}

	if params.Packages != "" {
		result.WriteString("--packages " + params.Packages)
	} else {
		return "", fmt.Errorf("missing required parameter: packages")
	}

	return strings.TrimSpace(result.String()), nil
}
