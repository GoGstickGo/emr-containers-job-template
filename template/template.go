package template

import (
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/emrcontainers/types"
	"gopkg.in/yaml.v2"
)

type TemplateParameterConfiguration struct {
	DefaultValue *string                         `yaml:"default_value"`
	Type         types.TemplateParameterDataType `yaml:"type"`
}

type ApplicationConfiguration struct {
	Classification string            `yaml:"classification"`
	Properties     map[string]string `yaml:"properties"`
}

type SparkSubmitParameters struct {
	Master     string   `yaml:"master"`
	DeployMode string   `yaml:"deploy_mode"`
	Class      string   `yaml:"class"`
	Conf       []string `yaml:"conf"`
	Packages   string   `yaml:"packages"`
}

type JobTemplateConfig struct {
	Name                      string                                    `yaml:"name"`
	ExecutionRoleArn          string                                    `yaml:"execution_role_arn"`
	ReleaseLabel              string                                    `yaml:"release_label"`
	EntryPoint                string                                    `yaml:"entry_point"`
	EntryPointArguments       []string                                  `yaml:"entry_point_arguments"`
	Tags                      map[string]string                         `yaml:"tags"`
	SparkSubmitParameters     SparkSubmitParameters                     `yaml:"spark_submit_pararmeters"`
	PersistentAppUI           string                                    `yaml:"persistent_app_ui"`
	LogGroupName              string                                    `yaml:"log_group_name"`
	ParameterConfiguration    map[string]TemplateParameterConfiguration `yaml:"parameter_configuration"`
	ApplicationConfigurations []ApplicationConfiguration                `yaml:"application_configurations"`
}

type Config struct {
	JobTemplates []JobTemplateConfig `yaml:"job_templates"`
}

func LoadConfig(filePath string) (*Config, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("opening file func returned error:%w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("read all func returned error:%w", err)
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("unmarshal func returned error:%w", err)
	}

	return &config, nil
}
