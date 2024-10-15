package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/GoGstickGo/emr-containers-template/awsutils"
	"github.com/GoGstickGo/emr-containers-template/template"
)

// Config holds the application configuration.
type Config struct {
	AWSRegion string
	PathYAML  string
	PmNames   []string
}

// loadConfigFromEnv loads and validates configuration from environment variables.
func loadConfigFromEnv(logger *logrus.Logger) (cfg Config, err error) {
	ssmStringList := os.Getenv("SSM_PM_NAMES") // Must be set; no default.
	var ssmList []string

	if ssmStringList == "" {
		errMsg := "SSM parameter name must be defined"
		logger.Error(errMsg)

		return cfg, errors.New(errMsg)
	}

	if strings.Contains(ssmStringList, ",") {
		ssmList = strings.Split(ssmStringList, ",")
	} else {
		ssmList = append(ssmList, ssmStringList)
	}

	cfg = Config{
		AWSRegion: getEnv("AWS_REGION", "us-east-1"),
		PathYAML:  getEnv("PATH_YAML", "example.yaml"),
		PmNames:   ssmList,
	}

	logger.Infof("Loaded configuration: %+v", cfg)

	return cfg, nil
}

// getEnv retrieves the value of the environment variable named by the key.
// If the variable is not present, it returns the provided default value.
func getEnv(key, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {

		return value
	}

	return defaultVal
}

// processJobTemplate handles the entire lifecycle of a single job template.
func processJobTemplate(ctx context.Context, logger *logrus.Logger, clients *awsutils.AWSClients, jobTemplate template.JobTemplateConfig, cfg Config, random rand.Rand) error {
	logger.Infof("Processing job template: %s", jobTemplate.Name)

	// Initialize helper implementations using interfaces
	var parameterConfigurator awsutils.ParameterConfigurator = &awsutils.RealParameterConfigurator{}
	var sparkSubmitCommandBuilder awsutils.SparkSubmitCommandBuilder = &awsutils.RealSparkSubmitCommandBuilder{}

	// Inject the actual random number generator.
	randomIntn := random.Intn

	// Prepare the job template input.
	temp, err := awsutils.PrepareJobTemplateInput(jobTemplate, parameterConfigurator, sparkSubmitCommandBuilder, randomIntn)
	if err != nil {

		return fmt.Errorf("error preparing job template '%s': %w", jobTemplate.Name, err)
	}
	logger.Infof("Prepared job template input for '%s'", jobTemplate.Name)

	// Create the job template.
	jobTemplateID, err := awsutils.CreateJobTemplate(ctx, clients.EMRContainers, temp)
	if err != nil {

		return fmt.Errorf("error creating job template '%s': %w", jobTemplate.Name, err)
	}
	logger.Infof("Created job template '%s' with ID: %s", jobTemplate.Name, jobTemplateID)

	// Describe the created job template.
	jobTemplateDesc, err := awsutils.DescribeJobTemplate(ctx, clients.EMRContainers, jobTemplateID)
	if err != nil {

		return fmt.Errorf("failed to describe job template '%s': %w", jobTemplate.Name, err)
	}

	// Marshal the job template description to JSON for logging.
	jobTemplateJSON, err := json.MarshalIndent(jobTemplateDesc, "", "  ")
	if err != nil {

		return fmt.Errorf("failed to marshal job template '%s' to JSON: %w", jobTemplate.Name, err)
	}

	logger.Infof("Job template '%s' content:\n%s\n", jobTemplate.Name, string(jobTemplateJSON))

	// Update the SSM parameter with the new job template ID.
	for i := range cfg.PmNames {
		if err = awsutils.UpdateSSMParameter(ctx, clients.SSM, cfg.PmNames[i], jobTemplateID); err != nil {
			return fmt.Errorf("failed to update SSM parameter '%s': %w", cfg.PmNames[i], err)
		}
		logger.Infof("Updated SSM parameter '%s' with job template ID: %s", cfg.PmNames[i], jobTemplateID)
	}

	return nil
}

func main() {
	// Set seed for random number generator.
	random := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Initialize logger.
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	// Load configuration.
	cfg, err := loadConfigFromEnv(logger)
	if err != nil {
		logger.Fatal(err)
	}

	// Create a root context with a timeout for the entire application run.
	ctxTimeOut, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Initialize AWS clients.
	configLoader := &awsutils.RealAWSConfigLoader{}
	clients, err := awsutils.InitializeAWSClients(ctxTimeOut, configLoader, cfg.AWSRegion)
	if err != nil {
		logger.Fatalf("AWS auth error: %v", err)
	}
	logger.Info("AWS clients initialized successfully")

	// Load job templates from YAML configuration.
	jobConfigs, err := template.LoadConfig(cfg.PathYAML)
	if err != nil {
		logger.Fatalf("Error loading YAML config file: %v", err)
	}
	logger.Infof("Loaded %d job templates from configuration", len(jobConfigs.JobTemplates))

	if len(jobConfigs.JobTemplates) != len(cfg.PmNames) {
		logger.Fatalf("Error: The number of job templates must match the number of SSM parameters. Please review your job template configurations and corresponding SSM parameters.")
	}
	// Process each job template.
	for _, jobTemplate := range jobConfigs.JobTemplates {
		if err := processJobTemplate(ctxTimeOut, logger, clients, jobTemplate, cfg, *random); err != nil {
			logger.Fatalf("Processing failed: %v", err)
		}
	}
}
