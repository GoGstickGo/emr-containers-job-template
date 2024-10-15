package template_test

import (
	"testing"

	"github.com/GoGstickGo/emr-containers-template/template"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/google/go-cmp/cmp"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()
	type args struct {
		filePath string
	}
	tests := []struct {
		name    string
		args    args
		want    *template.Config
		wantErr bool
	}{
		{
			name: "Successful Config Load",
			args: args{
				filePath: "testdata/valid_config.yaml",
			},
			want: &template.Config{
				JobTemplates: []template.JobTemplateConfig{
					{
						Name:             "custom-job",
						ExecutionRoleArn: "arn:aws:iam::123456789012:role/CustomRole",
						ReleaseLabel:     "emr-6.4.0-latest",
						EntryPoint:       "s3://bucket/path/to/script.py",
						EntryPointArguments: []string{
							"--conf", "spark.executor.instances=4",
						},
						Tags: map[string]string{
							"Environment": "production",
							"Owner":       "team-x",
						},
						SparkSubmitParameters: template.SparkSubmitParameters{
							Class:      "org.example.ClassName",
							Master:     "yarn",
							DeployMode: "cluster",
							Conf: []string{
								"spark.dynamicAllocation.shuffleTracking.enabled=true",
								"spark.dynamicAllocation.minExecutors=${MinExecutors}",
							},
							Packages: "org.reactivestreams:reactive-streams:1.0.4,io.projectreactor:reactor-core:3.6.6",
						},
						PersistentAppUI: "DISABLED",
						LogGroupName:    "my-log-group",
						ParameterConfiguration: map[string]template.TemplateParameterConfiguration{
							"MaxExecutors": {
								DefaultValue: aws.String("10"),
								Type:         "NUMBER",
							},
							"ConfigLocation": {
								DefaultValue: aws.String("s3://another-config-location"),
								Type:         "STRING",
							},
						},
						ApplicationConfigurations: []template.ApplicationConfiguration{
							{
								Classification: "spark-hive-site",
								Properties: map[string]string{
									"spark.executor.instances": "4",
									"spark.executor.memory":    "8G",
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "File Not Found",
			args: args{
				filePath: "testdata/nonexistent.yaml",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel() // Mark each sub-test as parallel.

			got, err := template.LoadConfig(tt.args.filePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadConfig() error = %v, wantErr %v", err, tt.wantErr)

				return
			}
			if diff := cmp.Diff(got, tt.want); diff != "" {
				t.Errorf("test failed, diff ==> %v\n,", diff)
			}
		})
	}
}
