# emr-containers-job-template


This documentation provides an overview and guide on how to create an EMR (containers)  Job Template using the AWS SDK for Go v2.
It leverages a YAML configuration file to dynamically set up job parameters, application configurations, and tags.

## Requirements

- **Go**: Version 1.22.4 or later
- **AWS SDK for Go v2**
- **AWS Credentials**: Ensure your AWS credentials are correctly set up in `~/.aws/credentials` or via environment variables.
- **YAML Configuration File**: A structured YAML file for job configuration.

## Installation

To use this code, ensure you have the following dependencies installed:

1. **Install Go**: [Download and install Go](https://golang.org/doc/install)
2. **Get AWS SDK for Go v2**:
```bash
   go get github.com/aws/aws-sdk-go-v2
   go get github.com/aws/aws-sdk-go-v2/config
   go get github.com/aws/aws-sdk-go-v2/service/emrcontainers
```
## Yaml Strcuture
Please seee the example.yaml for detailed structure.

## RunTime variables
App requires to environment variables
1. **AWS_REGION** for the region wher job template should be created , defaults to **us-east-1**.
2. **PATH_YAML** for the path and yaml file , defaults to **example.yaml**.
3. (Required)**SSM_NAME** SSM to be updated with jobconfig ID , **no default**.

## Values behind the seen ##
- Name: name value passdown to the LogStreamprefix and JobTags/Tags
- JobTags/Tags: set to be the same value
- ClientToken: generated from math/rand package at each run