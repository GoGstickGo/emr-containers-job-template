package awsutils

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
)

// SSMPutParameterAPI defines the interface for the PutParameter function.
// We use this interface to test the function using a mock.
type SSM interface {
	PutParameter(ctx context.Context, params *ssm.PutParameterInput, optFns ...func(*ssm.Options)) (*ssm.PutParameterOutput, error)
}

// UpdateSSMParameter updates an SSM parameter with the given name and value.
func UpdateSSMParameter(ctx context.Context, client SSM, name, value string) error {
	overwrite := true
	input := &ssm.PutParameterInput{
		Name:      &name,
		Value:     &value,
		Type:      types.ParameterTypeString,
		Overwrite: &overwrite,
	}

	_, err := client.PutParameter(ctx, input)
	if err != nil {
		return fmt.Errorf("ssm update failed err: %v", err)
	}
	return nil
}
