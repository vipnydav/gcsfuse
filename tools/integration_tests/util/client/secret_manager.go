package client

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strings"

	"cloud.google.com/go/compute/metadata"
	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/creds_tests"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/operations"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/setup"
)

const (
	AccessTokenSecretName = "gcloud-auth-access-token"
)

func getGloudAuthToken() ([]byte, error) {
	return operations.ExecuteGcloudCommandf("auth print-access-token")
}

func CreateSecretManagerClient(ctx context.Context) (client *secretmanager.Client, err error) {
	client, err = secretmanager.NewClient(ctx)
	if err != nil {
		log.Fatalf("failed to setup client: %v", err)
	}
	return client, err
}

func CreateSecretManagerClientWithCancel(ctx *context.Context, secretManagerClient **secretmanager.Client) func() error {
	var err error
	var cancel context.CancelFunc
	*ctx, cancel = context.WithCancel(*ctx)
	*secretManagerClient, err = CreateSecretManagerClient(*ctx)
	if err != nil {
		log.Fatalf("client.CreateSecretManagerClient: %v", err)
	}
	// Return func to close storage client and release resources.
	return func() error {
		err := (*secretManagerClient).Close()
		if err != nil {
			return fmt.Errorf("failed to close secret manager client: %v", err)
		}
		defer cancel()
		return nil
	}
}
func CreateAccessTokenSecret(ctx context.Context, client *secretmanager.Client) {
	id, err := metadata.ProjectIDWithContext(ctx)
	if err != nil {
		setup.LogAndExit(fmt.Sprintf("Error in fetching project id: %v", err))
	}
	// return if active GCP project is not in whitelisted gcp projects
	if !slices.Contains(creds_tests.WhitelistedGcpProjects, id) {
		log.Printf("The active GCP project is not one of: %s. So the credentials test will not run.", strings.Join(creds_tests.WhitelistedGcpProjects, ", "))
	}
	// Create the request to create the secret.
	createSecretReq := &secretmanagerpb.CreateSecretRequest{
		Parent:   fmt.Sprintf("projects/%s", id),
		SecretId: AccessTokenSecretName,
		Secret: &secretmanagerpb.Secret{
			Replication: &secretmanagerpb.Replication{
				Replication: &secretmanagerpb.Replication_Automatic_{
					Automatic: &secretmanagerpb.Replication_Automatic{},
				},
			},
		},
	}

	secret, err := client.CreateSecret(ctx, createSecretReq)
	if err != nil && !strings.Contains(err.Error(), "rpc error: code = AlreadyExists") {
		log.Fatalf("failed to create secret: %v", err)
	}

	// Declare the payload to store.
	payload, err := getGloudAuthToken()
	if err != nil {
		log.Fatalf("failed to get gcloud auth access token : %v", err)
	}

	// Build the request.
	addSecretVersionReq := &secretmanagerpb.AddSecretVersionRequest{
		Parent: secret.Name,
		Payload: &secretmanagerpb.SecretPayload{
			Data: payload,
		},
	}

	// Call the API.
	_, err = client.AddSecretVersion(ctx, addSecretVersionReq)
	if err != nil {
		log.Fatalf("failed to add secret version: %v", err)
	}
}

func GetAccessTokenSecret(ctx context.Context, client *secretmanager.Client) (acessToken string) {
	id, err := metadata.ProjectIDWithContext(ctx)
	if err != nil {
		setup.LogAndExit(fmt.Sprintf("Error in fetching project id: %v", err))
	}
	req := &secretmanagerpb.AccessSecretVersionRequest{
		Name: fmt.Sprintf("projects/%s/secrets/%s/versions/latest", id, AccessTokenSecretName),
	}
	accessToken, err := client.AccessSecretVersion(ctx, req)
	if err != nil {
		setup.LogAndExit(fmt.Sprintf("Error while fetching access token secret %v", err))
	}
	return string(accessToken.Payload.Data)
}
