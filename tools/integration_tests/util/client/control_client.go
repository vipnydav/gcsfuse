package client

import (
	"context"
	"fmt"
	"log"

	control "cloud.google.com/go/storage/control/apiv2"
)

func CreateControlClient(ctx context.Context) (client *control.StorageControlClient, err error) {
	client, err = control.NewStorageControlClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("control.NewStorageControlClient: #{err}")
	}
	return client, nil
}

func CreateControlClientWithCancel(ctx *context.Context, controlClient **control.StorageControlClient) func() error {
	var err error
	var cancel context.CancelFunc
	*ctx, cancel = context.WithCancel(*ctx)
	*controlClient, err = CreateControlClient(*ctx)
	if err != nil {
		log.Fatalf("client.CreateControlClient: %v", err)
	}
	// Return func to close storage client and release resources.
	return func() error {
		err := (*controlClient).Close()
		if err != nil {
			return fmt.Errorf("failed to close control client: %v", err)
		}
		defer cancel()
		return nil
	}
}
