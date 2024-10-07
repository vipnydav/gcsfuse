package client

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	control "cloud.google.com/go/storage/control/apiv2"
	"cloud.google.com/go/storage/control/apiv2/controlpb"
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

func DeleteManagedFoldersInBucket(ctx context.Context, client *control.StorageControlClient, managedFolderPath, bucket string) {
	//gcloudDeleteManagedFolderCmd := fmt.Sprintf("alpha storage rm -r gs://%s/%s", bucket, managedFolderPath)
	folderPath := fmt.Sprintf("projects/_/buckets/%v/managedFolders/%v/", bucket, managedFolderPath)
	req := &controlpb.DeleteManagedFolderRequest{
		Name: folderPath,
	}
	if err := client.DeleteManagedFolder(ctx, req); err != nil && !strings.Contains(err.Error(), "The following URLs matched no objects or files") {
		log.Fatalf(fmt.Sprintf("Error while deleting managed folder: %v", err))
	}
}

func CreateManagedFoldersInBucket(ctx context.Context, client *control.StorageControlClient, managedFolderPath, bucket string) {
	//gcloudCreateManagedFolderCmd := fmt.Sprintf("alpha storage managed-folders create gs://%s/%s", bucket, managedFolderPath)
	mf := &controlpb.ManagedFolder{}
	req := &controlpb.CreateManagedFolderRequest{
		Parent:          fmt.Sprintf("projects/_/buckets/%v", bucket),
		ManagedFolder:   mf,
		ManagedFolderId: managedFolderPath,
	}
	if _, err := client.CreateManagedFolder(ctx, req); err != nil && !strings.Contains(err.Error(), "The specified managed folder already exists") {
		log.Fatalf(fmt.Sprintf("Error while creating managed folder: %v", err))
	}
}

func CopyFileInBucket(ctx context.Context, storageClient *storage.Client, srcfilePath, destFilePath, bucket string, t *testing.T) {
	//gcloudCopyFileCmd := fmt.Sprintf("alpha storage cp %s gs://%s/%s/", srcfilePath, bucket, destFilePath)
	err := UploadGcsObject(ctx, storageClient, srcfilePath, bucket, destFilePath, false)
	if err != nil {
		t.Fatalf("Error while copying file : %w", err)
	}
}
