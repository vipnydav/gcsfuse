// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Provide a helper for directory operations.
package operations

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	control "cloud.google.com/go/storage/control/apiv2"
	"cloud.google.com/go/storage/control/apiv2/controlpb"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/client"
	"golang.org/x/net/context"
)

const FilePermission_0400 = 0400
const FilePermission_0600 = 0600
const FilePermission_0777 = 0777
const DirPermission_0755 = 0755
const MiB = 1024 * 1024

func executeCommandForOperation(cmd *exec.Cmd) (err error) {
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err != nil {
		err = fmt.Errorf("command execution %s failed: %v", cmd, cmd.Stderr)
	}
	return
}

func CopyDir(srcDirPath string, destDirPath string) (err error) {
	cmd := exec.Command("cp", "--recursive", srcDirPath, destDirPath)

	err = executeCommandForOperation(cmd)

	return
}

func CopyObject(srcPath string, destPath string) (err error) {
	cmd := exec.Command("cp", srcPath, destPath)

	err = executeCommandForOperation(cmd)

	return
}

func Move(srcPath string, destPath string) (err error) {
	cmd := exec.Command("mv", srcPath, destPath)

	err = executeCommandForOperation(cmd)

	return
}

func RenameDir(dirName string, newDirName string) (err error) {
	if _, err = os.Stat(newDirName); err == nil {
		err = fmt.Errorf("renamed directory %s already present", newDirName)
		return
	}

	if err = os.Rename(dirName, newDirName); err != nil {
		err = fmt.Errorf("rename unsuccessful: %v", err)
		return
	}

	if _, err = os.Stat(dirName); err == nil {
		err = fmt.Errorf("original directory %s still exists", dirName)
		return
	}
	if _, err = os.Stat(newDirName); err != nil {
		err = fmt.Errorf("renamed directory %s not found", newDirName)
		return
	}
	return
}

func CreateDirectoryWithNFiles(numberOfFiles int, dirPath string, prefix string, t *testing.T) {
	err := os.Mkdir(dirPath, FilePermission_0777)
	if err != nil && !strings.Contains(err.Error(), "file exists") {
		t.Errorf("Error in creating directory: %v", err)
	}

	for i := 1; i <= numberOfFiles; i++ {
		// Create file with name prefix + i
		// e.g. If prefix = temp  then temp1, temp2
		filePath := path.Join(dirPath, prefix+strconv.Itoa(i))
		file, err := os.Create(filePath)
		if err != nil {
			t.Errorf("Create file at %q: %v", dirPath, err)
		}

		// Closing file at the end.
		CloseFile(file)
	}
}

func RemoveDir(dirPath string) {
	if err := os.RemoveAll(dirPath); err != nil {
		log.Printf("os.RemoveAll(%s): %v", dirPath, err)
	}
}

func ReadDirectory(dirPath string, t *testing.T) (entries []os.DirEntry) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		t.Fatalf("os.ReadDir(%s) err: %v", dirPath, err)
	}
	return
}

func VerifyDirectoryEntry(entry os.DirEntry, dirName string, t *testing.T) {
	if !entry.IsDir() {
		t.Fatalf("Expected: directory entry, Got: file entry.")
	}
	if entry.Name() != dirName {
		t.Fatalf("File name, Expected: %s, Got: %s", dirName, entry.Name())
	}
}

func VerifyCountOfDirectoryEntries(expected, got int, t *testing.T) {
	if expected != got {
		t.Fatalf("directory entry count mismatch, expected: %d, got: %d", expected, got)
	}
}

func CreateDirectory(dirPath string, t *testing.T) {
	err := os.Mkdir(dirPath, DirPermission_0755)

	// Verify MkDir operation succeeds.
	if err != nil {
		t.Fatalf("Error while creating directory, err: %v", err)
	}
}

func DirSizeMiB(dirPath string) (dirSizeMB int64, err error) {
	var totalSize int64
	err = filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})
	dirSizeMB = totalSize / MiB
	return dirSizeMB, err
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
	err := client.UploadGcsObject(ctx, storageClient, srcfilePath, bucket, destFilePath, false)
	if err != nil {
		t.Fatalf("Error while copying file : %w", err)
	}
}
