// Copyright 2024 Google LLC
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

package read_cache

import (
	"context"
	"log"
	"path"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/client"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/log_parser/json_parser/read_logs"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/operations"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/setup"
	"github.com/googlecloudplatform/gcsfuse/v2/tools/integration_tests/util/test_setup"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////
// Boilerplate
////////////////////////////////////////////////////////////////////////

type cacheFileForRangeReadFalseTest struct {
	flags                      []string
	storageClient              *storage.Client
	ctx                        context.Context
	isParallelDownloadsEnabled bool
}

func (s *cacheFileForRangeReadFalseTest) Setup(t *testing.T) {
	setupForMountedDirectoryTests()
	// Clean up the cache directory path as gcsfuse don't clean up on mounting.
	operations.RemoveDir(cacheDirPath)
	mountGCSFuseAndSetupTestDir(s.flags, s.ctx, s.storageClient, testDirName)
}

func (s *cacheFileForRangeReadFalseTest) Teardown(t *testing.T) {
	if t.Failed() {
		setup.SaveLogFileToKOKOROArtifact("gcsfuse-failed-integration-test-logs-" + strings.Replace(t.Name(), "/", "-", -1))
	}
	setup.UnmountGCSFuseAndDeleteLogFile(rootDir)
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

func readFileAsync(t *testing.T, wg *sync.WaitGroup, testFileName string, expectedOutcome **Expected) {
	go func() {
		defer wg.Done()
		*expectedOutcome = readFileAndGetExpectedOutcome(testDirPath, testFileName, true, zeroOffset, t)
	}()
}

////////////////////////////////////////////////////////////////////////
// Test scenarios
////////////////////////////////////////////////////////////////////////

func (s *cacheFileForRangeReadFalseTest) TestRangeReadsWithCacheMiss(t *testing.T) {
	testFileName := setupFileInTestDir(s.ctx, s.storageClient, testDirName, fileSizeForRangeRead, t)

	// Do a random read on file and validate from gcs.
	expectedOutcome1 := readChunkAndValidateObjectContentsFromGCS(s.ctx, s.storageClient, testFileName, offset5000, t)
	// Read file again from offset 1000 and validate from gcs.
	expectedOutcome2 := readChunkAndValidateObjectContentsFromGCS(s.ctx, s.storageClient, testFileName, offset1000, t)

	structuredReadLogs := read_logs.GetStructuredLogsSortedByTimestamp(setup.LogFile(), t)
	validate(expectedOutcome1, structuredReadLogs[0], false, false, 1, t)
	validate(expectedOutcome2, structuredReadLogs[1], false, false, 1, t)
	validateFileIsNotCached(testFileName, t)
}

func (s *cacheFileForRangeReadFalseTest) TestConcurrentReads_ReadIsTreatedNonSequentialAfterFileIsRemovedFromCache(t *testing.T) {
	var testFileNames [2]string
	var expectedOutcome [2]*Expected
	testFileNames[0] = setupFileInTestDir(s.ctx, s.storageClient, testDirName, fileSizeSameAsCacheCapacity, t)
	testFileNames[1] = setupFileInTestDir(s.ctx, s.storageClient, testDirName, fileSizeSameAsCacheCapacity, t)
	randomReadChunkCount := fileSizeSameAsCacheCapacity / chunkSizeToRead

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		readFileAsync(t, &wg, testFileNames[i], &expectedOutcome[i])
	}
	wg.Wait()

	structuredReadLogs := read_logs.GetStructuredLogsSortedByTimestamp(setup.LogFile(), t)
	require.Equal(t, 2, len(structuredReadLogs))
	// Goroutine execution order isn't guaranteed.
	// If the object name in expected outcome doesn't align with the logs, swap
	// the expected outcome objects and file names at positions 0 and 1.
	if expectedOutcome[0].ObjectName != structuredReadLogs[0].ObjectName {
		expectedOutcome[0], expectedOutcome[1] = expectedOutcome[1], expectedOutcome[0]
		testFileNames[0], testFileNames[1] = testFileNames[1], testFileNames[0]
	}
	validate(expectedOutcome[0], structuredReadLogs[0], true, false, randomReadChunkCount, t)
	validate(expectedOutcome[1], structuredReadLogs[1], true, false, randomReadChunkCount, t)
	// Validate last chunk was considered non-sequential and cache hit false for first read.
	assert.False(t, structuredReadLogs[0].Chunks[randomReadChunkCount-1].IsSequential)
	assert.False(t, structuredReadLogs[0].Chunks[randomReadChunkCount-1].CacheHit)
	// Validate last chunk was considered sequential and cache hit true for second read.
	assert.True(t, structuredReadLogs[1].Chunks[randomReadChunkCount-1].IsSequential)
	if !s.isParallelDownloadsEnabled {
		// When parallel downloads are enabled, we can't concretely say that the read will be cache Hit.
		assert.True(t, structuredReadLogs[1].Chunks[randomReadChunkCount-1].CacheHit)
	}

	validateFileIsNotCached(testFileNames[0], t)
	validateFileInCacheDirectory(testFileNames[1], fileSizeSameAsCacheCapacity, s.ctx, s.storageClient, t)
}

////////////////////////////////////////////////////////////////////////
// Test Function (Runs once before all tests)
////////////////////////////////////////////////////////////////////////

func TestCacheFileForRangeReadFalseTest(t *testing.T) {
	ts := &cacheFileForRangeReadFalseTest{ctx: context.Background()}
	// Create storage client before running tests.
	closeStorageClient := client.CreateStorageClientWithCancel(&ts.ctx, &ts.storageClient)
	defer func() {
		err := closeStorageClient()
		if err != nil {
			t.Errorf("closeStorageClient failed: %v", err)
		}
	}()

	// Run tests for mounted directory if the flag is set.
	if setup.AreBothMountedDirectoryAndTestBucketFlagsSet() {
		test_setup.RunTests(t, ts)
		return
	}

	// Run with cache directory pointing to RAM based dir
	ramCacheDir := path.Join("/dev/shm", cacheDirName)

	// Run tests with parallel downloads disabled.
	flagsSet := []gcsfuseTestFlags{
		{
			cliFlags:                []string{"--implicit-dirs"},
			cacheSize:               cacheCapacityForRangeReadTestInMiB,
			cacheFileForRangeRead:   false,
			fileName:                configFileName,
			enableParallelDownloads: false,
			enableODirect:           false,
			cacheDirPath:            getDefaultCacheDirPathForTests(),
		},
		{
			cliFlags:                nil,
			cacheSize:               cacheCapacityForRangeReadTestInMiB,
			cacheFileForRangeRead:   false,
			fileName:                configFileName,
			enableParallelDownloads: false,
			enableODirect:           false,
			cacheDirPath:            ramCacheDir,
		},
	}
	flagsSet = appendClientProtocolConfigToFlagSet(flagsSet)
	for _, flags := range flagsSet {
		configFilePath := createConfigFile(&flags)
		ts.flags = []string{"--config-file=" + configFilePath}
		if flags.cliFlags != nil {
			ts.flags = append(ts.flags, flags.cliFlags...)
		}
		log.Printf("Running tests with flags: %s", ts.flags)
		test_setup.RunTests(t, ts)
	}

	// Run tests with parallel downloads enabled.
	flagsSet = []gcsfuseTestFlags{
		{
			cliFlags:                nil,
			cacheSize:               cacheCapacityForRangeReadTestInMiB,
			cacheFileForRangeRead:   false,
			fileName:                configFileNameForParallelDownloadTests,
			enableParallelDownloads: true,
			enableODirect:           false,
			cacheDirPath:            getDefaultCacheDirPathForTests(),
		},
		{
			cliFlags:                nil,
			cacheSize:               cacheCapacityForRangeReadTestInMiB,
			cacheFileForRangeRead:   false,
			fileName:                configFileNameForParallelDownloadTests,
			enableParallelDownloads: true,
			enableODirect:           false,
			cacheDirPath:            ramCacheDir,
		},
		{
			cliFlags:                nil,
			cacheSize:               cacheCapacityForRangeReadTestInMiB,
			cacheFileForRangeRead:   false,
			fileName:                configFileNameForParallelDownloadTests,
			enableParallelDownloads: true,
			enableODirect:           true,
			cacheDirPath:            getDefaultCacheDirPathForTests(),
		},
		{
			cliFlags:                nil,
			cacheSize:               cacheCapacityForRangeReadTestInMiB,
			cacheFileForRangeRead:   false,
			fileName:                configFileNameForParallelDownloadTests,
			enableParallelDownloads: true,
			enableODirect:           true,
			cacheDirPath:            ramCacheDir,
		},
	}
	flagsSet = appendClientProtocolConfigToFlagSet(flagsSet)
	for _, flags := range flagsSet {
		configFilePath := createConfigFile(&flags)
		ts.flags = []string{"--config-file=" + configFilePath}
		if flags.cliFlags != nil {
			ts.flags = append(ts.flags, flags.cliFlags...)
		}
		ts.isParallelDownloadsEnabled = true
		log.Printf("Running tests with flags: %s", ts.flags)
		test_setup.RunTests(t, ts)
	}
}
