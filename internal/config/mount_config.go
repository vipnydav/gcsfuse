// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"github.com/googlecloudplatform/gcsfuse/v2/cfg"
	"github.com/googlecloudplatform/gcsfuse/v2/internal/cache/util"
)

const (
	DefaultEnableEmptyManagedFoldersListing = false
	DefaultGrpcConnPoolSize                 = 1
	DefaultAnonymousAccess                  = false
	DefaultEnableHNS                        = true
	DefaultIgnoreInterrupts                 = true
	DefaultPrometheusPort                   = 0

	DefaultKernelListCacheTtlSeconds int64 = 0
	DefaultMaxRetryAttempts                = int64(0)

	// File Cache Config constants.

	DefaultFileCacheMaxSizeMB       = -1
	DefaultEnableCRC                = false
	DefaultEnableParallelDownloads  = false
	DefaultDownloadChunkSizeMB      = 50
	DefaultParallelDownloadsPerFile = 16
	DefaultWriteBufferSize          = int64(4 * util.MiB)
	DefaultEnableODirect            = false

	// DefaultTypeCacheMaxSizeMB is the default value of type-cache max-size for every directory in MiBs.
	// The value is set at the size needed for about 21k type-cache entries,
	// each of which is about 200 bytes in size.
	DefaultTypeCacheMaxSizeMB = 4
)

type LogConfig struct {
	Severity        string          `yaml:"severity"`
	Format          string          `yaml:"format"`
	FilePath        string          `yaml:"file-path"`
	LogRotateConfig LogRotateConfig `yaml:"log-rotate"`
}

type MetricsConfig struct {
	// Expose Prometheus metrics endpoint on this port and a path of /metrics.
	PrometheusPort int `yaml:"prometheus-port"`
}

type ListConfig struct {
	// This flag is specially added to handle the corner case in listing managed folders.
	// There are two corner cases (a) empty managed folder (b) nested managed folder which doesn't contain any descendent as object.
	// This flag always works in conjunction with ImplicitDirectories flag.
	//
	// (a) If only ImplicitDirectories is true, all managed folders are listed other than above two mentioned cases.
	// (b) If both ImplicitDirectories and EnableEmptyManagedFolders are true, then all the managed folders are listed including the above-mentioned corner case.
	// (c) If ImplicitDirectories is false then no managed folders are listed irrespective of EnableEmptyManagedFolders flag.
	EnableEmptyManagedFolders bool `yaml:"enable-empty-managed-folders"`
}

type GCSConnection struct {
	// GRPCConnPoolSize configures the number of gRPC channel in grpc client.
	GRPCConnPoolSize int `yaml:"grpc-conn-pool-size,omitempty"`
}

type GCSAuth struct {
	// Authentication is enabled by default. The skip flag disables authentication. For users of the --custom-endpoint flag,
	// please pass anonymous-access flag explicitly if you do not want authentication enabled for your workflow.
	AnonymousAccess bool `yaml:"anonymous-access"`
}

type FileSystemConfig struct {
	IgnoreInterrupts          bool  `yaml:"ignore-interrupts"`
	DisableParallelDirops     bool  `yaml:"disable-parallel-dirops"`
	KernelListCacheTtlSeconds int64 `yaml:"kernel-list-cache-ttl-secs"`
}

type FileCacheConfig struct {
	MaxSizeMB                int64 `yaml:"max-size-mb"`
	CacheFileForRangeRead    bool  `yaml:"cache-file-for-range-read"`
	EnableParallelDownloads  bool  `yaml:"enable-parallel-downloads,omitempty"`
	ParallelDownloadsPerFile int   `yaml:"parallel-downloads-per-file,omitempty"`
	MaxParallelDownloads     int   `yaml:"max-parallel-downloads,omitempty"`
	DownloadChunkSizeMB      int   `yaml:"download-chunk-size-mb,omitempty"`
	EnableCRC                bool  `yaml:"enable-crc"`
	WriteBufferSize          int64 `yaml:"write-buffer-size,omitempty"`
	EnableODirect            bool  `yaml:"enable-o-direct"`
}

type MetadataCacheConfig struct {
	// TtlInSeconds is the ttl
	// value in seconds, to be used for stat-cache and type-cache.
	// It can be set to -1 for no-ttl, 0 for
	// no cache and > 0 for ttl-controlled metadata-cache.
	// Any value set below -1 will throw an error.
	TtlInSeconds int64 `yaml:"ttl-secs,omitempty"`
	// TypeCacheMaxSizeMB is the upper limit
	// on the maximum size of type-cache maps,
	// which are currently
	// maintained at per-directory level.
	TypeCacheMaxSizeMB int `yaml:"type-cache-max-size-mb,omitempty"`

	// StatCacheMaxSizeMB is the maximum size of stat-cache
	// in MiBs.
	// It can also be set to -1 for no-size-limit, 0 for
	// no cache. Values below -1 are not supported.
	StatCacheMaxSizeMB int64 `yaml:"stat-cache-max-size-mb,omitempty"`
}

type GCSRetries struct {
	// Set max retry attempts in case of retryable errors. Default value is 6.
	MaxRetryAttempts int64 `yaml:"max-retry-attempts"`
}

type MountConfig struct {
	cfg.WriteConfig     `yaml:"write"`
	LogConfig           `yaml:"logging"`
	FileCacheConfig     `yaml:"file-cache"`
	CacheDir            string `yaml:"cache-dir"`
	MetadataCacheConfig `yaml:"metadata-cache"`
	ListConfig          `yaml:"list"`
	GCSConnection       `yaml:"gcs-connection"`
	GCSAuth             `yaml:"gcs-auth"`
	EnableHNS           bool `yaml:"enable-hns"`
	FileSystemConfig    `yaml:"file-system"`
	GCSRetries          `yaml:"gcs-retries"`
	MetricsConfig       `yaml:"metrics"`
}

// LogRotateConfig defines the parameters for log rotation. It consists of three
// configuration options:
// 1. max-file-size-mb: specifies the maximum size in megabytes that a log file
// can reach before it is rotated. The default value is 512 megabytes.
// 2. backup-file-count: determines the maximum number of backup log files to
// retain after they have been rotated. The default value is 10. When value is
// set to 0, all backup files are retained.
// 3. compress: indicates whether the rotated log files should be compressed
// using gzip. The default value is False.
type LogRotateConfig struct {
	MaxFileSizeMB   int  `yaml:"max-file-size-mb"`
	BackupFileCount int  `yaml:"backup-file-count"`
	Compress        bool `yaml:"compress"`
}

func NewMountConfig() *MountConfig {
	mountConfig := &MountConfig{}
	logConfig := cfg.DefaultLoggingConfig()
	logRotateConfig := logConfig.LogRotate
	mountConfig.LogConfig = LogConfig{
		// Making the default severity as INFO.
		Severity: string(logConfig.Severity),
		// Setting default values of log rotate config.
		LogRotateConfig: LogRotateConfig{
			MaxFileSizeMB:   int(logRotateConfig.MaxFileSizeMb),
			BackupFileCount: int(logRotateConfig.BackupFileCount),
			Compress:        logRotateConfig.Compress,
		},
	}
	mountConfig.FileCacheConfig = FileCacheConfig{
		MaxSizeMB:                DefaultFileCacheMaxSizeMB,
		EnableParallelDownloads:  DefaultEnableParallelDownloads,
		ParallelDownloadsPerFile: DefaultParallelDownloadsPerFile,
		MaxParallelDownloads:     cfg.DefaultMaxParallelDownloads(),
		DownloadChunkSizeMB:      DefaultDownloadChunkSizeMB,
		EnableCRC:                DefaultEnableCRC,
		WriteBufferSize:          DefaultWriteBufferSize,
		EnableODirect:            DefaultEnableODirect,
	}
	mountConfig.MetadataCacheConfig = MetadataCacheConfig{
		TtlInSeconds:       cfg.TtlInSecsUnsetSentinel,
		TypeCacheMaxSizeMB: DefaultTypeCacheMaxSizeMB,
		StatCacheMaxSizeMB: cfg.StatCacheMaxSizeMBUnsetSentinel,
	}
	mountConfig.ListConfig = ListConfig{
		EnableEmptyManagedFolders: DefaultEnableEmptyManagedFoldersListing,
	}
	mountConfig.GCSConnection = GCSConnection{
		GRPCConnPoolSize: DefaultGrpcConnPoolSize,
	}
	mountConfig.GCSAuth = GCSAuth{
		AnonymousAccess: DefaultAnonymousAccess,
	}
	mountConfig.EnableHNS = DefaultEnableHNS

	mountConfig.FileSystemConfig = FileSystemConfig{
		KernelListCacheTtlSeconds: DefaultKernelListCacheTtlSeconds,
	}

	mountConfig.FileSystemConfig.IgnoreInterrupts = DefaultIgnoreInterrupts

	mountConfig.GCSRetries = GCSRetries{
		MaxRetryAttempts: DefaultMaxRetryAttempts,
	}

	mountConfig.MetricsConfig = MetricsConfig{
		PrometheusPort: DefaultPrometheusPort,
	}

	return mountConfig
}
