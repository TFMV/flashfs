package cloudstorage

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	"gopkg.in/yaml.v3"
)

// StorageType represents the type of cloud storage
type StorageType string

const (
	// S3Storage represents Amazon S3 or compatible storage
	S3Storage StorageType = "s3"
	// GCSStorage represents Google Cloud Storage
	GCSStorage StorageType = "gcs"
)

// Config represents the configuration for cloud storage
type Config struct {
	Type StorageType

	// S3 specific configuration
	S3Config *S3Config

	// GCS specific configuration
	GCSConfig *GCSConfig
}

// S3Config represents the configuration for S3 storage
type S3Config struct {
	Endpoint       string
	AccessKey      string
	SecretKey      string
	Bucket         string
	Region         string
	Insecure       bool
	SignatureV2    bool
	ForcePathStyle bool
	// The following fields are not supported by the current Thanos objstore version
	// and are kept here for reference only
	// SSEEncryption   bool
	// SSEKMSKeyID     string
	// SSECustomerKey  string
	// SSECustomerAlgo string
}

// GCSConfig represents the configuration for GCS storage
type GCSConfig struct {
	Bucket         string
	ServiceAccount string
}

// CloudStorage represents a cloud storage client
type CloudStorage struct {
	bucket objstore.Bucket
	logger log.Logger
}

// ParseDestination parses a destination URL and returns the storage type, bucket, and prefix
func ParseDestination(destination string) (StorageType, string, string, error) {
	u, err := url.Parse(destination)
	if err != nil {
		return "", "", "", errors.Wrap(err, "parse destination URL")
	}

	// Check if the URL has the correct format with scheme and host
	if u.Host == "" {
		return "", "", "", errors.New("invalid URL format: missing host")
	}

	var storageType StorageType
	switch u.Scheme {
	case "s3":
		storageType = S3Storage
	case "gcs":
		storageType = GCSStorage
	default:
		return "", "", "", errors.Errorf("unsupported storage type: %s", u.Scheme)
	}

	bucket := u.Host
	prefix := strings.TrimPrefix(u.Path, "/")

	return storageType, bucket, prefix, nil
}

// NewCloudStorage creates a new cloud storage client
func NewCloudStorage(ctx context.Context, config Config, logger log.Logger) (*CloudStorage, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	var bucket objstore.Bucket
	var err error

	switch config.Type {
	case S3Storage:
		if config.S3Config == nil {
			return nil, errors.New("S3 configuration is required for S3 storage")
		}
		bucket, err = createS3Bucket(ctx, *config.S3Config, logger)
	case GCSStorage:
		if config.GCSConfig == nil {
			return nil, errors.New("GCS configuration is required for GCS storage")
		}
		bucket, err = createGCSBucket(ctx, *config.GCSConfig, logger)
	default:
		return nil, errors.Errorf("unsupported storage type: %s", config.Type)
	}

	if err != nil {
		return nil, err
	}

	return &CloudStorage{
		bucket: bucket,
		logger: logger,
	}, nil
}

// createS3Bucket creates a new S3 bucket client
func createS3Bucket(ctx context.Context, config S3Config, logger log.Logger) (objstore.Bucket, error) {
	// Convert our S3Config to the YAML format expected by Thanos objstore
	s3Config := map[string]interface{}{
		"bucket":             config.Bucket,
		"endpoint":           config.Endpoint,
		"access_key":         config.AccessKey,
		"secret_key":         config.SecretKey,
		"region":             config.Region,
		"insecure":           config.Insecure,
		"signature_version2": config.SignatureV2,
	}

	if config.ForcePathStyle {
		s3Config["bucket_lookup_type"] = "path"
	}

	// Create the full config with type
	fullConfig := map[string]interface{}{
		"type":   "S3",
		"config": s3Config,
	}

	// Convert to YAML
	confContentYaml, err := yaml.Marshal(fullConfig)
	if err != nil {
		return nil, errors.Wrap(err, "marshal S3 config to YAML")
	}

	// Use the factory method to create the bucket
	return client.NewBucket(logger, confContentYaml, "flashfs", nil)
}

// createGCSBucket creates a new GCS bucket client
func createGCSBucket(ctx context.Context, config GCSConfig, logger log.Logger) (objstore.Bucket, error) {
	// Convert our GCSConfig to the YAML format expected by Thanos objstore
	gcsConfig := map[string]interface{}{
		"bucket":          config.Bucket,
		"service_account": config.ServiceAccount,
	}

	// Create the full config with type
	fullConfig := map[string]interface{}{
		"type":   "GCS",
		"config": gcsConfig,
	}

	// Convert to YAML
	confContentYaml, err := yaml.Marshal(fullConfig)
	if err != nil {
		return nil, errors.Wrap(err, "marshal GCS config to YAML")
	}

	// Use the factory method to create the bucket
	return client.NewBucket(logger, confContentYaml, "flashfs", nil)
}

// NewFromEnv creates a new cloud storage client from environment variables
func NewFromEnv(ctx context.Context, storageType StorageType, bucket string, logger log.Logger) (*CloudStorage, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	var config Config
	config.Type = storageType

	switch storageType {
	case S3Storage:
		config.S3Config = &S3Config{
			Bucket:         bucket,
			Endpoint:       os.Getenv("S3_ENDPOINT"),
			AccessKey:      os.Getenv("S3_ACCESS_KEY"),
			SecretKey:      os.Getenv("S3_SECRET_KEY"),
			Region:         os.Getenv("S3_REGION"),
			Insecure:       os.Getenv("S3_INSECURE") == "true",
			ForcePathStyle: os.Getenv("S3_FORCE_PATH_STYLE") == "true",
		}
	case GCSStorage:
		config.GCSConfig = &GCSConfig{
			Bucket:         bucket,
			ServiceAccount: os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"),
		}
	default:
		return nil, errors.Errorf("unsupported storage type: %s", storageType)
	}

	return NewCloudStorage(ctx, config, logger)
}

// NewFromDestination creates a new cloud storage client from a destination URL
func NewFromDestination(ctx context.Context, destination string, logger log.Logger) (*CloudStorage, error) {
	storageType, bucket, _, err := ParseDestination(destination)
	if err != nil {
		return nil, err
	}

	return NewFromEnv(ctx, storageType, bucket, logger)
}

// Upload uploads a file to cloud storage
func (c *CloudStorage) Upload(ctx context.Context, localPath, objectName string, compress bool) error {
	file, err := os.Open(localPath)
	if err != nil {
		return errors.Wrap(err, "open local file")
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return errors.Wrap(err, "stat local file")
	}

	level.Info(c.logger).Log("msg", "Starting upload", "file", localPath, "object", objectName, "size", fileInfo.Size())

	var reader io.Reader = file

	if compress {
		// If compression is enabled, use a pipe to compress on-the-fly
		pr, pw := io.Pipe()
		encoder, err := zstd.NewWriter(pw)
		if err != nil {
			return errors.Wrap(err, "create zstd encoder")
		}

		go func() {
			_, copyErr := io.Copy(encoder, file)
			if copyErr != nil {
				_ = pw.CloseWithError(copyErr)
				return
			}
			if err := encoder.Close(); err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			_ = pw.Close()
		}()

		reader = pr
		objectName = objectName + ".zst"
	}

	startTime := time.Now()
	if err := c.bucket.Upload(ctx, objectName, reader); err != nil {
		return errors.Wrap(err, "upload data")
	}

	duration := time.Since(startTime)
	level.Info(c.logger).Log(
		"msg", "Upload complete",
		"file", localPath,
		"object", objectName,
		"size", fileInfo.Size(),
		"duration", duration,
		"speed_mbps", float64(fileInfo.Size())/duration.Seconds()/1024/1024*8,
	)

	return nil
}

// Download downloads a file from cloud storage
func (c *CloudStorage) Download(ctx context.Context, objectName, localPath string, decompress bool) error {
	level.Info(c.logger).Log("msg", "Starting download", "object", objectName, "file", localPath)

	// Create the directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return errors.Wrap(err, "create directory")
	}

	// Create the local file
	file, err := os.Create(localPath)
	if err != nil {
		return errors.Wrap(err, "create local file")
	}
	defer file.Close()

	// Get the object
	reader, err := c.bucket.Get(ctx, objectName)
	if err != nil {
		return errors.Wrap(err, "get object")
	}
	defer reader.Close()

	var writer io.Writer = file

	if decompress && strings.HasSuffix(objectName, ".zst") {
		// If decompression is enabled and the file has a .zst extension, decompress on-the-fly
		decoder, err := zstd.NewReader(reader)
		if err != nil {
			return errors.Wrap(err, "create zstd decoder")
		}
		defer decoder.Close()

		startTime := time.Now()
		n, err := io.Copy(writer, decoder)
		if err != nil {
			return errors.Wrap(err, "download and decompress data")
		}

		duration := time.Since(startTime)
		level.Info(c.logger).Log(
			"msg", "Download and decompress complete",
			"object", objectName,
			"file", localPath,
			"size", n,
			"duration", duration,
			"speed_mbps", float64(n)/duration.Seconds()/1024/1024*8,
		)
	} else {
		// Otherwise, just download the file
		startTime := time.Now()
		n, err := io.Copy(writer, reader)
		if err != nil {
			return errors.Wrap(err, "download data")
		}

		duration := time.Since(startTime)
		level.Info(c.logger).Log(
			"msg", "Download complete",
			"object", objectName,
			"file", localPath,
			"size", n,
			"duration", duration,
			"speed_mbps", float64(n)/duration.Seconds()/1024/1024*8,
		)
	}

	return nil
}

// List lists objects in cloud storage with the given prefix
func (c *CloudStorage) List(ctx context.Context, prefix string) ([]string, error) {
	var objects []string

	err := c.bucket.Iter(ctx, prefix, func(name string) error {
		objects = append(objects, name)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "list objects")
	}

	return objects, nil
}

// Delete deletes an object from cloud storage
func (c *CloudStorage) Delete(ctx context.Context, objectName string) error {
	return c.bucket.Delete(ctx, objectName)
}

// Exists checks if an object exists in cloud storage
func (c *CloudStorage) Exists(ctx context.Context, objectName string) (bool, error) {
	return c.bucket.Exists(ctx, objectName)
}

// Close closes the cloud storage client
func (c *CloudStorage) Close() error {
	return c.bucket.Close()
}

// GetBucket returns the underlying objstore.Bucket
func (c *CloudStorage) GetBucket() objstore.Bucket {
	return c.bucket
}

// ValidateDestination validates a destination URL
func ValidateDestination(destination string) error {
	_, _, _, err := ParseDestination(destination)
	return err
}

// GetObjectName generates an object name for a snapshot
func GetObjectName(prefix, snapshotName string) string {
	if prefix == "" {
		return snapshotName
	}
	return fmt.Sprintf("%s/%s", strings.TrimSuffix(prefix, "/"), snapshotName)
}
