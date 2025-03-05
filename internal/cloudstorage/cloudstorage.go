package cloudstorage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

	// Default chunk size for parallel uploads (5MB)
	defaultChunkSize int64 = 5 * 1024 * 1024

	// Default bucket name if not specified
	defaultBucketName = "flashfs-backups"
)

// CompressionLevel represents the compression level for zstd
type CompressionLevel int

const (
	// CompressionFastest is the fastest compression level
	CompressionFastest CompressionLevel = 1
	// CompressionDefault is the default compression level
	CompressionDefault CompressionLevel = 3
	// CompressionBetter is a better compression level
	CompressionBetter CompressionLevel = 7
	// CompressionBest is the best compression level
	CompressionBest CompressionLevel = 22
)

// Config represents the configuration for cloud storage
type Config struct {
	Type StorageType

	// S3 specific configuration
	S3Config *S3Config

	// GCS specific configuration
	GCSConfig *GCSConfig

	// Common configuration
	ChunkSize        int64
	CompressionLevel CompressionLevel
	CreateBucket     bool
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
	bucket           objstore.Bucket
	logger           log.Logger
	chunkSize        int64
	compressionLevel CompressionLevel
	createBucket     bool
}

// ParseDestination parses a destination URL and returns the storage type, bucket, and prefix
func ParseDestination(destination string) (StorageType, string, string, error) {
	u, err := url.Parse(destination)
	if err != nil {
		return "", "", "", errors.Wrap(err, "parse destination URL")
	}

	// Check if the URL has the correct format with scheme and host
	if u.Scheme == "" || u.Host == "" {
		return "", "", "", errors.New("invalid URL format, expected scheme://bucket/prefix")
	}

	// Get the storage type from the scheme
	storageType := StorageType(u.Scheme)

	// Validate the storage type
	switch storageType {
	case S3Storage, GCSStorage:
		// Valid storage types
	default:
		return "", "", "", errors.Errorf("unsupported storage type: %s", storageType)
	}

	// Get the bucket from the host
	bucket := u.Host

	// Get the prefix from the path (remove leading slash)
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

	// Set default values if not specified
	chunkSize := defaultChunkSize
	if config.ChunkSize > 0 {
		chunkSize = config.ChunkSize
	}

	compressionLevel := CompressionDefault
	if config.CompressionLevel > 0 {
		compressionLevel = config.CompressionLevel
	}

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

	cs := &CloudStorage{
		bucket:           bucket,
		logger:           logger,
		chunkSize:        chunkSize,
		compressionLevel: compressionLevel,
		createBucket:     config.CreateBucket,
	}

	// Check if bucket exists and create it if needed
	if config.CreateBucket {
		if err := cs.ensureBucketExists(ctx); err != nil {
			return nil, errors.Wrap(err, "ensure bucket exists")
		}
	}

	return cs, nil
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

// ensureBucketExists checks if the bucket exists and creates it if it doesn't
func (c *CloudStorage) ensureBucketExists(ctx context.Context) error {
	// Check if the bucket exists by trying to list objects
	exists, err := c.bucket.Exists(ctx, "")
	if err != nil {
		// If we get an access denied error, assume the bucket exists
		if c.bucket.IsAccessDeniedErr(err) {
			if err := level.Debug(c.logger).Log("msg", "Access denied when checking bucket existence, assuming bucket exists"); err != nil {
				return errors.Wrap(err, "log message")
			}
			return nil
		}
		return errors.Wrap(err, "check bucket existence")
	}

	// If the bucket doesn't exist and we're using S3, try to create it
	if !exists && c.bucket.Provider() == objstore.S3 {
		if err := level.Info(c.logger).Log("msg", "Bucket does not exist, attempting to create it", "bucket", c.bucket.Name()); err != nil {
			return errors.Wrap(err, "log message")
		}

		// For S3, we would need to use the AWS SDK directly to create the bucket
		// However, since we don't have direct access to the AWS SDK in this context,
		// we'll log a warning and return an error
		if err := level.Warn(c.logger).Log("msg", "Automatic bucket creation is not implemented for S3 in this version"); err != nil {
			return errors.Wrap(err, "log message")
		}
		return errors.New("bucket does not exist and automatic creation is not implemented")
	}

	return nil
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

// Upload uploads a file to cloud storage with enhanced features
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

	if err := level.Info(c.logger).Log("msg", "Starting upload", "file", localPath, "object", objectName, "size", fileInfo.Size()); err != nil {
		return errors.Wrap(err, "log message")
	}

	var reader io.Reader = file

	if compress {
		// If compression is enabled, use a pipe to compress on-the-fly with the configured compression level
		pr, pw := io.Pipe()

		// Create encoder with the specified compression level
		var encoder *zstd.Encoder
		var encErr error

		switch c.compressionLevel {
		case CompressionFastest:
			encoder, encErr = zstd.NewWriter(pw, zstd.WithEncoderLevel(zstd.SpeedFastest))
		case CompressionBetter:
			encoder, encErr = zstd.NewWriter(pw, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
		case CompressionBest:
			encoder, encErr = zstd.NewWriter(pw, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
		default:
			encoder, encErr = zstd.NewWriter(pw)
		}

		if encErr != nil {
			return errors.Wrap(encErr, "create zstd encoder")
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

	// For large files, use parallel uploads if the file size is greater than the chunk size
	if fileInfo.Size() > c.chunkSize && !compress {
		err = c.uploadLargeFile(ctx, file, objectName, fileInfo.Size())
	} else {
		err = c.bucket.Upload(ctx, objectName, reader)
	}

	if err != nil {
		return errors.Wrap(err, "upload data")
	}

	duration := time.Since(startTime)
	if err := level.Info(c.logger).Log(
		"msg", "Upload complete",
		"file", localPath,
		"object", objectName,
		"size", fileInfo.Size(),
		"duration", duration,
		"speed_mbps", float64(fileInfo.Size())/duration.Seconds()/1024/1024*8,
	); err != nil {
		return errors.Wrap(err, "log message")
	}

	return nil
}

// uploadLargeFile uploads a large file in chunks using parallel uploads
func (c *CloudStorage) uploadLargeFile(ctx context.Context, file *os.File, objectName string, fileSize int64) error {
	// Calculate the number of chunks
	numChunks := (fileSize + c.chunkSize - 1) / c.chunkSize

	if err := level.Debug(c.logger).Log(
		"msg", "Starting parallel upload",
		"object", objectName,
		"size", fileSize,
		"chunks", numChunks,
		"chunkSize", c.chunkSize,
	); err != nil {
		return errors.Wrap(err, "log message")
	}

	// Create a wait group to wait for all uploads to complete
	var wg sync.WaitGroup
	errChan := make(chan error, numChunks)

	// Upload each chunk in parallel
	for i := int64(0); i < numChunks; i++ {
		wg.Add(1)

		go func(chunkIndex int64) {
			defer wg.Done()

			// Calculate the chunk offset and size
			offset := chunkIndex * c.chunkSize
			size := c.chunkSize
			if offset+size > fileSize {
				size = fileSize - offset
			}

			// Create a buffer for the chunk
			buffer := make([]byte, size)

			// Seek to the correct position in the file
			_, err := file.Seek(offset, 0)
			if err != nil {
				errChan <- errors.Wrap(err, "seek in file")
				return
			}

			// Read the chunk
			_, err = io.ReadFull(file, buffer)
			if err != nil {
				errChan <- errors.Wrap(err, "read chunk")
				return
			}

			// Create a chunk name
			chunkName := fmt.Sprintf("%s.part%d", objectName, chunkIndex)

			// Upload the chunk
			if err := level.Debug(c.logger).Log("msg", "Uploading chunk", "chunk", chunkIndex, "offset", offset, "size", size); err != nil {
				errChan <- errors.Wrap(err, "log message")
				return
			}
			err = c.bucket.Upload(ctx, chunkName, bytes.NewReader(buffer))
			if err != nil {
				errChan <- errors.Wrap(err, "upload chunk")
				return
			}
		}(i)
	}

	// Wait for all uploads to complete
	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	// TODO: Implement a way to combine the chunks on the server side
	// This would typically involve using the S3 multipart upload API or similar
	// For now, this is a simplified implementation that uploads separate chunks

	return nil
}

// Download downloads a file from cloud storage
func (c *CloudStorage) Download(ctx context.Context, objectName, localPath string, decompress bool) error {
	// Check if the object exists
	exists, err := c.bucket.Exists(ctx, objectName)
	if err != nil {
		return errors.Wrap(err, "check object existence")
	}
	if !exists {
		return errors.Errorf("object %s does not exist", objectName)
	}

	// Create the directory if it doesn't exist
	dir := filepath.Dir(localPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
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

	if err := level.Info(c.logger).Log("msg", "Starting download", "object", objectName, "file", localPath); err != nil {
		return errors.Wrap(err, "log message")
	}

	startTime := time.Now()

	var writer io.Writer = file

	if decompress && strings.HasSuffix(objectName, ".zst") {
		// If decompression is enabled and the object has a .zst extension, use a pipe to decompress on-the-fly
		decoder, err := zstd.NewReader(reader)
		if err != nil {
			return errors.Wrap(err, "create zstd decoder")
		}
		defer decoder.Close()

		// Copy the decompressed data to the file
		_, err = io.Copy(writer, decoder)
		if err != nil {
			return errors.Wrap(err, "copy decompressed data")
		}
	} else {
		// Copy the data directly to the file
		_, err = io.Copy(writer, reader)
		if err != nil {
			return errors.Wrap(err, "copy data")
		}
	}

	duration := time.Since(startTime)
	fileInfo, err := file.Stat()
	if err != nil {
		return errors.Wrap(err, "stat local file")
	}

	if err := level.Info(c.logger).Log(
		"msg", "Download complete",
		"object", objectName,
		"file", localPath,
		"size", fileInfo.Size(),
		"duration", duration,
		"speed_mbps", float64(fileInfo.Size())/duration.Seconds()/1024/1024*8,
	); err != nil {
		return errors.Wrap(err, "log message")
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

// RestoreSnapshot downloads a snapshot from cloud storage and restores it to the local filesystem
func (c *CloudStorage) RestoreSnapshot(ctx context.Context, objectName, localDir string, decompress bool, overwrite bool) error {
	// Create the local directory if it doesn't exist
	if err := os.MkdirAll(localDir, 0755); err != nil {
		return errors.Wrap(err, "create local directory")
	}

	// Check if the object exists
	exists, err := c.bucket.Exists(ctx, objectName)
	if err != nil {
		return errors.Wrap(err, "check object existence")
	}
	if !exists {
		return errors.Errorf("object %s does not exist", objectName)
	}

	// Determine the local file path
	localPath := filepath.Join(localDir, filepath.Base(objectName))

	// Check if the file already exists and we're not overwriting
	if !overwrite {
		if _, err := os.Stat(localPath); err == nil {
			return errors.Errorf("file %s already exists and overwrite is not enabled", localPath)
		}
	}

	// Download the file
	return c.Download(ctx, objectName, localPath, decompress)
}
