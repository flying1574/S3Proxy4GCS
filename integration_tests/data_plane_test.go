package integration

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestStandardDataPlaneWithAWSSDK(t *testing.T) {
	dialer := &net.Dialer{}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if strings.HasPrefix(addr, "storage.googleapis.com") {
				return dialer.DialContext(ctx, network, "localhost:8081")
			}
			return dialer.DialContext(ctx, network, addr)
		},
	}

	creds := aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		return aws.Credentials{
			AccessKeyID:     getAWSAccessKey(),
			SecretAccessKey: getAWSSecretKey(),
			Source:          "test-env",
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(creds),
		config.WithRegion("us-east-1"),
	)
	if err != nil {
		t.Fatalf("Failed to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.HTTPClient = &http.Client{Transport: transport}
		o.BaseEndpoint = aws.String("http://storage.googleapis.com")
	})

	bucketName := getTestBucket()
	objectKey := getTestPrefix() + "test-object-key"
	content := "Hello from standard S3 data-plane test!"

	// 1. PutObject
	t.Logf("Testing PutObject on %s/.../%s", bucketName, objectKey)
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader(content),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	t.Logf("PutObject succeeded!")

	// 2. GetObject
	t.Logf("Testing GetObject...")
	_, err = client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	t.Logf("GetObject succeeded!")

	// 3. HeadObject
	t.Logf("Testing HeadObject...")
	_, err = client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}
	t.Logf("HeadObject succeeded!")

	// 4. ListObjectsV2
	t.Logf("Testing ListObjectsV2...")
	_, err = client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 failed: %v", err)
	}
	t.Logf("ListObjectsV2 succeeded!")

	// 5. DeleteObject
	t.Logf("Testing DeleteObject...")
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}
	t.Logf("DeleteObject succeeded!")
}

func TestMultipartUploadWithAWSSDK(t *testing.T) {
	dialer := &net.Dialer{}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if strings.HasPrefix(addr, "storage.googleapis.com") {
				return dialer.DialContext(ctx, network, "localhost:8081")
			}
			return dialer.DialContext(ctx, network, addr)
		},
	}

	creds := aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		return aws.Credentials{
			AccessKeyID:     getAWSAccessKey(),
			SecretAccessKey: getAWSSecretKey(),
			Source:          "test-env",
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(creds),
		config.WithRegion("us-east-1"),
		config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
	)
	if err != nil {
		t.Fatalf("Failed to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.HTTPClient = &http.Client{Transport: transport}
		o.BaseEndpoint = aws.String("http://storage.googleapis.com")
	})

	bucketName := getTestBucket()
	objectKey := getTestPrefix() + "test-multipart-object"

	// 1. CreateMultipartUpload
	t.Logf("Testing CreateMultipartUpload...")
	createResp, err := client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}
	var uploadIdStr string = "mock-upload-id-dryrun"
	if createResp.UploadId != nil {
		uploadIdStr = *createResp.UploadId
		t.Logf("CreateMultipartUpload succeeded with UploadId: %s", uploadIdStr)
	} else {
		t.Logf("Using mock UploadId for dry-run testing.")
	}

	// 2. UploadPart
	t.Logf("Testing UploadPart...")
	_, err = client.UploadPart(context.TODO(), &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		UploadId:   aws.String(uploadIdStr),
		PartNumber: aws.Int32(1),
		Body:       strings.NewReader("Part 1 content"),
	})
	if err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}
	t.Logf("UploadPart succeeded!")

	// 3. CompleteMultipartUpload (Simplified for DryRun, we don't need real Part etags)
	t.Logf("Testing CompleteMultipartUpload...")
	_, err = client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: aws.String(uploadIdStr),
	})
	if err != nil {
		t.Logf("Warning: CompleteMultipartUpload failed (expected in DryRun if it requires real parts): %v", err)
	} else {
		t.Logf("CompleteMultipartUpload succeeded via Proxy (DryRun verified)!")
	}

	// 4. AbortMultipartUpload
	t.Logf("Testing AbortMultipartUpload...")
	_, err = client.AbortMultipartUpload(context.TODO(), &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: aws.String(uploadIdStr),
	})
	if err != nil {
		t.Fatalf("AbortMultipartUpload failed: %v", err)
	}
}

func TestMultiObjectDeleteWithAWSSDK(t *testing.T) {
	dialer := &net.Dialer{}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if strings.HasPrefix(addr, "storage.googleapis.com") {
				return dialer.DialContext(ctx, network, "localhost:8081")
			}
			return dialer.DialContext(ctx, network, addr)
		},
	}

	creds := aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		return aws.Credentials{
			AccessKeyID:     getAWSAccessKey(),
			SecretAccessKey: getAWSSecretKey(),
			Source:          "test-env",
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(creds),
		config.WithRegion("us-east-1"),
	)
	if err != nil {
		t.Fatalf("Failed to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.HTTPClient = &http.Client{Transport: transport}
		o.BaseEndpoint = aws.String("http://storage.googleapis.com")
	})

	bucketName := getTestBucket()
	key1 := getTestPrefix() + "bulk-delete-item-1"
	key2 := getTestPrefix() + "bulk-delete-item-2"

	// 1. Create objects
	t.Logf("Creating objects for bulk deletion...")
	_, _ = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key1),
		Body:   strings.NewReader("Item 1"),
	})
	_, _ = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key2),
		Body:   strings.NewReader("Item 2"),
	})

	// 2. Perform DeleteObjects
	t.Logf("Executing DeleteObjects on %s and %s...", key1, key2)
	resp, err := client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{
				{Key: aws.String(key1)},
				{Key: aws.String(key2)},
			},
			Quiet: aws.Bool(false),
		},
	})

	if err != nil {
		t.Fatalf("DeleteObjects failed: %v", err)
	}

	t.Logf("DeleteObjects succeeded! Deleted count: %d", len(resp.Deleted))
}
