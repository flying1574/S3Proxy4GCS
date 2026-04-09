package gov2

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestObjectCRUD(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket
	key := GenerateTestKey(testEnv, "gov2-crud")
	content := "Hello from Go V2 SDK test!"

	t.Cleanup(func() { Cleanup(t, client, bucket, key) })

	// PutObject
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(content),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// GetObject
	getOut, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	body, _ := io.ReadAll(getOut.Body)
	getOut.Body.Close()
	if string(body) != content {
		t.Fatalf("body mismatch: got %q, want %q", string(body), content)
	}

	// HeadObject
	headOut, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}
	if headOut.ContentLength == nil || *headOut.ContentLength == 0 {
		t.Errorf("HeadObject returned zero ContentLength")
	}

	// DeleteObject
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// GetObject after delete — expect error
	_, err = client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		t.Fatal("GetObject after delete should have failed")
	}
}

func TestMultipartUpload(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket
	key := GenerateTestKey(testEnv, "gov2-multipart")

	t.Cleanup(func() { Cleanup(t, client, bucket, key) })

	part1 := strings.Repeat("A", 5*1024*1024)
	part2 := "Final part"

	createOut, err := client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	up1, err := client.UploadPart(context.TODO(), &s3.UploadPartInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		UploadId: createOut.UploadId, PartNumber: aws.Int32(1),
		Body: strings.NewReader(part1),
	})
	if err != nil {
		t.Fatalf("UploadPart #1 failed: %v", err)
	}

	up2, err := client.UploadPart(context.TODO(), &s3.UploadPartInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		UploadId: createOut.UploadId, PartNumber: aws.Int32(2),
		Body: strings.NewReader(part2),
	})
	if err != nil {
		t.Fatalf("UploadPart #2 failed: %v", err)
	}

	_, err = client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		UploadId: createOut.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: up1.ETag},
				{PartNumber: aws.Int32(2), ETag: up2.ETag},
			},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	getOut, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject after multipart failed: %v", err)
	}
	merged, _ := io.ReadAll(getOut.Body)
	getOut.Body.Close()
	if !bytes.Equal(merged, []byte(part1+part2)) {
		t.Fatalf("Multipart body mismatch: got %d bytes, want %d", len(merged), len(part1)+len(part2))
	}
}

func TestListObjectsV2(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket
	prefix := testEnv.TestPrefix + "gov2-list/"

	keys := []string{prefix + "a", prefix + "b", prefix + "c"}
	t.Cleanup(func() { Cleanup(t, client, bucket, keys...) })

	for _, key := range keys {
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
			Body: strings.NewReader("list test"),
		})
		if err != nil {
			t.Fatalf("PutObject(%s) failed: %v", key, err)
		}
	}

	listOut, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket), Prefix: aws.String(prefix),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 failed: %v", err)
	}
	if len(listOut.Contents) < 3 {
		t.Fatalf("Expected at least 3 objects, got %d", len(listOut.Contents))
	}
}

func TestStorageClass(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket
	key := GenerateTestKey(testEnv, "gov2-storageclass")

	t.Cleanup(func() { Cleanup(t, client, bucket, key) })

	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		Body:         strings.NewReader("storage class test"),
		StorageClass: types.StorageClassStandardIa,
	})
	if err != nil {
		t.Fatalf("PutObject with STANDARD_IA failed: %v", err)
	}

	_, err = client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}
}

func TestVersioning(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket
	key := GenerateTestKey(testEnv, "gov2-versioned")

	t.Cleanup(func() { Cleanup(t, client, bucket, key) })

	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		Body: strings.NewReader("version 1"),
	})
	if err != nil {
		t.Fatalf("PutObject v1 failed: %v", err)
	}

	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		Body: strings.NewReader("version 2"),
	})
	if err != nil {
		t.Fatalf("PutObject v2 failed: %v", err)
	}

	headOut, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}
	if headOut.VersionId != nil {
		t.Logf("VersionId: %s", *headOut.VersionId)
	}
}
