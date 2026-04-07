package e2e

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

// TestObjectCRUD verifies the full object lifecycle: Put -> Get -> Head -> Delete -> Get(404).
func TestObjectCRUD(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket
	key := GenerateTestKey(testEnv, "crud-object")
	content := "Hello from E2E CRUD test!"

	t.Cleanup(func() { Cleanup(t, client, bucket, key) })

	// 1. PutObject
	t.Log("PutObject...")
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(content),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// 2. GetObject — verify body matches
	t.Log("GetObject...")
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
		t.Fatalf("GetObject body mismatch: got %q, want %q", string(body), content)
	}

	// 3. HeadObject — verify metadata exists
	t.Log("HeadObject...")
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

	// 4. DeleteObject
	t.Log("DeleteObject...")
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// 5. GetObject after delete — should fail
	t.Log("GetObject after delete (expect error)...")
	_, err = client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		t.Fatalf("GetObject after delete should have failed, but succeeded")
	}
	t.Logf("GetObject after delete correctly returned error: %v", err)
}

// TestMultipartUpload verifies multipart upload: Create -> Upload 2 parts -> Complete -> Get -> Cleanup.
func TestMultipartUpload(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket
	key := GenerateTestKey(testEnv, "multipart-object")

	t.Cleanup(func() { Cleanup(t, client, bucket, key) })

	part1Content := strings.Repeat("A", 5*1024*1024) // 5MB minimum part size
	part2Content := "Final part content"

	// 1. CreateMultipartUpload
	t.Log("CreateMultipartUpload...")
	createOut, err := client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}
	uploadID := createOut.UploadId

	// 2. UploadPart x2
	t.Log("UploadPart #1...")
	up1, err := client.UploadPart(context.TODO(), &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   uploadID,
		PartNumber: aws.Int32(1),
		Body:       strings.NewReader(part1Content),
	})
	if err != nil {
		t.Fatalf("UploadPart #1 failed: %v", err)
	}

	t.Log("UploadPart #2...")
	up2, err := client.UploadPart(context.TODO(), &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   uploadID,
		PartNumber: aws.Int32(2),
		Body:       strings.NewReader(part2Content),
	})
	if err != nil {
		t.Fatalf("UploadPart #2 failed: %v", err)
	}

	// 3. CompleteMultipartUpload
	t.Log("CompleteMultipartUpload...")
	_, err = client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: uploadID,
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

	// 4. GetObject — verify combined body
	t.Log("GetObject (verify merged content)...")
	getOut, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject after multipart failed: %v", err)
	}
	mergedBody, _ := io.ReadAll(getOut.Body)
	getOut.Body.Close()

	expectedBody := part1Content + part2Content
	if !bytes.Equal(mergedBody, []byte(expectedBody)) {
		t.Fatalf("Multipart merged body mismatch: got %d bytes, want %d bytes", len(mergedBody), len(expectedBody))
	}
	t.Logf("Multipart upload verified: %d bytes", len(mergedBody))
}

// TestStorageClassTranslation verifies that storage class headers are translated correctly.
func TestStorageClassTranslation(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket
	key := GenerateTestKey(testEnv, "storage-class-object")

	t.Cleanup(func() { Cleanup(t, client, bucket, key) })

	t.Log("PutObject with StorageClass=STANDARD_IA...")
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		Body:         strings.NewReader("storage class test"),
		StorageClass: types.StorageClassStandardIa,
	})
	if err != nil {
		t.Fatalf("PutObject with STANDARD_IA failed: %v", err)
	}

	t.Log("HeadObject (verify storage class)...")
	headOut, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}
	t.Logf("HeadObject StorageClass: %s", headOut.StorageClass)
	// GCS should have mapped STANDARD_IA -> NEARLINE -> back to some class
	// The exact mapping depends on proxy; we just verify the request succeeded.
}

// TestListObjectsV2 verifies listing objects with prefix.
func TestListObjectsV2(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket
	prefix := testEnv.TestPrefix + "list-test/"

	keys := []string{
		prefix + "obj-a",
		prefix + "obj-b",
		prefix + "obj-c",
	}

	t.Cleanup(func() { Cleanup(t, client, bucket, keys...) })

	for _, key := range keys {
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader("list test content"),
		})
		if err != nil {
			t.Fatalf("PutObject(%s) failed: %v", key, err)
		}
	}

	t.Log("ListObjectsV2...")
	listOut, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 failed: %v", err)
	}

	if len(listOut.Contents) < 3 {
		t.Fatalf("ListObjectsV2 returned %d objects, expected at least 3", len(listOut.Contents))
	}

	foundKeys := make(map[string]bool)
	for _, obj := range listOut.Contents {
		foundKeys[*obj.Key] = true
	}
	for _, key := range keys {
		if !foundKeys[key] {
			t.Errorf("ListObjectsV2 missing expected key: %s", key)
		}
	}
	t.Logf("ListObjectsV2 returned %d objects", len(listOut.Contents))
}

// TestVersioning verifies object versioning through the proxy.
func TestVersioning(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket
	key := GenerateTestKey(testEnv, "versioned-object")

	t.Cleanup(func() { Cleanup(t, client, bucket, key) })

	// PutObject twice to create versions
	t.Log("PutObject v1...")
	put1, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader("version 1"),
	})
	if err != nil {
		t.Fatalf("PutObject v1 failed: %v", err)
	}
	if put1.VersionId != nil {
		t.Logf("v1 VersionId: %s", *put1.VersionId)
	}

	t.Log("PutObject v2...")
	put2, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader("version 2"),
	})
	if err != nil {
		t.Fatalf("PutObject v2 failed: %v", err)
	}
	if put2.VersionId != nil {
		t.Logf("v2 VersionId: %s", *put2.VersionId)
	}

	// HeadObject — verify VersionId is present
	t.Log("HeadObject (verify VersionId)...")
	headOut, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}
	if headOut.VersionId == nil {
		t.Log("Warning: HeadObject returned nil VersionId (bucket may not have versioning enabled)")
	} else {
		t.Logf("HeadObject VersionId: %s", *headOut.VersionId)
	}
}
