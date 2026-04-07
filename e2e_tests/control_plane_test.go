package e2e

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestLifecycleCRUD verifies Put -> Get -> Delete -> Get(empty) for lifecycle configuration.
func TestLifecycleCRUD(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket

	// Cleanup: always delete lifecycle at the end
	t.Cleanup(func() {
		client.DeleteBucketLifecycle(context.TODO(), &s3.DeleteBucketLifecycleInput{
			Bucket: aws.String(bucket),
		})
	})

	// 1. PutBucketLifecycleConfiguration
	t.Log("PutBucketLifecycleConfiguration...")
	_, err := client.PutBucketLifecycleConfiguration(context.TODO(), &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{
				{
					ID:     aws.String("E2E-Expiration"),
					Status: types.ExpirationStatusEnabled,
					Filter: &types.LifecycleRuleFilter{Prefix: aws.String("e2e-test/")},
					Expiration: &types.LifecycleExpiration{
						Days: aws.Int32(365),
					},
					Transitions: []types.Transition{
						{
							Days:         aws.Int32(30),
							StorageClass: types.TransitionStorageClassGlacier,
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("PutBucketLifecycleConfiguration failed: %v", err)
	}

	// 2. GetBucketLifecycleConfiguration — verify rules
	t.Log("GetBucketLifecycleConfiguration...")
	getOut, err := client.GetBucketLifecycleConfiguration(context.TODO(), &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("GetBucketLifecycleConfiguration failed: %v", err)
	}
	if len(getOut.Rules) == 0 {
		t.Fatal("GetBucketLifecycleConfiguration returned 0 rules, expected at least 1")
	}
	t.Logf("Got %d lifecycle rules", len(getOut.Rules))

	// 3. DeleteBucketLifecycle
	t.Log("DeleteBucketLifecycle...")
	_, err = client.DeleteBucketLifecycle(context.TODO(), &s3.DeleteBucketLifecycleInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("DeleteBucketLifecycle failed: %v", err)
	}

	// 4. GetBucketLifecycleConfiguration — should be empty or 404
	t.Log("GetBucketLifecycleConfiguration after delete (expect empty/error)...")
	getOut2, err := client.GetBucketLifecycleConfiguration(context.TODO(), &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Logf("GetBucketLifecycleConfiguration after delete returned error (expected): %v", err)
	} else if len(getOut2.Rules) > 0 {
		t.Fatalf("Expected 0 rules after delete, got %d", len(getOut2.Rules))
	}
}

// TestCORSCRUD verifies Put -> Get -> Delete -> Get(empty) for CORS configuration.
func TestCORSCRUD(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket

	t.Cleanup(func() {
		client.DeleteBucketCors(context.TODO(), &s3.DeleteBucketCorsInput{
			Bucket: aws.String(bucket),
		})
	})

	// 1. PutBucketCors
	t.Log("PutBucketCors...")
	_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket: aws.String(bucket),
		CORSConfiguration: &types.CORSConfiguration{
			CORSRules: []types.CORSRule{
				{
					AllowedMethods: []string{"GET", "PUT", "HEAD"},
					AllowedOrigins: []string{"https://e2e-test.example.com"},
					AllowedHeaders: []string{"Authorization", "Content-Type"},
					ExposeHeaders:  []string{"x-amz-request-id"},
					MaxAgeSeconds:  aws.Int32(3600),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("PutBucketCors failed: %v", err)
	}

	// 2. GetBucketCors — verify
	t.Log("GetBucketCors...")
	getOut, err := client.GetBucketCors(context.TODO(), &s3.GetBucketCorsInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("GetBucketCors failed: %v", err)
	}
	if len(getOut.CORSRules) == 0 {
		t.Fatal("GetBucketCors returned 0 rules")
	}
	rule := getOut.CORSRules[0]
	if len(rule.AllowedOrigins) == 0 || rule.AllowedOrigins[0] != "https://e2e-test.example.com" {
		t.Errorf("Unexpected AllowedOrigins: %v", rule.AllowedOrigins)
	}
	t.Logf("CORS rule verified: origins=%v methods=%v", rule.AllowedOrigins, rule.AllowedMethods)

	// 3. DeleteBucketCors
	t.Log("DeleteBucketCors...")
	_, err = client.DeleteBucketCors(context.TODO(), &s3.DeleteBucketCorsInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("DeleteBucketCors failed: %v", err)
	}

	// 4. GetBucketCors — should be empty
	t.Log("GetBucketCors after delete...")
	getOut2, err := client.GetBucketCors(context.TODO(), &s3.GetBucketCorsInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Logf("GetBucketCors after delete returned error (expected): %v", err)
	} else if len(getOut2.CORSRules) > 0 {
		t.Fatalf("Expected 0 CORS rules after delete, got %d", len(getOut2.CORSRules))
	}
}

// TestLoggingCRUD verifies Put -> Get -> Delete -> Get(disabled) for logging configuration.
func TestLoggingCRUD(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket

	t.Cleanup(func() {
		client.PutBucketLogging(context.TODO(), &s3.PutBucketLoggingInput{
			Bucket:              aws.String(bucket),
			BucketLoggingStatus: &types.BucketLoggingStatus{},
		})
	})

	// 1. PutBucketLogging
	t.Log("PutBucketLogging...")
	_, err := client.PutBucketLogging(context.TODO(), &s3.PutBucketLoggingInput{
		Bucket: aws.String(bucket),
		BucketLoggingStatus: &types.BucketLoggingStatus{
			LoggingEnabled: &types.LoggingEnabled{
				TargetBucket: aws.String(bucket),
				TargetPrefix: aws.String("e2e-logs/"),
			},
		},
	})
	if err != nil {
		t.Fatalf("PutBucketLogging failed: %v", err)
	}

	// 2. GetBucketLogging — verify
	t.Log("GetBucketLogging...")
	getOut, err := client.GetBucketLogging(context.TODO(), &s3.GetBucketLoggingInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("GetBucketLogging failed: %v", err)
	}
	if getOut.LoggingEnabled == nil {
		t.Fatal("GetBucketLogging returned nil LoggingEnabled")
	}
	if *getOut.LoggingEnabled.TargetPrefix != "e2e-logs/" {
		t.Errorf("Unexpected TargetPrefix: %s", *getOut.LoggingEnabled.TargetPrefix)
	}
	t.Logf("Logging verified: TargetBucket=%s TargetPrefix=%s",
		*getOut.LoggingEnabled.TargetBucket, *getOut.LoggingEnabled.TargetPrefix)

	// 3. DeleteBucketLogging (put empty logging)
	t.Log("DeleteBucketLogging...")
	_, err = client.PutBucketLogging(context.TODO(), &s3.PutBucketLoggingInput{
		Bucket:              aws.String(bucket),
		BucketLoggingStatus: &types.BucketLoggingStatus{},
	})
	if err != nil {
		t.Fatalf("DeleteBucketLogging failed: %v", err)
	}

	// 4. GetBucketLogging — should be disabled
	t.Log("GetBucketLogging after delete...")
	getOut2, err := client.GetBucketLogging(context.TODO(), &s3.GetBucketLoggingInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("GetBucketLogging after delete failed: %v", err)
	}
	if getOut2.LoggingEnabled != nil && getOut2.LoggingEnabled.TargetBucket != nil {
		t.Fatalf("Expected logging disabled after delete, but still enabled")
	}
}

// TestWebsiteCRUD verifies Put -> Get -> Delete -> Get(404) for website configuration.
func TestWebsiteCRUD(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket

	t.Cleanup(func() {
		client.DeleteBucketWebsite(context.TODO(), &s3.DeleteBucketWebsiteInput{
			Bucket: aws.String(bucket),
		})
	})

	// 1. PutBucketWebsite
	t.Log("PutBucketWebsite...")
	_, err := client.PutBucketWebsite(context.TODO(), &s3.PutBucketWebsiteInput{
		Bucket: aws.String(bucket),
		WebsiteConfiguration: &types.WebsiteConfiguration{
			IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")},
			ErrorDocument: &types.ErrorDocument{Key: aws.String("error.html")},
		},
	})
	if err != nil {
		t.Fatalf("PutBucketWebsite failed: %v", err)
	}

	// 2. GetBucketWebsite — verify
	t.Log("GetBucketWebsite...")
	getOut, err := client.GetBucketWebsite(context.TODO(), &s3.GetBucketWebsiteInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("GetBucketWebsite failed: %v", err)
	}
	if getOut.IndexDocument == nil || *getOut.IndexDocument.Suffix != "index.html" {
		t.Errorf("Unexpected IndexDocument: %v", getOut.IndexDocument)
	}
	if getOut.ErrorDocument == nil || *getOut.ErrorDocument.Key != "error.html" {
		t.Errorf("Unexpected ErrorDocument: %v", getOut.ErrorDocument)
	}
	t.Log("Website configuration verified")

	// 3. DeleteBucketWebsite
	t.Log("DeleteBucketWebsite...")
	_, err = client.DeleteBucketWebsite(context.TODO(), &s3.DeleteBucketWebsiteInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("DeleteBucketWebsite failed: %v", err)
	}

	// 4. GetBucketWebsite — should fail or be empty
	t.Log("GetBucketWebsite after delete (expect error)...")
	_, err = client.GetBucketWebsite(context.TODO(), &s3.GetBucketWebsiteInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Logf("GetBucketWebsite after delete correctly returned error: %v", err)
	} else {
		t.Log("Warning: GetBucketWebsite after delete succeeded (may return empty config)")
	}
}

// TestTaggingCRUD verifies Put -> Get -> Delete -> Get(empty) for object tagging.
func TestTaggingCRUD(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket
	key := GenerateTestKey(testEnv, "tagging-object")

	t.Cleanup(func() { Cleanup(t, client, bucket, key) })

	// Create object first
	t.Log("PutObject for tagging test...")
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader("tagging test content"),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// 1. PutObjectTagging
	t.Log("PutObjectTagging...")
	_, err = client.PutObjectTagging(context.TODO(), &s3.PutObjectTaggingInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Tagging: &types.Tagging{
			TagSet: []types.Tag{
				{Key: aws.String("Project"), Value: aws.String("E2E-Test")},
				{Key: aws.String("Stage"), Value: aws.String("Acceptance")},
			},
		},
	})
	if err != nil {
		t.Fatalf("PutObjectTagging failed: %v", err)
	}

	// 2. GetObjectTagging — verify
	t.Log("GetObjectTagging...")
	getOut, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObjectTagging failed: %v", err)
	}
	if len(getOut.TagSet) < 2 {
		t.Fatalf("Expected at least 2 tags, got %d", len(getOut.TagSet))
	}
	tagMap := make(map[string]string)
	for _, tag := range getOut.TagSet {
		tagMap[*tag.Key] = *tag.Value
	}
	if tagMap["Project"] != "E2E-Test" {
		t.Errorf("Expected tag Project=E2E-Test, got %s", tagMap["Project"])
	}
	if tagMap["Stage"] != "Acceptance" {
		t.Errorf("Expected tag Stage=Acceptance, got %s", tagMap["Stage"])
	}
	t.Logf("Tags verified: %v", tagMap)

	// 3. DeleteObjectTagging
	t.Log("DeleteObjectTagging...")
	_, err = client.DeleteObjectTagging(context.TODO(), &s3.DeleteObjectTaggingInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("DeleteObjectTagging failed: %v", err)
	}

	// 4. GetObjectTagging — should be empty
	t.Log("GetObjectTagging after delete...")
	getOut2, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObjectTagging after delete failed: %v", err)
	}
	if len(getOut2.TagSet) > 0 {
		t.Fatalf("Expected 0 tags after delete, got %d", len(getOut2.TagSet))
	}
	t.Log("Tags successfully cleared")
}
