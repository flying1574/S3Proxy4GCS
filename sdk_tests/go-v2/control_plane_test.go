package gov2

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestLifecycleCRUD(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket

	t.Cleanup(func() {
		client.DeleteBucketLifecycle(context.TODO(), &s3.DeleteBucketLifecycleInput{
			Bucket: aws.String(bucket),
		})
	})

	_, err := client.PutBucketLifecycleConfiguration(context.TODO(), &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{
				{
					ID:     aws.String("GoV2-Expiration"),
					Status: types.ExpirationStatusEnabled,
					Filter: &types.LifecycleRuleFilter{Prefix: aws.String("gov2-test/")},
					Expiration: &types.LifecycleExpiration{
						Days: aws.Int32(365),
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("PutBucketLifecycleConfiguration failed: %v", err)
	}

	getOut, err := client.GetBucketLifecycleConfiguration(context.TODO(), &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("GetBucketLifecycleConfiguration failed: %v", err)
	}
	if len(getOut.Rules) == 0 {
		t.Fatal("Expected at least 1 lifecycle rule")
	}

	_, err = client.DeleteBucketLifecycle(context.TODO(), &s3.DeleteBucketLifecycleInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("DeleteBucketLifecycle failed: %v", err)
	}
}

func TestCORSCRUD(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket

	t.Cleanup(func() {
		client.DeleteBucketCors(context.TODO(), &s3.DeleteBucketCorsInput{
			Bucket: aws.String(bucket),
		})
	})

	_, err := client.PutBucketCors(context.TODO(), &s3.PutBucketCorsInput{
		Bucket: aws.String(bucket),
		CORSConfiguration: &types.CORSConfiguration{
			CORSRules: []types.CORSRule{
				{
					AllowedMethods: []string{"GET", "PUT"},
					AllowedOrigins: []string{"https://gov2-test.example.com"},
					AllowedHeaders: []string{"Authorization"},
					MaxAgeSeconds:  aws.Int32(3600),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("PutBucketCors failed: %v", err)
	}

	getOut, err := client.GetBucketCors(context.TODO(), &s3.GetBucketCorsInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("GetBucketCors failed: %v", err)
	}
	if len(getOut.CORSRules) == 0 {
		t.Fatal("Expected at least 1 CORS rule")
	}

	_, err = client.DeleteBucketCors(context.TODO(), &s3.DeleteBucketCorsInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("DeleteBucketCors failed: %v", err)
	}
}

func TestLoggingCRUD(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket

	t.Cleanup(func() {
		client.PutBucketLogging(context.TODO(), &s3.PutBucketLoggingInput{
			Bucket:              aws.String(bucket),
			BucketLoggingStatus: &types.BucketLoggingStatus{},
		})
	})

	_, err := client.PutBucketLogging(context.TODO(), &s3.PutBucketLoggingInput{
		Bucket: aws.String(bucket),
		BucketLoggingStatus: &types.BucketLoggingStatus{
			LoggingEnabled: &types.LoggingEnabled{
				TargetBucket: aws.String(bucket),
				TargetPrefix: aws.String("gov2-logs/"),
			},
		},
	})
	if err != nil {
		t.Fatalf("PutBucketLogging failed: %v", err)
	}

	getOut, err := client.GetBucketLogging(context.TODO(), &s3.GetBucketLoggingInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("GetBucketLogging failed: %v", err)
	}
	if getOut.LoggingEnabled == nil {
		t.Fatal("Expected LoggingEnabled to be set")
	}

	_, err = client.PutBucketLogging(context.TODO(), &s3.PutBucketLoggingInput{
		Bucket:              aws.String(bucket),
		BucketLoggingStatus: &types.BucketLoggingStatus{},
	})
	if err != nil {
		t.Fatalf("Clear logging failed: %v", err)
	}
}

func TestWebsiteCRUD(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket

	t.Cleanup(func() {
		client.DeleteBucketWebsite(context.TODO(), &s3.DeleteBucketWebsiteInput{
			Bucket: aws.String(bucket),
		})
	})

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

	getOut, err := client.GetBucketWebsite(context.TODO(), &s3.GetBucketWebsiteInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("GetBucketWebsite failed: %v", err)
	}
	if getOut.IndexDocument == nil || *getOut.IndexDocument.Suffix != "index.html" {
		t.Errorf("Unexpected IndexDocument")
	}

	_, err = client.DeleteBucketWebsite(context.TODO(), &s3.DeleteBucketWebsiteInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("DeleteBucketWebsite failed: %v", err)
	}
}

func TestTaggingCRUD(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket
	key := GenerateTestKey(testEnv, "gov2-tagging")

	t.Cleanup(func() { Cleanup(t, client, bucket, key) })

	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		Body: strings.NewReader("tagging test"),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	_, err = client.PutObjectTagging(context.TODO(), &s3.PutObjectTaggingInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		Tagging: &types.Tagging{
			TagSet: []types.Tag{
				{Key: aws.String("Env"), Value: aws.String("GoV2Test")},
			},
		},
	})
	if err != nil {
		t.Fatalf("PutObjectTagging failed: %v", err)
	}

	getOut, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObjectTagging failed: %v", err)
	}
	if len(getOut.TagSet) == 0 {
		t.Fatal("Expected at least 1 tag")
	}

	_, err = client.DeleteObjectTagging(context.TODO(), &s3.DeleteObjectTaggingInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("DeleteObjectTagging failed: %v", err)
	}
}
