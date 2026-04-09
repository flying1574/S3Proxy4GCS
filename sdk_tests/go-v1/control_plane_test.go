package gov1

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func TestLifecycleCRUD(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket

	t.Cleanup(func() {
		client.DeleteBucketLifecycle(&s3.DeleteBucketLifecycleInput{
			Bucket: aws.String(bucket),
		})
	})

	_, err := client.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					ID:     aws.String("GoV1-Expiration"),
					Status: aws.String("Enabled"),
					Filter: &s3.LifecycleRuleFilter{Prefix: aws.String("gov1-test/")},
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(365),
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("PutBucketLifecycleConfiguration failed: %v", err)
	}

	getOut, err := client.GetBucketLifecycleConfiguration(&s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("GetBucketLifecycleConfiguration failed: %v", err)
	}
	if len(getOut.Rules) == 0 {
		t.Fatal("Expected at least 1 lifecycle rule")
	}

	_, err = client.DeleteBucketLifecycle(&s3.DeleteBucketLifecycleInput{
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
		client.DeleteBucketCors(&s3.DeleteBucketCorsInput{
			Bucket: aws.String(bucket),
		})
	})

	_, err := client.PutBucketCors(&s3.PutBucketCorsInput{
		Bucket: aws.String(bucket),
		CORSConfiguration: &s3.CORSConfiguration{
			CORSRules: []*s3.CORSRule{
				{
					AllowedMethods: []*string{aws.String("GET"), aws.String("PUT")},
					AllowedOrigins: []*string{aws.String("https://gov1-test.example.com")},
					AllowedHeaders: []*string{aws.String("Authorization")},
					MaxAgeSeconds:  aws.Int64(3600),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("PutBucketCors failed: %v", err)
	}

	getOut, err := client.GetBucketCors(&s3.GetBucketCorsInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("GetBucketCors failed: %v", err)
	}
	if len(getOut.CORSRules) == 0 {
		t.Fatal("Expected at least 1 CORS rule")
	}

	_, err = client.DeleteBucketCors(&s3.DeleteBucketCorsInput{
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
		client.PutBucketLogging(&s3.PutBucketLoggingInput{
			Bucket:              aws.String(bucket),
			BucketLoggingStatus: &s3.BucketLoggingStatus{},
		})
	})

	_, err := client.PutBucketLogging(&s3.PutBucketLoggingInput{
		Bucket: aws.String(bucket),
		BucketLoggingStatus: &s3.BucketLoggingStatus{
			LoggingEnabled: &s3.LoggingEnabled{
				TargetBucket: aws.String(bucket),
				TargetPrefix: aws.String("gov1-logs/"),
			},
		},
	})
	if err != nil {
		t.Fatalf("PutBucketLogging failed: %v", err)
	}

	getOut, err := client.GetBucketLogging(&s3.GetBucketLoggingInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("GetBucketLogging failed: %v", err)
	}
	if getOut.LoggingEnabled == nil {
		t.Fatal("Expected LoggingEnabled to be set")
	}

	_, err = client.PutBucketLogging(&s3.PutBucketLoggingInput{
		Bucket:              aws.String(bucket),
		BucketLoggingStatus: &s3.BucketLoggingStatus{},
	})
	if err != nil {
		t.Fatalf("Clear logging failed: %v", err)
	}
}

func TestWebsiteCRUD(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket

	t.Cleanup(func() {
		client.DeleteBucketWebsite(&s3.DeleteBucketWebsiteInput{
			Bucket: aws.String(bucket),
		})
	})

	_, err := client.PutBucketWebsite(&s3.PutBucketWebsiteInput{
		Bucket: aws.String(bucket),
		WebsiteConfiguration: &s3.WebsiteConfiguration{
			IndexDocument: &s3.IndexDocument{Suffix: aws.String("index.html")},
			ErrorDocument: &s3.ErrorDocument{Key: aws.String("error.html")},
		},
	})
	if err != nil {
		t.Fatalf("PutBucketWebsite failed: %v", err)
	}

	getOut, err := client.GetBucketWebsite(&s3.GetBucketWebsiteInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("GetBucketWebsite failed: %v", err)
	}
	if getOut.IndexDocument == nil || *getOut.IndexDocument.Suffix != "index.html" {
		t.Errorf("Unexpected IndexDocument")
	}

	_, err = client.DeleteBucketWebsite(&s3.DeleteBucketWebsiteInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("DeleteBucketWebsite failed: %v", err)
	}
}

func TestTaggingCRUD(t *testing.T) {
	client := NewS3Client(t, testEnv)
	bucket := testEnv.TestBucket
	key := GenerateTestKey(testEnv, "gov1-tagging")

	t.Cleanup(func() { Cleanup(t, client, bucket, key) })

	_, err := client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		Body: strings.NewReader("tagging test"),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	_, err = client.PutObjectTagging(&s3.PutObjectTaggingInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		Tagging: &s3.Tagging{
			TagSet: []*s3.Tag{
				{Key: aws.String("Env"), Value: aws.String("GoV1Test")},
			},
		},
	})
	if err != nil {
		t.Fatalf("PutObjectTagging failed: %v", err)
	}

	getOut, err := client.GetObjectTagging(&s3.GetObjectTaggingInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObjectTagging failed: %v", err)
	}
	if len(getOut.TagSet) == 0 {
		t.Fatal("Expected at least 1 tag")
	}

	_, err = client.DeleteObjectTagging(&s3.DeleteObjectTaggingInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("DeleteObjectTagging failed: %v", err)
	}
}
