"""Control plane tests using Python boto3 SDK."""
import pytest


class TestLifecycleCRUD:
    def test_lifecycle(self, s3_client, bucket):
        try:
            s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket,
                LifecycleConfiguration={
                    "Rules": [
                        {
                            "ID": "PyTest-Expiration",
                            "Status": "Enabled",
                            "Filter": {"Prefix": "py-test/"},
                            "Expiration": {"Days": 365},
                        }
                    ]
                },
            )

            resp = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)
            assert len(resp["Rules"]) >= 1

            s3_client.delete_bucket_lifecycle(Bucket=bucket)
        except Exception:
            s3_client.delete_bucket_lifecycle(Bucket=bucket)
            raise


class TestCORSCRUD:
    def test_cors(self, s3_client, bucket):
        try:
            s3_client.put_bucket_cors(
                Bucket=bucket,
                CORSConfiguration={
                    "CORSRules": [
                        {
                            "AllowedMethods": ["GET", "PUT"],
                            "AllowedOrigins": ["https://py-test.example.com"],
                            "AllowedHeaders": ["Authorization"],
                            "MaxAgeSeconds": 3600,
                        }
                    ]
                },
            )

            resp = s3_client.get_bucket_cors(Bucket=bucket)
            assert len(resp["CORSRules"]) >= 1

            s3_client.delete_bucket_cors(Bucket=bucket)
        except Exception:
            s3_client.delete_bucket_cors(Bucket=bucket)
            raise


class TestLoggingCRUD:
    def test_logging(self, s3_client, bucket):
        try:
            s3_client.put_bucket_logging(
                Bucket=bucket,
                BucketLoggingStatus={
                    "LoggingEnabled": {
                        "TargetBucket": bucket,
                        "TargetPrefix": "py-logs/",
                    }
                },
            )

            resp = s3_client.get_bucket_logging(Bucket=bucket)
            assert resp.get("LoggingEnabled") is not None

            # Clear logging
            s3_client.put_bucket_logging(
                Bucket=bucket, BucketLoggingStatus={}
            )
        except Exception:
            s3_client.put_bucket_logging(
                Bucket=bucket, BucketLoggingStatus={}
            )
            raise


class TestWebsiteCRUD:
    def test_website(self, s3_client, bucket):
        try:
            s3_client.put_bucket_website(
                Bucket=bucket,
                WebsiteConfiguration={
                    "IndexDocument": {"Suffix": "index.html"},
                    "ErrorDocument": {"Key": "error.html"},
                },
            )

            resp = s3_client.get_bucket_website(Bucket=bucket)
            assert resp["IndexDocument"]["Suffix"] == "index.html"

            s3_client.delete_bucket_website(Bucket=bucket)
        except Exception:
            s3_client.delete_bucket_website(Bucket=bucket)
            raise


class TestTaggingCRUD:
    def test_object_tagging(self, s3_client, bucket, test_key):
        try:
            s3_client.put_object(Bucket=bucket, Key=test_key, Body=b"tag test")

            s3_client.put_object_tagging(
                Bucket=bucket,
                Key=test_key,
                Tagging={
                    "TagSet": [
                        {"Key": "Env", "Value": "PyTest"},
                    ]
                },
            )

            resp = s3_client.get_object_tagging(Bucket=bucket, Key=test_key)
            assert len(resp["TagSet"]) >= 1

            s3_client.delete_object_tagging(Bucket=bucket, Key=test_key)
        finally:
            s3_client.delete_object(Bucket=bucket, Key=test_key)
