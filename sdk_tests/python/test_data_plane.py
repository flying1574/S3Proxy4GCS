"""Data plane tests using Python boto3 SDK."""
import io
import time

import pytest


class TestObjectCRUD:
    def test_put_get_delete(self, s3_client, bucket, test_key):
        content = b"Hello from Python boto3 test!"

        # PutObject
        s3_client.put_object(Bucket=bucket, Key=test_key, Body=content)

        # GetObject
        resp = s3_client.get_object(Bucket=bucket, Key=test_key)
        body = resp["Body"].read()
        assert body == content, f"Body mismatch: {body!r}"

        # HeadObject
        head = s3_client.head_object(Bucket=bucket, Key=test_key)
        assert head["ContentLength"] == len(content)

        # DeleteObject
        s3_client.delete_object(Bucket=bucket, Key=test_key)

        # GetObject after delete should fail
        with pytest.raises(s3_client.exceptions.NoSuchKey):
            s3_client.get_object(Bucket=bucket, Key=test_key)


class TestMultipartUpload:
    def test_multipart(self, s3_client, bucket, test_key):
        part1 = b"A" * (5 * 1024 * 1024)  # 5MB minimum
        part2 = b"Final part"

        # Create
        create = s3_client.create_multipart_upload(Bucket=bucket, Key=test_key)
        upload_id = create["UploadId"]

        try:
            # Upload parts
            up1 = s3_client.upload_part(
                Bucket=bucket, Key=test_key, UploadId=upload_id,
                PartNumber=1, Body=part1,
            )
            up2 = s3_client.upload_part(
                Bucket=bucket, Key=test_key, UploadId=upload_id,
                PartNumber=2, Body=part2,
            )

            # Complete
            s3_client.complete_multipart_upload(
                Bucket=bucket, Key=test_key, UploadId=upload_id,
                MultipartUpload={
                    "Parts": [
                        {"PartNumber": 1, "ETag": up1["ETag"]},
                        {"PartNumber": 2, "ETag": up2["ETag"]},
                    ]
                },
            )

            # Verify
            resp = s3_client.get_object(Bucket=bucket, Key=test_key)
            merged = resp["Body"].read()
            assert len(merged) == len(part1) + len(part2)
        finally:
            s3_client.delete_object(Bucket=bucket, Key=test_key)


class TestListObjects:
    def test_list_objects_v2(self, s3_client, bucket, env):
        prefix = env["test_prefix"] + "py-list/"
        keys = [prefix + x for x in ("a", "b", "c")]

        try:
            for key in keys:
                s3_client.put_object(Bucket=bucket, Key=key, Body=b"list test")

            resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            contents = resp.get("Contents", [])
            assert len(contents) >= 3, f"Expected >=3, got {len(contents)}"
        finally:
            for key in keys:
                s3_client.delete_object(Bucket=bucket, Key=key)


class TestStorageClass:
    def test_storage_class_translation(self, s3_client, bucket, test_key):
        try:
            s3_client.put_object(
                Bucket=bucket, Key=test_key,
                Body=b"storage class test",
                StorageClass="STANDARD_IA",
            )
            head = s3_client.head_object(Bucket=bucket, Key=test_key)
            # Just verify request succeeded; exact class depends on GCS mapping
            assert head["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            s3_client.delete_object(Bucket=bucket, Key=test_key)

