package s3proxy;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import org.junit.jupiter.api.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Functional tests using AWS Java SDK V1 (1.12.500).
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class S3ProxyV1Test {

    private static AmazonS3 s3;
    private static String bucket;
    private static String prefix;

    @BeforeAll
    static void setup() {
        String endpoint = System.getenv("PROXY_ENDPOINT");
        String access = System.getenv("GCS_HMAC_ACCESS");
        String secret = System.getenv("GCS_HMAC_SECRET");
        bucket = System.getenv("TEST_BUCKET");
        prefix = System.getenv("TEST_PREFIX") != null ? System.getenv("TEST_PREFIX") : "";

        assertNotNull(endpoint, "PROXY_ENDPOINT required");
        assertNotNull(access, "GCS_HMAC_ACCESS required");
        assertNotNull(secret, "GCS_HMAC_SECRET required");
        assertNotNull(bucket, "TEST_BUCKET required");

        s3 = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(access, secret)))
                .withPathStyleAccessEnabled(true)
                .build();
    }

    private String testKey(String suffix) {
        return prefix + "javav1-" + suffix + "-" + System.nanoTime();
    }

    // ---- Data Plane ----

    @Test
    @Order(1)
    void testObjectCRUD() {
        String key = testKey("crud");
        String content = "Hello from Java V1 SDK!";

        // Put
        s3.putObject(bucket, key, content);

        // Get
        S3Object obj = s3.getObject(bucket, key);
        String body = new String(readAll(obj.getObjectContent()), StandardCharsets.UTF_8);
        assertEquals(content, body);

        // Head
        ObjectMetadata meta = s3.getObjectMetadata(bucket, key);
        assertTrue(meta.getContentLength() > 0);

        // Delete
        s3.deleteObject(bucket, key);

        // Get after delete
        assertThrows(AmazonS3Exception.class, () -> s3.getObject(bucket, key));
    }

    @Test
    @Order(2)
    void testMultipartUpload() {
        String key = testKey("multipart");
        byte[] part1 = new byte[5 * 1024 * 1024]; // 5MB
        java.util.Arrays.fill(part1, (byte) 'A');
        byte[] part2 = "Final".getBytes(StandardCharsets.UTF_8);

        InitiateMultipartUploadResult init = s3.initiateMultipartUpload(
                new InitiateMultipartUploadRequest(bucket, key));
        String uploadId = init.getUploadId();

        try {
            UploadPartResult r1 = s3.uploadPart(new UploadPartRequest()
                    .withBucketName(bucket).withKey(key).withUploadId(uploadId)
                    .withPartNumber(1).withInputStream(new ByteArrayInputStream(part1))
                    .withPartSize(part1.length));

            UploadPartResult r2 = s3.uploadPart(new UploadPartRequest()
                    .withBucketName(bucket).withKey(key).withUploadId(uploadId)
                    .withPartNumber(2).withInputStream(new ByteArrayInputStream(part2))
                    .withPartSize(part2.length));

            List<PartETag> parts = new ArrayList<>();
            parts.add(r1.getPartETag());
            parts.add(r2.getPartETag());

            s3.completeMultipartUpload(new CompleteMultipartUploadRequest(bucket, key, uploadId, parts));

            S3Object obj = s3.getObject(bucket, key);
            byte[] merged = readAll(obj.getObjectContent());
            assertEquals(part1.length + part2.length, merged.length);
        } finally {
            s3.deleteObject(bucket, key);
        }
    }

    @Test
    @Order(3)
    void testListObjectsV2() {
        String listPrefix = prefix + "javav1-list/";
        String[] keys = {listPrefix + "a", listPrefix + "b", listPrefix + "c"};

        try {
            for (String key : keys) {
                s3.putObject(bucket, key, "list test");
            }
            ListObjectsV2Result result = s3.listObjectsV2(bucket, listPrefix);
            assertTrue(result.getObjectSummaries().size() >= 3);
        } finally {
            for (String key : keys) {
                s3.deleteObject(bucket, key);
            }
        }
    }

    // ---- Control Plane ----

    @Test
    @Order(10)
    void testLifecycleCRUD() {
        try {
            BucketLifecycleConfiguration config = new BucketLifecycleConfiguration()
                    .withRules(new BucketLifecycleConfiguration.Rule()
                            .withId("JavaV1-Exp")
                            .withPrefix("javav1-test/")
                            .withExpirationInDays(365)
                            .withStatus(BucketLifecycleConfiguration.ENABLED));
            s3.setBucketLifecycleConfiguration(bucket, config);

            BucketLifecycleConfiguration got = s3.getBucketLifecycleConfiguration(bucket);
            assertFalse(got.getRules().isEmpty());

            s3.deleteBucketLifecycleConfiguration(bucket);
        } catch (Exception e) {
            try { s3.deleteBucketLifecycleConfiguration(bucket); } catch (Exception ignored) {}
            throw e;
        }
    }

    @Test
    @Order(11)
    void testCORSCRUD() {
        try {
            BucketCrossOriginConfiguration cors = new BucketCrossOriginConfiguration()
                    .withRules(new CORSRule()
                            .withAllowedMethods(java.util.Arrays.asList(CORSRule.AllowedMethods.GET, CORSRule.AllowedMethods.PUT))
                            .withAllowedOrigins("https://javav1-test.example.com")
                            .withAllowedHeaders("Authorization")
                            .withMaxAgeSeconds(3600));
            s3.setBucketCrossOriginConfiguration(bucket, cors);

            BucketCrossOriginConfiguration got = s3.getBucketCrossOriginConfiguration(bucket);
            assertFalse(got.getRules().isEmpty());

            s3.deleteBucketCrossOriginConfiguration(bucket);
        } catch (Exception e) {
            try { s3.deleteBucketCrossOriginConfiguration(bucket); } catch (Exception ignored) {}
            throw e;
        }
    }

    @Test
    @Order(12)
    void testWebsiteCRUD() {
        try {
            BucketWebsiteConfiguration website = new BucketWebsiteConfiguration("index.html", "error.html");
            s3.setBucketWebsiteConfiguration(bucket, website);

            BucketWebsiteConfiguration got = s3.getBucketWebsiteConfiguration(bucket);
            assertEquals("index.html", got.getIndexDocumentSuffix());

            s3.deleteBucketWebsiteConfiguration(bucket);
        } catch (Exception e) {
            try { s3.deleteBucketWebsiteConfiguration(bucket); } catch (Exception ignored) {}
            throw e;
        }
    }

    @Test
    @Order(13)
    void testTaggingCRUD() {
        String key = testKey("tagging");
        try {
            s3.putObject(bucket, key, "tag test");

            List<com.amazonaws.services.s3.model.Tag> tags = new ArrayList<>();
            tags.add(new com.amazonaws.services.s3.model.Tag("Env", "JavaV1Test"));
            s3.setObjectTagging(new SetObjectTaggingRequest(bucket, key,
                    new ObjectTagging(tags)));

            GetObjectTaggingResult tagResult = s3.getObjectTagging(
                    new GetObjectTaggingRequest(bucket, key));
            assertFalse(tagResult.getTagSet().isEmpty());

            s3.deleteObjectTagging(new DeleteObjectTaggingRequest(bucket, key));
        } finally {
            s3.deleteObject(bucket, key);
        }
    }

    // ---- Helpers ----

    private static byte[] readAll(InputStream is) {
        try {
            return is.readAllBytes();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try { is.close(); } catch (Exception ignored) {}
        }
    }
}
