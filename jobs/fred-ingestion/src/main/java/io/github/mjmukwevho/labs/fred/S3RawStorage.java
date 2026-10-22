package io.github.mjmukwevho.labs.fred;

import java.io.IOException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Raw layer in an S3 bucket (Amazon S3 or any S3-compatible store such as RustFS).
 *
 * <p>Connection settings come from the standard AWS environment variables, so the same
 * code runs locally and on AWS:
 * <ul>
 *   <li>{@code AWS_ACCESS_KEY_ID} / {@code AWS_SECRET_ACCESS_KEY}: credentials (on AWS, an IAM role instead)</li>
 *   <li>{@code AWS_REGION}: region</li>
 *   <li>{@code AWS_ENDPOINT_URL_S3}: only for a non-AWS store, e.g. {@code http://object-store:9000}</li>
 * </ul>
 * S3 writes are atomic, so readers never see a half-written object.
 */
public class S3RawStorage implements RawStorage {

    private final S3Client s3;
    private final String bucket;
    private final String prefix;

    S3RawStorage(S3Client s3, String bucket, String prefix) {
        this.s3 = s3;
        this.bucket = bucket;
        this.prefix = prefix;
    }

    /**
     * @param location         {@code s3://bucket} or {@code s3://bucket/prefix}
     * @param forcePathStyle   true for local stores that don't support bucket subdomains
     */
    static S3RawStorage create(String location, boolean forcePathStyle) {
        S3Location parsed = S3Location.parse(location);
        S3Client client = S3Client.builder()
                .httpClient(UrlConnectionHttpClient.create())
                .forcePathStyle(forcePathStyle)
                .build();
        return new S3RawStorage(client, parsed.bucket(), parsed.prefix());
    }

    @Override
    public String put(String relativePath, String content) throws IOException {
        String key = prefix.isEmpty() ? relativePath : prefix + "/" + relativePath;
        try {
            s3.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .contentType("application/json")
                            .build(),
                    RequestBody.fromString(content));
        } catch (SdkException e) {
            throw new IOException("Could not write s3://" + bucket + "/" + key + ": " + e.getMessage(), e);
        }
        return "s3://" + bucket + "/" + key;
    }

    /** Bucket and key prefix parsed from an {@code s3://} URI. */
    record S3Location(String bucket, String prefix) {

        static S3Location parse(String location) {
            if (!location.startsWith("s3://")) {
                throw new IllegalArgumentException("Not an s3:// location: " + location);
            }
            String rest = location.substring("s3://".length());
            int slash = rest.indexOf('/');
            String bucket = slash < 0 ? rest : rest.substring(0, slash);
            String prefix = slash < 0 ? "" : rest.substring(slash + 1).replaceAll("^/+|/+$", "");
            if (bucket.isEmpty()) {
                throw new IllegalArgumentException("No bucket in " + location);
            }
            return new S3Location(bucket, prefix);
        }
    }
}
