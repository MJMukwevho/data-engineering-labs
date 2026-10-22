package io.github.mjmukwevho.labs.fred;

import java.io.IOException;

/** Where the raw layer lives: a local folder or an S3 bucket. */
public interface RawStorage {

    /**
     * Stores {@code content} at {@code relativePath} under the raw layer root.
     *
     * @return the full location written, for logging (a file path or an s3:// URI)
     */
    String put(String relativePath, String content) throws IOException;

    /**
     * Picks the storage for a location: {@code s3://bucket/prefix} goes to S3,
     * anything else is treated as a local folder.
     */
    static RawStorage forLocation(String location, boolean s3ForcePathStyle) {
        if (location.startsWith("s3://")) {
            return S3RawStorage.create(location, s3ForcePathStyle);
        }
        return new LocalRawStorage(java.nio.file.Path.of(location));
    }
}
